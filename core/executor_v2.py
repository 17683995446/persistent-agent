"""
执行引擎模块 V2 - 修复版
修复问题:
1. 执行器未自动启动
2. 任务状态统计混乱
3. 缺少超时机制
4. 缺少结果持久化
5. [H-线程安全] _restore_pending_tasks() 未持锁
6. [H-内存泄漏] completed_tasks 列表无限增长 -> deque(maxlen=1000)
7. [H-内存泄漏] active_tasks 中已完成任务从不清理
8. [H-资源管理] SQLite多线程并发写入可能locked -> WAL模式 + timeout=30
9. [H-资源管理] 超时线程无法终止 -> 协作式取消(threading.Event)
10. [H-逻辑错误] 重试计数在恢复+失败时双重递增
11. [H-性能] submit_task 在锁内执行I/O -> I/O移到锁外
12. [H-停启安全] start() 不检查残留线程
13. [M] task_done() 用 try/finally 保证调用
14. [M] _running 改用 threading.Event
15. [M] start()/stop() 加锁保护
16. [M] _handle_bug_fix 和 _handle_refactor 添加文件存在性检查
17. [M] 空文件列表统一返回 success=False
18. [M] _handle_implement 添加 try/except 保护
19. [M] stop() 清空队列时将任务标记为 CANCELLED
20. [M] handler 中的延迟导入移到文件顶部
21. [M] __dict__ 改用 dataclasses.asdict()
22. [R24-Bug#197] TaskStore.close() 只关闭当前线程连接
23. [R24-Bug#198] _worker_loop 添加 finally 块关闭连接
24. [R24-Bug#199] stop() 调用 task_store.close() 和 checkpoint_wal()
25. [R24-Bug#200] 添加 checkpoint_wal() 方法防止WAL文件无限增长
26. [R25-Bug#206] stop() 延长worker退出等待时间
27. [R25-Bug#207] 确保worker完全退出后再关闭task_store
"""
import os
import re
import ast
import json
import subprocess
import threading
import queue
import traceback
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
import logging
import sqlite3

# 顶部导入依赖模块，避免handler中的延迟导入
from core.executor import CodeAnalyzer
from core.knowledge_graph import Experience

# 从config导入配置常量
from config import KNOWLEDGE_BASE_DIR, MAX_WORKERS, TASK_TIMEOUT, RETRY_COUNT

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """任务类型"""
    IMPLEMENT = "implement"
    REFACTOR = "refactor"
    ANALYZE = "analyze"
    BUG_FIX = "bug_fix"
    OPTIMIZE = "optimize"
    TEST = "test"


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """任务定义 V2"""
    id: str
    task_type: TaskType
    description: str
    target_files: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    priority: int = 1
    status: TaskStatus = TaskStatus.PENDING
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = RETRY_COUNT
    timeout_seconds: int = TASK_TIMEOUT
    related_experiences: List[str] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """执行结果 V2"""
    success: bool
    task_id: str
    output: str
    changes: List[Dict[str, Any]]
    issues_found: List[Dict[str, Any]]
    suggestions: List[str]
    execution_time: float
    experience_id: Optional[str] = None
    error_type: Optional[str] = None


class TaskStore:
    """任务持久化存储 - 使用WAL模式支持多线程并发"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """获取线程本地的数据库连接，使用WAL模式和超时"""
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            logger.debug(f"[TRACE] _get_connection: creating new connection for thread={threading.current_thread().name}, db={self.db_path}")
            conn = sqlite3.connect(
                self.db_path,
                timeout=30
                # [H-Bug修复] 移除check_same_thread=False，使用线程本地存储已保证单线程使用
            )
            # 启用WAL模式，提升多线程并发读写性能
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    def close(self):
        """[H-Bug修复] 关闭数据库连接，释放资源
        
        Bug#197修复: 只关闭当前线程的连接
        注意: 每个worker线程需要在退出时调用此方法
        """
        try:
            conn = getattr(self._local, 'conn', None)
            if conn:
                logger.debug(f"[TRACE] close: closing connection for thread={threading.current_thread().name}")
                conn.close()
                self._local.conn = None
        except Exception as e:
            logger.warning(f"关闭数据库连接失败: {e}")

    def checkpoint_wal(self):
        """[Bug#200修复] 执行WAL checkpoint，防止WAL文件无限增长"""
        conn = None
        try:
            conn = self._get_connection()
            # TRUNCATE模式: checkpoint后将WAL文件截断为0字节
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.commit()
            logger.debug("[TRACE] checkpoint_wal: WAL checkpoint completed")
        except Exception as e:
            logger.warning(f"WAL checkpoint失败: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _init_db(self):
        """初始化任务表"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                description TEXT,
                target_files TEXT,
                context TEXT,
                priority INTEGER,
                status TEXT,
                created_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                result TEXT,
                error TEXT,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                execution_time REAL
            )
        ''')
        conn.commit()
        logger.debug(f"[TRACE] _init_db: database initialized, db={self.db_path}")

    def save_task(self, task: Task):
        """[H-Bug修复] 保存任务 - 添加异常处理防止任务卡在RUNNING状态"""
        conn = None
        try:
            logger.debug(f"[TRACE] save_task: saving task={task.id}, status={task.status.value}, retry_count={task.retry_count}")
            conn = self._get_connection()
            cursor = conn.cursor()
            # [Bug#192修复] 使用INSERT ON CONFLICT DO UPDATE(UPSERT)替代INSERT OR IGNORE
            # INSERT OR IGNORE会导致状态更新(RUNNING/COMPLETED/FAILED)被静默丢弃
            # ON CONFLICT DO UPDATE确保已有任务的状态变更被正确持久化
            # [Bug#187兼容] submit_task中已检查completed_tasks防止覆盖已完成任务
            cursor.execute('''
                INSERT INTO tasks
                (id, task_type, description, target_files, context, priority, status,
                 created_at, started_at, completed_at, result, error, retry_count, max_retries, execution_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    status = excluded.status,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    result = excluded.result,
                    error = excluded.error,
                    retry_count = excluded.retry_count,
                    execution_time = excluded.execution_time
            ''', (
                task.id,
                task.task_type.value,
                task.description,
                json.dumps(task.target_files),
                json.dumps(task.context),
                task.priority,
                task.status.value,
                task.created_at or "",
                task.started_at,
                task.completed_at,
                json.dumps(task.result) if task.result else None,
                task.error,
                task.retry_count,
                task.max_retries,
                task.result.get("execution_time", 0) if task.result else 0
            ))
            conn.commit()
            logger.debug(f"[TRACE] save_task: committed task={task.id}")
        except Exception as e:
            logger.error(f"保存任务 {task.id} 失败: {e}")
            logger.debug(f"[TRACE] save_task: rollback task={task.id}, error={e}")
            # 尝试回滚
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass

    def load_pending_tasks(self) -> List[Task]:
        """[H-Bug修复] 加载待处理任务 - 逐条JSON解析保护，一条损坏不影响其他
        [Bug#184修复] 移除SQL中的retry_count过滤，在Python中统一使用max_retries判断
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        # [Bug#184修复] 不再使用RETRY_COUNT过滤，加载所有非终态任务
        cursor.execute('''
            SELECT * FROM tasks
            WHERE status IN ('pending', 'running', 'failed', 'timeout')
            ORDER BY priority DESC, created_at ASC
        ''')
        rows = cursor.fetchall()
        logger.debug(f"[TRACE] load_pending_tasks: query returned {len(rows)} rows")

        tasks = []
        for row in rows:
            try:
                logger.debug(f"[TRACE] load_pending_tasks: parsing row id={row[0]}, status={row[6]}")
                # [H-013修复] 加载max_retries字段(row[13])
                task = Task(
                    id=row[0],
                    task_type=TaskType(row[1]),
                    description=row[2] or "",
                    target_files=self._safe_json_loads(row[3], []),
                    context=self._safe_json_loads(row[4], {}),
                    priority=row[5] or 1,
                    status=TaskStatus(row[6]) if row[6] else TaskStatus.PENDING,
                    created_at=row[7] or datetime.now().isoformat(),
                    started_at=row[8],
                    completed_at=row[9],
                    result=self._safe_json_loads(row[10], None),
                    error=row[11],
                    retry_count=row[12] or 0,
                    max_retries=row[13] if row[13] is not None else RETRY_COUNT
                )
                tasks.append(task)
            except Exception as e:
                logger.warning(f"跳过损坏的任务记录 {row[0]}: {e}")
                continue
        return tasks

    @staticmethod
    def _safe_json_loads(text, default):
        """[H-Bug修复] 安全JSON反序列化，解析失败返回默认值"""
        if not text:
            return default
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return default


class TaskExecutorV2:
    """
    任务执行器 V2
    修复版 - 自动启动、正确统计、超时机制、持久化、线程安全
    """

    def __init__(self, knowledge_graph, work_dir: str, auto_start: bool = True):
        self.kg = knowledge_graph
        self.work_dir = Path(work_dir)
        self.task_queue = queue.PriorityQueue()
        self.active_tasks: Dict[str, Task] = {}
        # [H-内存泄漏] 使用固定长度的deque，防止completed_tasks无限增长
        self.completed_tasks: deque = deque(maxlen=1000)
        self._lock = threading.RLock()
        # [M] _running 改用 threading.Event，支持更可靠的线程间通信
        self._running = threading.Event()
        # [M-属性名兼容] 提供 _stop_event 别名，兼容外部代码对 _stop_event 的访问
        # _stop_event 与 _running 语义一致：set() 表示停止，clear() 表示运行中
        self._stop_event = self._running
        self._workers: List[threading.Thread] = []
        self._num_workers = MAX_WORKERS

        # 统计
        self.stats = {
            "submitted": 0,
            "completed": 0,
            "failed": 0,
            "timeout": 0
        }

        # 任务存储 - [H-Bug修复] 使用work_dir而不是硬编码路径，支持多Agent隔离
        self.work_dir.mkdir(parents=True, exist_ok=True)
        task_db = str(self.work_dir / "tasks.db")
        self.task_store = TaskStore(task_db)

        # 注册任务处理器
        self.handlers: Dict[TaskType, Callable] = {
            TaskType.ANALYZE: self._handle_analyze,
            TaskType.BUG_FIX: self._handle_bug_fix,
            TaskType.REFACTOR: self._handle_refactor,
            TaskType.IMPLEMENT: self._handle_implement,
            TaskType.OPTIMIZE: self._handle_optimize,
            TaskType.TEST: self._handle_test,
        }

        # 自动启动
        if auto_start:
            logger.debug(f"[TRACE] __init__: auto_start=True, triggering start with {self._num_workers} workers")
            self.start(self._num_workers)
            # 恢复未完成任务
            self._restore_pending_tasks()
        logger.debug(f"[TRACE] __init__: initialization complete, auto_start={auto_start}")

    def _restore_pending_tasks(self):
        """[H-线程安全] 恢复待处理任务 - 加锁保护共享状态修改"""
        try:
            # I/O操作在锁外执行
            pending_tasks = self.task_store.load_pending_tasks()
            logger.debug(f"[TRACE] _restore_pending_tasks: loaded {len(pending_tasks)} pending tasks from store")

            logger.debug(f"[TRACE][LOCK] acquire _lock in _restore_pending_tasks")
            with self._lock:
                for task in pending_tasks:
                    if task.status == TaskStatus.RUNNING:
                        # [R19-L009修复] 恢复RUNNING任务时不递增retry_count
                        # 仅标记为FAILED/PENDING，让_worker_loop统一管理重试计数
                        logger.debug(f"[TRACE][STATE] task={task.id} {task.status.value} -> failed (executor restart)")
                        task.status = TaskStatus.FAILED
                        task.error = "执行器重启，任务中断"

                    if task.retry_count < task.max_retries:
                        logger.debug(f"[TRACE][STATE] task={task.id} {task.status.value} -> pending (restore)")
                        task.status = TaskStatus.PENDING
                        # [M-Bug修复] 清除旧的错误信息
                        task.error = None
                        # [M-Bug修复] 确保 created_at 有有效值，防止排序异常
                        if not task.created_at:
                            task.created_at = datetime.now().isoformat()
                        self.task_queue.put((-task.priority, task.created_at or "", task.id))
                        self.active_tasks[task.id] = task
                        logger.info(f"恢复任务: {task.id}")
                    else:
                        # [R19-L010修复] retry_count >= max_retries 的任务标记为最终失败
                        # 防止任务既不被恢复也不被标记，成为"丢失任务"
                        logger.debug(f"[TRACE][STATE] task={task.id} {task.status.value} -> failed (max retries reached on restore)")
                        task.status = TaskStatus.FAILED
                        task.error = "执行器重启恢复时已超过最大重试次数"
                        task.completed_at = datetime.now().isoformat()
                        self.completed_tasks.append(task)
                        logger.warning(f"任务 {task.id} 恢复时已超过最大重试次数 ({task.retry_count}/{task.max_retries})，标记为最终失败")

                if pending_tasks:
                    logger.info(f"恢复了 {len(pending_tasks)} 个待处理任务")
            logger.debug(f"[TRACE][LOCK] release _lock in _restore_pending_tasks")
        except Exception as e:
            logger.error(f"恢复任务失败: {e}")

    def submit_task(self, task: Task) -> str:
        """[H-Bug修复] 提交任务 - 参数校验 + 运行状态检查"""
        # [Bug#171修复] 参数校验：拒绝None和非Task类型
        if task is None:
            raise ValueError("task must not be None")
        if not isinstance(task, Task):
            raise TypeError(f"expected Task, got {type(task).__name__}")
        # [Bug#172修复] 运行状态检查：停止后拒绝新任务
        if not self._running.is_set():
            raise RuntimeError("Executor is not running, cannot submit tasks")

        logger.debug(f"[TRACE] submit_task: task={task.id}, type={task.task_type.value}, priority={task.priority}")
        # 确保created_at有值
        if not task.created_at:
            task.created_at = datetime.now().isoformat()

        # I/O操作：查找相关经验（在锁外执行）
        try:
            similar = self.kg.get_similar_experiences(task.description, limit=3)
            task.related_experiences = [exp.id for exp, _ in similar]
            logger.debug(f"[TRACE] submit_task: task={task.id}, found {len(similar)} similar experiences: {task.related_experiences}")
        except Exception as e:
            logger.warning(f"查找相关经验失败: {e}")
            logger.debug(f"[TRACE] submit_task: task={task.id}, experience lookup failed: {e}")

        # 锁内只修改共享状态
        logger.debug(f"[TRACE][LOCK] acquire _lock in submit_task")
        with self._lock:
            # [M-Bug修复] 检查任务是否已存在于 active_tasks，防止重复入队
            if task.id in self.active_tasks:
                logger.warning(f"任务 {task.id} 已存在于 active_tasks，跳过重复提交（返回已有任务ID，调用方请注意：返回值无法区分新提交与重复）")
                logger.debug(f"[TRACE][LOCK] release _lock in submit_task (duplicate)")
                return task.id

            # [Bug#187修复] 检查任务是否已存在于 completed_tasks，防止覆盖已完成任务
            for completed_task in self.completed_tasks:
                if completed_task.id == task.id:
                    logger.warning(f"任务 {task.id} 已完成，跳过重复提交")
                    logger.debug(f"[TRACE][LOCK] release _lock in submit_task (already completed)")
                    return task.id

            self.task_queue.put((-task.priority, task.created_at, task.id))
            self.active_tasks[task.id] = task
            self.stats["submitted"] += 1
        logger.debug(f"[TRACE][LOCK] release _lock in submit_task")
        logger.debug(f"[TRACE] submit_task: task={task.id} enqueued, active_tasks count={len(self.active_tasks)}")

        # I/O操作：持久化（在锁外执行）
        self.task_store.save_task(task)
        logger.debug(f"[TRACE] submit_task: task={task.id} persisted")

        logger.info(f"任务已提交: {task.id} - {task.task_type.value}")
        return task.id

    def start(self, num_workers: int = MAX_WORKERS):
        """[H-停启安全] 启动执行器 - 检查残留线程并加锁保护"""
        # [Bug#126] num_workers 至少为1，防止创建0个worker导致任务永远不执行
        num_workers = max(1, num_workers)
        logger.debug(f"[TRACE][LOCK] acquire _lock in start")
        with self._lock:
            if self._running.is_set():
                logger.warning("执行器已在运行")
                logger.debug(f"[TRACE][LOCK] release _lock in start (already running)")
                return

            # [H-停启安全] 检查并清理残留线程
            self._cleanup_stale_workers()
            logger.debug(f"[TRACE] start: stale workers cleaned, remaining alive={len(self._workers)}")

            self._running.set()
            self._num_workers = num_workers

            for i in range(num_workers):
                worker = threading.Thread(target=self._worker_loop, name=f"Worker-{i}")
                worker.daemon = True
                worker.start()
                self._workers.append(worker)
                logger.debug(f"[TRACE] start: worker thread Worker-{i} started")

        logger.debug(f"[TRACE][LOCK] release _lock in start")
        logger.info(f"执行器已启动, {num_workers}个工作线程")

    def _cleanup_stale_workers(self):
        """清理残留的工作线程"""
        alive_workers = []
        for worker in self._workers:
            if worker.is_alive():
                alive_workers.append(worker)
            else:
                logger.warning(f"清理残留线程: {worker.name}")
        self._workers = alive_workers

    def stop(self):
        """[M] 停止执行器 - 加锁保护，清空队列时将任务标记为CANCELLED"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in stop")
        with self._lock:
            if not self._running.is_set():
                logger.warning("执行器未在运行")
                logger.debug(f"[TRACE][LOCK] release _lock in stop (not running)")
                return

            self._running.clear()

            # 清空队列，将未执行的任务标记为CANCELLED
            cancelled_count = 0
            while not self.task_queue.empty():
                try:
                    _, _, task_id = self.task_queue.get_nowait()
                    task = self.active_tasks.get(task_id)
                    if task and task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                        logger.debug(f"[TRACE][STATE] task={task_id} {task.status.value} -> cancelled (executor stop)")
                        task.status = TaskStatus.CANCELLED
                        task.error = "执行器停止，任务被取消"
                        self.completed_tasks.append(task)
                        # [H-Bug修复] 从active_tasks中移除，防止内存泄漏
                        self.active_tasks.pop(task_id, None)
                        cancelled_count += 1
                    # [H-018修复] 只有被get()取出的任务才应调用task_done()
                    # 这里的任务确实被get()取出了，所以调用task_done()是正确的
                    self.task_queue.task_done()
                except queue.Empty:
                    break

            if cancelled_count > 0:
                logger.info(f"取消了 {cancelled_count} 个未执行任务")

        logger.debug(f"[TRACE][LOCK] release _lock in stop")

        # 等待工作线程退出（在锁外等待，避免死锁）
        logger.debug(f"[TRACE] stop: waiting for {len(self._workers)} workers to exit")
        for worker in self._workers:
            logger.debug(f"[TRACE] stop: joining worker {worker.name}")
            worker.join(timeout=10)

        # [H-Bug修复] 将仍在 RUNNING 状态的任务标记为 FAILED
        with self._lock:
            for task_id, task in list(self.active_tasks.items()):
                if task.status == TaskStatus.RUNNING:
                    logger.debug(f"[TRACE][STATE] task={task.id} running -> failed (executor stop, worker did not finish)")
                    task.status = TaskStatus.FAILED
                    task.error = "执行器停止，任务被强制中断"
                    self.completed_tasks.append(task)
                    self.active_tasks.pop(task_id, None)

        logger.debug(f"[TRACE][LOCK] acquire _lock in stop (cleanup workers)")
        with self._lock:
            self._workers.clear()
        logger.debug(f"[TRACE][LOCK] release _lock in stop (cleanup workers)")

        # [R25-Bug#206修复] 延长等待时间，确保worker有足够时间完成当前任务
        # 先等待所有worker真正退出
        logger.debug("[TRACE] stop: waiting for all workers to fully exit")
        max_wait = 15  # 最多等待15秒
        waited = 0
        while waited < max_wait:
            alive_workers = [w for w in self._workers if w.is_alive()]
            if not alive_workers:
                break
            time.sleep(0.5)
            waited += 0.5
        if waited >= max_wait:
            logger.warning(f"[TRACE] stop: some workers did not exit within {max_wait}s")

        # [Bug#199修复] 所有worker退出后关闭task_store连接并执行WAL checkpoint
        # [R25-Bug#207修复] 确保worker完全退出后再关闭task_store
        logger.debug("[TRACE] stop: closing task_store and checkpointing WAL")
        try:
            self.task_store.checkpoint_wal()
            self.task_store.close()
        except Exception as e:
            logger.warning(f"关闭task_store失败: {e}")

        logger.info("执行器已停止")

    def _worker_loop(self):
        """工作线程主循环 V2"""
        logger.debug(f"[TRACE] _worker_loop: started in thread={threading.current_thread().name}")
        try:
            while self._running.is_set():
                try:
                    # 获取任务
                    try:
                        _, _, task_id = self.task_queue.get(timeout=1.0)
                        logger.debug(f"[TRACE] _worker_loop: dequeued task_id={task_id} in thread={threading.current_thread().name}")
                    except queue.Empty:
                        continue

                    # [M-Bug修复] 获取任务后检查执行器是否已停止，防止竞态
                    if not self._running.is_set():
                        logger.debug(f"[TRACE] _worker_loop: executor stopped after dequeue, putting task back and breaking")
                        self.task_queue.task_done()
                        break

                    task = None
                    try:
                        logger.debug(f"[TRACE][LOCK] acquire _lock in _worker_loop (get task object)")
                        with self._lock:
                            task = self.active_tasks.get(task_id)
                            if not task:
                                logger.debug(f"[TRACE] _worker_loop: task_id={task_id} not found in active_tasks, skipping")
                                logger.debug(f"[TRACE][LOCK] release _lock in _worker_loop (task not found)")
                                # task_done() 由 finally 块统一调用，此处不再显式调用
                                continue

                            # [H-Bug#77修复] 检查任务是否已处于终态（被外部取消或已完成）
                            # 防止终态任务被重新执行
                            terminal_states = {TaskStatus.CANCELLED, TaskStatus.COMPLETED,
                                              TaskStatus.FAILED, TaskStatus.TIMEOUT}
                            if task.status in terminal_states:
                                logger.debug(f"[TRACE] _worker_loop: task={task.id} already in terminal state {task.status.value}, skipping execution")
                                logger.debug(f"[TRACE][LOCK] release _lock in _worker_loop (task in terminal state)")
                                self.active_tasks.pop(task.id, None)
                                # task_done() 由 finally 块统一调用，此处不再显式调用
                                continue

                            logger.debug(f"[TRACE][STATE] task={task.id} {task.status.value} -> running")
                            task.status = TaskStatus.RUNNING
                            task.started_at = datetime.now().isoformat()
                        logger.debug(f"[TRACE][LOCK] release _lock in _worker_loop (state updated)")

                        # 持久化在锁外
                        self.task_store.save_task(task)

                        # 执行任务（带超时，使用协作式取消）
                        logger.debug(f"[TRACE] _worker_loop: executing task={task.id}, type={task.task_type.value}, timeout={task.timeout_seconds}s")
                        result = self._execute_task_with_timeout(task)
                        logger.debug(f"[TRACE] _worker_loop: task={task.id} execution returned, success={result.success}, error_type={result.error_type}")

                        # 更新任务状态
                        will_retry = False
                        logger.debug(f"[TRACE][LOCK] acquire _lock in _worker_loop (update result)")
                        with self._lock:
                            if result.success:
                                logger.debug(f"[TRACE][STATE] task={task.id} running -> completed")
                                task.status = TaskStatus.COMPLETED
                                self.stats["completed"] += 1
                                task.completed_at = datetime.now().isoformat()
                                # [M] 使用 dataclasses.asdict() 替代 __dict__
                                task.result = asdict(result) if result else None
                                self.completed_tasks.append(task)
                                # [H-内存泄漏] 完成后从 active_tasks 中删除
                                self.active_tasks.pop(task.id, None)
                            else:
                                # 增加重试计数
                                task.retry_count += 1

                                # 检查是否需要重试
                                if task.retry_count < task.max_retries:
                                    will_retry = True
                                    logger.debug(f"[TRACE][STATE] task={task.id} running -> pending (retry {task.retry_count}/{task.max_retries})")
                                    task.status = TaskStatus.PENDING
                                    # 重新加入队列
                                    self.task_queue.put((-task.priority, task.created_at, task.id))
                                    logger.info(f"任务 {task.id} 将重试 ({task.retry_count}/{task.max_retries})")
                                else:
                                    # 超过重试次数，标记为最终失败
                                    if result.error_type == "timeout":
                                        logger.debug(f"[TRACE][STATE] task={task.id} running -> timeout (max retries reached)")
                                        task.status = TaskStatus.TIMEOUT
                                        self.stats["timeout"] += 1
                                    else:
                                        logger.debug(f"[TRACE][STATE] task={task.id} running -> failed (max retries reached)")
                                        task.status = TaskStatus.FAILED
                                        self.stats["failed"] += 1

                                    task.completed_at = datetime.now().isoformat()
                                    # [M] 使用 dataclasses.asdict() 替代 __dict__
                                    task.result = asdict(result) if result else None
                                    task.error = result.output[:500] if result.output else "Unknown error"
                                    self.completed_tasks.append(task)
                                    # [H-内存泄漏] 完成后从 active_tasks 中删除
                                    self.active_tasks.pop(task.id, None)
                        logger.debug(f"[TRACE][LOCK] release _lock in _worker_loop (result updated)")

                        # 持久化在锁外
                        self.task_store.save_task(task)
                        logger.debug(f"[TRACE] _worker_loop: task={task.id} persisted after execution, will_retry={will_retry}")

                    finally:
                        # [M] task_done() 用 try/finally 保证调用
                        self.task_queue.task_done()
                        logger.debug(f"[TRACE] _worker_loop: task_done called for task_id={task_id}")

                except Exception as e:
                    logger.error(f"工作线程异常: {traceback.format_exc()}")
                    logger.debug(f"[TRACE] _worker_loop: exception in thread={threading.current_thread().name}: {e}")

        finally:
            # [Bug#198修复] 线程退出时关闭数据库连接
            logger.debug(f"[TRACE] _worker_loop: closing task_store connection for thread={threading.current_thread().name}")
            self.task_store.close()
        logger.debug(f"[TRACE] _worker_loop: exiting thread={threading.current_thread().name}")

    def _execute_task_with_timeout(self, task: Task) -> ExecutionResult:
        """[H-资源管理] 带超时的任务执行 - 使用协作式取消(threading.Event)"""
        logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, timeout={task.timeout_seconds}s")
        # [H-Bug修复] timeout_seconds 为 0 或负数时使用默认值
        if task.timeout_seconds <= 0:
            task.timeout_seconds = TASK_TIMEOUT
            logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, timeout was <=0, reset to TASK_TIMEOUT={TASK_TIMEOUT}s")
        # [H-016修复] 使用queue.Queue替代字典，确保线程安全的结果传递
        result_queue = queue.Queue(maxsize=1)
        # 协作式取消标志，handler可以通过检查此标志来提前退出
        cancel_event = threading.Event()

        def execute():
            try:
                result = self._execute_task(task, cancel_event)
                result_queue.put(('result', result))
            except Exception as e:
                result_queue.put(('error', e))

        thread = threading.Thread(target=execute)
        thread.daemon = True  # [M-Bug修复] 设置为守护线程，防止超时后成为僵尸线程
        logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, execution thread started")
        thread.start()
        # 等待任务完成或超时
        thread.join(timeout=task.timeout_seconds)

        if thread.is_alive():
            # 超时 - 设置取消标志通知handler
            logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, timeout reached, setting cancel_event")
            cancel_event.set()
            # 给予短暂时间让handler优雅退出
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning(f"任务 {task.id} 超时后未能优雅退出")
            # [H-Bug修复] 超时边界上，执行线程可能已完成，检查 result_queue
            try:
                result_type, result_value = result_queue.get_nowait()
                if result_type == 'result':
                    logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, result found after timeout boundary, returning actual result")
                    return result_value
            except queue.Empty:
                pass
            logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, returning timeout result")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output=f"任务执行超时 ({task.timeout_seconds}秒)",
                changes=[],
                issues_found=[],
                suggestions=["考虑增加超时时间或优化任务逻辑"],
                execution_time=task.timeout_seconds,
                error_type="timeout"
            )

        # [H-016修复] 从线程安全的queue中获取结果
        try:
            result_type, result_value = result_queue.get(timeout=1.0)
            if result_type == 'error':
                error = result_value
                logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, execution thread raised exception: {type(error).__name__}")
                return ExecutionResult(
                    success=False,
                    task_id=task.id,
                    output=f"执行异常: {str(error)}\n{traceback.format_exc()}",
                    changes=[],
                    issues_found=[],
                    suggestions=[],
                    execution_time=0,
                    error_type=type(error).__name__
                )
            logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, execution thread completed successfully")
            return result_value
        except queue.Empty:
            logger.debug(f"[TRACE] _execute_task_with_timeout: task={task.id}, result queue empty")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="未获取到执行结果",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=0
            )

    def _execute_task(self, task: Task, cancel_event: threading.Event = None) -> ExecutionResult:
        """执行单个任务"""
        start_time = time.time()
        logger.debug(f"[TRACE] _execute_task: task={task.id}, type={task.task_type.value}, handler={self.handlers.get(task.task_type).__name__ if self.handlers.get(task.task_type) else 'None'}")

        try:
            handler = self.handlers.get(task.task_type)
            if not handler:
                logger.debug(f"[TRACE] _execute_task: task={task.id}, no handler for type={task.task_type.value}, returning unknown_task_type")
                return ExecutionResult(
                    success=False,
                    task_id=task.id,
                    output=f"未知的任务类型: {task.task_type}",
                    changes=[],
                    issues_found=[],
                    suggestions=[],
                    execution_time=0,
                    error_type="unknown_task_type"
                )

            # 检查是否已被取消
            if cancel_event and cancel_event.is_set():
                logger.debug(f"[TRACE] _execute_task: task={task.id}, cancelled before execution")
                return ExecutionResult(
                    success=False,
                    task_id=task.id,
                    output="任务在执行前已被取消",
                    changes=[],
                    issues_found=[],
                    suggestions=[],
                    execution_time=time.time() - start_time,
                    error_type="cancelled"
                )

            logger.debug(f"[TRACE] _execute_task: task={task.id}, calling handler {handler.__name__}")
            result = handler(task)
            result.execution_time = time.time() - start_time
            logger.debug(f"[TRACE] _execute_task: task={task.id}, handler returned, success={result.success}, execution_time={result.execution_time:.3f}s")

            # 记录经验
            try:
                experience_id = self._record_experience(task, result)
                result.experience_id = experience_id
                logger.debug(f"[TRACE] _execute_task: task={task.id}, experience recorded, experience_id={experience_id}")
            except Exception as e:
                logger.warning(f"记录经验失败: {e}")
                logger.debug(f"[TRACE] _execute_task: task={task.id}, record experience failed: {e}")

            return result

        except Exception as e:
            logger.debug(f"[TRACE] _execute_task: task={task.id}, exception during execution: {type(e).__name__}: {e}")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output=f"执行异常: {str(e)}\n{traceback.format_exc()}",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=time.time() - start_time,
                error_type=type(e).__name__
            )

    def _resolve_safe_path(self, file_path: str) -> Optional[Path]:
        """[M-003修复] 安全地解析文件路径，防止路径遍历攻击
        
        Args:
            file_path: 用户提供的文件路径
            
        Returns:
            解析后的安全路径，如果路径试图遍历到工作目录外则返回None
        """
        try:
            # 如果是绝对路径，直接使用；否则基于work_dir
            if os.path.isabs(file_path):
                full_path = Path(file_path).resolve()
            else:
                full_path = (self.work_dir / file_path).resolve()
            
            # 检查解析后的路径是否在工作目录内
            # 使用resolve()来消除..和符号链接
            work_dir_resolved = self.work_dir.resolve()
            
            # 检查路径是否以工作目录为前缀
            try:
                full_path.relative_to(work_dir_resolved)
                return full_path
            except ValueError:
                # 路径在工作目录之外
                logger.warning(f"[M-003] 路径遍历尝试被阻止: {file_path}")
                return None
        except (OSError, ValueError) as e:
            logger.warning(f"[M-003] 路径解析失败: {file_path}, 错误: {e}")
            return None

    def _handle_analyze(self, task: Task) -> ExecutionResult:
        """处理代码分析任务"""
        logger.debug(f"[TRACE] _handle_analyze: enter task={task.id}, files={task.target_files}")
        # [M] 空文件列表统一返回 success=False
        if not task.target_files:
            logger.debug(f"[TRACE] _handle_analyze: exit task={task.id}, success=False (no target files)")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="未指定目标文件，无法执行分析",
                changes=[],
                issues_found=[],
                suggestions=["请提供需要分析的文件列表"],
                execution_time=0
            )

        results = []
        issues = []
        suggestions = []
        files_analyzed = 0
        files_not_found = 0
        files_error = 0

        for file_path in task.target_files:
            try:
                # [M-003修复] 使用安全路径解析
                full_path = self._resolve_safe_path(file_path)
                if full_path is None:
                    files_not_found += 1
                    continue

                if not full_path.exists():
                    files_not_found += 1
                    continue

                analysis = CodeAnalyzer.analyze_file(str(full_path))
                results.append(analysis)
                files_analyzed += 1

                bugs = CodeAnalyzer.detect_bugs(str(full_path))
                issues.extend(bugs)

                if analysis.get("complexity", 0) > 20:
                    suggestions.append(f"{file_path}: 复杂度过高, 建议拆分模块")

                if len(analysis.get("functions", [])) > 15:
                    suggestions.append(f"{file_path}: 函数过多, 建议按职责拆分文件")
            except Exception as e:
                logger.warning(f"分析文件 {file_path} 失败: {e}")
                files_error += 1

        output = f"分析了 {files_analyzed} 个文件"
        if files_not_found > 0:
            output += f" (跳过 {files_not_found} 个不存在文件)"
        if files_error > 0:
            output += f" (跳过 {files_error} 个异常文件)"
        output += f"\n发现 {len(issues)} 个潜在问题\n"
        output += f"生成 {len(suggestions)} 条建议"

        # 只要有文件被分析，就算成功
        success = files_analyzed > 0

        logger.debug(f"[TRACE] _handle_analyze: exit task={task.id}, success={success}, files_analyzed={files_analyzed}, issues={len(issues)}")
        return ExecutionResult(
            success=success,
            task_id=task.id,
            output=output,
            changes=[],
            issues_found=issues,
            suggestions=suggestions,
            execution_time=0
        )

    def _handle_bug_fix(self, task: Task) -> ExecutionResult:
        """处理Bug修复任务"""
        logger.debug(f"[TRACE] _handle_bug_fix: enter task={task.id}, files={task.target_files}")
        # [M] 空文件列表统一返回 success=False
        if not task.target_files:
            logger.debug(f"[TRACE] _handle_bug_fix: exit task={task.id}, success=False (no target files)")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="未指定目标文件，无法执行Bug修复",
                changes=[],
                issues_found=[],
                suggestions=["请提供需要修复Bug的文件列表"],
                execution_time=0
            )

        issues = []
        changes = []
        files_processed = 0

        for file_path in task.target_files:
            # [M-003修复] 使用安全路径解析
            full_path = self._resolve_safe_path(file_path)
            if full_path is None:
                logger.warning(f"Bug修复: 路径不安全或不存在 {file_path}")
                continue
            # [M] 添加文件存在性检查
            if not os.path.exists(str(full_path)):
                logger.warning(f"Bug修复: 文件不存在 {file_path}")
                continue
            # [Bug#195修复] 添加逐文件异常保护，单个文件失败不影响其他文件
            try:
                bugs = CodeAnalyzer.detect_bugs(str(full_path))
                issues.extend(bugs)
                files_processed += 1
            except Exception as e:
                logger.warning(f"[TRACE] _handle_bug_fix: 分析文件 {file_path} 失败: {e}")
                files_error += 1

        # [H-Bug修复] 所有文件都不存在时返回 success=False
        if files_processed == 0:
            logger.debug(f"[TRACE] _handle_bug_fix: exit task={task.id}, success=False (no files processed)")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="所有目标文件均不存在，无法执行Bug修复",
                changes=changes,
                issues_found=issues,
                suggestions=["请检查目标文件路径是否正确"],
                execution_time=0
            )

        if not issues:
            logger.debug(f"[TRACE] _handle_bug_fix: exit task={task.id}, success=True (no issues found)")
            return ExecutionResult(
                success=True,
                task_id=task.id,
                output="未发现明显Bug模式",
                changes=changes,
                issues_found=issues,
                suggestions=[],
                execution_time=0
            )

        suggestions = [f"行 {issue['line']}: {issue['message']}" for issue in issues]

        output = f"检测到 {len(issues)} 个潜在Bug\n"
        output += "建议检查以下问题:\n" + "\n".join(suggestions)

        logger.debug(f"[TRACE] _handle_bug_fix: exit task={task.id}, success=True, issues={len(issues)}")
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=output,
            changes=changes,
            issues_found=issues,
            suggestions=suggestions,
            execution_time=0
        )

    def _handle_refactor(self, task: Task) -> ExecutionResult:
        """处理重构任务"""
        logger.debug(f"[TRACE] _handle_refactor: enter task={task.id}, files={task.target_files}")
        # [M] 空文件列表统一返回 success=False
        if not task.target_files:
            logger.debug(f"[TRACE] _handle_refactor: exit task={task.id}, success=False (no target files)")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="未指定目标文件，无法执行重构分析",
                changes=[],
                issues_found=[],
                suggestions=["请提供需要重构的文件列表"],
                execution_time=0
            )

        suggestions = []
        files_processed = 0

        for file_path in task.target_files:
            # [M-003修复] 使用安全路径解析
            full_path = self._resolve_safe_path(file_path)
            if full_path is None:
                logger.warning(f"重构分析: 路径不安全或不存在 {file_path}")
                continue
            # [M] 添加文件存在性检查
            if not os.path.exists(str(full_path)):
                logger.warning(f"重构分析: 文件不存在 {file_path}")
                continue
            # [Bug#195修复] 添加逐文件异常保护
            try:
                analysis = CodeAnalyzer.analyze_file(str(full_path))
                files_processed += 1

                if analysis.get("complexity", 0) > 15:
                    suggestions.append(f"{file_path}: 复杂度过高, 考虑拆分")

                for func in analysis.get("functions", []):
                    if func.get("docstring") == "":
                        suggestions.append(f"{file_path}: 函数 {func['name']} 缺少文档字符串")
            except Exception as e:
                logger.warning(f"[TRACE] _handle_refactor: 分析文件 {file_path} 失败: {e}")

        # [H-Bug修复] 所有文件都不存在时返回 success=False
        if files_processed == 0:
            logger.debug(f"[TRACE] _handle_refactor: exit task={task.id}, success=False (no files processed)")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="所有目标文件均不存在，无法执行重构分析",
                changes=[],
                issues_found=[],
                suggestions=["请检查目标文件路径是否正确"],
                execution_time=0
            )

        if not suggestions:
            logger.debug(f"[TRACE] _handle_refactor: exit task={task.id}, success=True (no suggestions)")
            return ExecutionResult(
                success=True,
                task_id=task.id,
                output="未生成重构建议，代码质量良好",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=0
            )

        logger.debug(f"[TRACE] _handle_refactor: exit task={task.id}, success=True, suggestions={len(suggestions)}")
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=f"生成了 {len(suggestions)} 条重构建议",
            changes=[],
            issues_found=[],
            suggestions=suggestions,
            execution_time=0
        )

    def _handle_implement(self, task: Task) -> ExecutionResult:
        """[M] 处理实现任务 - 添加 try/except 保护"""
        logger.debug(f"[TRACE] _handle_implement: enter task={task.id}")
        try:
            # [Bug#128] kg查询单独用try/except包裹，失败时similar=[]，任务仍返回success=True
            try:
                similar = self.kg.get_similar_experiences(task.description)
            except Exception as kg_err:
                logger.warning(f"kg.get_similar_experiences() 失败，将使用空经验列表继续: {kg_err}")
                similar = []

            suggestions = []
            if similar:
                for exp, score in similar[:3]:
                    solution_preview = (exp.solution or "")[:100]
                    suggestions.append(f"参考经验 {exp.id}: {solution_preview}...")

            output = f"实现任务: {task.description}\n"
            output += f"找到 {len(similar)} 条相关经验"

            logger.debug(f"[TRACE] _handle_implement: exit task={task.id}, success=True, similar={len(similar)}")
            return ExecutionResult(
                success=True,
                task_id=task.id,
                output=output,
                changes=[],
                issues_found=[],
                suggestions=suggestions,
                execution_time=0
            )
        except Exception as e:
            logger.error(f"实现任务处理异常: {e}")
            logger.debug(f"[TRACE] _handle_implement: exit task={task.id}, success=False, exception={type(e).__name__}")
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output=f"实现任务处理失败: {str(e)}",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=0,
                error_type=type(e).__name__
            )

    def _handle_optimize(self, task: Task) -> ExecutionResult:
        """处理优化任务"""
        logger.debug(f"[TRACE] _handle_optimize: enter task={task.id}")
        suggestions = ["考虑使用性能分析工具 (cProfile, line_profiler)"]

        logger.debug(f"[TRACE] _handle_optimize: exit task={task.id}, success=True, suggestions={len(suggestions)}")
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=f"生成了 {len(suggestions)} 条优化建议",
            changes=[],
            issues_found=[],
            suggestions=suggestions,
            execution_time=0
        )

    def _handle_test(self, task: Task) -> ExecutionResult:
        """处理测试任务"""
        logger.debug(f"[TRACE] _handle_test: enter task={task.id}")
        logger.debug(f"[TRACE] _handle_test: exit task={task.id}, success=True")
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output="测试任务已处理",
            changes=[],
            issues_found=[],
            suggestions=[],
            execution_time=0
        )

    def _record_experience(self, task: Task, result: ExecutionResult) -> str:
        """记录经验"""
        logger.debug(f"[TRACE] _record_experience: task={task.id}, outcome={'success' if result.success else 'failure'}")
        experience = Experience(
            id=f"exp_{task.id}",
            task_type=task.task_type.value,
            context=json.dumps(task.context, ensure_ascii=False),
            problem=task.description,
            solution=result.output[:500] if result.output else "",
            outcome="success" if result.success else "failure",
            lessons=self._extract_lessons(result),
            code_snippets=[],
            tags=[task.task_type.value] + [Path(f).stem for f in task.target_files[:3]],
            created_at=datetime.now().isoformat(),
            success_rate=1.0 if result.success else 0.0,
            usage_count=1
        )

        exp_id = self.kg.add_experience(experience)
        logger.debug(f"[TRACE] _record_experience: task={task.id}, experience_id={exp_id}")
        return exp_id

    def _extract_lessons(self, result: ExecutionResult) -> str:
        """提取经验教训"""
        if result.success:
            return f"成功完成; 关键建议: {result.suggestions[0] if result.suggestions else '无'}"
        else:
            return f"失败原因: {result.output[:200] if result.output else '未知'}"

    def get_status(self) -> Dict[str, Any]:
        """获取执行器状态 V2"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in get_status")
        with self._lock:
            # 正确统计
            pending_count = sum(1 for t in self.active_tasks.values() if t.status == TaskStatus.PENDING)
            running_count = sum(1 for t in self.active_tasks.values() if t.status == TaskStatus.RUNNING)

            status = {
                "running": self._running.is_set(),
                "workers": len(self._workers),
                "stats": self.stats.copy(),
                "pending_tasks": pending_count,
                "running_tasks": running_count,
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.completed_tasks),
                "queue_size": self.task_queue.qsize()
            }
        logger.debug(f"[TRACE][LOCK] release _lock in get_status")
        logger.debug(f"[TRACE] get_status: running={status['running']}, workers={status['workers']}, pending={pending_count}, running={running_count}, queue={status['queue_size']}")
        return status

    def get_task_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果"""
        logger.debug(f"[TRACE] get_task_result: looking for task_id={task_id}")
        logger.debug(f"[TRACE][LOCK] acquire _lock in get_task_result")
        # [H-019修复] 使用try/finally确保锁在所有路径都被释放
        try:
            with self._lock:
                if task_id in self.active_tasks:
                    task = self.active_tasks[task_id]
                    logger.debug(f"[TRACE] get_task_result: task_id={task_id} found in active_tasks, status={task.status.value}")
                    return {
                        "id": task.id,
                        "status": task.status.value,
                        "result": task.result,
                        "error": task.error
                    }

                for task in self.completed_tasks:
                    if task.id == task_id:
                        logger.debug(f"[TRACE] get_task_result: task_id={task_id} found in completed_tasks, status={task.status.value}")
                        return {
                            "id": task.id,
                            "status": task.status.value,
                            "result": task.result,
                            "error": task.error
                        }

                logger.debug(f"[TRACE] get_task_result: task_id={task_id} not found")
                return None
        finally:
            logger.debug(f"[TRACE][LOCK] release _lock in get_task_result")


# 保持向后兼容
TaskExecutor = TaskExecutorV2
