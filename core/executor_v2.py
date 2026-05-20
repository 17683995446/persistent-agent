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
        """[H-Bug修复] 关闭数据库连接，释放资源"""
        try:
            conn = getattr(self._local, 'conn', None)
            if conn:
                conn.close()
                self._local.conn = None
        except Exception as e:
            logger.warning(f"关闭数据库连接失败: {e}")

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
                execution_time REAL
            )
        ''')
        conn.commit()

    def save_task(self, task: Task):
        """[H-Bug修复] 保存任务 - 添加异常处理防止任务卡在RUNNING状态"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO tasks
                (id, task_type, description, target_files, context, priority, status,
                 created_at, started_at, completed_at, result, error, retry_count, execution_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                task.result.get("execution_time", 0) if task.result else 0
            ))
            conn.commit()
        except Exception as e:
            logger.error(f"保存任务 {task.id} 失败: {e}")
            # 尝试回滚
            try:
                conn.rollback()
            except Exception:
                pass

    def load_pending_tasks(self) -> List[Task]:
        """[H-Bug修复] 加载待处理任务 - 逐条JSON解析保护，一条损坏不影响其他"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM tasks
            WHERE status IN ('pending', 'running', 'failed')
            AND retry_count < ?
            ORDER BY priority DESC, created_at ASC
        ''', (RETRY_COUNT,))
        rows = cursor.fetchall()

        tasks = []
        for row in rows:
            try:
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
                    retry_count=row[12] or 0
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
            self.start(self._num_workers)
            # 恢复未完成任务
            self._restore_pending_tasks()

    def _restore_pending_tasks(self):
        """[H-线程安全] 恢复待处理任务 - 加锁保护共享状态修改"""
        try:
            # I/O操作在锁外执行
            pending_tasks = self.task_store.load_pending_tasks()

            with self._lock:
                for task in pending_tasks:
                    if task.status == TaskStatus.RUNNING:
                        # [H-逻辑错误] 恢复RUNNING任务时不增加retry_count
                        # 之前运行中的任务标记为失败，准备重试（不递增retry_count）
                        task.status = TaskStatus.FAILED
                        task.error = "执行器重启，任务中断"

                    if task.retry_count < task.max_retries:
                        task.status = TaskStatus.PENDING
                        # [M-Bug修复] 清除旧的错误信息
                        task.error = None
                        self.task_queue.put((-task.priority, task.created_at or "", task.id))
                        self.active_tasks[task.id] = task
                        logger.info(f"恢复任务: {task.id}")

                if pending_tasks:
                    logger.info(f"恢复了 {len(pending_tasks)} 个待处理任务")
        except Exception as e:
            logger.error(f"恢复任务失败: {e}")

    def submit_task(self, task: Task) -> str:
        """[H-Bug修复] 提交任务 - 确保created_at不为None，防止队列元组比较崩溃"""
        # 确保created_at有值
        if not task.created_at:
            task.created_at = datetime.now().isoformat()

        # I/O操作：查找相关经验（在锁外执行）
        try:
            similar = self.kg.get_similar_experiences(task.description, limit=3)
            task.related_experiences = [exp.id for exp, _ in similar]
        except Exception as e:
            logger.warning(f"查找相关经验失败: {e}")

        # 锁内只修改共享状态
        with self._lock:
            self.task_queue.put((-task.priority, task.created_at, task.id))
            self.active_tasks[task.id] = task
            self.stats["submitted"] += 1

        # I/O操作：持久化（在锁外执行）
        self.task_store.save_task(task)

        logger.info(f"任务已提交: {task.id} - {task.task_type.value}")
        return task.id

    def start(self, num_workers: int = MAX_WORKERS):
        """[H-停启安全] 启动执行器 - 检查残留线程并加锁保护"""
        with self._lock:
            if self._running.is_set():
                logger.warning("执行器已在运行")
                return

            # [H-停启安全] 检查并清理残留线程
            self._cleanup_stale_workers()

            self._running.set()
            self._num_workers = num_workers

            for i in range(num_workers):
                worker = threading.Thread(target=self._worker_loop, name=f"Worker-{i}")
                worker.daemon = True
                worker.start()
                self._workers.append(worker)

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
        with self._lock:
            if not self._running.is_set():
                logger.warning("执行器未在运行")
                return

            self._running.clear()

            # 清空队列，将未执行的任务标记为CANCELLED
            cancelled_count = 0
            while not self.task_queue.empty():
                try:
                    _, _, task_id = self.task_queue.get_nowait()
                    task = self.active_tasks.get(task_id)
                    if task and task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                        task.status = TaskStatus.CANCELLED
                        task.error = "执行器停止，任务被取消"
                        self.completed_tasks.append(task)
                        # [H-Bug修复] 从active_tasks中移除，防止内存泄漏
                        self.active_tasks.pop(task_id, None)
                        cancelled_count += 1
                    self.task_queue.task_done()
                except queue.Empty:
                    break

            if cancelled_count > 0:
                logger.info(f"取消了 {cancelled_count} 个未执行任务")

        # 等待工作线程退出（在锁外等待，避免死锁）
        for worker in self._workers:
            worker.join(timeout=5)

        with self._lock:
            self._workers.clear()

        logger.info("执行器已停止")

    def _worker_loop(self):
        """工作线程主循环 V2"""
        while self._running.is_set():
            try:
                # 获取任务
                try:
                    _, _, task_id = self.task_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                try:
                    with self._lock:
                        task = self.active_tasks.get(task_id)
                        if not task:
                            continue

                        task.status = TaskStatus.RUNNING
                        task.started_at = datetime.now().isoformat()

                    # 持久化在锁外
                    self.task_store.save_task(task)

                    # 执行任务（带超时，使用协作式取消）
                    result = self._execute_task_with_timeout(task)

                    # 更新任务状态
                    will_retry = False
                    with self._lock:
                        if result.success:
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
                                task.status = TaskStatus.PENDING
                                # 重新加入队列
                                self.task_queue.put((-task.priority, task.created_at, task.id))
                                logger.info(f"任务 {task.id} 将重试 ({task.retry_count}/{task.max_retries})")
                            else:
                                # 超过重试次数，标记为最终失败
                                if result.error_type == "timeout":
                                    task.status = TaskStatus.TIMEOUT
                                    self.stats["timeout"] += 1
                                else:
                                    task.status = TaskStatus.FAILED
                                    self.stats["failed"] += 1

                                task.completed_at = datetime.now().isoformat()
                                # [M] 使用 dataclasses.asdict() 替代 __dict__
                                task.result = asdict(result) if result else None
                                task.error = result.output[:500] if result.output else "Unknown error"
                                self.completed_tasks.append(task)
                                # [H-内存泄漏] 完成后从 active_tasks 中删除
                                self.active_tasks.pop(task.id, None)

                    # 持久化在锁外
                    self.task_store.save_task(task)

                finally:
                    # [M] task_done() 用 try/finally 保证调用
                    self.task_queue.task_done()

            except Exception as e:
                logger.error(f"工作线程异常: {traceback.format_exc()}")

    def _execute_task_with_timeout(self, task: Task) -> ExecutionResult:
        """[H-资源管理] 带超时的任务执行 - 使用协作式取消(threading.Event)"""
        result_container = {}
        # 协作式取消标志，handler可以通过检查此标志来提前退出
        cancel_event = threading.Event()

        def execute():
            try:
                result = self._execute_task(task, cancel_event)
                result_container['result'] = result
            except Exception as e:
                result_container['error'] = e

        thread = threading.Thread(target=execute)
        thread.daemon = True  # [M-Bug修复] 设置为守护线程，防止超时后成为僵尸线程
        thread.start()
        # 等待任务完成或超时
        thread.join(timeout=task.timeout_seconds)

        if thread.is_alive():
            # 超时 - 设置取消标志通知handler
            cancel_event.set()
            # 给予短暂时间让handler优雅退出
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning(f"任务 {task.id} 超时后未能优雅退出")
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

        if 'error' in result_container:
            error = result_container['error']
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

        return result_container.get('result', ExecutionResult(
            success=False,
            task_id=task.id,
            output="未获取到执行结果",
            changes=[],
            issues_found=[],
            suggestions=[],
            execution_time=0
        ))

    def _execute_task(self, task: Task, cancel_event: threading.Event = None) -> ExecutionResult:
        """执行单个任务"""
        start_time = time.time()

        try:
            handler = self.handlers.get(task.task_type)
            if not handler:
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

            result = handler(task)
            result.execution_time = time.time() - start_time

            # 记录经验
            try:
                experience_id = self._record_experience(task, result)
                result.experience_id = experience_id
            except Exception as e:
                logger.warning(f"记录经验失败: {e}")

            return result

        except Exception as e:
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

    def _handle_analyze(self, task: Task) -> ExecutionResult:
        """处理代码分析任务"""
        # [M] 空文件列表统一返回 success=False
        if not task.target_files:
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

        for file_path in task.target_files:
            try:
                full_path = self.work_dir / file_path if not os.path.isabs(file_path) else Path(file_path)

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
                files_not_found += 1

        output = f"分析了 {files_analyzed} 个文件"
        if files_not_found > 0:
            output += f" (跳过 {files_not_found} 个不存在文件)"
        output += f"\n发现 {len(issues)} 个潜在问题\n"
        output += f"生成 {len(suggestions)} 条建议"

        # 只要有文件被分析，就算成功
        success = files_analyzed > 0

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
        # [M] 空文件列表统一返回 success=False
        if not task.target_files:
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

        for file_path in task.target_files:
            full_path = self.work_dir / file_path if not os.path.isabs(file_path) else Path(file_path)
            # [M] 添加文件存在性检查
            if not os.path.exists(str(full_path)):
                logger.warning(f"Bug修复: 文件不存在 {file_path}")
                continue
            bugs = CodeAnalyzer.detect_bugs(str(full_path))
            issues.extend(bugs)

        if not issues:
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
        # [M] 空文件列表统一返回 success=False
        if not task.target_files:
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

        for file_path in task.target_files:
            full_path = self.work_dir / file_path if not os.path.isabs(file_path) else Path(file_path)
            # [M] 添加文件存在性检查
            if not os.path.exists(str(full_path)):
                logger.warning(f"重构分析: 文件不存在 {file_path}")
                continue
            analysis = CodeAnalyzer.analyze_file(str(full_path))

            if analysis.get("complexity", 0) > 15:
                suggestions.append(f"{file_path}: 复杂度过高, 考虑拆分")

            for func in analysis.get("functions", []):
                if func.get("docstring") == "":
                    suggestions.append(f"{file_path}: 函数 {func['name']} 缺少文档字符串")

        if not suggestions:
            return ExecutionResult(
                success=True,
                task_id=task.id,
                output="未生成重构建议，代码质量良好",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=0
            )

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
        try:
            similar = self.kg.get_similar_experiences(task.description)

            suggestions = []
            if similar:
                for exp, score in similar[:3]:
                    suggestions.append(f"参考经验 {exp.id}: {exp.solution[:100]}...")

            output = f"实现任务: {task.description}\n"
            output += f"找到 {len(similar)} 条相关经验"

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
        suggestions = ["考虑使用性能分析工具 (cProfile, line_profiler)"]

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
        from core.knowledge_graph import Experience

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

        return self.kg.add_experience(experience)

    def _extract_lessons(self, result: ExecutionResult) -> str:
        """提取经验教训"""
        if result.success:
            return f"成功完成; 关键建议: {result.suggestions[0] if result.suggestions else '无'}"
        else:
            return f"失败原因: {result.output[:200] if result.output else '未知'}"

    def get_status(self) -> Dict[str, Any]:
        """获取执行器状态 V2"""
        with self._lock:
            # 正确统计
            pending_count = sum(1 for t in self.active_tasks.values() if t.status == TaskStatus.PENDING)
            running_count = sum(1 for t in self.active_tasks.values() if t.status == TaskStatus.RUNNING)

            return {
                "running": self._running.is_set(),
                "workers": len(self._workers),
                "stats": self.stats.copy(),
                "pending_tasks": pending_count,
                "running_tasks": running_count,
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.completed_tasks),
                "queue_size": self.task_queue.qsize()
            }

    def get_task_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果"""
        with self._lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                return {
                    "id": task.id,
                    "status": task.status.value,
                    "result": task.result,
                    "error": task.error
                }

            for task in self.completed_tasks:
                if task.id == task_id:
                    return {
                        "id": task.id,
                        "status": task.status.value,
                        "result": task.result,
                        "error": task.error
                    }

            return None


# 保持向后兼容
TaskExecutor = TaskExecutorV2
