"""
保活模块 - 持续运行保障
支持：心跳监控、崩溃重启、健康检查、资源监控
"""
import os
import sys
import time
import signal
import threading
import subprocess
import traceback
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass, field
from pathlib import Path
import logging
import json

from config import (
    HEARTBEAT_INTERVAL,
    MAX_CRASH_COUNT,
    CRASH_RESET_TIME,
    WATCH_PATTERNS,
    IGNORE_PATTERNS,
)

logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    """健康状态"""
    is_healthy: bool
    last_check: str
    uptime_seconds: float
    memory_usage_mb: float
    cpu_usage_percent: float
    active_threads: int
    pending_tasks: int
    errors: List[str] = field(default_factory=list)


@dataclass
class CrashRecord:
    """崩溃记录"""
    timestamp: str
    exit_code: int
    error_message: str
    stack_trace: str
    recovery_action: str


class Watchdog:
    """
    保活监控器
    确保Agent持续运行,崩溃自动恢复
    """

    def __init__(self, main_process_callback: Optional[Callable] = None):
        self.main_process_callback = main_process_callback

        # [H-线程安全] 使用RLock保护所有共享状态
        self._lock = threading.RLock()

        # [H-线程安全] 使用threading.Event替代普通布尔值
        self._running = threading.Event()

        self._start_time: Optional[datetime] = None
        # [M-线程安全] _last_heartbeat在锁保护下访问
        self._last_heartbeat: Optional[datetime] = None

        # 崩溃追踪
        # [LOW] crash_history使用deque限制最大长度，防止无限增长
        self.crash_history: deque = deque(maxlen=100)
        self.crash_count = 0
        self.crash_window_start: Optional[datetime] = None

        # [H-逻辑错误] 安全模式标志，实际控制是否接受新任务
        self._safe_mode = False

        # 健康检查
        # [M-线程安全] health_status在锁保护下访问
        self.health_status: Optional[HealthStatus] = None
        self._health_check_interval = 30  # 秒

        # [LOW] 清理死代码：移除未使用的_subprocess属性
        self._restart_count = 0

        # [H-资源管理] 将线程存为实例属性，stop()中可以join
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._health_thread: Optional[threading.Thread] = None

        # [M-信号安全] 信号处理器只设置Event标志，不在信号处理中执行复杂逻辑
        self._stop_event = threading.Event()

        # 注册信号处理
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """[H-Bug修复] 设置信号处理器 - 只设置Event标志，确保信号安全"""
        def signal_handler(signum, frame):
            logger.info(f"收到信号 {signum}, 准备优雅退出...")
            self._stop_event.set()
            # 不在信号处理中调用stop()，由主循环检测_stop_event后自行退出
            # signal_handler中调用stop()可能导致死锁（stop内部调用thread.join）

        # 捕获终止信号
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def start(self):
        """启动保活监控"""
        self._running.set()
        self._start_time = datetime.now()

        # [H-资源管理] 将线程存为实例属性
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="Heartbeat")
        self._heartbeat_thread.daemon = True
        self._heartbeat_thread.start()

        self._health_thread = threading.Thread(target=self._health_check_loop, name="HealthCheck")
        self._health_thread.daemon = True
        self._health_thread.start()

        logger.info("保活监控已启动")

    def stop(self):
        """停止保活监控"""
        self._running.clear()
        # [H-资源管理] join已创建的线程，避免线程泄漏
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=5)
        if self._health_thread and self._health_thread.is_alive():
            self._health_thread.join(timeout=5)
        logger.info("保活监控已停止")

    def _heartbeat_loop(self):
        """心跳循环"""
        while self._running.is_set():
            try:
                with self._lock:
                    self._last_heartbeat = datetime.now()

                    # 记录心跳日志
                    uptime = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
                    logger.debug(f"心跳正常, 运行时间: {uptime:.0f}秒")

                    # [H-线程安全] 崩溃窗口检查在锁保护下执行
                    self._check_crash_window()

                time.sleep(HEARTBEAT_INTERVAL)

            except Exception as e:
                logger.error(f"心跳异常: {e}")

    def _health_check_loop(self):
        """健康检查循环"""
        while self._running.is_set():
            try:
                status = self._perform_health_check()
                with self._lock:
                    self.health_status = status

                if not status.is_healthy:
                    logger.warning(f"健康检查发现问题: {status.errors}")

                time.sleep(self._health_check_interval)

            except Exception as e:
                logger.error(f"健康检查异常: {e}")

    def _perform_health_check(self) -> HealthStatus:
        """执行健康检查"""
        errors = []

        # 检查运行时间
        uptime = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0

        # 检查内存使用
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=1)

            # 内存警告
            if memory_mb > 1000:  # 超过1GB
                errors.append(f"内存使用过高: {memory_mb:.1f}MB")
        except ImportError:
            # [M-psutil不可用] 标记状态为"未知"，而非假装一切正常
            memory_mb = -1.0
            cpu_percent = -1.0
            errors.append("psutil不可用，无法获取资源使用信息")

        # 检查线程状态
        active_threads = threading.active_count()
        if active_threads > 50:
            errors.append(f"线程数过多: {active_threads}")

        # 检查心跳（在锁保护下读取）
        with self._lock:
            last_hb = self._last_heartbeat
        if last_hb:
            heartbeat_age = (datetime.now() - last_hb).total_seconds()
            if heartbeat_age > 60:
                errors.append(f"心跳延迟: {heartbeat_age:.0f}秒")

        return HealthStatus(
            is_healthy=len(errors) == 0,
            last_check=datetime.now().isoformat(),
            uptime_seconds=uptime,
            memory_usage_mb=memory_mb,
            cpu_usage_percent=cpu_percent,
            active_threads=active_threads,
            pending_tasks=0,  # 由主控制器更新
            errors=errors
        )

    def _check_crash_window(self):
        """
        检查崩溃窗口,必要时重置计数
        注意：此方法必须在self._lock保护下调用
        """
        if self.crash_window_start:
            elapsed = (datetime.now() - self.crash_window_start).total_seconds()
            if elapsed > CRASH_RESET_TIME:
                # 重置崩溃计数
                self.crash_count = 0
                self.crash_window_start = None
                logger.info("崩溃计数已重置")
                # [M-Bug修复] 崩溃计数重置后检查是否应退出安全模式
                self._check_safe_mode_exit()

        # [H-逻辑错误] 统一崩溃判断逻辑，超过阈值进入安全模式
        if self.crash_count >= MAX_CRASH_COUNT:
            logger.critical(f"短时间内崩溃次数过多({self.crash_count}次), 进入安全模式")
            self._enter_safe_mode()

    def _enter_safe_mode(self):
        """
        进入安全模式
        [LOW] 实现实际安全模式：设置标志，通过callback通知主控制器
        注意：此方法必须在self._lock保护下调用
        """
        if self._safe_mode:
            return  # 已经在安全模式中，避免重复通知
        self._safe_mode = True
        self._safe_mode_entered_at = datetime.now()
        logger.warning("进入安全模式, 暂停新任务接受...")
        # 通过callback通知主控制器暂停接受新任务
        if self.main_process_callback:
            try:
                self.main_process_callback("safe_mode", {
                    "reason": "crash_limit_exceeded",
                    "crash_count": self.crash_count,
                    "timestamp": datetime.now().isoformat(),
                })
            except Exception as e:
                logger.error(f"安全模式回调通知失败: {e}")

    def _check_safe_mode_exit(self):
        """[M-Bug修复] 检查是否应该退出安全模式 - 崩溃计数重置后自动退出"""
        if not self._safe_mode:
            return
        if self.crash_count == 0:
            self._safe_mode = False
            logger.info("崩溃计数已重置，退出安全模式")
            if self.main_process_callback:
                try:
                    self.main_process_callback("safe_mode_exit", {
                        "timestamp": datetime.now().isoformat(),
                    })
                except Exception as e:
                    logger.error(f"安全模式退出回调通知失败: {e}")

    @property
    def in_safe_mode(self) -> bool:
        """是否处于安全模式"""
        with self._lock:
            return self._safe_mode

    def record_crash(self, error: Exception, exit_code: int = 1) -> CrashRecord:
        """记录崩溃"""
        error_msg = str(error) if error is not None else "未知错误"
        record = CrashRecord(
            timestamp=datetime.now().isoformat(),
            exit_code=exit_code,
            error_message=error_msg,
            stack_trace=traceback.format_exc(),
            recovery_action="logged"
        )

        with self._lock:
            self.crash_history.append(record)
            self.crash_count += 1

            if not self.crash_window_start:
                self.crash_window_start = datetime.now()

            # [H-逻辑错误] 记录崩溃后立即检查是否需要进入安全模式
            if self.crash_count >= MAX_CRASH_COUNT:
                logger.critical(f"短时间内崩溃次数过多({self.crash_count}次), 进入安全模式")
                self._enter_safe_mode()

        logger.error(f"记录崩溃: {record.error_message}")
        return record

    def should_restart(self) -> bool:
        """判断是否应该重启"""
        with self._lock:
            return self.crash_count < MAX_CRASH_COUNT

    def get_status(self) -> Dict[str, Any]:
        """获取保活状态"""
        with self._lock:
            last_hb = self._last_heartbeat
            hs = self.health_status
            cc = self.crash_count
            rc = self._restart_count
            sm = self._safe_mode
            recent = list(self.crash_history)[-5:]

        return {
            "running": self._running.is_set(),
            "uptime_seconds": (datetime.now() - self._start_time).total_seconds() if self._start_time else 0,
            "crash_count": cc,
            "restart_count": rc,
            "safe_mode": sm,
            "last_heartbeat": last_hb.isoformat() if last_hb else None,
            "health_status": {
                "is_healthy": hs.is_healthy if hs else None,
                "memory_usage_mb": hs.memory_usage_mb if hs else 0,
                "cpu_usage_percent": hs.cpu_usage_percent if hs else 0,
                "active_threads": hs.active_threads if hs else 0
            } if hs else None,
            "recent_crashes": [
                {
                    "timestamp": c.timestamp,
                    # [LOW] 空值检查，避免error_message为None时截断失败
                    "error": (c.error_message or "")[:100]
                } for c in recent
            ]
        }


class FileWatcher:
    """
    文件监控器
    监控代码文件变化,触发相应任务
    """

    def __init__(self, watch_dir: str, callback: Callable[[str, str], None]):
        self.watch_dir = Path(watch_dir)
        self.callback = callback
        self._running = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # [M-线程安全] _file_mtimes加锁保护
        # 存储元组 (mtime_ns, file_size)，解决 mtime 精度不足导致修改事件遗漏的问题
        self._mtimes_lock = threading.Lock()
        self._file_mtimes: Dict[str, tuple] = {}
        # [M-文件临时不可访问] 记录连续不存在的次数，连续多次才报告删除
        self._missing_count: Dict[str, int] = {}
        self._missing_threshold = 3  # 连续3次不存在才报告删除

        # [M-延迟导入] 移到文件顶部统一导入
        self.watch_patterns = WATCH_PATTERNS
        self.ignore_patterns = IGNORE_PATTERNS

    def start(self):
        """启动文件监控"""
        self._running.set()
        self._thread = threading.Thread(target=self._watch_loop, name="FileWatcher")
        self._thread.daemon = True
        self._thread.start()
        logger.info(f"文件监控已启动: {self.watch_dir}")

    def stop(self):
        """停止文件监控"""
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("文件监控已停止")

    def _watch_loop(self):
        """监控循环"""
        scan_interval = 5  # 扫描间隔(秒)

        while self._running.is_set():
            try:
                self._scan_files()
                time.sleep(scan_interval)
            except Exception as e:
                logger.error(f"文件扫描异常: {e}")

    def _is_ignored(self, file_path: Path) -> bool:
        """
        检查文件是否在忽略列表中
        [M-文件忽略模式] 使用路径组件匹配，而非子字符串匹配
        """
        parts = file_path.parts
        for ignore in self.ignore_patterns:
            if ignore in parts:
                return True
        return False

    def _scan_files(self):
        """扫描文件变化"""
        if not self.watch_dir.exists():
            return

        current_files = {}
        # 记录本次扫描检测到的事件，用于调试
        scan_events = []

        # 遍历目录
        for pattern in self.watch_patterns:
            for file_path in self.watch_dir.rglob(pattern):
                # [M-文件忽略模式] 使用路径组件匹配
                if self._is_ignored(file_path):
                    continue

                str_path = str(file_path)
                try:
                    stat_info = file_path.stat()
                    # 使用纳秒精度 mtime，避免文件系统 mtime 精度为1秒导致短时间内创建再修改检测不到
                    mtime_ns = stat_info.st_mtime_ns
                    file_size = stat_info.st_size
                    current_files[str_path] = (mtime_ns, file_size)

                    # [M-文件临时不可访问] 文件重新出现，清除缺失计数
                    with self._mtimes_lock:
                        self._missing_count.pop(str_path, None)

                    # 检查是否是新增或修改
                    with self._mtimes_lock:
                        if str_path not in self._file_mtimes:
                            # 文件路径不在记录中 → 新增文件
                            self.callback(str_path, "created")
                            logger.info(f"检测到新文件: {str_path}")
                            scan_events.append(f"CREATED: {str_path} (mtime_ns={mtime_ns}, size={file_size})")
                        else:
                            # 文件路径在记录中，比较 mtime(纳秒精度) 和文件大小
                            old_mtime_ns, old_size = self._file_mtimes[str_path]
                            if old_mtime_ns != mtime_ns or old_size != file_size:
                                # mtime 或 size 变化 → 文件被修改
                                self.callback(str_path, "modified")
                                logger.info(f"检测到文件修改: {str_path} "
                                            f"(mtime_ns: {old_mtime_ns} -> {mtime_ns}, "
                                            f"size: {old_size} -> {file_size})")
                                scan_events.append(
                                    f"MODIFIED: {str_path} "
                                    f"(mtime_ns: {old_mtime_ns} -> {mtime_ns}, "
                                    f"size: {old_size} -> {file_size})"
                                )
                            else:
                                # 无变化 → 跳过
                                scan_events.append(f"UNCHANGED: {str_path}")

                except OSError:
                    continue

        # [M-文件临时不可访问] 连续多次不存在才报告删除
        with self._mtimes_lock:
            old_mtimes = dict(self._file_mtimes)
            missing = set(old_mtimes.keys()) - set(current_files.keys())

            deleted_files = []
            for old_file in missing:
                self._missing_count[old_file] = self._missing_count.get(old_file, 0) + 1
                if self._missing_count[old_file] >= self._missing_threshold:
                    deleted_files.append(old_file)
                    self._missing_count.pop(old_file, None)

            for old_file in deleted_files:
                self.callback(old_file, "deleted")
                logger.info(f"检测到文件删除: {old_file}")
                scan_events.append(f"DELETED: {old_file}")

            # [H-Bug修复] 只从_file_mtimes中移除已确认删除的文件
            # 保留未达阈值的缺失文件，避免临时缺失导致文件被错误报告为"created"
            for old_file in deleted_files:
                self._file_mtimes.pop(old_file, None)

            # 合并新增文件到记录中（不替换整个字典）
            for path, (mtime_ns, size) in current_files.items():
                self._file_mtimes[path] = (mtime_ns, size)

        # 打印本次扫描的调试日志
        logger.debug(f"文件扫描完成: 共 {len(current_files)} 个文件, "
                     f"事件: {scan_events if scan_events else '无'}")

    def get_watched_files(self) -> List[str]:
        """获取当前监控的文件列表（返回副本，避免外部修改）"""
        with self._mtimes_lock:
            return list(self._file_mtimes.keys())


class ProcessSupervisor:
    """
    进程监督器
    管理子进程的启动、监控和重启
    """

    def __init__(self, command: List[str], work_dir: str = "."):
        self.command = command
        self.work_dir = work_dir
        self._process: Optional[subprocess.Popen] = None
        self._running = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._restart_count = 0
        self._max_restarts = 10
        # [H-资源管理] 管道消费线程，防止管道缓冲区阻塞
        self._stdout_drain_thread: Optional[threading.Thread] = None
        self._stderr_drain_thread: Optional[threading.Thread] = None

    def _drain_stream(self, stream: Any, stream_name: str):
        """消费子进程管道输出，防止缓冲区满导致阻塞"""
        try:
            for line in stream:
                logger.debug(f"[{stream_name}] {line.rstrip()}")
        except Exception:
            pass
        finally:
            try:
                stream.close()
            except Exception:
                pass

    def start(self):
        """启动进程监督"""
        self._running.set()
        self._thread = threading.Thread(target=self._supervise_loop, name="ProcessSupervisor")
        self._thread.daemon = True
        self._thread.start()
        logger.info(f"进程监督已启动: {' '.join(self.command)}")

    def stop(self):
        """停止进程监督"""
        self._running.clear()
        if self._process:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
        # [H-资源管理] join管道消费线程
        if self._stdout_drain_thread and self._stdout_drain_thread.is_alive():
            self._stdout_drain_thread.join(timeout=3)
        if self._stderr_drain_thread and self._stderr_drain_thread.is_alive():
            self._stderr_drain_thread.join(timeout=3)
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("进程监督已停止")

    def _supervise_loop(self):
        """监督循环"""
        while self._running.is_set():
            try:
                # [H-资源管理] 使用DEVNULL避免管道缓冲区阻塞
                # 同时启动线程消费stdout/stderr以保留日志
                self._process = subprocess.Popen(
                    self.command,
                    cwd=self.work_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                # 启动管道消费线程
                self._stdout_drain_thread = threading.Thread(
                    target=self._drain_stream,
                    args=(self._process.stdout, "STDOUT"),
                    name="StdoutDrain",
                    daemon=True,
                )
                self._stdout_drain_thread.start()

                self._stderr_drain_thread = threading.Thread(
                    target=self._drain_stream,
                    args=(self._process.stderr, "STDERR"),
                    name="StderrDrain",
                    daemon=True,
                )
                self._stderr_drain_thread.start()

                logger.info(f"子进程已启动, PID: {self._process.pid}")

                # 等待进程结束
                return_code = self._process.wait()

                # 等待管道消费线程完成
                if self._stdout_drain_thread:
                    self._stdout_drain_thread.join(timeout=3)
                if self._stderr_drain_thread:
                    self._stderr_drain_thread.join(timeout=3)

                if return_code != 0:
                    logger.warning(f"子进程异常退出, 返回码: {return_code}")

                    # 检查是否需要重启
                    if self._restart_count < self._max_restarts and self._running.is_set():
                        self._restart_count += 1
                        logger.info(f"将在5秒后重启子进程 (第{self._restart_count}次)")
                        time.sleep(5)
                    else:
                        logger.error("达到最大重启次数, 停止监督")
                        self._running.clear()
                else:
                    logger.info("子进程正常退出")
                    if not self._running.is_set():
                        break

            except Exception as e:
                logger.error(f"进程监督异常: {e}")
                time.sleep(5)

    def get_status(self) -> Dict[str, Any]:
        """获取进程状态"""
        return {
            "running": self._process is not None and self._process.poll() is None,
            "pid": self._process.pid if self._process else None,
            "restart_count": self._restart_count,
            "command": " ".join(self.command)
        }
