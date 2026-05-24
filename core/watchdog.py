"""
保活模块 - 持续运行保障
支持：心跳监控、崩溃重启、健康检查、资源监控

R25修复:
- [R25-Bug#203] 添加信号监控线程，自动检测_stop_event并调用stop()
- [R25-Bug#204] 信号处理器同时清除_running，保持状态一致性
- [R25-Bug#205] stop()清理线程引用，防止资源泄漏

R26修复:
- [R26-Bug#215] 使用弱引用包装回调，避免循环引用
"""
import os
import sys
import time
import signal
import threading
import subprocess
import traceback
import weakref
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
        # [R26-Bug#215修复] 使用弱引用包装回调，避免循环引用
        self._callback_ref = None
        if main_process_callback is not None:
            try:
                # 如果是绑定方法，使用WeakMethod
                self._callback_ref = weakref.WeakMethod(main_process_callback)
            except TypeError:
                # 不是绑定方法，尝试普通弱引用
                try:
                    self._callback_ref = weakref.ref(main_process_callback)
                except TypeError:
                    # 无法创建弱引用，保留原始引用但记录警告
                    logger.warning("[TRACE] callback无法创建弱引用，可能导致循环引用")
                    self._callback_ref = lambda: main_process_callback
        
        logger.debug(f"[TRACE] __init__: callback_ref type={type(self._callback_ref).__name__}")

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
        # [H-Bug修复] _safe_mode_entered_at 在 __init__ 中初始化
        self._safe_mode_entered_at: Optional[datetime] = None

        # 健康检查
        # [M-线程安全] health_status在锁保护下访问
        self.health_status: Optional[HealthStatus] = None
        self._health_check_interval = 30  # 秒

        # [LOW] 清理死代码：移除未使用的_subprocess属性
        self._restart_count = 0

        # [H-资源管理] 将线程存为实例属性，stop()中可以join
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._health_thread: Optional[threading.Thread] = None
        # [R25-Bug#205修复] 信号监控线程
        self._signal_monitor_thread: Optional[threading.Thread] = None

        # [M-信号安全] 信号处理器只设置Event标志，不在信号处理中执行复杂逻辑
        self._stop_event = threading.Event()

        # 注册信号处理
        self._setup_signal_handlers()
        logger.debug("[TRACE] Watchdog.__init__ completed: callback=%s, health_check_interval=%ds",
                     main_process_callback is not None, self._health_check_interval)

    def _invoke_callback(self, data: Any) -> None:
        """[R26-Bug#215修复] 安全调用回调，自动处理弱引用"""
        if self._callback_ref is None:
            return
        try:
            callback = self._callback_ref()
            if callback is not None:
                callback(data)
            else:
                logger.debug("[TRACE] _invoke_callback: callback已被垃圾回收")
        except Exception as e:
            logger.error(f"回调调用失败: {e}")

    def _setup_signal_handlers(self):
        """[H-Bug修复] 设置信号处理器 - 只设置Event标志，确保信号安全
        
        R25-Bug#203修复: 添加信号监控线程，检测_stop_event后自动调用stop()
        R25-Bug#204修复: 确保_stop_event设置后_running也被清除
        """
        def signal_handler(signum, frame):
            # [R19-021修复] 信号处理器中不使用logger，只设置Event标志
            self._stop_event.set()
            # [R25-Bug#204修复] 同时清除_running，保持状态一致性
            self._running.clear()
            # 注意: 不直接调用stop()，因为信号处理器中调用thread.join()可能导致死锁

        # 捕获终止信号
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.debug("[TRACE] _setup_signal_handlers: registered SIGTERM and SIGINT handlers")
        
        # [R25-Bug#203修复] 启动信号监控线程，自动检测_stop_event并调用stop()
        self._signal_monitor_thread = threading.Thread(target=self._signal_monitor_loop, name="SignalMonitor", daemon=True)
        self._signal_monitor_thread.start()
        logger.debug("[TRACE] _setup_signal_handlers: signal monitor thread started")

    def _signal_monitor_loop(self):
        """[R25-Bug#203修复] 信号监控循环 - 检测_stop_event并自动调用stop()"""
        logger.debug("[TRACE] _signal_monitor_loop: started")
        while not self._stop_event.is_set():
            time.sleep(0.1)
        
        # _stop_event被设置，自动调用stop()
        logger.debug("[TRACE] _signal_monitor_loop: _stop_event detected, calling stop()")
        try:
            self.stop()
        except Exception as e:
            # 信号监控线程中不能抛出异常
            pass
        logger.debug("[TRACE] _signal_monitor_loop: exited")

    def start(self):
        """[R19-013修复] 启动保活监控 - 添加锁保护"""
        with self._lock:
            if self._running.is_set():
                return
            self._running.set()
            self._start_time = datetime.now()
            logger.debug("[TRACE] Watchdog.start: _running set, start_time=%s", self._start_time.isoformat())

            # [H-资源管理] 将线程存为实例属性
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="Heartbeat")
            self._heartbeat_thread.daemon = True
            self._heartbeat_thread.start()
            logger.debug("[TRACE] Watchdog.start: heartbeat thread started (name=Heartbeat)")

            self._health_thread = threading.Thread(target=self._health_check_loop, name="HealthCheck")
            self._health_thread.daemon = True
            self._health_thread.start()
            logger.debug("[TRACE] Watchdog.start: health thread started (name=HealthCheck)")

        logger.info("保活监控已启动")

    def stop(self):
        """[R19-014修复] 停止保活监控 - 设置标志在锁内，join在锁外避免死锁
        
        R25-Bug#205修复: 清理线程引用，防止资源泄漏
        """
        with self._lock:
            if not self._running.is_set():
                return
            self._running.clear()
            # [R25-Bug#204修复] 同时清除_stop_event，保持状态一致性
            self._stop_event.set()
            logger.debug("[TRACE] Watchdog.stop: _running and _stop_event cleared")
            # 保存线程引用，在锁外join
            hb_thread = self._heartbeat_thread
            hc_thread = self._health_thread
            sm_thread = self._signal_monitor_thread
        # [R19-014修复] 在锁外join线程，避免与_heartbeat_loop/_health_check_loop中的锁竞争导致死锁
        if hb_thread and hb_thread.is_alive():
            logger.debug("[TRACE] Watchdog.stop: joining heartbeat thread (timeout=5s)")
            hb_thread.join(timeout=5)
        if hc_thread and hc_thread.is_alive():
            logger.debug("[TRACE] Watchdog.stop: joining health thread (timeout=5s)")
            hc_thread.join(timeout=5)
        if sm_thread and sm_thread.is_alive():
            logger.debug("[TRACE] Watchdog.stop: joining signal monitor thread (timeout=1s)")
            sm_thread.join(timeout=1)
        
        # [R25-Bug#205修复] 清理线程引用
        with self._lock:
            self._heartbeat_thread = None
            self._health_thread = None
            self._signal_monitor_thread = None
        
        logger.info("保活监控已停止")

    def _heartbeat_loop(self):
        """心跳循环"""
        while self._running.is_set() and not self._stop_event.is_set():
            try:
                logger.debug("[TRACE] _heartbeat_loop: loop iteration start")
                logger.debug("[TRACE][LOCK] acquire _lock in _heartbeat_loop")
                with self._lock:
                    self._last_heartbeat = datetime.now()
                    logger.debug("[TRACE] _heartbeat_loop: _last_heartbeat updated to %s",
                                 self._last_heartbeat.isoformat())

                    # 记录心跳日志
                    uptime = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
                    logger.debug(f"心跳正常, 运行时间: {uptime:.0f}秒")

                    # [H-线程安全] 崩溃窗口检查在锁保护下执行
                    logger.debug("[TRACE] _heartbeat_loop: calling _check_crash_window")
                    self._check_crash_window()
                logger.debug("[TRACE][LOCK] release _lock in _heartbeat_loop")

                logger.debug("[TRACE] _heartbeat_loop: sleeping %ds", HEARTBEAT_INTERVAL)
                # [R19修复] 使用短间隔轮询替代Event.wait，因为Event.wait不响应clear()
                for _ in range(HEARTBEAT_INTERVAL * 10):
                    if not self._running.is_set():
                        break
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"心跳异常: {e}")

    def _health_check_loop(self):
        """健康检查循环"""
        while self._running.is_set() and not self._stop_event.is_set():
            try:
                logger.debug("[TRACE] _health_check_loop: loop iteration start")
                status = self._perform_health_check()
                logger.debug("[TRACE] _health_check_loop: _perform_health_check returned is_healthy=%s, errors=%s",
                             status.is_healthy, status.errors)
                logger.debug("[TRACE][LOCK] acquire _lock in _health_check_loop")
                with self._lock:
                    self.health_status = status
                logger.debug("[TRACE][LOCK] release _lock in _health_check_loop")

                if not status.is_healthy:
                    logger.warning(f"健康检查发现问题: {status.errors}")

                logger.debug("[TRACE] _health_check_loop: sleeping %ds", self._health_check_interval)
                # [R19修复] 使用短间隔轮询替代Event.wait，因为Event.wait不响应clear()
                for _ in range(int(self._health_check_interval * 10)):
                    if not self._running.is_set():
                        break
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"健康检查异常: {e}")

    def _perform_health_check(self) -> HealthStatus:
        """执行健康检查"""
        errors = []

        # 检查运行时间
        uptime = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
        logger.debug("[TRACE] _perform_health_check: uptime=%.1fs", uptime)

        # 检查内存使用
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=1)
            logger.debug("[TRACE] _perform_health_check: memory=%.1fMB, cpu=%.1f%%", memory_mb, cpu_percent)

            # 内存警告
            if memory_mb > 1000:  # 超过1GB
                errors.append(f"内存使用过高: {memory_mb:.1f}MB")
        except ImportError:
            # [M-psutil不可用] 标记状态为"未知"，而非假装一切正常
            memory_mb = -1.0
            cpu_percent = -1.0
            errors.append("psutil不可用，无法获取资源使用信息")
            logger.debug("[TRACE] _perform_health_check: psutil not available, using defaults")

        # 检查线程状态
        active_threads = threading.active_count()
        logger.debug("[TRACE] _perform_health_check: active_threads=%d", active_threads)
        if active_threads > 50:
            errors.append(f"线程数过多: {active_threads}")

        # 检查心跳（在锁保护下读取）
        logger.debug("[TRACE][LOCK] acquire _lock in _perform_health_check (read heartbeat)")
        with self._lock:
            last_hb = self._last_heartbeat
        logger.debug("[TRACE][LOCK] release _lock in _perform_health_check (read heartbeat)")
        if last_hb:
            heartbeat_age = (datetime.now() - last_hb).total_seconds()
            logger.debug("[TRACE] _perform_health_check: heartbeat_age=%.1fs", heartbeat_age)
            if heartbeat_age > 60:
                errors.append(f"心跳延迟: {heartbeat_age:.0f}秒")

        is_healthy = len(errors) == 0
        logger.debug("[TRACE] _perform_health_check: final result is_healthy=%s, errors_count=%d",
                     is_healthy, len(errors))
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
        logger.debug("[TRACE] _check_crash_window: crash_count=%d, window_start=%s",
                     self.crash_count, self.crash_window_start)
        if self.crash_window_start:
            elapsed = (datetime.now() - self.crash_window_start).total_seconds()
            logger.debug("[TRACE] _check_crash_window: window_elapsed=%.1fs, reset_threshold=%ds",
                         elapsed, CRASH_RESET_TIME)
            if elapsed > CRASH_RESET_TIME:
                # 重置崩溃计数
                self.crash_count = 0
                self.crash_window_start = None
                logger.info("崩溃计数已重置")
                logger.debug("[TRACE] _check_crash_window: crash_count reset to 0, window_start cleared")
                # [M-Bug修复] 崩溃计数重置后检查是否应退出安全模式
                logger.debug("[TRACE] _check_crash_window: calling _check_safe_mode_exit")
                self._check_safe_mode_exit()

        # [H-逻辑错误] 统一崩溃判断逻辑，超过阈值进入安全模式
        if self.crash_count >= MAX_CRASH_COUNT and not self._safe_mode:
            logger.debug("[TRACE] _check_crash_window: crash_count=%d >= MAX_CRASH_COUNT=%d, entering safe mode",
                         self.crash_count, MAX_CRASH_COUNT)
            logger.critical(f"短时间内崩溃次数过多({self.crash_count}次), 进入安全模式")
            self._enter_safe_mode()

    def _enter_safe_mode(self):
        """
        进入安全模式
        [R19-001修复] 在锁内收集回调数据，释放锁后再调用回调，避免死锁
        注意：此方法必须在self._lock保护下调用
        """
        logger.debug("[TRACE] _enter_safe_mode: _safe_mode=%s", self._safe_mode)
        if self._safe_mode:
            logger.debug("[TRACE] _enter_safe_mode: already in safe mode, skipping")
            return  # 已经在安全模式中，避免重复通知
        self._safe_mode = True
        self._safe_mode_entered_at = datetime.now()
        logger.debug("[TRACE] _enter_safe_mode: _safe_mode set to True, entered_at=%s",
                     self._safe_mode_entered_at.isoformat())
        logger.warning("进入安全模式, 暂停新任务接受...")
        # [R19-001修复] 在锁内收集回调数据
        # [R26-Bug#215修复] 使用_invoke_callback代替直接调用
        callback_data = {
            "type": "safe_mode",
            "data": {
                "reason": "crash_limit_exceeded",
                "crash_count": self.crash_count,
                "timestamp": datetime.now().isoformat(),
            }
        }
        # [R19-001修复] 释放锁后再调用回调，避免死锁
        self._lock.release()
        try:
            logger.debug("[TRACE] _enter_safe_mode: invoking callback('safe_mode') outside lock")
            self._invoke_callback(callback_data)
        except Exception as e:
            logger.error(f"安全模式回调通知失败: {e}")
        finally:
            self._lock.acquire()

    def _check_safe_mode_exit(self):
        """[R19-002修复] 检查是否应该退出安全模式 - 崩溃计数重置后自动退出
        在锁内收集回调数据，释放锁后再调用回调，避免死锁
        注意：此方法必须在self._lock保护下调用
        """
        logger.debug("[TRACE] _check_safe_mode_exit: _safe_mode=%s, crash_count=%d",
                     self._safe_mode, self.crash_count)
        if not self._safe_mode:
            logger.debug("[TRACE] _check_safe_mode_exit: not in safe mode, skipping")
            return
        if self.crash_count == 0:
            self._safe_mode = False
            logger.debug("[TRACE] _check_safe_mode_exit: crash_count==0, _safe_mode set to False")
            logger.info("崩溃计数已重置，退出安全模式")
            # [R19-002修复] 在锁内收集回调数据
            # [R26-Bug#215修复] 使用_invoke_callback代替直接调用
            callback_data = {
                "type": "safe_mode_exit",
                "data": {"timestamp": datetime.now().isoformat()}
            }
            # [R19-002修复] 释放锁后再调用回调，避免死锁
            self._lock.release()
            try:
                logger.debug("[TRACE] _check_safe_mode_exit: invoking callback('safe_mode_exit') outside lock")
                self._invoke_callback(callback_data)
            except Exception as e:
                logger.error(f"安全模式退出回调通知失败: {e}")
            finally:
                self._lock.acquire()

    @property
    def in_safe_mode(self) -> bool:
        """是否处于安全模式"""
        with self._lock:
            return self._safe_mode

    def record_crash(self, error: Exception, exit_code: int = 1) -> CrashRecord:
        """记录崩溃"""
        logger.debug("[TRACE] record_crash: error=%s, exit_code=%d", error, exit_code)
        error_msg = str(error) if error is not None else "未知错误"
        record = CrashRecord(
            timestamp=datetime.now().isoformat(),
            exit_code=exit_code,
            error_message=error_msg,
            stack_trace=traceback.format_exc(),
            recovery_action="logged"
        )

        logger.debug("[TRACE][LOCK] acquire _lock in record_crash")
        with self._lock:
            self.crash_history.append(record)
            self.crash_count += 1
            logger.debug("[TRACE] record_crash: crash_count incremented to %d, history_len=%d",
                         self.crash_count, len(self.crash_history))

            if not self.crash_window_start:
                self.crash_window_start = datetime.now()
                logger.debug("[TRACE] record_crash: crash_window_start set to %s",
                             self.crash_window_start.isoformat())

            # [H-逻辑错误] 记录崩溃后立即检查是否需要进入安全模式
            if self.crash_count >= MAX_CRASH_COUNT and not self._safe_mode:
                logger.debug("[TRACE] record_crash: crash_count=%d >= MAX_CRASH_COUNT=%d, entering safe mode",
                             self.crash_count, MAX_CRASH_COUNT)
                logger.critical(f"短时间内崩溃次数过多({self.crash_count}次), 进入安全模式")
                self._enter_safe_mode()
        logger.debug("[TRACE][LOCK] release _lock in record_crash")

        logger.error(f"记录崩溃: {record.error_message}")
        return record

    def should_restart(self) -> bool:
        """判断是否应该重启"""
        logger.debug("[TRACE][LOCK] acquire _lock in should_restart")
        with self._lock:
            result = self.crash_count < MAX_CRASH_COUNT
        logger.debug("[TRACE][LOCK] release _lock in should_restart")
        logger.debug("[TRACE] should_restart: crash_count=%d, MAX_CRASH_COUNT=%d, result=%s",
                     self.crash_count, MAX_CRASH_COUNT, result)
        return result

    def get_status(self) -> Dict[str, Any]:
        """获取保活状态"""
        logger.debug("[TRACE][LOCK] acquire _lock in get_status")
        with self._lock:
            last_hb = self._last_heartbeat
            hs = self.health_status
            cc = self.crash_count
            rc = self._restart_count
            sm = self._safe_mode
            recent = list(self.crash_history)[-5:]
        logger.debug("[TRACE][LOCK] release _lock in get_status")
        logger.debug("[TRACE] get_status: crash_count=%d, restart_count=%d, safe_mode=%s, recent_crashes=%d",
                     cc, rc, sm, len(recent))

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

        logger.debug("[TRACE] FileWatcher.__init__: watch_dir=%s, watch_patterns=%s, ignore_patterns=%s",
                     self.watch_dir, self.watch_patterns, self.ignore_patterns)

        if not self.watch_dir.exists():
            logger.warning("FileWatcher watch_dir不存在: %s", self.watch_dir)

    def start(self):
        """启动文件监控"""
        if self._running.is_set():
            return
        self._running.set()
        self._thread = threading.Thread(target=self._watch_loop, name="FileWatcher")
        self._thread.daemon = True
        self._thread.start()
        logger.debug("[TRACE] FileWatcher.start: thread started (name=FileWatcher)")
        logger.info(f"文件监控已启动: {self.watch_dir}")

    def stop(self):
        """停止文件监控"""
        self._running.clear()
        logger.debug("[TRACE] FileWatcher.stop: _running cleared")
        if self._thread:
            logger.debug("[TRACE] FileWatcher.stop: joining thread (timeout=5s)")
            self._thread.join(timeout=5)
        logger.info("文件监控已停止")

    def _watch_loop(self):
        """监控循环"""
        scan_interval = 5  # 扫描间隔(秒)

        while self._running.is_set():
            try:
                logger.debug("[TRACE] _watch_loop: loop iteration start")
                self._scan_files()
                logger.debug("[TRACE] _watch_loop: sleeping %ds", scan_interval)
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
                logger.debug("[TRACE] _is_ignored: %s is ignored (matched pattern '%s')", file_path, ignore)
                return True
        logger.debug("[TRACE] _is_ignored: %s is not ignored", file_path)
        return False

    def _scan_files(self):
        """扫描文件变化"""
        if not self.watch_dir.exists():
            logger.warning("FileWatcher watch_dir不存在，跳过本次扫描: %s", self.watch_dir)
            logger.debug("[TRACE] _scan_files: watch_dir does not exist: %s", self.watch_dir)
            return

        current_files = {}
        # 记录本次扫描检测到的事件，用于调试
        scan_events = []
        # [Bug#101修复] 在锁内收集事件，锁外调用回调，避免回调中获取其他锁导致死锁
        pending_events = []

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
                    logger.debug("[TRACE] _scan_files: scanned %s (mtime_ns=%d, size=%d)",
                                 str_path, mtime_ns, file_size)

                    # [M-文件临时不可访问] 文件重新出现，清除缺失计数
                    with self._mtimes_lock:
                        self._missing_count.pop(str_path, None)

                    # 检查是否是新增或修改（在锁内收集事件，锁外调用回调）
                    with self._mtimes_lock:
                        if str_path not in self._file_mtimes:
                            # 文件路径不在记录中 → 新增文件
                            pending_events.append(("created", str_path, mtime_ns, file_size, None, None))
                            scan_events.append(f"CREATED: {str_path} (mtime_ns={mtime_ns}, size={file_size})")
                        else:
                            # 文件路径在记录中，比较 mtime(纳秒精度) 和文件大小
                            old_mtime_ns, old_size = self._file_mtimes[str_path]
                            if old_mtime_ns != mtime_ns or old_size != file_size:
                                # mtime 或 size 变化 → 文件被修改
                                pending_events.append(("modified", str_path, mtime_ns, file_size, old_mtime_ns, old_size))
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
        logger.debug("[TRACE][LOCK] acquire _mtimes_lock in _scan_files (missing check)")
        with self._mtimes_lock:
            old_mtimes = dict(self._file_mtimes)
            missing = set(old_mtimes.keys()) - set(current_files.keys())
            logger.debug("[TRACE] _scan_files: total_known=%d, current=%d, missing=%d",
                         len(old_mtimes), len(current_files), len(missing))

            deleted_files = []
            for old_file in missing:
                self._missing_count[old_file] = self._missing_count.get(old_file, 0) + 1
                if self._missing_count[old_file] >= self._missing_threshold:
                    deleted_files.append(old_file)
                    self._missing_count.pop(old_file, None)

            for old_file in deleted_files:
                pending_events.append(("deleted", old_file, None, None, None, None))
                logger.info(f"检测到文件删除: {old_file}")
                scan_events.append(f"DELETED: {old_file}")

            # [H-Bug修复] 只从_file_mtimes中移除已确认删除的文件
            # 保留未达阈值的缺失文件，避免临时缺失导致文件被错误报告为"created"
            for old_file in deleted_files:
                self._file_mtimes.pop(old_file, None)

        logger.debug("[TRACE][LOCK] release _mtimes_lock in _scan_files (missing check)")

        # [M-Bug修复] 先更新_file_mtimes，再调用callback，避免callback异常导致重复触发同一事件
        # 更新文件修改时间记录（包括新增和修改的文件）
        logger.debug("[TRACE][LOCK] acquire _mtimes_lock in _scan_files (update mtimes)")
        with self._mtimes_lock:
            for path, (mtime_ns, size) in current_files.items():
                self._file_mtimes[path] = (mtime_ns, size)
        logger.debug("[TRACE][LOCK] release _mtimes_lock in _scan_files (update mtimes)")
        logger.debug("[TRACE] _scan_files: _file_mtimes updated, total_files=%d", len(self._file_mtimes))

        # [Bug#101修复] 在锁外调用回调，避免回调中获取其他锁导致死锁
        # [H-Bug修复] 将每个callback调用包裹在try/except中，防止单个callback失败导致后续事件丢失
        for event in pending_events:
            event_type, file_path, mtime_ns, file_size, old_mtime_ns, old_size = event
            try:
                if event_type == "created":
                    logger.info(f"检测到新文件: {file_path}")
                    self.callback(file_path, "created")
                elif event_type == "modified":
                    logger.info(f"检测到文件修改: {file_path} "
                                f"(mtime_ns: {old_mtime_ns} -> {mtime_ns}, "
                                f"size: {old_size} -> {file_size})")
                    self.callback(file_path, "modified")
                elif event_type == "deleted":
                    self.callback(file_path, "deleted")
            except Exception as e:
                logger.error(f"文件事件回调处理失败 ({event_type} {file_path}): {e}")

        # 打印本次扫描的调试日志
        logger.debug(f"文件扫描完成: 共 {len(current_files)} 个文件, "
                     f"事件: {scan_events if scan_events else '无'}")

    def get_watched_files(self) -> List[str]:
        """获取当前监控的文件列表（返回副本，避免外部修改）"""
        logger.debug("[TRACE][LOCK] acquire _mtimes_lock in get_watched_files")
        with self._mtimes_lock:
            files = list(self._file_mtimes.keys())
        logger.debug("[TRACE][LOCK] release _mtimes_lock in get_watched_files")
        logger.debug("[TRACE] get_watched_files: returning %d files", len(files))
        return files


class ProcessSupervisor:
    """
    进程监督器
    管理子进程的启动、监控和重启
    """

    def __init__(self, command: List[str], work_dir: str = "."):
        if not command:
            raise ValueError("ProcessSupervisor command must not be empty")
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
                logger.debug("[TRACE] _drain_stream: %s line received", stream_name)
        except Exception:
            pass
        finally:
            try:
                stream.close()
                logger.debug("[TRACE] _drain_stream: %s stream closed", stream_name)
            except Exception:
                pass

    def start(self):
        """启动进程监督"""
        if self._running.is_set():
            return
        self._running.set()
        self._thread = threading.Thread(target=self._supervise_loop, name="ProcessSupervisor")
        self._thread.daemon = True
        self._thread.start()
        logger.debug("[TRACE] ProcessSupervisor.start: thread started (name=ProcessSupervisor)")
        logger.info(f"进程监督已启动: {' '.join(self.command)}")

    def stop(self):
        """停止进程监督"""
        self._running.clear()
        logger.debug("[TRACE] ProcessSupervisor.stop: _running cleared")
        if self._process:
            try:
                logger.debug("[TRACE] ProcessSupervisor.stop: terminating process (pid=%s)", self._process.pid)
                self._process.terminate()
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.debug("[TRACE] ProcessSupervisor.stop: terminate timed out, killing process")
                    try:
                        self._process.kill()
                    except OSError:
                        pass
            except OSError:
                pass  # Process already exited
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
                logger.debug("[TRACE] _supervise_loop: loop iteration start, restart_count=%d/%d",
                             self._restart_count, self._max_restarts)
                # [H-Bug修复] 空command检查，防止无限错误循环
                if not self.command:
                    logger.error("ProcessSupervisor command为空，退出监督循环")
                    break
                # [H-资源管理] 使用DEVNULL避免管道缓冲区阻塞
                # 同时启动线程消费stdout/stderr以保留日志
                # [H-Bug修复] Popen可能抛出OSError，需要try/except包裹
                try:
                    self._process = subprocess.Popen(
                        self.command,
                        cwd=self.work_dir,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    logger.debug("[TRACE] _supervise_loop: subprocess started, pid=%d", self._process.pid)
                except OSError as e:
                    logger.error(f"启动子进程失败: {e}")
                    time.sleep(5)
                    continue

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
                logger.debug("[TRACE] _supervise_loop: subprocess exited, return_code=%d", return_code)

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
                        logger.debug("[TRACE] _supervise_loop: will restart in 5s, restart_count=%d",
                                     self._restart_count)
                        logger.info(f"将在5秒后重启子进程 (第{self._restart_count}次)")
                        time.sleep(5)
                    else:
                        logger.debug("[TRACE] _supervise_loop: max restarts reached or _running cleared, stopping")
                        logger.error("达到最大重启次数, 停止监督")
                        self._running.clear()
                else:
                    logger.debug("[TRACE] _supervise_loop: subprocess exited normally")
                    logger.info("子进程正常退出")
                    if not self._running.is_set():
                        break

            except Exception as e:
                logger.error(f"进程监督异常: {e}")
                time.sleep(5)

    def get_status(self) -> Dict[str, Any]:
        """获取进程状态"""
        running = self._process is not None and self._process.poll() is None
        pid = self._process.pid if self._process else None
        logger.debug("[TRACE] ProcessSupervisor.get_status: running=%s, pid=%s, restart_count=%d",
                     running, pid, self._restart_count)
        return {
            "running": running,
            "pid": pid,
            "restart_count": self._restart_count,
            "command": " ".join(self.command)
        }
