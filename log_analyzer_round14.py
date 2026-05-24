"""
Round 14 - 日志分析器
自动检测细粒度追踪日志中的异常模式，发现隐藏Bug

检测规则：
1. 锁配对检查：每个 acquire 必须有对应的 release
2. 状态转换完整性：任务不能跳过 RUNNING 状态
3. 重复执行检测：同一 task_id 不应重复进入同一状态
4. 序列中断检测：关键操作序列不应中断
5. 异常传播检测：ERROR/WARNING 级别日志分析
6. 死锁检测：锁持有时间过长
7. 资源泄漏检测：连接创建/关闭不匹配
"""
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional


@dataclass
class LogIssue:
    """日志问题记录"""
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    category: str  # lock_mismatch, state_skip, duplicate, sequence_break, etc.
    description: str
    line_numbers: List[int] = field(default_factory=list)
    context: str = ""


class LogAnalyzer:
    """细粒度日志分析器"""

    def __init__(self, log_file: str):
        self.log_file = log_file
        self.lines: List[str] = []
        self.issues: List[LogIssue] = []

    def load(self) -> bool:
        """加载日志文件"""
        path = Path(self.log_file)
        if not path.exists():
            print(f"  [ERROR] 日志文件不存在: {self.log_file}")
            return False
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            self.lines = f.readlines()
        print(f"  [INFO] 加载了 {len(self.lines)} 行日志")
        return True

    def analyze(self) -> List[LogIssue]:
        """执行全部分析规则"""
        self.issues = []
        print("\n  === 开始日志分析 ===")

        self._check_lock_pairing()
        self._check_state_transitions()
        self._check_duplicate_operations()
        self._check_sequence_integrity()
        self._check_error_patterns()
        self._check_lock_hold_time()
        self._check_resource_leaks()
        self._check_task_lifecycle()

        # 按严重程度排序
        severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        self.issues.sort(key=lambda x: severity_order.get(x.severity, 99))

        return self.issues

    def _find_trace_lines(self, pattern: str) -> List[Tuple[int, str]]:
        """查找包含 [TRACE] 的日志行"""
        results = []
        for i, line in enumerate(self.lines):
            if "[TRACE]" in line and re.search(pattern, line):
                results.append((i + 1, line.strip()))
        return results

    def _check_lock_pairing(self):
        """规则1：锁配对检查 - 每个 acquire 必须有对应的 release（按模块+锁名+方法名配对）"""
        print("\n  [规则1] 锁配对检查...")
        lock_stack = []  # (line_no, lock_name, method_name, module_name)
        unpaired = []

        for i, line in enumerate(self.lines):
            if "[TRACE][LOCK] acquire" in line:
                # 提取锁名和方法名
                match = re.search(r'acquire (\S+) in (\S+)', line)
                if match:
                    lock_name = match.group(1)
                    # 方法名可能带逗号和参数，只取第一个单词
                    method_name = match.group(2).rstrip(',').split('(')[0]
                    # 提取模块名
                    module_match = re.search(r'core\.(\w+):', line)
                    module_name = module_match.group(1) if module_match else "unknown"
                    lock_stack.append((i + 1, lock_name, method_name, module_name))

            elif "[TRACE][LOCK] release" in line:
                match = re.search(r'release (\S+) in (\S+)', line)
                if match:
                    lock_name = match.group(1)
                    method_name = match.group(2).rstrip(',').split('(')[0]
                    module_match = re.search(r'core\.(\w+):', line)
                    module_name = module_match.group(1) if module_match else "unknown"
                    # 查找匹配的acquire（同锁名+同方法名+同模块）
                    found = False
                    for j in range(len(lock_stack) - 1, -1, -1):
                        s_line, s_lock, s_method, s_module = lock_stack[j]
                        if s_lock == lock_name and s_method == method_name and s_module == module_name:
                            lock_stack.pop(j)
                            found = True
                            break
                    if not found:
                        self.issues.append(LogIssue(
                            severity="MEDIUM",
                            category="lock_mismatch",
                            description=f"锁 release 没有 matching acquire: {lock_name} in {method_name} ({module_name})",
                            line_numbers=[i + 1],
                            context=line.strip()[:200]
                        ))

        # 检查未配对的 acquire
        for line_no, lock_name, method_name, module_name in lock_stack:
            unpaired.append((line_no, lock_name, method_name, module_name))

        if unpaired:
            self.issues.append(LogIssue(
                severity="HIGH",
                category="lock_mismatch",
                description=f"发现 {len(unpaired)} 个未配对的锁 acquire（可能死锁或异常导致release未执行）",
                line_numbers=[x[0] for x in unpaired[:10]],
                context="\n".join(f"  L{x[0]}: {x[1]} in {x[2]} ({x[3]})" for x in unpaired[:10])
            ))

        paired = sum(1 for _, _, _, _ in lock_stack) if lock_stack else 0
        total_acquires = paired + len(unpaired)
        print(f"    acquire总数: {total_acquires}, 未配对: {len(unpaired)}")

    def _check_state_transitions(self):
        """规则2：状态转换完整性 - 任务不能跳过 RUNNING 状态"""
        print("\n  [规则2] 状态转换完整性检查...")
        state_pattern = re.compile(r'\[TRACE\]\[STATE\] task=(\S+) (\S+) -> (\S+)')
        task_states = defaultdict(list)  # task_id -> [(line_no, from_state, to_state)]

        for i, line in enumerate(self.lines):
            match = state_pattern.search(line)
            if match:
                task_id = match.group(1)
                from_state = match.group(2)
                to_state = match.group(3)
                task_states[task_id].append((i + 1, from_state, to_state))

        issues_found = 0
        for task_id, transitions in task_states.items():
            # 检查是否有 PENDING -> COMPLETED/FAILED/TIMEOUT（跳过RUNNING）
            for line_no, from_s, to_s in transitions:
                if from_s == "pending" and to_s in ("completed", "failed", "timeout"):
                    self.issues.append(LogIssue(
                        severity="HIGH",
                        category="state_skip",
                        description=f"任务 {task_id} 从 {from_s} 直接跳到 {to_s}，跳过了 running 状态",
                        line_numbers=[line_no],
                        context=f"task={task_id} {from_s} -> {to_s}"
                    ))
                    issues_found += 1

                # 检查终态后再转换
                if from_s in ("completed", "failed", "timeout", "cancelled"):
                    self.issues.append(LogIssue(
                        severity="CRITICAL",
                        category="state_after_terminal",
                        description=f"任务 {task_id} 从终态 {from_s} 再次转换到 {to_s}",
                        line_numbers=[line_no],
                        context=f"task={task_id} {from_s} -> {to_s}"
                    ))
                    issues_found += 1

        print(f"    检查了 {len(task_states)} 个任务的状态转换，发现 {issues_found} 个问题")

    def _check_duplicate_operations(self):
        """规则3：重复执行检测 - 同一 task_id 不应重复进入同一状态"""
        print("\n  [规则3] 重复执行检测...")
        state_pattern = re.compile(r'\[TRACE\]\[STATE\] task=(\S+) (\S+) -> (\S+)')
        task_state_entries = defaultdict(list)  # (task_id, to_state) -> [line_nos]

        for i, line in enumerate(self.lines):
            match = state_pattern.search(line)
            if match:
                task_id = match.group(1)
                to_state = match.group(3)
                key = (task_id, to_state)
                task_state_entries[key].append(i + 1)

        issues_found = 0
        for (task_id, state), line_nos in task_state_entries.items():
            if len(line_nos) > 1 and state in ("pending", "running"):
                # PENDING 和 RUNNING 不应重复进入（重试除外）
                # 重试时 pending -> running -> failed -> pending 是正常的
                # 但连续两次 pending 或 running 可能是bug
                for j in range(1, len(line_nos)):
                    self.issues.append(LogIssue(
                        severity="MEDIUM",
                        category="duplicate_state",
                        description=f"任务 {task_id} 第 {len(line_nos)} 次进入 {state} 状态（可能是重试，也可能是bug）",
                        line_numbers=[line_nos[j]],
                        context=f"task={task_id} -> {state} at lines {line_nos}"
                    ))
                    issues_found += 1

        print(f"    发现 {issues_found} 个潜在重复状态进入")

    def _check_sequence_integrity(self):
        """规则4：序列中断检测 - 关键操作序列不应中断"""
        print("\n  [规则4] 序列完整性检查...")

        # 检查任务提交序列：submit -> enqueue -> persist
        submit_lines = [i for i, l in enumerate(self.lines) if "[TRACE] submit_task" in l and "task_id=" in l]
        enqueue_lines = [i for i, l in enumerate(self.lines) if "[TRACE] task enqueued" in l]
        persist_lines = [i for i, l in enumerate(self.lines) if "[TRACE] task persisted" in l]

        issues_found = 0

        # 检查 save_task 是否有对应的 commit 或 rollback
        save_lines = [i for i, l in enumerate(self.lines) if "[TRACE] save_task" in l and "saving" in l]
        commit_lines = [i for i, l in enumerate(self.lines) if "[TRACE] save_task" in l and "committed" in l]
        rollback_lines = [i for i, l in enumerate(self.lines) if "[TRACE] save_task" in l and "rollback" in l]

        if len(save_lines) > len(commit_lines) + len(rollback_lines):
            missing = len(save_lines) - len(commit_lines) - len(rollback_lines)
            self.issues.append(LogIssue(
                severity="HIGH",
                category="sequence_break",
                description=f"save_task 调用了 {len(save_lines)} 次，但只有 {len(commit_lines)} 次 commit 和 {len(rollback_lines)} 次 rollback，{missing} 次操作结果未知",
                line_numbers=[],
                context=f"saves={len(save_lines)}, commits={len(commit_lines)}, rollbacks={len(rollback_lines)}"
            ))
            issues_found += 1

        # 检查 worker_loop 中 task_done 是否总是被调用
        task_start_lines = [i for i, l in enumerate(self.lines) if "[TRACE] _worker_loop" in l and "dequeued task" in l]
        task_done_lines = [i for i, l in enumerate(self.lines) if "[TRACE] _worker_loop" in l and "task_done called" in l]

        if len(task_start_lines) > len(task_done_lines):
            missing = len(task_start_lines) - len(task_done_lines)
            self.issues.append(LogIssue(
                severity="CRITICAL",
                category="sequence_break",
                description=f"worker 出队了 {len(task_start_lines)} 个任务，但只调用了 {len(task_done_lines)} 次 task_done，{missing} 次可能导致队列阻塞",
                line_numbers=[],
                context=f"dequeued={len(task_start_lines)}, task_done={len(task_done_lines)}"
            ))
            issues_found += 1

        print(f"    发现 {issues_found} 个序列中断问题")

    def _check_error_patterns(self):
        """规则5：异常传播检测 - ERROR/WARNING 级别日志分析"""
        print("\n  [规则5] 异常模式检查...")

        error_lines = [(i + 1, l.strip()) for i, l in enumerate(self.lines)
                       if "ERROR" in l and "[TRACE]" not in l]
        warning_lines = [(i + 1, l.strip()) for i, l in enumerate(self.lines)
                         if "WARNING" in l and "[TRACE]" not in l]

        # 统计错误类型
        error_categories = defaultdict(int)
        for line_no, line in error_lines:
            # 提取错误类别
            if "database is locked" in line.lower():
                error_categories["database_locked"] += 1
            elif "timeout" in line.lower():
                error_categories["timeout"] += 1
            elif "connection" in line.lower():
                error_categories["connection_error"] += 1
            elif "memory" in line.lower():
                error_categories["memory_error"] += 1
            else:
                error_categories["other_error"] += 1

        issues_found = 0
        for cat, count in error_categories.items():
            if cat == "database_locked" and count > 0:
                self.issues.append(LogIssue(
                    severity="HIGH",
                    category="database_locked",
                    description=f"检测到 {count} 次 'database is locked' 错误，可能存在并发问题",
                    line_numbers=[l[0] for l in error_lines[:5]],
                    context="\n".join(f"  L{l[0]}: {l[1][:150]}" for l in error_lines[:5])
                ))
                issues_found += 1
            elif cat == "timeout" and count > 5:
                self.issues.append(LogIssue(
                    severity="MEDIUM",
                    category="excessive_timeouts",
                    description=f"检测到 {count} 次超时错误",
                    line_numbers=[l[0] for l in error_lines[:5]],
                    context=""
                ))
                issues_found += 1

        # 检查 [TRACE][ERROR] 日志（我们新添加的异常捕获日志）
        trace_errors = [(i + 1, l.strip()) for i, l in enumerate(self.lines)
                        if "[TRACE][ERROR]" in l]
        if trace_errors:
            self.issues.append(LogIssue(
                severity="HIGH",
                category="trace_error",
                description=f"检测到 {len(trace_errors)} 个 [TRACE][ERROR] 日志，这些是方法内部捕获的异常",
                line_numbers=[l[0] for l in trace_errors[:10]],
                context="\n".join(f"  L{l[0]}: {l[1][:200]}" for l in trace_errors[:10])
            ))
            issues_found += 1

        print(f"    ERROR日志: {len(error_lines)}, WARNING日志: {len(warning_lines)}, TRACE_ERROR: {len(trace_errors)}")
        print(f"    错误分类: {dict(error_categories)}")
        print(f"    发现 {issues_found} 个异常模式问题")

    def _check_lock_hold_time(self):
        """规则6：死锁检测 - 锁持有时间过长"""
        print("\n  [规则6] 锁持有时间检查...")

        # 解析日志时间戳并计算锁持有时间
        lock_acquires = {}  # lock_name -> (line_no, timestamp)
        long_holds = []

        for i, line in enumerate(self.lines):
            timestamp = self._extract_timestamp(line)
            if timestamp is None:
                continue

            if "[TRACE][LOCK] acquire" in line:
                match = re.search(r'acquire (\S+)', line)
                if match:
                    lock_name = match.group(1)
                    lock_acquires[lock_name] = (i + 1, timestamp)

            elif "[TRACE][LOCK] release" in line:
                match = re.search(r'release (\S+)', line)
                if match:
                    lock_name = match.group(1)
                    if lock_name in lock_acquires:
                        acquire_line, acquire_time = lock_acquires.pop(lock_name)
                        hold_time = (timestamp - acquire_time).total_seconds()
                        if hold_time > 5.0:  # 超过5秒
                            long_holds.append((lock_name, hold_time, acquire_line, i + 1))

        issues_found = 0
        if long_holds:
            # 按持有时间排序
            long_holds.sort(key=lambda x: x[1], reverse=True)
            self.issues.append(LogIssue(
                severity="HIGH",
                category="long_lock_hold",
                description=f"发现 {len(long_holds)} 次锁持有时间超过5秒（可能存在性能问题或死锁）",
                line_numbers=[x[2] for x in long_holds[:5]],
                context="\n".join(f"  {x[0]}: 持有 {x[1]:.2f}s (L{x[2]}-L{x[3]})" for x in long_holds[:10])
            ))
            issues_found += 1

        print(f"    发现 {len(long_holds)} 次长时间锁持有")

    def _check_resource_leaks(self):
        """规则7：资源泄漏检测 - 连接创建/关闭不匹配"""
        print("\n  [规则7] 资源泄漏检查...")

        # 检查数据库连接创建/关闭
        conn_create = [i for i, l in enumerate(self.lines) if "[TRACE] new connection created" in l]
        conn_close = [i for i, l in enumerate(self.lines) if "[TRACE] connection closed" in l]

        issues_found = 0
        if len(conn_create) > len(conn_close) * 2:  # 允许一些差异（线程本地连接复用）
            leak = len(conn_create) - len(conn_close)
            self.issues.append(LogIssue(
                severity="MEDIUM",
                category="resource_leak",
                description=f"数据库连接创建 {len(conn_create)} 次，关闭 {len(conn_close)} 次，可能泄漏 {leak} 个连接",
                line_numbers=[],
                context=f"created={len(conn_create)}, closed={len(conn_close)}"
            ))
            issues_found += 1

        print(f"    连接创建: {len(conn_create)}, 关闭: {len(conn_close)}")

    def _check_task_lifecycle(self):
        """规则8：任务生命周期完整性检查"""
        print("\n  [规则8] 任务生命周期检查...")

        # 收集所有任务ID
        submitted = set()
        completed = set()
        failed = set()
        timed_out = set()

        for line in self.lines:
            if "[TRACE] submit_task" in line and "task_id=" in line:
                match = re.search(r'task_id=(\S+)', line)
                if match:
                    submitted.add(match.group(1))
            if "[TRACE][STATE]" in line:
                match = re.search(r'task=(\S+) .* -> (completed|failed|timeout)', line)
                if match:
                    task_id = match.group(1)
                    final_state = match.group(2)
                    if final_state == "completed":
                        completed.add(task_id)
                    elif final_state == "failed":
                        failed.add(task_id)
                    elif final_state == "timeout":
                        timed_out.add(task_id)

        # 检查已提交但没有任何终态的任务
        finished = completed | failed | timed_out
        unfinished = submitted - finished

        issues_found = 0
        if unfinished and len(unfinished) < 50:  # 只报告少量未完成任务（测试结束时可能有些在队列中）
            self.issues.append(LogIssue(
                severity="LOW",
                category="unfinished_task",
                description=f"有 {len(unfinished)} 个已提交任务未达到终态（可能仍在队列中或测试提前结束）",
                line_numbers=[],
                context=f"submitted={len(submitted)}, completed={len(completed)}, failed={len(failed)}, timeout={len(timed_out)}, unfinished={len(unfinished)}"
            ))
            issues_found += 1

        print(f"    提交: {len(submitted)}, 完成: {len(completed)}, 失败: {len(failed)}, 超时: {len(timed_out)}, 未完成: {len(unfinished)}")

    @staticmethod
    def _extract_timestamp(line: str):
        """从日志行提取时间戳"""
        # 格式: 2024-01-01 12:00:00,123
        match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', line)
        if match:
            try:
                return time.strptime(match.group(1), "%Y-%m-%d %H:%M:%S,%f")
            except ValueError:
                pass
        return None

    def print_report(self):
        """打印分析报告"""
        print("\n" + "=" * 70)
        print("  日志分析报告")
        print("=" * 70)

        if not self.issues:
            print("\n  ✅ 未发现异常模式，系统运行正常！")
            return

        # 按严重程度分组
        by_severity = defaultdict(list)
        for issue in self.issues:
            by_severity[issue.severity].append(issue)

        severity_labels = {
            "CRITICAL": "🔴 严重",
            "HIGH": "🟠 高",
            "MEDIUM": "🟡 中",
            "LOW": "🔵 低"
        }

        total = len(self.issues)
        print(f"\n  共发现 {total} 个潜在问题:\n")

        for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            if sev in by_severity:
                issues = by_severity[sev]
                print(f"  {severity_labels.get(sev, sev)} ({len(issues)}个):")
                for idx, issue in enumerate(issues, 1):
                    print(f"    {idx}. [{issue.category}] {issue.description}")
                    if issue.context:
                        ctx_lines = issue.context.split('\n')
                        for ctx_line in ctx_lines[:3]:
                            print(f"       {ctx_line}")
                print()

        print("=" * 70)


def main():
    if len(sys.argv) < 2:
        print("用法: python log_analyzer_round14.py <日志文件路径>")
        sys.exit(1)

    log_file = sys.argv[1]
    analyzer = LogAnalyzer(log_file)

    if not analyzer.load():
        sys.exit(1)

    issues = analyzer.analyze()
    analyzer.print_report()

    # 返回非零退出码如果有严重问题
    critical_count = sum(1 for i in issues if i.severity in ("CRITICAL", "HIGH"))
    sys.exit(1 if critical_count > 0 else 0)


if __name__ == "__main__":
    main()
