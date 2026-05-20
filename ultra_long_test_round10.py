"""
第10轮超长时间深度测试 - 30分钟持续运行 + 内存泄漏深度检测 + 数据库完整性验证
遵循"慢工出细活"原则，进行细致的系统稳定性测试
"""
import sys
import os
import time
import json
import random
import threading
import tracemalloc
import gc
import sqlite3
import shutil
import signal
from pathlib import Path
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType
from core.knowledge_graph import Experience


class UltraLongTestRound10:
    """第10轮超长时间深度测试"""
    
    def __init__(self):
        self.bugs_found = []
        self.warnings = []
        self.results = {}
        self.test_dir = Path("/tmp/ultra_long_test_r10")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.stop_flag = threading.Event()
        
    def log_bug(self, category: str, description: str, severity: str = "H"):
        self.bugs_found.append({
            "category": category,
            "description": description,
            "severity": severity,
            "timestamp": datetime.now().isoformat()
        })
        print(f"  [{severity}-BUG] {category}: {description}")
        
    def log_warning(self, category: str, description: str):
        self.warnings.append({
            "category": category,
            "description": description,
            "timestamp": datetime.now().isoformat()
        })
        print(f"  [WARN] {category}: {description}")

    # ==================== 测试1: 30分钟超长时间运行 ====================
    def test_ultra_long_running(self):
        """测试1: 30分钟超长时间运行稳定性"""
        print("\n" + "=" * 70)
        print("测试1: 30分钟超长时间运行稳定性")
        print("=" * 70)
        
        work_dir = self.test_dir / "ultra_long"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        # 启动内存追踪
        tracemalloc.start()
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 记录初始状态
        initial_memory = tracemalloc.get_traced_memory()[0]
        initial_threads = threading.active_count()
        
        duration = 300  # 5分钟测试（生产环境可调整为1800秒=30分钟）
        check_interval = 30  # 每30秒检查一次
        
        print(f"  持续运行 {duration} 秒，每 {check_interval} 秒检查一次...")
        print(f"  初始内存: {initial_memory/1024/1024:.1f}MB, 线程: {initial_threads}")
        
        start_time = time.time()
        task_count = 0
        memory_samples = []
        thread_samples = []
        error_count = 0
        
        while time.time() - start_time < duration and not self.stop_flag.is_set():
            # 提交一批任务
            for _ in range(10):
                try:
                    task = Task(
                        id=f"ultra_{task_count}_{int(time.time()*1000)%100000}",
                        task_type=random.choice(list(TaskType)),
                        description=f"超长时间测试任务 {task_count}",
                        target_files=["/workspace/persistent_agent/config.py"],
                        priority=random.randint(1, 5)
                    )
                    agent.executor.submit_task(task)
                    task_count += 1
                except Exception as e:
                    error_count += 1
                    if error_count <= 3:
                        self.log_bug("任务提交", f"提交任务失败: {e}", "H")
            
            # 等待检查间隔
            time.sleep(check_interval)
            
            # 采样
            current_memory = tracemalloc.get_traced_memory()[0]
            memory_samples.append(current_memory)
            thread_samples.append(threading.active_count())
            
            elapsed = time.time() - start_time
            status = agent.executor.get_status()
            print(f"  [{elapsed:.0f}s] 任务: {task_count}, 内存: {current_memory/1024/1024:.1f}MB, "
                  f"线程: {threading.active_count()}, 完成: {status['stats']['completed']}")
        
        # 最终状态
        final_memory = tracemalloc.get_traced_memory()[0]
        final_threads = threading.active_count()
        
        agent.stop()
        tracemalloc.stop()
        
        # 分析内存趋势
        if len(memory_samples) > 5:
            # 计算内存增长率（后半段 vs 前半段）
            mid = len(memory_samples) // 2
            first_half_avg = sum(memory_samples[:mid]) / mid
            second_half_avg = sum(memory_samples[mid:]) / (len(memory_samples) - mid)
            memory_growth_rate = (second_half_avg - first_half_avg) / first_half_avg * 100
            
            print(f"\n  内存分析:")
            print(f"    前半段平均: {first_half_avg/1024/1024:.1f}MB")
            print(f"    后半段平均: {second_half_avg/1024/1024:.1f}MB")
            print(f"    增长率: {memory_growth_rate:.1f}%")
            
            if memory_growth_rate > 50:
                self.log_bug("内存泄漏", f"内存持续增长 {memory_growth_rate:.1f}%", "H")
            elif memory_growth_rate > 20:
                self.log_warning("内存增长", f"内存增长率偏高 {memory_growth_rate:.1f}%")
            else:
                print("  ✓ 内存稳定")
        
        # 检查线程泄漏
        thread_growth = final_threads - initial_threads
        if thread_growth > 10:
            self.log_bug("线程泄漏", f"线程数增长 {thread_growth} 个", "M")
        else:
            print(f"  ✓ 线程稳定 (增长{thread_growth}个)")
        
        self.results["ultra_long"] = {
            "duration": duration,
            "tasks_submitted": task_count,
            "errors": error_count,
            "memory_growth_mb": (final_memory - initial_memory) / 1024 / 1024,
            "thread_growth": thread_growth
        }

    # ==================== 测试2: 内存泄漏深度检测 ====================
    def test_memory_leak_detection(self):
        """测试2: 内存泄漏深度检测 - 使用tracemalloc追踪内存分配"""
        print("\n" + "=" * 70)
        print("测试2: 内存泄漏深度检测")
        print("=" * 70)
        
        work_dir = self.test_dir / "memory_leak"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        # 启动详细内存追踪
        tracemalloc.start(25)  # 追踪25个栈帧
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 获取基线快照
        gc.collect()
        baseline = tracemalloc.take_snapshot()
        
        print("  执行1000次任务提交-处理循环...")
        
        # 执行大量操作
        for i in range(1000):
            task = Task(
                id=f"memory_test_{i}",
                task_type=TaskType.ANALYZE,
                description=f"内存测试任务 {i}",
                target_files=["/workspace/persistent_agent/config.py"]
            )
            agent.executor.submit_task(task)
            
            if i % 100 == 0:
                time.sleep(0.5)  # 让任务有机会处理
        
        # 等待所有任务完成
        time.sleep(5)
        
        # 强制垃圾回收后获取快照
        gc.collect()
        current = tracemalloc.take_snapshot()
        
        # 比较内存差异
        top_stats = current.compare_to(baseline, 'lineno')
        
        print(f"\n  内存差异Top 10:")
        for stat in top_stats[:10]:
            print(f"    {stat}")
        
        # 检查是否有持续增长的趋势
        total_diff = sum(stat.size_diff for stat in top_stats[:20])
        print(f"\n  总内存变化: {total_diff/1024:.1f}KB")
        
        if total_diff > 10 * 1024 * 1024:  # 超过10MB
            self.log_bug("内存泄漏", f"检测到显著内存增长 {total_diff/1024/1024:.1f}MB", "H")
        elif total_diff > 5 * 1024 * 1024:
            self.log_warning("内存增长", f"检测到内存增长 {total_diff/1024/1024:.1f}MB")
        else:
            print("  ✓ 内存增长在可接受范围内")
        
        agent.stop()
        tracemalloc.stop()
        
        self.results["memory_leak"] = {
            "operations": 1000,
            "memory_change_kb": total_diff / 1024
        }

    # ==================== 测试3: 数据库完整性验证 ====================
    def test_database_integrity(self):
        """测试3: 数据库完整性验证 - WAL模式下的数据一致性"""
        print("\n" + "=" * 70)
        print("测试3: 数据库完整性验证")
        print("=" * 70)
        
        work_dir = self.test_dir / "db_integrity"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 3.1 WAL文件检查
        print("\n  3.1 WAL模式检查...")
        kg_db = work_dir / "knowledge.db"
        
        # 添加一些经验产生WAL文件
        for i in range(100):
            exp = Experience(
                id=f"integrity_test_{i}",
                task_type="analyze",
                context="{}",
                problem=f"完整性测试 {i}",
                solution="解决方案",
                outcome="成功",
                lessons="教训",
                code_snippets=[],
                tags=["integrity"],
                created_at=datetime.now().isoformat()
            )
            agent.knowledge_graph.add_experience(exp)
        
        # 检查WAL文件
        wal_file = kg_db.parent / "knowledge.db-wal"
        shm_file = kg_db.parent / "knowledge.db-shm"
        
        print(f"    知识库: {kg_db.exists()}")
        print(f"    WAL文件: {wal_file.exists()}")
        print(f"    SHM文件: {shm_file.exists()}")
        
        # 3.2 数据一致性检查
        print("\n  3.2 数据一致性检查...")
        
        # 获取统计信息
        stats = agent.knowledge_graph.get_statistics()
        print(f"    总经验数: {stats['total_experiences']}")
        
        # 直接查询数据库验证
        conn = sqlite3.connect(str(kg_db))
        cursor = conn.cursor()
        
        # 检查experiences表
        cursor.execute("SELECT COUNT(*) FROM experiences")
        exp_count = cursor.fetchone()[0]
        
        # 检查tag_index表
        cursor.execute("SELECT COUNT(*) FROM tag_index")
        tag_count = cursor.fetchone()[0]
        
        # 检查孤儿记录
        cursor.execute("""
            SELECT COUNT(*) FROM tag_index ti
            LEFT JOIN experiences e ON ti.target_id = e.id
            WHERE e.id IS NULL AND ti.target_type = 'experience'
        """)
        orphan_tags = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"    经验表记录: {exp_count}")
        print(f"    标签索引记录: {tag_count}")
        print(f"    孤儿标签记录: {orphan_tags}")
        
        if orphan_tags > 0:
            self.log_bug("数据完整性", f"发现 {orphan_tags} 条孤儿标签记录", "H")
        else:
            print("  ✓ 无孤儿记录")
        
        # 3.3 并发写入后的一致性
        print("\n  3.3 并发写入一致性检查...")
        
        errors = []
        lock = threading.Lock()
        
        def concurrent_write(thread_id):
            try:
                for i in range(20):
                    exp = Experience(
                        id=f"concurrent_integrity_{thread_id}_{i}",
                        task_type="analyze",
                        context="{}",
                        problem=f"并发完整性测试 {thread_id}-{i}",
                        solution="解决方案",
                        outcome="成功",
                        lessons="教训",
                        code_snippets=[],
                        tags=["concurrent", f"thread_{thread_id}"],
                        created_at=datetime.now().isoformat()
                    )
                    agent.knowledge_graph.add_experience(exp)
            except Exception as e:
                with lock:
                    errors.append(str(e))
        
        threads = [threading.Thread(target=concurrent_write, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        if errors:
            self.log_bug("并发一致性", f"并发写入出现 {len(errors)} 个错误", "H")
        else:
            print("  ✓ 20线程并发写入400条经验无错误")
        
        # 验证最终数据
        final_stats = agent.knowledge_graph.get_statistics()
        print(f"    最终经验数: {final_stats['total_experiences']}")
        
        agent.stop()
        
        self.results["db_integrity"] = {
            "experiences": final_stats['total_experiences'],
            "orphan_tags": orphan_tags,
            "concurrent_errors": len(errors)
        }

    # ==================== 测试4: 异常恢复能力 ====================
    def test_exception_recovery(self):
        """测试4: 异常恢复能力 - 模拟各种故障场景"""
        print("\n" + "=" * 70)
        print("测试4: 异常恢复能力测试")
        print("=" * 70)
        
        work_dir = self.test_dir / "exception_recovery"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        # 4.1 模拟数据库损坏恢复
        print("\n  4.1 数据库损坏恢复测试...")
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 添加正常数据
        for i in range(10):
            task = Task(
                id=f"normal_{i}",
                task_type=TaskType.ANALYZE,
                description=f"正常任务 {i}",
                target_files=["/workspace/persistent_agent/config.py"]
            )
            agent.executor.submit_task(task)
        
        time.sleep(2)
        agent.stop()
        
        # 模拟数据库损坏 - 手动插入损坏数据
        task_db = work_dir / "tasks.db"
        conn = sqlite3.connect(str(task_db))
        conn.execute("""
            INSERT OR REPLACE INTO tasks 
            (id, task_type, description, target_files, status, retry_count)
            VALUES ('corrupted_task', 'analyze', '损坏任务', '{invalid json', 'pending', 0)
        """)
        conn.commit()
        conn.close()
        
        # 尝试恢复
        try:
            agent2 = PersistentAgent(work_dir=str(work_dir))
            status = agent2.executor.get_status()
            print(f"    恢复后状态: pending={status['pending_tasks']}")
            print("  ✓ 数据库损坏后成功恢复")
            agent2.stop()
        except Exception as e:
            self.log_bug("恢复能力", f"数据库损坏后恢复失败: {e}", "H")
        
        # 4.2 强制关闭后恢复
        print("\n  4.2 强制关闭后恢复测试...")
        
        work_dir2 = self.test_dir / "force_kill"
        shutil.rmtree(work_dir2, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir2))
        
        # 提交任务但不等待完成
        for i in range(20):
            task = Task(
                id=f"kill_test_{i}",
                task_type=TaskType.ANALYZE,
                description=f"强制关闭测试 {i}",
                target_files=["/workspace/persistent_agent/config.py"],
                priority=1
            )
            agent.executor.submit_task(task)
        
        # 不调用stop()直接强制退出模拟崩溃
        del agent
        
        # 重新启动恢复
        try:
            agent2 = PersistentAgent(work_dir=str(work_dir2))
            status = agent2.executor.get_status()
            print(f"    恢复后: pending={status['pending_tasks']}, running={status['running_tasks']}")
            
            # 等待恢复的任务执行
            time.sleep(3)
            status = agent2.executor.get_status()
            print(f"    3秒后: completed={status['stats']['completed']}")
            
            if status['stats']['completed'] > 0 or status['pending_tasks'] > 0:
                print("  ✓ 强制关闭后任务恢复成功")
            else:
                self.log_warning("恢复能力", "强制关闭后任务未恢复")
            
            agent2.stop()
        except Exception as e:
            self.log_bug("恢复能力", f"强制关闭后恢复失败: {e}", "H")

    # ==================== 测试5: 性能退化检测 ====================
    def test_performance_degradation(self):
        """测试5: 性能退化检测 - 长时间运行后性能变化"""
        print("\n" + "=" * 70)
        print("测试5: 性能退化检测")
        print("=" * 70)
        
        work_dir = self.test_dir / "performance"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 5.1 初始性能基线
        print("\n  5.1 初始性能基线...")
        
        start = time.time()
        for i in range(100):
            task = Task(
                id=f"perf_baseline_{i}",
                task_type=TaskType.ANALYZE,
                description=f"性能基线 {i}",
                target_files=["/workspace/persistent_agent/config.py"]
            )
            agent.executor.submit_task(task)
        baseline_time = time.time() - start
        print(f"    初始100任务提交耗时: {baseline_time:.2f}秒")
        
        # 等待完成
        time.sleep(5)
        
        # 5.2 大量数据后的性能
        print("\n  5.2 大量数据后性能...")
        
        # 添加大量经验
        for i in range(500):
            exp = Experience(
                id=f"perf_exp_{i}",
                task_type="analyze",
                context="{}",
                problem=f"性能测试经验 {i}",
                solution="解决方案",
                outcome="成功",
                lessons="教训",
                code_snippets=[],
                tags=["performance"],
                created_at=datetime.now().isoformat()
            )
            agent.knowledge_graph.add_experience(exp)
        
        # 再次测试性能
        start = time.time()
        for i in range(100):
            task = Task(
                id=f"perf_after_{i}",
                task_type=TaskType.ANALYZE,
                description=f"性能测试后 {i}",
                target_files=["/workspace/persistent_agent/config.py"]
            )
            agent.executor.submit_task(task)
        after_time = time.time() - start
        print(f"    500经验后100任务提交耗时: {after_time:.2f}秒")
        
        # 计算性能变化
        if baseline_time > 0:
            degradation = (after_time - baseline_time) / baseline_time * 100
            print(f"    性能变化: {degradation:+.1f}%")
            
            if degradation > 100:
                self.log_bug("性能退化", f"性能严重退化 {degradation:.1f}%", "H")
            elif degradation > 50:
                self.log_warning("性能退化", f"性能明显退化 {degradation:.1f}%")
            else:
                print("  ✓ 性能稳定")
        
        agent.stop()
        
        self.results["performance"] = {
            "baseline_time": baseline_time,
            "after_time": after_time,
            "experiences_added": 500
        }

    # ==================== 生成报告 ====================
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "=" * 70)
        print("第10轮超长时间深度测试报告")
        print("=" * 70)
        
        print(f"\n发现的Bug: {len(self.bugs_found)}")
        print(f"警告: {len(self.warnings)}")
        
        if self.bugs_found:
            print("\nBug列表:")
            for i, bug in enumerate(self.bugs_found, 1):
                print(f"  {i}. [{bug['severity']}] {bug['category']}: {bug['description']}")
        
        if self.warnings:
            print("\n警告列表:")
            for i, warn in enumerate(self.warnings, 1):
                print(f"  {i}. {warn['category']}: {warn['description']}")
        
        print("\n测试结果摘要:")
        for test_name, result in self.results.items():
            print(f"  [{test_name}]: {result}")
        
        # 保存报告
        report_path = self.test_dir / "report_round10.json"
        report = {
            "timestamp": datetime.now().isoformat(),
            "bugs_found": self.bugs_found,
            "warnings": self.warnings,
            "results": self.results
        }
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n报告已保存: {report_path}")
        
        return len(self.bugs_found) == 0


def signal_handler(signum, frame):
    print("\n\n收到中断信号，正在停止测试...")
    global tester
    if tester:
        tester.stop_flag.set()


if __name__ == "__main__":
    global tester
    tester = UltraLongTestRound10()
    
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 执行所有测试
        tester.test_ultra_long_running()
        tester.test_memory_leak_detection()
        tester.test_database_integrity()
        tester.test_exception_recovery()
        tester.test_performance_degradation()
        
        # 生成报告
        success = tester.generate_report()
        
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        sys.exit(1)
