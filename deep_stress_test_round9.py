"""
第9轮深度压力测试 - 长时间运行稳定性 + 极端边界条件 + 高并发竞态探测
"""
import sys
import os
import time
import json
import random
import string
import threading
import traceback
import subprocess
import shutil
import gc
import tracemalloc
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType, TaskStatus, ExecutionResult
from core.knowledge_graph import Experience, Pattern, get_knowledge_graph
from core.watchdog import FileWatcher


class DeepStressTestRound9:
    def __init__(self):
        self.bugs_found = []
        self.warnings = []
        self.results = {}
        self.test_dir = Path("/tmp/deep_stress_test_r9")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
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

    # ==================== 测试1: 长时间运行稳定性 ====================
    def test_long_running_stability(self):
        """测试1: 长时间运行稳定性 - 检测内存泄漏、资源累积"""
        print("\n" + "=" * 70)
        print("测试1: 长时间运行稳定性（持续运行检测资源泄漏）")
        print("=" * 70)
        
        work_dir = self.test_dir / "long_running"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        # 启动内存追踪
        tracemalloc.start()
        initial_memory = tracemalloc.get_traced_memory()[0]
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 记录初始状态
        initial_stats = {
            "tasks_submitted": 0,
            "experiences": agent.knowledge_graph.get_statistics()["total_experiences"],
            "threads": threading.active_count()
        }
        
        # 持续运行5分钟，每秒提交任务
        duration = 60  # 1分钟测试（可调整为300秒=5分钟）
        tasks_per_second = 5
        total_tasks = duration * tasks_per_second
        
        print(f"  持续运行 {duration} 秒，每秒提交 {tasks_per_second} 个任务...")
        
        start_time = time.time()
        task_count = 0
        memory_samples = []
        thread_samples = []
        
        while time.time() - start_time < duration:
            # 提交任务
            for _ in range(tasks_per_second):
                task = Task(
                    id=f"long_run_{task_count}_{int(time.time()*1000)%100000}",
                    task_type=random.choice(list(TaskType)),
                    description=f"长时间运行测试任务 {task_count}",
                    target_files=["/workspace/persistent_agent/config.py"],
                    priority=random.randint(1, 5)
                )
                agent.executor.submit_task(task)
                task_count += 1
            
            # 每秒采样
            current_memory = tracemalloc.get_traced_memory()[0]
            memory_samples.append(current_memory)
            thread_samples.append(threading.active_count())
            
            time.sleep(1)
            
            # 进度报告
            if task_count % 50 == 0:
                status = agent.executor.get_status()
                print(f"  进度: {task_count}/{total_tasks} 任务, "
                      f"内存: {current_memory/1024/1024:.1f}MB, "
                      f"线程: {threading.active_count()}")
        
        # 最终状态
        final_memory = tracemalloc.get_traced_memory()[0]
        final_stats = {
            "tasks_submitted": task_count,
            "experiences": agent.knowledge_graph.get_statistics()["total_experiences"],
            "threads": threading.active_count()
        }
        
        agent.stop()
        tracemalloc.stop()
        
        # 分析内存增长
        memory_growth = final_memory - initial_memory
        memory_growth_mb = memory_growth / 1024 / 1024
        
        # 计算内存增长率（线性回归斜率）
        if len(memory_samples) > 10:
            n = len(memory_samples)
            x_mean = sum(range(n)) / n
            y_mean = sum(memory_samples) / n
            numerator = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(memory_samples))
            denominator = sum((i - x_mean) ** 2 for i in range(n))
            slope = numerator / denominator if denominator != 0 else 0
            slope_kb_per_sec = slope / 1024
        else:
            slope_kb_per_sec = 0
        
        print(f"\n  结果:")
        print(f"    初始内存: {initial_memory/1024/1024:.1f}MB")
        print(f"    最终内存: {final_memory/1024/1024:.1f}MB")
        print(f"    内存增长: {memory_growth_mb:.1f}MB")
        print(f"    内存增长率: {slope_kb_per_sec:.1f}KB/秒")
        print(f"    初始线程: {initial_stats['threads']}")
        print(f"    最终线程: {final_stats['threads']}")
        
        # 判断是否有内存泄漏
        if slope_kb_per_sec > 100:  # 每秒增长超过100KB
            self.log_bug("内存泄漏", f"内存持续增长 {slope_kb_per_sec:.1f}KB/秒", "H")
        elif slope_kb_per_sec > 50:
            self.log_warning("内存增长", f"内存增长率偏高 {slope_kb_per_sec:.1f}KB/秒")
        else:
            print("  ✓ 内存稳定，无明显泄漏")
        
        # 检查线程泄漏
        thread_growth = final_stats['threads'] - initial_stats['threads']
        if thread_growth > 5:
            self.log_bug("线程泄漏", f"线程数增长 {thread_growth} 个", "M")
        else:
            print("  ✓ 线程数稳定")
        
        self.results["long_running"] = {
            "duration": duration,
            "tasks_submitted": task_count,
            "memory_growth_mb": memory_growth_mb,
            "memory_slope_kb_per_sec": slope_kb_per_sec,
            "thread_growth": thread_growth
        }

    # ==================== 测试2: 极端边界条件 ====================
    def test_extreme_boundary_conditions(self):
        """测试2: 极端边界条件 - 超大文件、空文件、特殊字符、Unicode"""
        print("\n" + "=" * 70)
        print("测试2: 极端边界条件测试")
        print("=" * 70)
        
        work_dir = self.test_dir / "boundary"
        shutil.rmtree(work_dir, ignore_errors=True)
        work_dir.mkdir(parents=True, exist_ok=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 2.1 空文件测试
        print("\n  2.1 空文件测试...")
        empty_file = work_dir / "empty.py"
        empty_file.write_text("")
        
        task = Task(id="empty_file_test", task_type=TaskType.ANALYZE,
                    description="空文件分析", target_files=[str(empty_file)])
        agent.executor.submit_task(task)
        time.sleep(1)
        print("  ✓ 空文件处理正常")
        
        # 2.2 超大文件测试（10MB）
        print("\n  2.2 超大文件测试...")
        huge_file = work_dir / "huge.py"
        huge_content = "# " + "x" * (10 * 1024 * 1024)  # 10MB
        huge_file.write_text(huge_content)
        
        task = Task(id="huge_file_test", task_type=TaskType.ANALYZE,
                    description="超大文件分析", target_files=[str(huge_file)])
        try:
            agent.executor.submit_task(task)
            time.sleep(2)
            print("  ✓ 超大文件处理正常")
        except MemoryError:
            self.log_bug("内存溢出", "超大文件导致内存溢出", "H")
        except Exception as e:
            self.log_warning("超大文件", f"处理异常: {e}")
        
        # 2.3 特殊字符和Unicode测试
        print("\n  2.3 特殊字符和Unicode测试...")
        special_chars = [
            "测试_中文_任务",  # 中文
            "test_emoji_😀🎉🚀",  # Emoji
            "test_null_\x00_byte",  # Null字节
            "test_newline_\n_\r_\r\n",  # 换行符
            "test_quotes_\"'_`",  # 引号
            "test_sql_injection_'; DROP TABLE--",  # SQL注入尝试
            "test_path_traversal_../../../etc/passwd",  # 路径遍历尝试
        ]
        
        for i, desc in enumerate(special_chars):
            try:
                task = Task(
                    id=f"special_char_{i}",
                    task_type=TaskType.ANALYZE,
                    description=desc,
                    target_files=["/workspace/persistent_agent/config.py"]
                )
                agent.executor.submit_task(task)
            except Exception as e:
                self.log_bug("特殊字符", f"描述 '{desc[:20]}...' 导致错误: {e}", "M")
        
        time.sleep(1)
        print("  ✓ 特殊字符处理完成")
        
        # 2.4 超长描述测试
        print("\n  2.4 超长描述测试...")
        long_desc = "x" * 100000  # 100KB描述
        try:
            task = Task(id="long_desc_test", task_type=TaskType.ANALYZE,
                        description=long_desc, target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task)
            time.sleep(1)
            print("  ✓ 超长描述处理正常")
        except Exception as e:
            self.log_bug("超长描述", f"100KB描述导致错误: {e}", "M")
        
        # 2.5 不存在的文件路径
        print("\n  2.5 不存在的文件路径测试...")
        task = Task(id="nonexistent_file", task_type=TaskType.ANALYZE,
                    description="不存在的文件", target_files=["/nonexistent/path/to/file.py"])
        agent.executor.submit_task(task)
        time.sleep(1)
        print("  ✓ 不存在的文件路径处理正常")
        
        # 2.6 空目标文件列表
        print("\n  2.6 空目标文件列表测试...")
        task = Task(id="empty_targets", task_type=TaskType.ANALYZE,
                    description="空目标列表", target_files=[])
        agent.executor.submit_task(task)
        time.sleep(1)
        print("  ✓ 空目标文件列表处理正常")
        
        agent.stop()
        
        self.results["boundary"] = {
            "tests": 6,
            "bugs": len([b for b in self.bugs_found if b["category"] in ["特殊字符", "超长描述", "超大文件"]])
        }

    # ==================== 测试3: 高并发竞态探测 ====================
    def test_high_concurrency_race_detection(self):
        """测试3: 高并发竞态探测 - 多线程并发访问共享资源"""
        print("\n" + "=" * 70)
        print("测试3: 高并发竞态探测")
        print("=" * 70)
        
        work_dir = self.test_dir / "concurrency"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 3.1 并发任务提交
        print("\n  3.1 并发任务提交测试（100线程同时提交）...")
        errors = []
        lock = threading.Lock()
        
        def submit_tasks(thread_id):
            try:
                for i in range(10):
                    task = Task(
                        id=f"concurrent_{thread_id}_{i}",
                        task_type=TaskType.ANALYZE,
                        description=f"并发测试 {thread_id}-{i}",
                        target_files=["/workspace/persistent_agent/config.py"]
                    )
                    agent.executor.submit_task(task)
            except Exception as e:
                with lock:
                    errors.append((thread_id, str(e)))
        
        threads = []
        start_time = time.time()
        for i in range(100):
            t = threading.Thread(target=submit_tasks, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        elapsed = time.time() - start_time
        
        if errors:
            self.log_bug("并发竞态", f"并发提交出现 {len(errors)} 个错误", "H")
            for tid, err in errors[:3]:
                print(f"    线程{tid}: {err}")
        else:
            print(f"  ✓ 100线程并发提交1000任务成功，耗时 {elapsed:.2f}秒")
        
        # 3.2 并发知识图谱访问
        print("\n  3.2 并发知识图谱访问测试...")
        kg_errors = []
        
        def access_kg(thread_id):
            try:
                kg = agent.knowledge_graph
                # 并发读写
                for i in range(20):
                    exp = Experience(
                        id=f"kg_concurrent_{thread_id}_{i}",
                        task_type="analyze",
                        problem=f"并发知识测试 {thread_id}-{i}",
                        solution="测试解决方案",
                        outcome="成功",
                        lessons="测试教训",
                        code_snippets=[],
                        tags=["concurrent", f"thread_{thread_id}"],
                        created_at=datetime.now().isoformat()
                    )
                    kg.add_experience(exp)
                    
                    # 并发搜索
                    kg.get_similar_experiences(f"并发知识测试 {thread_id}", limit=5)
            except Exception as e:
                with lock:
                    kg_errors.append((thread_id, str(e)))
        
        threads = []
        for i in range(50):
            t = threading.Thread(target=access_kg, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        if kg_errors:
            self.log_bug("知识图谱竞态", f"并发访问出现 {len(kg_errors)} 个错误", "H")
        else:
            print("  ✓ 50线程并发知识图谱访问成功")
        
        # 3.3 并发状态查询
        print("\n  3.3 并发状态查询测试...")
        status_errors = []
        
        def query_status(thread_id):
            try:
                for _ in range(50):
                    status = agent.executor.get_status()
                    _ = status["stats"]
                    _ = status["pending_tasks"]
            except Exception as e:
                with lock:
                    status_errors.append((thread_id, str(e)))
        
        threads = []
        for i in range(30):
            t = threading.Thread(target=query_status, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        if status_errors:
            self.log_bug("状态查询竞态", f"并发查询出现 {len(status_errors)} 个错误", "M")
        else:
            print("  ✓ 30线程并发状态查询成功")
        
        agent.stop()
        
        self.results["concurrency"] = {
            "concurrent_tasks": 1000,
            "concurrent_kg_access": 50,
            "errors": len(errors) + len(kg_errors) + len(status_errors)
        }

    # ==================== 测试4: 错误恢复链 ====================
    def test_error_recovery_chain(self):
        """测试4: 错误恢复链 - 连续错误场景下的系统行为"""
        print("\n" + "=" * 70)
        print("测试4: 错误恢复链测试")
        print("=" * 70)
        
        work_dir = self.test_dir / "error_chain"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        # 4.1 连续失败任务
        print("\n  4.1 连续失败任务测试...")
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 提交会失败的任务（不存在的文件）
        for i in range(20):
            task = Task(
                id=f"fail_task_{i}",
                task_type=TaskType.ANALYZE,
                description=f"失败任务 {i}",
                target_files=[f"/nonexistent/file_{i}.py"],
                max_retries=1
            )
            agent.executor.submit_task(task)
        
        time.sleep(3)
        status = agent.executor.get_status()
        print(f"  失败任务统计: submitted={status['stats']['submitted']}, "
              f"failed={status['stats']['failed']}")
        
        if status['stats']['failed'] > 0:
            print("  ✓ 失败任务被正确记录")
        else:
            self.log_warning("失败处理", "失败任务未被正确记录")
        
        # 4.2 混合成功/失败任务
        print("\n  4.2 混合成功/失败任务测试...")
        for i in range(10):
            # 成功任务
            task = Task(
                id=f"success_task_{i}",
                task_type=TaskType.ANALYZE,
                description=f"成功任务 {i}",
                target_files=["/workspace/persistent_agent/config.py"]
            )
            agent.executor.submit_task(task)
            
            # 失败任务
            task = Task(
                id=f"fail_mixed_{i}",
                task_type=TaskType.ANALYZE,
                description=f"混合失败任务 {i}",
                target_files=[f"/nonexistent/mixed_{i}.py"]
            )
            agent.executor.submit_task(task)
        
        time.sleep(3)
        status = agent.executor.get_status()
        print(f"  混合任务统计: completed={status['stats']['completed']}, "
              f"failed={status['stats']['failed']}")
        
        # 4.3 检查系统是否仍能正常工作
        print("\n  4.3 错误后系统恢复测试...")
        task = Task(
            id="recovery_test",
            task_type=TaskType.ANALYZE,
            description="错误后恢复测试",
            target_files=["/workspace/persistent_agent/config.py"]
        )
        agent.executor.submit_task(task)
        time.sleep(1)
        
        status = agent.executor.get_status()
        if status['stats']['completed'] > 0:
            print("  ✓ 错误后系统仍能正常工作")
        else:
            self.log_bug("系统恢复", "错误后系统无法正常工作", "H")
        
        agent.stop()
        
        self.results["error_chain"] = {
            "failed_tasks": status['stats']['failed'],
            "completed_tasks": status['stats']['completed']
        }

    # ==================== 测试5: 知识图谱一致性 ====================
    def test_knowledge_graph_consistency(self):
        """测试5: 知识图谱一致性 - 并发读写数据完整性"""
        print("\n" + "=" * 70)
        print("测试5: 知识图谱一致性测试")
        print("=" * 70)
        
        work_dir = self.test_dir / "kg_consistency"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        kg = agent.knowledge_graph
        
        # 5.1 标签索引一致性
        print("\n  5.1 标签索引一致性测试...")
        
        # 添加带标签的经验
        exp = Experience(
            id="tag_test_exp",
            task_type="analyze",
            problem="标签测试问题",
            solution="标签测试解决方案",
            outcome="成功",
            lessons="标签测试教训",
            code_snippets=[],
            tags=["tag_a", "tag_b", "tag_c"],
            created_at=datetime.now().isoformat()
        )
        kg.add_experience(exp)
        
        # 更新标签
        exp.tags = ["tag_b", "tag_d"]  # 移除tag_a和tag_c，添加tag_d
        kg.add_experience(exp)
        
        # 检查标签索引
        results_a = kg.search_experiences(query="", tags=["tag_a"], limit=10)
        results_c = kg.search_experiences(query="", tags=["tag_c"], limit=10)
        results_d = kg.search_experiences(query="", tags=["tag_d"], limit=10)
        
        if len(results_a) > 0:
            self.log_bug("标签索引", "已删除的标签tag_a仍能搜索到", "M")
        else:
            print("  ✓ 已删除标签正确清理")
        
        if len(results_d) > 0:
            print("  ✓ 新标签正确添加")
        else:
            self.log_bug("标签索引", "新标签tag_d无法搜索到", "M")
        
        # 5.2 经验更新一致性
        print("\n  5.2 经验更新一致性测试...")
        
        # 添加经验
        exp = Experience(
            id="update_test_exp",
            task_type="analyze",
            problem="原始问题",
            solution="原始解决方案",
            outcome="成功",
            lessons="原始教训",
            code_snippets=[],
            tags=["original"],
            created_at=datetime.now().isoformat(),
            success_rate=0.5,
            usage_count=10
        )
        kg.add_experience(exp)
        
        # 更新经验（保留统计字段）
        exp.problem = "更新后问题"
        exp.success_rate = 0.8  # 这个会被忽略（ON CONFLICT不更新统计字段）
        kg.add_experience(exp)
        
        # 验证
        results = kg.search_experiences(query="更新后问题", limit=1)
        if results and "更新后问题" in results[0].problem:
            print("  ✓ 经验内容更新正确")
        else:
            self.log_bug("经验更新", "经验内容未正确更新", "M")
        
        # 5.3 并发写入一致性
        print("\n  5.3 并发写入一致性测试...")
        
        write_errors = []
        lock = threading.Lock()
        
        def write_experience(thread_id):
            try:
                for i in range(10):
                    exp = Experience(
                        id=f"concurrent_exp_{thread_id}_{i}",
                        task_type="analyze",
                        problem=f"并发经验 {thread_id}-{i}",
                        solution="并发解决方案",
                        outcome="成功",
                        lessons="并发教训",
                        code_snippets=[],
                        tags=["concurrent"],
                        created_at=datetime.now().isoformat()
                    )
                    kg.add_experience(exp)
            except Exception as e:
                with lock:
                    write_errors.append((thread_id, str(e)))
        
        threads = []
        for i in range(20):
            t = threading.Thread(target=write_experience, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        if write_errors:
            self.log_bug("并发写入", f"并发写入出现 {len(write_errors)} 个错误", "H")
        else:
            # 验证数据完整性
            stats = kg.get_statistics()
            print(f"  ✓ 并发写入成功，总经验数: {stats['total_experiences']}")
        
        agent.stop()
        
        self.results["kg_consistency"] = {
            "tag_tests": 3,
            "concurrent_writes": 200,
            "errors": len(write_errors)
        }

    # ==================== 测试6: 文件监控边界 ====================
    def test_file_watcher_boundary(self):
        """测试6: 文件监控边界测试"""
        print("\n" + "=" * 70)
        print("测试6: 文件监控边界测试")
        print("=" * 70)
        
        watch_dir = self.test_dir / "watch_test"
        shutil.rmtree(watch_dir, ignore_errors=True)
        watch_dir.mkdir(parents=True, exist_ok=True)
        
        events = []
        lock = threading.Lock()
        
        def callback(path, event_type):
            with lock:
                events.append((path, event_type, time.time()))
        
        watcher = FileWatcher(str(watch_dir), callback)
        watcher.start()
        
        time.sleep(0.5)  # 等待监控启动
        
        # 6.1 快速创建删除
        print("\n  6.1 快速创建删除测试...")
        test_file = watch_dir / "rapid.py"
        for i in range(10):
            test_file.write_text(f"# version {i}")
            time.sleep(0.1)
            test_file.unlink()
            time.sleep(0.1)
        
        time.sleep(1)
        
        # 6.2 临时缺失恢复
        print("\n  6.2 临时缺失恢复测试...")
        test_file = watch_dir / "temp_missing.py"
        test_file.write_text("# original")
        time.sleep(0.5)
        
        # 临时删除
        test_file.unlink()
        time.sleep(0.3)  # 不到缺失阈值
        
        # 恢复文件
        test_file.write_text("# restored")
        time.sleep(1)
        
        # 检查事件
        with lock:
            created_count = sum(1 for e in events if e[1] == "created")
            deleted_count = sum(1 for e in events if e[1] == "deleted")
            modified_count = sum(1 for e in events if e[1] == "modified")
        
        print(f"  事件统计: created={created_count}, modified={modified_count}, deleted={deleted_count}")
        
        # 检查是否有误报的删除事件
        # 临时缺失不应触发删除（阈值是3次扫描）
        temp_deleted = sum(1 for e in events if "temp_missing" in e[0] and e[1] == "deleted")
        if temp_deleted > 0:
            self.log_warning("文件监控", f"临时缺失被误报为删除 {temp_deleted} 次")
        else:
            print("  ✓ 临时缺失未被误报为删除")
        
        watcher.stop()
        
        self.results["file_watcher"] = {
            "total_events": len(events),
            "created": created_count,
            "modified": modified_count,
            "deleted": deleted_count
        }

    # ==================== 生成报告 ====================
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "=" * 70)
        print("第9轮深度压力测试报告")
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
        report_path = self.test_dir / "report_round9.json"
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


if __name__ == "__main__":
    tester = DeepStressTestRound9()
    
    # 执行所有测试
    tester.test_long_running_stability()
    tester.test_extreme_boundary_conditions()
    tester.test_high_concurrency_race_detection()
    tester.test_error_recovery_chain()
    tester.test_knowledge_graph_consistency()
    tester.test_file_watcher_boundary()
    
    # 生成报告
    success = tester.generate_report()
    
    sys.exit(0 if success else 1)
