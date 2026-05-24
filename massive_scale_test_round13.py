"""
第13轮小时级长时间大规模测试 - 慢工出细活
测试目标:
1. 1小时持续运行稳定性
2. 万级任务压力测试
3. 复杂工作流组合
4. 性能退化检测
5. 资源极限测试
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
from collections import deque, defaultdict

sys.path.insert(0, str(Path(__file__).parent))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType
from core.knowledge_graph import Experience


class MassiveScaleTestRound13:
    """第13轮小时级长时间大规模测试"""
    
    def __init__(self):
        self.bugs_found = []
        self.warnings = []
        self.results = {}
        self.test_dir = Path("/tmp/massive_scale_test_r13")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.stop_flag = threading.Event()
        self.performance_samples = deque(maxlen=1000)
        
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

    # ==================== 测试1: 1小时长时间运行 ====================
    def test_one_hour_stability(self):
        """测试1: 1小时持续运行，每10秒采样一次"""
        print("\n" + "=" * 70)
        print("测试1: 1小时长时间运行稳定性")
        print("=" * 70)
        
        work_dir = self.test_dir / "one_hour"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        tracemalloc.start()
        agent = PersistentAgent(work_dir=str(work_dir))
        
        initial_memory = tracemalloc.get_traced_memory()[0]
        initial_threads = threading.active_count()
        
        # 1小时 = 3600秒，每10秒一个周期
        duration = 3600  # 3600秒 = 1小时
        interval = 10    # 每10秒提交一批任务
        
        print(f"  持续运行 {duration//60} 分钟，每 {interval} 秒提交任务...")
        print(f"  初始内存: {initial_memory/1024/1024:.1f}MB, 线程: {initial_threads}")
        
        start_time = time.time()
        task_count = 0
        error_count = 0
        memory_samples = []
        thread_samples = []
        completed_samples = []
        
        while time.time() - start_time < duration and not self.stop_flag.is_set():
            elapsed = time.time() - start_time
            
            # 每10秒提交10个任务
            for _ in range(10):
                try:
                    task = Task(
                        id=f"hour_{task_count}_{int(time.time()*1000)%100000}",
                        task_type=random.choice(list(TaskType)),
                        description=f"小时级测试任务 {task_count}",
                        target_files=["/workspace/persistent_agent/config.py"],
                        priority=random.randint(1, 5)
                    )
                    agent.executor.submit_task(task)
                    task_count += 1
                except Exception as e:
                    error_count += 1
                    if error_count <= 3:
                        self.log_bug("任务提交", f"提交失败: {e}")
            
            # 采样
            current_memory = tracemalloc.get_traced_memory()[0]
            memory_samples.append(current_memory)
            thread_samples.append(threading.active_count())
            
            status = agent.executor.get_status()
            completed_samples.append(status['stats']['completed'])
            
            # 每5分钟报告一次
            if int(elapsed) % 300 == 0:
                print(f"  [{int(elapsed)//60}m] 任务: {task_count}, 内存: {current_memory/1024/1024:.1f}MB, "
                      f"线程: {threading.active_count()}, 完成: {status['stats']['completed']}")
            
            time.sleep(interval)
        
        # 最终状态
        final_memory = tracemalloc.get_traced_memory()[0]
        final_threads = threading.active_count()
        
        agent.stop()
        tracemalloc.stop()
        
        # 分析内存趋势
        if len(memory_samples) > 10:
            first_avg = sum(memory_samples[:10]) / 10
            last_avg = sum(memory_samples[-10:]) / 10
            growth_rate = (last_avg - first_avg) / first_avg * 100 if first_avg > 0 else 0
            
            print(f"\n  内存分析:")
            print(f"    初期平均: {first_avg/1024/1024:.1f}MB")
            print(f"    末期平均: {last_avg/1024/1024:.1f}MB")
            print(f"    增长率: {growth_rate:.1f}%")
            
            if growth_rate > 100:
                self.log_bug("内存泄漏", f"1小时运行内存增长 {growth_rate:.1f}%", "H")
            elif growth_rate > 50:
                self.log_warning("内存增长", f"内存增长率 {growth_rate:.1f}%")
            else:
                print("  ✓ 内存稳定")
        
        thread_growth = final_threads - initial_threads
        if thread_growth > 10:
            self.log_bug("线程泄漏", f"线程增长 {thread_growth} 个", "M")
        else:
            print(f"  ✓ 线程稳定 (增长{thread_growth}个)")
        
        self.results["one_hour"] = {
            "duration_minutes": duration // 60,
            "tasks_submitted": task_count,
            "errors": error_count,
            "memory_growth_rate": growth_rate if len(memory_samples) > 10 else 0,
            "thread_growth": thread_growth
        }

    # ==================== 测试2: 万级任务压力测试 ====================
    def test_ten_thousand_tasks(self):
        """测试2: 10000任务连续提交和处理"""
        print("\n" + "=" * 70)
        print("测试2: 万级任务压力测试 (10000任务)")
        print("=" * 70)
        
        work_dir = self.test_dir / "ten_thousand"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        total_tasks = 10000
        batch_size = 100
        errors = []
        
        print(f"  提交 {total_tasks} 个任务...")
        start = time.time()
        
        for batch in range(0, total_tasks, batch_size):
            for i in range(batch_size):
                try:
                    task = Task(
                        id=f"10k_{batch+i}",
                        task_type=random.choice(list(TaskType)),
                        description=f"万级测试任务 {batch+i}",
                        target_files=["/workspace/persistent_agent/config.py"],
                        priority=random.randint(1, 5)
                    )
                    agent.executor.submit_task(task)
                except Exception as e:
                    errors.append(str(e))
                    if len(errors) <= 3:
                        self.log_bug("万级任务", f"提交失败: {e}")
            
            if batch % 1000 == 0:
                print(f"    进度: {batch}/{total_tasks}")
        
        submit_time = time.time() - start
        print(f"  提交完成: {submit_time:.1f}秒, 错误: {len(errors)}")
        
        # 等待处理完成
        print("  等待任务处理完成...")
        max_wait = 300  # 最多等5分钟
        wait_start = time.time()
        
        while time.time() - wait_start < max_wait:
            status = agent.executor.get_status()
            pending = status['pending_tasks'] + status['running_tasks']
            
            if pending == 0:
                break
            
            if int(time.time() - wait_start) % 30 == 0:
                print(f"    剩余: {pending} 任务")
            
            time.sleep(1)
        
        final_status = agent.executor.get_status()
        process_time = time.time() - start
        
        print(f"\n  结果:")
        print(f"    提交耗时: {submit_time:.1f}秒")
        print(f"    总耗时: {process_time:.1f}秒")
        print(f"    完成: {final_status['stats']['completed']}")
        print(f"    失败: {final_status['stats']['failed']}")
        print(f"    错误: {len(errors)}")
        
        if final_status['stats']['completed'] < total_tasks * 0.95:
            self.log_bug("万级任务", f"完成率过低: {final_status['stats']['completed']}/{total_tasks}", "H")
        else:
            print("  ✓ 万级任务测试通过")
        
        agent.stop()
        
        self.results["ten_thousand"] = {
            "total_tasks": total_tasks,
            "submit_time": submit_time,
            "process_time": process_time,
            "completed": final_status['stats']['completed'],
            "errors": len(errors)
        }

    # ==================== 测试3: 复杂工作流组合 ====================
    def test_complex_workflows(self):
        """测试3: 复杂多步骤工作流"""
        print("\n" + "=" * 70)
        print("测试3: 复杂工作流组合")
        print("=" * 70)
        
        work_dir = self.test_dir / "complex_workflow"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 工作流1: 分析→重构→优化→测试
        print("\n  工作流1: 分析→重构→优化→测试")
        for i in range(50):
            # 分析
            task1 = Task(id=f"wf1_analyze_{i}", task_type=TaskType.ANALYZE,
                        description=f"工作流1分析 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task1)
            
            # 重构
            task2 = Task(id=f"wf1_refactor_{i}", task_type=TaskType.REFACTOR,
                        description=f"工作流1重构 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task2)
            
            # 优化
            task3 = Task(id=f"wf1_optimize_{i}", task_type=TaskType.OPTIMIZE,
                        description=f"工作流1优化 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task3)
            
            # 测试
            task4 = Task(id=f"wf1_test_{i}", task_type=TaskType.TEST,
                        description=f"工作流1测试 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task4)
        
        print("    提交200个任务 (50组×4步骤)")
        
        # 工作流2: Bug修复链
        print("\n  工作流2: Bug修复链")
        for i in range(30):
            # Bug检测
            task1 = Task(id=f"wf2_detect_{i}", task_type=TaskType.ANALYZE,
                        description=f"工作流2检测 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task1)
            
            # Bug修复
            task2 = Task(id=f"wf2_fix_{i}", task_type=TaskType.BUG_FIX,
                        description=f"工作流2修复 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task2)
            
            # 验证修复
            task3 = Task(id=f"wf2_verify_{i}", task_type=TaskType.ANALYZE,
                        description=f"工作流2验证 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task3)
        
        print("    提交90个任务 (30组×3步骤)")
        
        # 等待完成
        print("\n  等待工作流完成...")
        time.sleep(10)
        
        status = agent.executor.get_status()
        print(f"  完成: {status['stats']['completed']}, 失败: {status['stats']['failed']}")
        
        if status['stats']['completed'] >= 200:
            print("  ✓ 复杂工作流测试通过")
        else:
            self.log_warning("复杂工作流", f"完成率偏低: {status['stats']['completed']}/290")
        
        agent.stop()
        
        self.results["complex_workflow"] = {
            "total_tasks": 290,
            "completed": status['stats']['completed'],
            "failed": status['stats']['failed']
        }

    # ==================== 测试4: 性能退化检测 ====================
    def test_performance_degradation(self):
        """测试4: 长时间运行后性能是否退化"""
        print("\n" + "=" * 70)
        print("测试4: 性能退化检测")
        print("=" * 70)
        
        work_dir = self.test_dir / "performance"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 基线测试
        print("\n  基线性能测试...")
        baseline_times = []
        for _ in range(5):
            start = time.time()
            for i in range(100):
                task = Task(id=f"baseline_{i}_{time.time()}", task_type=TaskType.ANALYZE,
                           description="基线测试", target_files=["/workspace/persistent_agent/config.py"])
                agent.executor.submit_task(task)
            baseline_times.append(time.time() - start)
            time.sleep(1)
        
        baseline_avg = sum(baseline_times) / len(baseline_times)
        print(f"    基线平均: {baseline_avg:.2f}秒/100任务")
        
        # 添加大量数据
        print("\n  添加1000条经验...")
        for i in range(1000):
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
        
        # 压力测试后性能
        print("\n  压力后性能测试...")
        pressure_times = []
        for _ in range(5):
            start = time.time()
            for i in range(100):
                task = Task(id=f"pressure_{i}_{time.time()}", task_type=TaskType.ANALYZE,
                           description="压力后测试", target_files=["/workspace/persistent_agent/config.py"])
                agent.executor.submit_task(task)
            pressure_times.append(time.time() - start)
            time.sleep(1)
        
        pressure_avg = sum(pressure_times) / len(pressure_times)
        print(f"    压力后平均: {pressure_avg:.2f}秒/100任务")
        
        # 计算退化率
        if baseline_avg > 0:
            degradation = (pressure_avg - baseline_avg) / baseline_avg * 100
            print(f"\n  性能退化: {degradation:+.1f}%")
            
            if degradation > 100:
                self.log_bug("性能退化", f"性能严重退化 {degradation:.1f}%", "H")
            elif degradation > 50:
                self.log_warning("性能退化", f"性能明显退化 {degradation:.1f}%")
            else:
                print("  ✓ 性能稳定")
        
        agent.stop()
        
        self.results["performance"] = {
            "baseline_avg": baseline_avg,
            "pressure_avg": pressure_avg,
            "degradation_percent": degradation if baseline_avg > 0 else 0
        }

    # ==================== 测试5: 资源极限测试 ====================
    def test_resource_limits(self):
        """测试5: 资源极限测试"""
        print("\n" + "=" * 70)
        print("测试5: 资源极限测试")
        print("=" * 70)
        
        # 5.1 超大知识库测试
        print("\n  5.1 超大知识库测试 (5000条经验)...")
        work_dir = self.test_dir / "huge_kg"
        shutil.rmtree(work_dir, ignore_errors=True)
        agent = PersistentAgent(work_dir=str(work_dir))
        
        start = time.time()
        for i in range(5000):
            exp = Experience(
                id=f"huge_exp_{i}",
                task_type="analyze",
                context=json.dumps({"index": i}),
                problem=f"超大知识库测试经验 {i}",
                solution="解决方案" * 100,  # 大文本
                outcome="成功",
                lessons="教训" * 50,
                code_snippets=["code"] * 20,
                tags=["huge", f"tag_{i%100}"],
                created_at=datetime.now().isoformat()
            )
            agent.knowledge_graph.add_experience(exp)
            
            if i % 1000 == 0:
                print(f"    进度: {i}/5000")
        
        kg_time = time.time() - start
        print(f"    5000条经验添加耗时: {kg_time:.1f}秒")
        
        # 搜索性能
        start = time.time()
        for i in range(100):
            agent.knowledge_graph.get_similar_experiences(f"搜索测试 {i}", limit=10)
        search_time = time.time() - start
        print(f"    100次搜索耗时: {search_time:.1f}秒")
        
        if search_time > 30:
            self.log_warning("知识库性能", f"大知识库搜索慢: {search_time:.1f}秒/100次")
        else:
            print("  ✓ 超大知识库性能正常")
        
        agent.stop()
        
        # 5.2 超大文件分析测试
        print("\n  5.2 超大文件分析测试...")
        work_dir2 = self.test_dir / "huge_file"
        shutil.rmtree(work_dir2, ignore_errors=True)
        
        # 创建大文件
        huge_file = work_dir2 / "huge.py"
        huge_file.parent.mkdir(parents=True, exist_ok=True)
        with open(huge_file, "w") as f:
            for i in range(10000):
                f.write(f"def function_{i}():\n    pass\n")
        
        agent = PersistentAgent(work_dir=str(work_dir2))
        
        task = Task(id="huge_file_test", task_type=TaskType.ANALYZE,
                   description="超大文件分析", target_files=[str(huge_file)])
        agent.executor.submit_task(task)
        
        time.sleep(5)
        status = agent.executor.get_status()
        print(f"    完成: {status['stats']['completed']}")
        
        agent.stop()
        
        self.results["resource_limits"] = {
            "kg_add_time": kg_time,
            "kg_search_time": search_time,
            "huge_file_completed": status['stats']['completed']
        }

    # ==================== 报告 ====================
    def generate_report(self):
        print("\n" + "=" * 70)
        print("第13轮小时级长时间大规模测试报告")
        print("=" * 70)
        
        print(f"\nBug: {len(self.bugs_found)}, 警告: {len(self.warnings)}")
        
        for i, b in enumerate(self.bugs_found, 1):
            print(f"  {i}. [{b['severity']}] {b['category']}: {b['description']}")
        
        for i, w in enumerate(self.warnings, 1):
            print(f"  {i}. {w['category']}: {w['description']}")
        
        print("\n结果:")
        for k, v in self.results.items():
            print(f"  [{k}]: {v}")
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "bugs": self.bugs_found,
            "warnings": self.warnings,
            "results": self.results
        }
        with open(self.test_dir / "report_r13.json", "w") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        return len(self.bugs_found) == 0


def signal_handler(signum, frame):
    print("\n\n收到中断信号，正在停止测试...")
    global tester
    if tester:
        tester.stop_flag.set()


if __name__ == "__main__":
    global tester
    tester = MassiveScaleTestRound13()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 执行所有测试
        tester.test_one_hour_stability()      # 1小时
        tester.test_ten_thousand_tasks()      # 万级任务
        tester.test_complex_workflows()       # 复杂工作流
        tester.test_performance_degradation() # 性能退化
        tester.test_resource_limits()         # 资源极限
        
        success = tester.generate_report()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n测试异常: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
