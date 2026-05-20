"""
第六轮极限压力测试 - 发现系统在极端条件下的Bug
"""
import sys
import os
import time
import random
import threading
import traceback
import gc
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType, TaskStatus
from core.knowledge_graph import Experience


class ExtremeStressTest:
    """极限压力测试器"""
    
    def __init__(self):
        self.agent = PersistentAgent(work_dir="/workspace/persistent_agent")
        self.bugs_found = []
        self.warnings = []
        self.results = {}
    
    def run_all_tests(self):
        """运行所有极限测试"""
        print("=" * 70)
        print("第六轮极限压力测试")
        print("=" * 70)
        
        # 测试1: 极限并发
        self.test_1_extreme_concurrency()
        
        # 测试2: 内存压力
        self.test_2_memory_pressure()
        
        # 测试3: 真实代码重构
        self.test_3_real_refactoring()
        
        # 测试4: 长时间稳定性
        self.test_4_long_term_stability()
        
        # 测试5: 故障注入
        self.test_5_fault_injection()
        
        # 生成报告
        self.generate_report()
    
    def test_1_extreme_concurrency(self):
        """测试1: 极限并发 - 1000任务风暴"""
        print("\n" + "=" * 70)
        print("测试1: 极限并发 - 1000任务风暴")
        print("=" * 70)
        
        # 创建测试文件
        test_file = "/tmp/extreme_test.py"
        with open(test_file, 'w') as f:
            f.write("def test():\n    pass\n")
        
        # 准备1000个任务
        tasks = []
        for i in range(1000):
            task = Task(
                id=f"extreme_{i}_{int(time.time()*1000)%100000}",
                task_type=random.choice([TaskType.ANALYZE, TaskType.BUG_FIX, TaskType.REFACTOR]),
                description=f"极限测试任务 #{i}",
                target_files=[test_file],
                priority=random.randint(1, 5)
            )
            tasks.append(task)
        
        print(f"准备提交 {len(tasks)} 个任务...")
        
        # 多线程并发提交
        start_time = time.time()
        errors = []
        
        def submit_batch(batch):
            local_errors = []
            for task in batch:
                try:
                    self.agent.executor.submit_task(task)
                except Exception as e:
                    local_errors.append(str(e))
            return local_errors
        
        # 分10批提交，每批100个
        batches = [tasks[i:i+100] for i in range(0, len(tasks), 100)]
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(submit_batch, batch) for batch in batches]
            for future in as_completed(futures):
                errors.extend(future.result())
        
        submit_time = time.time() - start_time
        submit_rate = len(tasks) / submit_time
        
        print(f"提交完成: {len(tasks) - len(errors)}/{len(tasks)} 成功")
        print(f"耗时: {submit_time:.2f}s, 速率: {submit_rate:.0f} 任务/秒")
        
        if errors:
            print(f"✗ 提交错误: {len(errors)} 个")
            self.bugs_found.extend(errors[:5])
        
        # 等待处理
        print("\n等待任务处理...")
        time.sleep(30)
        
        # 检查状态
        status = self.agent.executor.get_status()
        stats = status["stats"]
        
        success_rate = stats['completed'] / max(stats['completed'] + stats['failed'], 1) * 100
        
        print(f"\n处理结果:")
        print(f"  完成: {stats['completed']}")
        print(f"  失败: {stats['failed']}")
        print(f"  超时: {stats['timeout']}")
        print(f"  成功率: {success_rate:.1f}%")
        print(f"  队列: {status['queue_size']}")
        
        if success_rate < 95:
            self.bugs_found.append(f"极限并发成功率过低: {success_rate:.1f}%")
        
        self.results["extreme_concurrency"] = {
            "tasks": len(tasks),
            "submit_rate": f"{submit_rate:.0f}/秒",
            "completed": stats['completed'],
            "failed": stats['failed'],
            "success_rate": f"{success_rate:.1f}%"
        }
    
    def test_2_memory_pressure(self):
        """测试2: 内存压力 - 分析100个大文件"""
        print("\n" + "=" * 70)
        print("测试2: 内存压力 - 分析100个大文件")
        print("=" * 70)
        
        # 创建100个大文件(每个约50KB)
        large_files = []
        for i in range(100):
            file_path = f"/tmp/large_file_{i}.py"
            with open(file_path, 'w') as f:
                f.write(f'"""Large file {i}"""\n\n')
                # 生成大量代码
                for j in range(500):
                    f.write(f"def func_{i}_{j}(x, y, z):\n")
                    f.write(f"    \"\"\"Function {i}-{j}\"\"\"\n")
                    f.write(f"    a = x + y + z + {j}\n")
                    f.write(f"    b = a * 2\n")
                    f.write(f"    return b\n\n")
            large_files.append(file_path)
        
        total_size = sum(os.path.getsize(f) for f in large_files)
        print(f"创建了 {len(large_files)} 个大文件")
        print(f"总大小: {total_size / 1024 / 1024:.1f} MB")
        
        # 获取初始内存
        try:
            import psutil
            process = psutil.Process(os.getpid())
            initial_mem = process.memory_info().rss / 1024 / 1024
            print(f"初始内存: {initial_mem:.1f} MB")
        except:
            initial_mem = 0
        
        # 提交分析任务
        start_time = time.time()
        for i, file_path in enumerate(large_files):
            task = Task(
                id=f"memory_{i}_{int(time.time()*1000)%100000}",
                task_type=TaskType.ANALYZE,
                description=f"内存压力测试 {i}",
                target_files=[file_path],
                priority=2
            )
            self.agent.executor.submit_task(task)
        
        submit_time = time.time() - start_time
        print(f"提交耗时: {submit_time:.2f}s")
        
        # 等待处理
        print("等待处理...")
        time.sleep(45)
        
        # 检查内存
        if initial_mem > 0:
            final_mem = process.memory_info().rss / 1024 / 1024
            mem_increase = final_mem - initial_mem
            print(f"\n内存使用:")
            print(f"  初始: {initial_mem:.1f} MB")
            print(f"  最终: {final_mem:.1f} MB")
            print(f"  增长: {mem_increase:.1f} MB")
            
            if mem_increase > 500:
                self.warnings.append(f"内存增长过高: {mem_increase:.1f}MB")
        
        # 强制垃圾回收
        gc.collect()
        
        status = self.agent.executor.get_status()
        print(f"\n处理结果: 完成={status['stats']['completed']}, 失败={status['stats']['failed']}")
        
        self.results["memory_pressure"] = {
            "files": len(large_files),
            "total_size_mb": f"{total_size / 1024 / 1024:.1f}",
            "memory_increase_mb": f"{mem_increase:.1f}" if initial_mem > 0 else "N/A"
        }
    
    def test_3_real_refactoring(self):
        """测试3: 真实代码重构 - 让系统重构自己的代码"""
        print("\n" + "=" * 70)
        print("测试3: 真实代码重构")
        print("=" * 70)
        
        # 创建一个需要重构的代码文件
        messy_code = '''
"""
需要重构的混乱代码
"""
import os
import sys

# 过长的函数，需要拆分
def process_everything(data, config, options, flags, callback, logger, timeout, retries, cache, db):
    """这个函数做了太多事情"""
    # 验证输入
    if not data:
        return None
    if not config:
        return None
    if not options:
        return None
    
    # 处理数据
    result = []
    for item in data:
        if item is None:
            continue
        if isinstance(item, str):
            item = item.strip()
        if isinstance(item, int):
            item = str(item)
        result.append(item)
    
    # 应用配置
    if 'filter' in config:
        result = [r for r in result if config['filter'](r)]
    
    if 'transform' in config:
        result = [config['transform'](r) for r in result]
    
    # 缓存结果
    if cache:
        cache_key = str(hash(str(result)))
        if hasattr(cache, 'get'):
            cached = cache.get(cache_key)
            if cached:
                return cached
        if hasattr(cache, 'set'):
            cache.set(cache_key, result)
    
    # 记录日志
    if logger:
        if hasattr(logger, 'info'):
            logger.info(f"Processed {len(result)} items")
    
    # 回调
    if callback:
        if callable(callback):
            callback(result)
    
    # 保存到数据库
    if db:
        if hasattr(db, 'save'):
            for r in result:
                try:
                    db.save(r)
                except Exception as e:
                    if logger:
                        logger.error(str(e))
    
    return result

# 重复代码
def validate_name(name):
    if not name:
        return False
    if len(name) < 3:
        return False
    if len(name) > 50:
        return False
    return True

def validate_email(email):
    if not email:
        return False
    if len(email) < 5:
        return False
    if len(email) > 100:
        return False
    if '@' not in email:
        return False
    return True

def validate_phone(phone):
    if not phone:
        return False
    if len(phone) < 10:
        return False
    if len(phone) > 20:
        return False
    return True

# 魔法数字
def calculate_price(items):
    total = 0
    for item in items:
        total += item * 1.08  # 税率
    if total > 1000:
        total = total * 0.95  # 折扣
    return total + 50  # 运费
'''
        
        messy_file = "/tmp/messy_code.py"
        with open(messy_file, 'w') as f:
            f.write(messy_code)
        
        print(f"创建了需要重构的代码文件: {messy_file}")
        print(f"代码行数: {len(messy_code.split(chr(10)))}")
        
        # 提交重构任务
        task = Task(
            id=f"refactor_real_{int(time.time()*1000)%100000}",
            task_type=TaskType.REFACTOR,
            description="重构混乱代码: 拆分过长函数、消除重复代码、提取魔法数字",
            target_files=[messy_file],
            priority=5
        )
        
        task_id = self.agent.executor.submit_task(task)
        print(f"提交重构任务: {task_id}")
        
        # 等待处理
        time.sleep(10)
        
        # 获取结果
        result = self.agent.executor.get_task_result(task_id)
        if result and result.get('result'):
            suggestions = result['result'].get('suggestions', [])
            print(f"\n重构建议: {len(suggestions)} 条")
            for s in suggestions[:5]:
                print(f"  - {s}")
        
        # 验证知识积累
        similar = self.agent.knowledge_graph.get_similar_experiences("重构过长函数", limit=3)
        print(f"\n相关经验: {len(similar)} 条")
        
        self.results["real_refactoring"] = {
            "code_lines": len(messy_code.split(chr(10))),
            "suggestions": len(suggestions) if result and result.get('result') else 0,
            "experiences": len(similar)
        }
    
    def test_4_long_term_stability(self):
        """测试4: 长时间稳定性 - 5分钟持续运行"""
        print("\n" + "=" * 70)
        print("测试4: 长时间稳定性 (5分钟持续运行)")
        print("=" * 70)
        
        duration = 300  # 5分钟
        start_time = time.time()
        
        task_count = 0
        exp_count = 0
        error_count = 0
        status_checks = 0
        
        def continuous_work():
            nonlocal task_count, exp_count, error_count
            while time.time() - start_time < duration:
                try:
                    # 提交随机任务
                    task = Task(
                        id=f"longterm_{int(time.time()*1000)%100000}",
                        task_type=random.choice([TaskType.ANALYZE, TaskType.BUG_FIX]),
                        description="长时间稳定性测试",
                        target_files=["/workspace/persistent_agent/config.py"],
                        priority=random.randint(1, 3)
                    )
                    self.agent.executor.submit_task(task)
                    task_count += 1
                    
                    # 添加经验
                    if task_count % 5 == 0:
                        from datetime import datetime
                        exp = Experience(
                            id=f"longterm_exp_{int(time.time()*1000)%100000}",
                            task_type="bug_fix",
                            context="长时间测试",
                            problem=f"测试问题 {exp_count}",
                            solution="测试解决方案",
                            outcome="success",
                            lessons="测试教训",
                            code_snippets=[],
                            tags=["longterm"],
                            created_at=datetime.now().isoformat()
                        )
                        self.agent.knowledge_graph.add_experience(exp)
                        exp_count += 1
                    
                    time.sleep(0.5)
                except Exception as e:
                    error_count += 1
        
        def continuous_monitoring():
            nonlocal status_checks
            while time.time() - start_time < duration:
                try:
                    self.agent.executor.get_status()
                    self.agent.knowledge_graph.get_statistics()
                    status_checks += 1
                    time.sleep(2)
                except Exception as e:
                    error_count += 1
        
        # 启动工作线程
        threads = [
            threading.Thread(target=continuous_work),
            threading.Thread(target=continuous_work),
            threading.Thread(target=continuous_monitoring)
        ]
        
        for t in threads:
            t.start()
        
        # 显示进度
        while time.time() - start_time < duration:
            elapsed = time.time() - start_time
            progress = elapsed / duration * 100
            print(f"\r  进度: {progress:.0f}% | 任务: {task_count} | 经验: {exp_count} | 错误: {error_count}", end="")
            time.sleep(5)
        
        for t in threads:
            t.join()
        
        print(f"\n\n长时间运行结果:")
        print(f"  任务提交: {task_count}")
        print(f"  经验添加: {exp_count}")
        print(f"  状态检查: {status_checks}")
        print(f"  错误数: {error_count}")
        
        # 最终状态
        final_status = self.agent.executor.get_status()
        final_kg = self.agent.knowledge_graph.get_statistics()
        
        print(f"\n最终状态:")
        print(f"  完成: {final_status['stats']['completed']}")
        print(f"  失败: {final_status['stats']['failed']}")
        print(f"  经验: {final_kg['total_experiences']}")
        
        if error_count > 10:
            self.bugs_found.append(f"长时间运行错误过多: {error_count}")
        
        self.results["long_term"] = {
            "duration": duration,
            "tasks": task_count,
            "experiences": exp_count,
            "errors": error_count
        }
    
    def test_5_fault_injection(self):
        """测试5: 故障注入"""
        print("\n" + "=" * 70)
        print("测试5: 故障注入")
        print("=" * 70)
        
        # 5.1 无效文件路径
        print("\n--- 5.1 无效文件路径 ---")
        invalid_paths = [
            "/nonexistent/path/file.py",
            "/dev/null/file.py",
            "",
            "/tmp/" + "a" * 1000 + ".py",  # 超长路径
        ]
        
        for path in invalid_paths:
            try:
                task = Task(
                    id=f"fault_{int(time.time()*1000)%100000}",
                    task_type=TaskType.ANALYZE,
                    description=f"故障注入: 无效路径",
                    target_files=[path] if path else [],
                    priority=1
                )
                self.agent.executor.submit_task(task)
                print(f"  ✓ 路径 '{path[:50]}...' 正常处理")
            except Exception as e:
                print(f"  ✗ 路径 '{path[:50]}...' 异常: {e}")
                self.bugs_found.append(f"无效路径处理失败: {e}")
        
        time.sleep(3)
        
        # 5.2 并发状态访问
        print("\n--- 5.2 并发状态访问 ---")
        errors = []
        
        def stress_access():
            for i in range(50):
                try:
                    self.agent.executor.get_status()
                    self.agent.knowledge_graph.get_statistics()
                    self.agent.get_status()
                except Exception as e:
                    errors.append(str(e))
        
        threads = [threading.Thread(target=stress_access) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        if errors:
            print(f"  ✗ 发现 {len(errors)} 个并发访问错误")
            self.bugs_found.extend(errors[:3])
        else:
            print(f"  ✓ 500次并发访问全部成功")
        
        # 5.3 快速启停
        print("\n--- 5.3 快速启停 ---")
        for i in range(5):
            try:
                self.agent.executor.stop()
                time.sleep(0.1)
                self.agent.executor.start(num_workers=2)
                time.sleep(0.1)
            except Exception as e:
                print(f"  ✗ 第{i+1}轮启停失败: {e}")
                self.bugs_found.append(f"快速启停失败: {e}")
        
        print(f"  ✓ 5轮快速启停完成")
        
        self.results["fault_injection"] = {
            "invalid_paths": len(invalid_paths),
            "concurrent_access": 500,
            "start_stop_cycles": 5
        }
    
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "=" * 70)
        print("极限压力测试报告")
        print("=" * 70)
        
        print(f"\n发现的Bug: {len(self.bugs_found)}")
        print(f"警告: {len(self.warnings)}")
        
        if self.bugs_found:
            print("\nBug列表:")
            for i, bug in enumerate(self.bugs_found, 1):
                print(f"  {i}. {bug[:100]}")
        
        if self.warnings:
            print("\n警告列表:")
            for warn in self.warnings:
                print(f"  - {warn}")
        
        print("\n测试结果:")
        for test_name, result in self.results.items():
            print(f"\n  [{test_name}]")
            for key, value in result.items():
                print(f"    {key}: {value}")
        
        # 保存报告
        import json
        report = {
            "timestamp": datetime.now().isoformat(),
            "bugs_found": self.bugs_found,
            "warnings": self.warnings,
            "results": self.results
        }
        
        report_path = "/workspace/persistent_agent/logs/extreme_stress_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n报告已保存: {report_path}")
        
        return len(self.bugs_found) == 0


def main():
    tester = ExtremeStressTest()
    success = tester.run_all_tests()
    
    print("\n" + "=" * 70)
    if success:
        print("✓ 所有极限压力测试通过!")
    else:
        print(f"✗ 发现 {len(tester.bugs_found)} 个Bug需要修复")
    print("=" * 70)
    
    return success


if __name__ == "__main__":
    main()
