"""
第四轮真实运行测试 - 模拟实际使用场景
发现运行时Bug并修复
"""
import sys
import os
import time
import random
import threading
import traceback
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType, TaskStatus
from core.knowledge_graph import Experience


class RealWorldTest:
    """真实运行测试器"""
    
    def __init__(self):
        self.agent = PersistentAgent(work_dir="/workspace/persistent_agent")
        self.bugs_found = []
        self.warnings = []
        self.results = {}
    
    def run_all_tests(self):
        """运行所有测试"""
        print("=" * 70)
        print("第四轮真实运行测试 - 发现运行时Bug")
        print("=" * 70)
        
        # 测试1: 真实代码重构
        self.test_1_real_code_refactoring()
        
        # 测试2: Bug注入与检测
        self.test_2_bug_injection_detection()
        
        # 测试3: 跨模块依赖分析
        self.test_3_cross_module_dependency()
        
        # 测试4: 性能瓶颈探测
        self.test_4_performance_bottleneck()
        
        # 测试5: 异常恢复测试
        self.test_5_exception_recovery()
        
        # 测试6: 资源泄漏检测
        self.test_6_resource_leak_detection()
        
        # 生成报告
        self.generate_report()
    
    def test_1_real_code_refactoring(self):
        """测试1: 真实代码重构 - 让系统分析自己的代码"""
        print("\n" + "=" * 70)
        print("测试1: 真实代码重构分析")
        print("=" * 70)
        
        # 获取所有核心模块
        core_files = [
            "/workspace/persistent_agent/core/executor_v2.py",
            "/workspace/persistent_agent/core/knowledge_graph.py",
            "/workspace/persistent_agent/core/optimizer.py",
            "/workspace/persistent_agent/core/watchdog.py",
            "/workspace/persistent_agent/main.py"
        ]
        
        # 提交重构分析任务
        task_ids = []
        for file_path in core_files:
            if os.path.exists(file_path):
                task = Task(
                    id=f"refactor_{Path(file_path).stem}_{int(time.time()*1000)%100000}",
                    task_type=TaskType.REFACTOR,
                    description=f"分析代码结构，识别重构机会: {Path(file_path).name}",
                    target_files=[file_path],
                    priority=4
                )
                tid = self.agent.executor.submit_task(task)
                task_ids.append(tid)
        
        print(f"提交了 {len(task_ids)} 个重构分析任务")
        
        # 等待处理
        time.sleep(10)
        
        # 检查结果
        status = self.agent.executor.get_status()
        stats = status["stats"]
        
        print(f"完成: {stats['completed']}, 失败: {stats['failed']}")
        
        # 获取改进建议
        suggestions = self.agent.get_improvement_suggestions()
        print(f"获得 {len(suggestions)} 条改进建议")
        
        if suggestions:
            for i, s in enumerate(suggestions[:3], 1):
                print(f"  {i}. {s.get('suggestion', s.get('type', '未知'))}")
        
        self.results["refactoring"] = {
            "files_analyzed": len(task_ids),
            "completed": stats['completed'],
            "suggestions": len(suggestions)
        }
    
    def test_2_bug_injection_detection(self):
        """测试2: Bug注入与检测 - 创建有Bug的代码让系统检测"""
        print("\n" + "=" * 70)
        print("测试2: Bug注入与检测")
        print("=" * 70)
        
        # 创建包含各种Bug的测试文件
        buggy_code = '''
"""
Bug注入测试文件 - 包含多种常见Bug
"""
import threading

# Bug 1: 竞态条件
class Counter:
    def __init__(self):
        self.value = 0
    
    def increment(self):
        # Bug: 没有锁保护
        current = self.value
        current += 1
        self.value = current

# Bug 2: 资源泄漏
def read_file_bad(path):
    f = open(path, 'r')
    data = f.read()
    # Bug: 没有关闭文件
    return data

# Bug 3: 空指针风险
def process_data(data):
    # Bug: 没有检查data是否为None
    return data.strip().lower()

# Bug 4: 无限循环风险
def find_item(items, target):
    i = 0
    while i < len(items):
        if items[i] == target:
            return i
        # Bug: 没有递增i
    return -1

# Bug 5: 异常吞没
def safe_divide(a, b):
    try:
        return a / b
    except:
        # Bug: 吞没所有异常，没有日志
        return 0

# Bug 6: 默认可变参数
def add_item(item, items=[]):
    # Bug: 可变默认参数
    items.append(item)
    return items

# Bug 7: 未处理的边界条件
def get_first(items):
    # Bug: 空列表会抛出IndexError
    return items[0]

# Bug 8: 类型混淆
def calculate(x, y):
    # Bug: 没有类型检查，可能传入字符串
    return x + y
'''
        
        buggy_file = "/tmp/buggy_code.py"
        with open(buggy_file, 'w') as f:
            f.write(buggy_code)
        
        print(f"创建了包含8种Bug的测试文件: {buggy_file}")
        
        # 提交Bug检测任务
        task = Task(
            id=f"bug_detect_{int(time.time()*1000)%100000}",
            task_type=TaskType.BUG_FIX,
            description="检测代码中的Bug和潜在问题",
            target_files=[buggy_file],
            priority=5
        )
        
        task_id = self.agent.executor.submit_task(task)
        print(f"提交Bug检测任务: {task_id}")
        
        # 等待处理
        time.sleep(8)
        
        # 检查结果
        result = self.agent.executor.get_task_result(task_id)
        if result:
            print(f"任务状态: {result['status']}")
            if result.get('result'):
                issues = result['result'].get('issues_found', [])
                print(f"检测到 {len(issues)} 个问题")
                for issue in issues[:5]:
                    print(f"  - 行{issue.get('line', '?')}: {issue.get('message', issue)}")
        
        # 验证知识图谱是否记录了这次经验
        similar = self.agent.knowledge_graph.get_similar_experiences("检测代码Bug", limit=3)
        print(f"知识图谱中找到 {len(similar)} 条相关经验")
        
        self.results["bug_detection"] = {
            "bugs_injected": 8,
            "task_submitted": True,
            "experiences_recorded": len(similar) > 0
        }
    
    def test_3_cross_module_dependency(self):
        """测试3: 跨模块依赖分析"""
        print("\n" + "=" * 70)
        print("测试3: 跨模块依赖分析")
        print("=" * 70)
        
        # 分析所有核心模块的依赖关系
        modules = {
            "executor_v2": "/workspace/persistent_agent/core/executor_v2.py",
            "knowledge_graph": "/workspace/persistent_agent/core/knowledge_graph.py",
            "optimizer": "/workspace/persistent_agent/core/optimizer.py",
            "watchdog": "/workspace/persistent_agent/core/watchdog.py",
            "main": "/workspace/persistent_agent/main.py"
        }
        
        # 分析import语句
        dependencies = {}
        for name, path in modules.items():
            if os.path.exists(path):
                with open(path, 'r') as f:
                    content = f.read()
                
                # 提取import语句
                imports = []
                for line in content.split('\n'):
                    line = line.strip()
                    if line.startswith('from ') or line.startswith('import '):
                        # 简化处理
                        if 'core.' in line or 'config' in line:
                            imports.append(line)
                
                dependencies[name] = imports
                print(f"  {name}: {len(imports)} 个内部依赖")
        
        # 检测循环依赖
        print("\n检测循环依赖...")
        circular = self._detect_circular_dependency(dependencies)
        if circular:
            print(f"  ⚠️ 发现潜在循环依赖: {circular}")
            self.warnings.append(f"循环依赖: {circular}")
        else:
            print("  ✓ 未发现循环依赖")
        
        self.results["dependencies"] = {
            "modules": len(modules),
            "circular_found": len(circular) > 0 if circular else False
        }
    
    def _detect_circular_dependency(self, dependencies):
        """简单的循环依赖检测"""
        # 构建依赖图
        graph = {}
        for name, imports in dependencies.items():
            deps = []
            for imp in imports:
                for other in dependencies.keys():
                    if other != name and other in imp:
                        deps.append(other)
            graph[name] = deps
        
        # 检测循环
        for node in graph:
            visited = set()
            path = []
            if self._has_cycle(graph, node, visited, path):
                return path
        
        return None
    
    def _has_cycle(self, graph, node, visited, path):
        if node in path:
            return True
        if node in visited:
            return False
        
        visited.add(node)
        path.append(node)
        
        for neighbor in graph.get(node, []):
            if self._has_cycle(graph, neighbor, visited, path):
                return True
        
        path.pop()
        return False
    
    def test_4_performance_bottleneck(self):
        """测试4: 性能瓶颈探测"""
        print("\n" + "=" * 70)
        print("测试4: 性能瓶颈探测")
        print("=" * 70)
        
        # 4.1 任务提交性能
        print("\n--- 4.1 任务提交性能 ---")
        start = time.time()
        task_count = 500
        for i in range(task_count):
            task = Task(
                id=f"perf_{i}_{int(time.time()*1000)%100000}",
                task_type=TaskType.ANALYZE,
                description=f"性能测试任务 {i}",
                target_files=["/workspace/persistent_agent/config.py"],
                priority=random.randint(1, 5)
            )
            self.agent.executor.submit_task(task)
        
        submit_time = time.time() - start
        submit_rate = task_count / submit_time
        print(f"  提交 {task_count} 个任务, 耗时 {submit_time:.2f}s")
        print(f"  提交速率: {submit_rate:.0f} 任务/秒")
        
        if submit_rate < 100:
            self.warnings.append(f"任务提交速率较低: {submit_rate:.0f}/秒")
        
        # 4.2 知识图谱写入性能
        print("\n--- 4.2 知识图谱写入性能 ---")
        start = time.time()
        write_count = 200
        for i in range(write_count):
            exp = Experience(
                id=f"perf_exp_{i}_{random.randint(1000,9999)}",
                task_type="bug_fix",
                context="性能测试",
                problem=f"测试问题 {i}",
                solution="测试解决方案",
                outcome="success",
                lessons="测试教训",
                code_snippets=[],
                tags=["performance"],
                created_at=datetime.now().isoformat()
            )
            self.agent.knowledge_graph.add_experience(exp)
        
        write_time = time.time() - start
        write_rate = write_count / write_time
        print(f"  写入 {write_count} 条经验, 耗时 {write_time:.2f}s")
        print(f"  写入速率: {write_rate:.0f} 条/秒")
        
        if write_rate < 50:
            self.warnings.append(f"知识图谱写入速率较低: {write_rate:.0f}/秒")
        
        # 4.3 搜索性能
        print("\n--- 4.3 搜索性能 ---")
        start = time.time()
        search_count = 100
        for i in range(search_count):
            self.agent.knowledge_graph.search_experiences(f"测试问题 {i % 50}")
        
        search_time = time.time() - start
        search_rate = search_count / search_time
        print(f"  执行 {search_count} 次搜索, 耗时 {search_time:.2f}s")
        print(f"  搜索速率: {search_rate:.0f} 次/秒")
        
        # 等待任务处理完成
        time.sleep(15)
        
        # 4.4 最终状态
        status = self.agent.executor.get_status()
        print(f"\n--- 4.4 执行器状态 ---")
        print(f"  完成: {status['stats']['completed']}")
        print(f"  失败: {status['stats']['failed']}")
        print(f"  队列: {status['queue_size']}")
        
        self.results["performance"] = {
            "submit_rate": f"{submit_rate:.0f}/秒",
            "write_rate": f"{write_rate:.0f}/秒",
            "search_rate": f"{search_rate:.0f}/秒",
            "total_completed": status['stats']['completed']
        }
    
    def test_5_exception_recovery(self):
        """测试5: 异常恢复测试"""
        print("\n" + "=" * 70)
        print("测试5: 异常恢复测试")
        print("=" * 70)
        
        # 5.1 测试无效输入
        print("\n--- 5.1 无效输入测试 ---")
        invalid_inputs = [
            ("None描述", None),
            ("空字符串", ""),
            ("超长字符串", "A" * 100000),
        ]
        
        for name, value in invalid_inputs:
            try:
                if value is None:
                    # 测试None处理
                    results = self.agent.knowledge_graph.get_similar_experiences(None)
                elif value == "":
                    results = self.agent.knowledge_graph.get_similar_experiences("")
                else:
                    results = self.agent.knowledge_graph.get_similar_experiences(value[:1000])
                print(f"  ✓ {name}: 正常处理")
            except Exception as e:
                print(f"  ✗ {name}: {type(e).__name__}: {str(e)[:50]}")
                self.bugs_found.append(f"异常恢复测试 - {name}: {e}")
        
        # 5.2 测试并发异常
        print("\n--- 5.2 并发异常测试 ---")
        errors = []
        
        def stress_test(thread_id):
            try:
                for i in range(20):
                    # 随机执行各种操作
                    op = random.randint(1, 4)
                    if op == 1:
                        self.agent.executor.get_status()
                    elif op == 2:
                        self.agent.knowledge_graph.get_statistics()
                    elif op == 3:
                        self.agent.knowledge_graph.search_experiences("测试")
                    else:
                        self.agent.get_status()
            except Exception as e:
                errors.append(f"T{thread_id}: {e}")
        
        threads = [threading.Thread(target=stress_test, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        if errors:
            print(f"  ✗ 发现 {len(errors)} 个并发错误")
            self.bugs_found.extend(errors[:3])
        else:
            print("  ✓ 200次并发操作全部成功")
        
        self.results["exception_recovery"] = {
            "invalid_input_tests": len(invalid_inputs),
            "concurrent_operations": 200,
            "errors": len(errors)
        }
    
    def test_6_resource_leak_detection(self):
        """测试6: 资源泄漏检测"""
        print("\n" + "=" * 70)
        print("测试6: 资源泄漏检测")
        print("=" * 70)
        
        # 获取初始状态
        initial_status = self.agent.executor.get_status()
        initial_kg = self.agent.knowledge_graph.get_statistics()
        
        # 执行大量操作
        print("\n执行大量操作...")
        for i in range(100):
            task = Task(
                id=f"leak_test_{i}_{int(time.time()*1000)%100000}",
                task_type=TaskType.ANALYZE,
                description=f"资源泄漏测试 {i}",
                target_files=["/workspace/persistent_agent/config.py"],
                priority=1
            )
            self.agent.executor.submit_task(task)
        
        time.sleep(10)
        
        # 检查状态
        final_status = self.agent.executor.get_status()
        final_kg = self.agent.knowledge_graph.get_statistics()
        
        # 检查completed_tasks是否无限增长
        completed_count = final_status.get('completed_tasks', 0)
        print(f"  完成任务数: {completed_count}")
        
        # 检查active_tasks是否清理
        active_count = final_status.get('active_tasks', 0)
        print(f"  活跃任务数: {active_count}")
        
        # 检查经验数
        exp_count = final_kg.get('total_experiences', 0)
        print(f"  经验总数: {exp_count}")
        
        # 检查内存（如果psutil可用）
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / 1024 / 1024
            print(f"  内存使用: {mem_mb:.1f} MB")
            
            if mem_mb > 500:
                self.warnings.append(f"内存使用较高: {mem_mb:.1f}MB")
        except ImportError:
            pass
        
        self.results["resource_leak"] = {
            "completed_tasks": completed_count,
            "active_tasks": active_count,
            "experiences": exp_count
        }
    
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "=" * 70)
        print("真实运行测试报告")
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
        
        report_path = "/workspace/persistent_agent/logs/real_world_test_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n报告已保存: {report_path}")
        
        return len(self.bugs_found) == 0


def main():
    tester = RealWorldTest()
    success = tester.run_all_tests()
    
    print("\n" + "=" * 70)
    if success:
        print("✓ 所有真实运行测试通过!")
    else:
        print(f"✗ 发现 {len(tester.bugs_found)} 个Bug需要修复")
    print("=" * 70)
    
    return success


if __name__ == "__main__":
    main()
