"""
压力测试脚本 - 高强度实践测试
用于发现系统缺陷和性能瓶颈
"""
import sys
import os
import time
import threading
import random
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType


class StressTester:
    """压力测试器"""
    
    def __init__(self):
        self.agent = PersistentAgent(work_dir="/workspace/persistent_agent")
        self.results = {
            "tests": [],
            "errors": [],
            "performance": {}
        }
    
    def test_1_concurrent_task_storm(self, num_tasks=50):
        """测试1: 高并发任务风暴"""
        print("\n" + "="*60)
        print("测试1: 高并发任务风暴")
        print(f"提交 {num_tasks} 个并发任务...")
        print("="*60)
        
        task_ids = []
        start_time = time.time()
        
        # 快速提交大量任务
        for i in range(num_tasks):
            try:
                task_type = random.choice([
                    TaskType.ANALYZE,
                    TaskType.BUG_FIX,
                    TaskType.REFACTOR
                ])
                
                task = Task(
                    id=f"stress_task_{i}_{int(time.time()*1000)%10000}",
                    task_type=task_type,
                    description=f"压力测试任务 #{i}: {task_type.value}",
                    target_files=["main.py", "config.py"],
                    priority=random.randint(1, 5)
                )
                
                task_id = self.agent.executor.submit_task(task)
                task_ids.append(task_id)
                
                if i % 10 == 0:
                    print(f"  已提交 {i+1}/{num_tasks} 个任务")
                    
            except Exception as e:
                error_msg = f"提交任务 {i} 失败: {str(e)}"
                print(f"  ✗ {error_msg}")
                self.results["errors"].append({
                    "test": "concurrent_task_storm",
                    "error": error_msg,
                    "traceback": traceback.format_exc()
                })
        
        submit_time = time.time() - start_time
        print(f"\n提交完成: {len(task_ids)}/{num_tasks} 成功")
        print(f"提交耗时: {submit_time:.2f}秒")
        
        # 等待任务处理
        print("\n等待任务处理...")
        time.sleep(10)
        
        # 检查状态
        status = self.agent.executor.get_status()
        print(f"\n执行器状态:")
        print(f"  - 运行中: {status['running']}")
        print(f"  - 工作线程: {status['workers']}")
        print(f"  - 待处理: {status['pending_tasks']}")
        print(f"  - 运行中任务: {status['running_tasks']}")
        print(f"  - 已完成: {status['completed_tasks']}")
        print(f"  - 统计: {status['stats']}")
        
        self.results["tests"].append({
            "name": "concurrent_task_storm",
            "tasks_submitted": len(task_ids),
            "submit_time": submit_time,
            "status": status
        })
        
        return len(task_ids) == num_tasks
    
    def test_2_complex_code_analysis(self):
        """测试2: 复杂代码分析"""
        print("\n" + "="*60)
        print("测试2: 复杂代码分析")
        print("="*60)
        
        # 创建复杂测试文件
        complex_code = self._generate_complex_code()
        test_file = "/tmp/complex_test.py"
        
        with open(test_file, 'w') as f:
            f.write(complex_code)
        
        print(f"创建测试文件: {test_file} ({len(complex_code)} 字符)")
        
        # 提交分析任务
        start_time = time.time()
        task_id = self.agent.analyze_code([test_file], "复杂代码深度分析")
        
        # 等待处理
        time.sleep(5)
        
        elapsed = time.time() - start_time
        print(f"分析耗时: {elapsed:.2f}秒")
        
        # 检查知识图谱是否记录了经验
        similar = self.agent.knowledge_graph.get_similar_experiences("复杂代码分析")
        print(f"知识图谱中找到 {len(similar)} 条相关经验")
        
        self.results["tests"].append({
            "name": "complex_code_analysis",
            "file_size": len(complex_code),
            "analysis_time": elapsed,
            "experiences_found": len(similar)
        })
        
        return True
    
    def test_3_memory_pressure(self):
        """测试3: 内存压力测试"""
        print("\n" + "="*60)
        print("测试3: 内存压力测试")
        print("="*60)
        
        # 创建大量大文件
        large_files = []
        for i in range(20):
            file_path = f"/tmp/large_file_{i}.py"
            # 创建约100KB的文件
            with open(file_path, 'w') as f:
                for j in range(2000):
                    f.write(f"# Line {j}: This is a comment to make the file larger\n")
                    f.write(f"def function_{i}_{j}(x, y, z):\n")
                    f.write(f"    return x + y + z + {j}\n\n")
            large_files.append(file_path)
        
        print(f"创建 {len(large_files)} 个大文件")
        
        # 同时分析所有文件
        start_time = time.time()
        task_id = self.agent.analyze_code(large_files, "大规模文件分析")
        
        time.sleep(15)
        
        elapsed = time.time() - start_time
        print(f"分析耗时: {elapsed:.2f}秒")
        
        # 检查内存使用
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            print(f"当前内存使用: {memory_mb:.1f} MB")
            
            if memory_mb > 500:
                print("  ⚠️ 内存使用过高!")
        except:
            pass
        
        self.results["tests"].append({
            "name": "memory_pressure",
            "files_analyzed": len(large_files),
            "analysis_time": elapsed
        })
        
        return True
    
    def test_4_rapid_start_stop(self):
        """测试4: 快速启停测试"""
        print("\n" + "="*60)
        print("测试4: 快速启停测试")
        print("="*60)
        
        for i in range(3):
            print(f"\n第 {i+1} 轮启停...")
            
            # 启动
            start_time = time.time()
            self.agent.executor.start(num_workers=2)
            self.agent.optimizer.start()
            
            # 提交几个任务
            for j in range(5):
                self.agent.analyze_code(["main.py"], f"快速测试 {j}")
            
            time.sleep(2)
            
            # 停止
            self.agent.optimizer.stop()
            self.agent.executor.stop()
            
            elapsed = time.time() - start_time
            print(f"  本轮耗时: {elapsed:.2f}秒")
        
        print("\n快速启停测试完成")
        
        self.results["tests"].append({
            "name": "rapid_start_stop",
            "cycles": 3
        })
        
        return True
    
    def test_5_knowledge_graph_stress(self):
        """测试5: 知识图谱压力测试"""
        print("\n" + "="*60)
        print("测试5: 知识图谱压力测试")
        print("="*60)
        
        from core.knowledge_graph import Experience
        
        # 批量添加经验
        start_time = time.time()
        added_count = 0
        
        for i in range(100):
            try:
                exp = Experience(
                    id=f"stress_exp_{i}_{int(time.time()*1000)%10000}",
                    task_type=random.choice(["bug_fix", "refactor", "analyze"]),
                    context=f"压力测试上下文 {i}",
                    problem=f"测试问题 {i}: " + "A" * 100,  # 长文本
                    solution=f"测试解决方案 {i}: " + "B" * 200,
                    outcome=random.choice(["success", "failure"]),
                    lessons=f"经验教训 {i}",
                    code_snippets=[f"code_{j}" for j in range(10)],
                    tags=[f"tag_{j}" for j in range(5)],
                    created_at=datetime.now().isoformat()
                )
                
                self.agent.knowledge_graph.add_experience(exp)
                added_count += 1
                
                if i % 20 == 0:
                    print(f"  已添加 {i+1}/100 条经验")
                    
            except Exception as e:
                error_msg = f"添加经验 {i} 失败: {str(e)}"
                print(f"  ✗ {error_msg}")
                self.results["errors"].append({
                    "test": "knowledge_graph_stress",
                    "error": error_msg
                })
        
        elapsed = time.time() - start_time
        print(f"\n添加完成: {added_count}/100 成功")
        print(f"耗时: {elapsed:.2f}秒")
        
        # 测试搜索性能
        print("\n测试搜索性能...")
        search_start = time.time()
        
        for i in range(20):
            results = self.agent.knowledge_graph.search_experiences(
                f"测试问题 {random.randint(0, 99)}",
                limit=10
            )
        
        search_elapsed = time.time() - search_start
        print(f"20次搜索耗时: {search_elapsed:.2f}秒 (平均 {search_elapsed/20:.3f}秒/次)")
        
        # 获取统计
        stats = self.agent.knowledge_graph.get_statistics()
        print(f"\n知识图谱统计:")
        print(f"  - 总经验数: {stats['total_experiences']}")
        print(f"  - 平均成功率: {stats['average_success_rate']:.2f}")
        
        self.results["tests"].append({
            "name": "knowledge_graph_stress",
            "experiences_added": added_count,
            "add_time": elapsed,
            "search_time": search_elapsed
        })
        
        return added_count == 100
    
    def _generate_complex_code(self):
        """生成复杂测试代码"""
        code = '''"""
复杂测试模块 - 用于压力测试
"""
import os
import sys
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

class Status(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class DataItem:
    id: int
    name: str
    value: float
    metadata: Dict[str, Any]

class ComplexProcessor:
    """复杂处理器类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.items: List[DataItem] = []
        self.cache: Dict[int, DataItem] = {}
        self._initialized = False
    
    def initialize(self) -> bool:
        """初始化处理器"""
        if self._initialized:
            return False
        
        # 复杂的初始化逻辑
        for i in range(1000):
            item = DataItem(
                id=i,
                name=f"item_{i}",
                value=float(i) * 1.5,
                metadata={"index": i, "created": True}
            )
            self.items.append(item)
            self.cache[i] = item
        
        self._initialized = True
        return True
    
    def process_batch(self, batch_size: int = 100) -> List[DataItem]:
        """批量处理"""
        results = []
        
        for i in range(0, len(self.items), batch_size):
            batch = self.items[i:i+batch_size]
            
            for item in batch:
                # 复杂处理逻辑
                processed = self._process_item(item)
                if processed:
                    results.append(processed)
        
        return results
    
    def _process_item(self, item: DataItem) -> Optional[DataItem]:
        """处理单个项目"""
        if item.value < 0:
            return None
        
        # 复杂计算
        new_value = item.value ** 2 + item.id * 0.5
        
        return DataItem(
            id=item.id,
            name=item.name,
            value=new_value,
            metadata={**item.metadata, "processed": True}
        )
    
    def get_statistics(self) -> Dict[str, Union[int, float]]:
        """获取统计信息"""
        if not self.items:
            return {}
        
        values = [item.value for item in self.items]
        
        return {
            "count": len(self.items),
            "total": sum(values),
            "average": sum(values) / len(values),
            "max": max(values),
            "min": min(values)
        }
    
    def export_data(self, format: str = "json") -> str:
        """导出数据"""
        import json
        
        data = {
            "config": self.config,
            "items": [
                {
                    "id": item.id,
                    "name": item.name,
                    "value": item.value,
                    "metadata": item.metadata
                }
                for item in self.items
            ],
            "statistics": self.get_statistics()
        }
        
        return json.dumps(data, indent=2)

def main():
    """主函数"""
    config = {
        "debug": True,
        "batch_size": 100,
        "max_items": 10000
    }
    
    processor = ComplexProcessor(config)
    
    if processor.initialize():
        print("初始化成功")
        
        results = processor.process_batch()
        print(f"处理了 {len(results)} 个项目")
        
        stats = processor.get_statistics()
        print(f"统计: {stats}")
        
        data = processor.export_data()
        print(f"导出数据大小: {len(data)} 字符")
    else:
        print("初始化失败")

if __name__ == "__main__":
    main()
'''
        return code
    
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "="*60)
        print("压力测试报告")
        print("="*60)
        
        print(f"\n测试总数: {len(self.results['tests'])}")
        print(f"错误总数: {len(self.results['errors'])}")
        
        if self.results['errors']:
            print("\n发现的问题:")
            for i, error in enumerate(self.results['errors'], 1):
                print(f"\n{i}. [{error['test']}]")
                print(f"   错误: {error['error'][:100]}")
        
        print("\n各测试详情:")
        for test in self.results['tests']:
            print(f"\n- {test['name']}:")
            for key, value in test.items():
                if key != 'name':
                    print(f"    {key}: {value}")
        
        return self.results


def main():
    """运行压力测试"""
    print("="*60)
    print("极简全能持续运行Agent - 压力测试")
    print("="*60)
    
    tester = StressTester()
    
    # 运行所有测试
    tester.test_1_concurrent_task_storm(num_tasks=50)
    tester.test_2_complex_code_analysis()
    tester.test_3_memory_pressure()
    tester.test_4_rapid_start_stop()
    tester.test_5_knowledge_graph_stress()
    
    # 生成报告
    results = tester.generate_report()
    
    # 保存报告
    import json
    report_file = "/workspace/persistent_agent/logs/stress_test_report.json"
    with open(report_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n报告已保存到: {report_file}")
    
    return results


if __name__ == "__main__":
    main()
