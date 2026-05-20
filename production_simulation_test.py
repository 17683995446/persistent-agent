"""
第七轮生产环境模拟测试 - 万级数据 + 多Agent + 极端故障
"""
import sys
import os
import time
import random
import threading
import signal
import subprocess
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType, TaskStatus
from core.knowledge_graph import Experience, get_knowledge_graph


class ProductionSimulationTest:
    """生产环境模拟测试器"""
    
    def __init__(self):
        self.bugs_found = []
        self.warnings = []
        self.results = {}
    
    def run_all_tests(self):
        """运行所有生产环境模拟测试"""
        print("=" * 70)
        print("第七轮生产环境模拟测试")
        print("=" * 70)
        
        # 测试1: 多Agent并发
        self.test_1_multi_agent_concurrency()
        
        # 测试2: 真实项目分析
        self.test_2_real_project_analysis()
        
        # 测试3: 万级知识库压力
        self.test_3_massive_knowledge_base()
        
        # 测试4: 极端故障恢复
        self.test_4_extreme_fault_recovery()
        
        # 测试5: 性能基准测试
        self.test_5_performance_benchmark()
        
        # 生成报告
        self.generate_report()
    
    def test_1_multi_agent_concurrency(self):
        """测试1: 多Agent并发 - 检测资源共享冲突"""
        print("\n" + "=" * 70)
        print("测试1: 多Agent并发 (3个Agent同时运行)")
        print("=" * 70)
        
        # 创建3个独立的Agent实例，使用不同的工作目录
        agents = []
        work_dirs = [
            "/tmp/agent_test_1",
            "/tmp/agent_test_2", 
            "/tmp/agent_test_3"
        ]
        
        for i, work_dir in enumerate(work_dirs):
            try:
                agent = PersistentAgent(work_dir=work_dir)
                agents.append(agent)
                print(f"  Agent {i+1} 创建成功: {work_dir}")
            except Exception as e:
                print(f"  ✗ Agent {i+1} 创建失败: {e}")
                self.bugs_found.append(f"多Agent创建失败: {e}")
        
        if len(agents) < 3:
            print("  ⚠️ 无法创建足够的Agent，跳过此测试")
            return
        
        # 每个Agent提交任务
        print("\n  每个Agent提交100个任务...")
        errors = []
        
        def agent_work(agent_id, agent):
            local_errors = []
            for i in range(100):
                try:
                    task = Task(
                        id=f"multi_agent_{agent_id}_{i}_{int(time.time()*1000)%100000}",
                        task_type=random.choice([TaskType.ANALYZE, TaskType.BUG_FIX]),
                        description=f"多Agent测试 {agent_id}-{i}",
                        target_files=["/workspace/persistent_agent/config.py"],
                        priority=random.randint(1, 5)
                    )
                    agent.executor.submit_task(task)
                except Exception as e:
                    local_errors.append(f"Agent{agent_id}-{i}: {e}")
            return local_errors
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(agent_work, i, agents[i]) for i in range(3)]
            for future in as_completed(futures):
                errors.extend(future.result())
        
        print(f"  任务提交完成，错误: {len(errors)}")
        
        # 等待处理
        time.sleep(20)
        
        # 检查每个Agent的状态
        for i, agent in enumerate(agents):
            status = agent.executor.get_status()
            kg = agent.knowledge_graph.get_statistics()
            print(f"  Agent {i+1}: 完成={status['stats']['completed']}, 失败={status['stats']['failed']}, 经验={kg['total_experiences']}")
            agent.stop()
        
        if errors:
            self.bugs_found.extend(errors[:5])
        
        self.results["multi_agent"] = {
            "agents": len(agents),
            "tasks_per_agent": 100,
            "errors": len(errors)
        }
    
    def test_2_real_project_analysis(self):
        """测试2: 真实项目分析 - 分析开源项目代码"""
        print("\n" + "=" * 70)
        print("测试2: 真实项目分析")
        print("=" * 70)
        
        # 创建模拟的开源项目结构
        project_dir = "/tmp/mock_open_source_project"
        if os.path.exists(project_dir):
            shutil.rmtree(project_dir)
        os.makedirs(project_dir)
        
        # 创建多个模块文件
        modules = {
            "core.py": '''
"""核心模块"""
import json
import os
from typing import Dict, Any

class CoreEngine:
    """核心引擎类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data = {}
    
    def process(self, input_data: str) -> str:
        """处理输入数据"""
        if not input_data:
            return ""
        
        # 复杂处理逻辑
        result = input_data.strip().lower()
        result = result.replace("  ", " ")
        
        return result
    
    def save(self, path: str):
        """保存数据"""
        with open(path, 'w') as f:
            json.dump(self.data, f)
''',
            "utils.py": '''
"""工具模块"""
import re
import hashlib
from datetime import datetime

def validate_email(email: str) -> bool:
    """验证邮箱格式"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def hash_string(s: str) -> str:
    """计算字符串哈希"""
    return hashlib.md5(s.encode()).hexdigest()

def format_date(dt: datetime) -> str:
    """格式化日期"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")
''',
            "api.py": '''
"""API模块"""
from typing import List, Optional

class APIClient:
    """API客户端"""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.session = None
    
    def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """GET请求"""
        url = f"{self.base_url}{endpoint}"
        # 模拟请求
        return {"status": "ok", "data": []}
    
    def post(self, endpoint: str, data: dict) -> dict:
        """POST请求"""
        url = f"{self.base_url}{endpoint}"
        # 模拟请求
        return {"status": "ok", "id": 123}
''',
            "models.py": '''
"""数据模型模块"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class User:
    id: int
    name: str
    email: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    def is_active(self) -> bool:
        return self.id > 0

@dataclass
class Post:
    id: int
    title: str
    content: str
    author_id: int
    created_at: datetime
    
    def summary(self, length: int = 100) -> str:
        return self.content[:length]
''',
            "main.py": '''
"""主入口模块"""
import sys
import argparse
from pathlib import Path

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Mock Project")
    parser.add_argument("--config", help="配置文件路径")
    parser.add_argument("--verbose", action="store_true", help="详细输出")
    
    args = parser.parse_args()
    
    # 初始化
    print("Starting mock project...")
    
    # 主循环
    try:
        while True:
            # 处理任务
            pass
    except KeyboardInterrupt:
        print("\\nShutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()
'''
        }
        
        for filename, content in modules.items():
            with open(f"{project_dir}/{filename}", 'w') as f:
                f.write(content)
        
        print(f"  创建了模拟开源项目: {project_dir}")
        print(f"  模块数: {len(modules)}")
        
        # 创建Agent分析项目
        agent = PersistentAgent(work_dir=project_dir)
        
        # 分析每个模块
        task_ids = []
        for filename in modules.keys():
            task = Task(
                id=f"real_project_{filename}_{int(time.time()*1000)%100000}",
                task_type=TaskType.ANALYZE,
                description=f"分析开源项目模块: {filename}",
                target_files=[f"{project_dir}/{filename}"],
                priority=3
            )
            tid = agent.executor.submit_task(task)
            task_ids.append(tid)
        
        print(f"  提交了 {len(task_ids)} 个分析任务")
        
        # 等待处理
        time.sleep(15)
        
        # 检查结果
        status = agent.executor.get_status()
        print(f"  处理结果: 完成={status['stats']['completed']}, 失败={status['stats']['failed']}")
        
        # 获取改进建议
        suggestions = agent.get_improvement_suggestions()
        print(f"  改进建议: {len(suggestions)} 条")
        
        agent.stop()
        
        self.results["real_project"] = {
            "modules": len(modules),
            "tasks": len(task_ids),
            "completed": status['stats']['completed'],
            "suggestions": len(suggestions)
        }
    
    def test_3_massive_knowledge_base(self):
        """测试3: 万级知识库压力 - 10000条经验存储和检索"""
        print("\n" + "=" * 70)
        print("测试3: 万级知识库压力 (10000条经验)")
        print("=" * 70)
        
        agent = PersistentAgent(work_dir="/tmp/massive_kb_test")
        
        # 批量添加经验
        print("  添加10000条经验...")
        start_time = time.time()
        
        batch_size = 1000
        for batch in range(10):
            batch_start = time.time()
            for i in range(batch_size):
                exp = Experience(
                    id=f"massive_{batch}_{i}_{random.randint(1000,9999)}",
                    task_type=random.choice(["bug_fix", "refactor", "analyze", "implement", "optimize"]),
                    context=f"批量测试批次{batch}",
                    problem=f"测试问题 {batch}-{i}: " + random.choice([
                        "空指针异常导致崩溃",
                        "内存泄漏导致OOM",
                        "竞态条件导致数据不一致",
                        "SQL注入安全风险",
                        "性能瓶颈需要优化"
                    ]),
                    solution=random.choice([
                        "添加空值检查",
                        "使用with语句管理资源",
                        "添加线程锁保护",
                        "使用参数化查询",
                        "使用缓存减少重复计算"
                    ]),
                    outcome=random.choice(["success", "success", "success", "failure"]),
                    lessons=random.choice([
                        "始终检查边界条件",
                        "资源使用后要释放",
                        "并发访问要加锁",
                        "用户输入要验证",
                        "性能问题要 profiling"
                    ]),
                    code_snippets=[],
                    tags=[f"tag_{j}" for j in range(random.randint(1, 5))],
                    created_at=datetime.now().isoformat()
                )
                agent.knowledge_graph.add_experience(exp)
            
            batch_time = time.time() - batch_start
            print(f"    批次 {batch+1}/10: {batch_size}条, 耗时 {batch_time:.2f}s, 速率 {batch_size/batch_time:.0f}条/秒")
        
        total_time = time.time() - start_time
        print(f"\n  写入完成: 10000条, 总耗时 {total_time:.2f}s, 平均速率 {10000/total_time:.0f}条/秒")
        
        # 验证统计
        stats = agent.knowledge_graph.get_statistics()
        print(f"  知识库统计: {stats['total_experiences']} 条经验")
        
        # 测试检索性能
        print("\n  测试检索性能...")
        search_queries = [
            "空指针异常",
            "内存泄漏",
            "竞态条件",
            "SQL注入",
            "性能优化"
        ]
        
        search_start = time.time()
        for query in search_queries:
            results = agent.knowledge_graph.search_experiences(query, limit=10)
        search_time = time.time() - search_start
        
        print(f"  5次搜索耗时: {search_time:.3f}s, 平均 {search_time/5*1000:.1f}ms/次")
        
        # 测试相似度检索
        print("\n  测试相似度检索...")
        similar_start = time.time()
        similar = agent.knowledge_graph.get_similar_experiences("空指针异常导致程序崩溃", limit=5)
        similar_time = time.time() - similar_start
        
        print(f"  相似度检索: 找到 {len(similar)} 条, 耗时 {similar_time*1000:.1f}ms")
        
        agent.stop()
        
        self.results["massive_kb"] = {
            "experiences": 10000,
            "write_rate": f"{10000/total_time:.0f}条/秒",
            "search_time": f"{search_time/5*1000:.1f}ms",
            "similar_time": f"{similar_time*1000:.1f}ms"
        }
    
    def test_4_extreme_fault_recovery(self):
        """测试4: 极端故障恢复 - 模拟kill -9"""
        print("\n" + "=" * 70)
        print("测试4: 极端故障恢复 (模拟kill -9)")
        print("=" * 70)
        
        # 创建一个独立的测试进程
        test_script = '''
import sys
sys.path.insert(0, '/workspace/persistent_agent')

from main import PersistentAgent
from core.executor_v2 import Task, TaskType
import time

agent = PersistentAgent(work_dir='/tmp/fault_test')

# 提交一些任务
for i in range(20):
    task = Task(
        id=f'fault_test_{i}',
        task_type=TaskType.ANALYZE,
        description=f'故障恢复测试 {i}',
        target_files=['/workspace/persistent_agent/config.py'],
        priority=3
    )
    agent.executor.submit_task(task)

print('TASKS_SUBMITTED')

# 等待一段时间
time.sleep(5)

# 记录状态
status = agent.executor.get_status()
print(f'STATUS: completed={status["stats"]["completed"]}, failed={status["stats"]["failed"]}')

# 模拟崩溃 - 不调用stop()
import os
os._exit(1)
'''
        
        script_path = "/tmp/fault_test_script.py"
        with open(script_path, 'w') as f:
            f.write(test_script)
        
        print("  启动测试进程...")
        
        # 启动进程
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # 等待任务提交完成
        stdout, stderr = proc.communicate(timeout=15)
        
        if "TASKS_SUBMITTED" in stdout:
            print("  ✓ 任务已提交")
        else:
            print(f"  ✗ 任务提交失败: {stdout}")
        
        # 检查进程是否崩溃
        if proc.returncode != 0:
            print(f"  ✓ 进程已崩溃 (exit code: {proc.returncode})")
        
        # 等待一下确保文件写入完成
        time.sleep(2)
        
        # 创建新Agent，检查是否能恢复
        print("\n  创建新Agent检查恢复...")
        try:
            agent = PersistentAgent(work_dir="/tmp/fault_test")
            status = agent.executor.get_status()
            kg = agent.knowledge_graph.get_statistics()
            
            print(f"  恢复后状态: 完成={status['stats']['completed']}, 经验={kg['total_experiences']}")
            
            # 检查是否有任务被恢复
            if status['stats']['completed'] > 0 or status['stats']['pending'] > 0:
                print("  ✓ 任务恢复成功")
            else:
                print("  ⚠️ 未检测到任务恢复 (可能任务已完成或恢复机制有问题)")
            
            agent.stop()
        except Exception as e:
            print(f"  ✗ 恢复失败: {e}")
            self.bugs_found.append(f"故障恢复失败: {e}")
        
        self.results["fault_recovery"] = {
            "tasks_submitted": 20,
            "process_crashed": proc.returncode != 0
        }
    
    def test_5_performance_benchmark(self):
        """测试5: 性能基准测试"""
        print("\n" + "=" * 70)
        print("测试5: 性能基准测试")
        print("=" * 70)
        
        agent = PersistentAgent(work_dir="/tmp/perf_benchmark")
        
        benchmarks = {}
        
        # 1. 任务提交基准
        print("\n  1. 任务提交基准 (1000任务)...")
        start = time.time()
        for i in range(1000):
            task = Task(
                id=f"perf_{i}_{int(time.time()*1000)%100000}",
                task_type=TaskType.ANALYZE,
                description=f"性能测试 {i}",
                target_files=["/workspace/persistent_agent/config.py"],
                priority=3
            )
            agent.executor.submit_task(task)
        
        submit_time = time.time() - start
        benchmarks["task_submit"] = {
            "count": 1000,
            "time": submit_time,
            "rate": 1000 / submit_time
        }
        print(f"     耗时: {submit_time:.2f}s, 速率: {1000/submit_time:.0f} 任务/秒")
        
        # 2. 知识图谱写入基准
        print("\n  2. 知识图谱写入基准 (1000条经验)...")
        start = time.time()
        for i in range(1000):
            exp = Experience(
                id=f"perf_exp_{i}_{random.randint(1000,9999)}",
                task_type="bug_fix",
                context="性能测试",
                problem=f"测试问题 {i}",
                solution="测试解决方案",
                outcome="success",
                lessons="测试教训",
                code_snippets=[],
                tags=["perf"],
                created_at=datetime.now().isoformat()
            )
            agent.knowledge_graph.add_experience(exp)
        
        write_time = time.time() - start
        benchmarks["kg_write"] = {
            "count": 1000,
            "time": write_time,
            "rate": 1000 / write_time
        }
        print(f"     耗时: {write_time:.2f}s, 速率: {1000/write_time:.0f} 条/秒")
        
        # 3. 知识图谱搜索基准
        print("\n  3. 知识图谱搜索基准 (100次搜索)...")
        start = time.time()
        for i in range(100):
            agent.knowledge_graph.search_experiences(f"测试问题 {i}", limit=10)
        
        search_time = time.time() - start
        benchmarks["kg_search"] = {
            "count": 100,
            "time": search_time,
            "rate": 100 / search_time
        }
        print(f"     耗时: {search_time:.2f}s, 速率: {100/search_time:.0f} 次/秒")
        
        # 4. 相似度检索基准
        print("\n  4. 相似度检索基准 (50次)...")
        start = time.time()
        for i in range(50):
            agent.knowledge_graph.get_similar_experiences(f"测试问题 {i}", limit=5)
        
        similar_time = time.time() - start
        benchmarks["kg_similar"] = {
            "count": 50,
            "time": similar_time,
            "rate": 50 / similar_time
        }
        print(f"     耗时: {similar_time:.2f}s, 速率: {50/similar_time:.0f} 次/秒")
        
        agent.stop()
        
        # 性能评估
        print("\n  性能评估:")
        if benchmarks["task_submit"]["rate"] < 50:
            self.warnings.append(f"任务提交速率较低: {benchmarks['task_submit']['rate']:.0f}/秒")
        if benchmarks["kg_write"]["rate"] < 100:
            self.warnings.append(f"知识图谱写入速率较低: {benchmarks['kg_write']['rate']:.0f}/秒")
        
        self.results["performance"] = {
            "task_submit": f"{benchmarks['task_submit']['rate']:.0f}/秒",
            "kg_write": f"{benchmarks['kg_write']['rate']:.0f}/秒",
            "kg_search": f"{benchmarks['kg_search']['rate']:.0f}/秒",
            "kg_similar": f"{benchmarks['kg_similar']['rate']:.0f}/秒"
        }
    
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "=" * 70)
        print("生产环境模拟测试报告")
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
        
        report_path = "/workspace/persistent_agent/logs/production_simulation_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n报告已保存: {report_path}")
        
        return len(self.bugs_found) == 0


def main():
    tester = ProductionSimulationTest()
    success = tester.run_all_tests()
    
    print("\n" + "=" * 70)
    if success:
        print("✓ 所有生产环境模拟测试通过!")
    else:
        print(f"✗ 发现 {len(tester.bugs_found)} 个Bug需要修复")
    print("=" * 70)
    
    return success


if __name__ == "__main__":
    main()
