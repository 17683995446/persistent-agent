"""
高强度压力测试 V2 - 验证修复效果
"""
import sys
import os
import time
import random
import threading
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType
from core.knowledge_graph import Experience


def run_stress_test():
    print("=" * 60)
    print("高强度压力测试 V2")
    print("=" * 60)
    
    agent = PersistentAgent(work_dir="/workspace/persistent_agent")
    
    # 创建测试文件
    test_files = []
    for i in range(10):
        path = f"/tmp/stress_test_file_{i}.py"
        with open(path, 'w') as f:
            f.write(f"def func_{i}():\n")
            for j in range(20):
                f.write(f"    x_{j} = {j}\n")
            f.write(f"    return x_0 + x_1\n\n")
            f.write(f"class Class_{i}:\n")
            f.write(f"    def method(self):\n")
            f.write(f"        return {i}\n")
        test_files.append(path)
    
    errors = []
    
    # ===== 测试1: 高并发任务风暴 =====
    print("\n--- 测试1: 高并发任务风暴 (100任务) ---")
    start = time.time()
    task_ids = []
    for i in range(100):
        try:
            task = Task(
                id=f"storm_{i}_{int(time.time()*1000)%100000}",
                task_type=random.choice([TaskType.ANALYZE, TaskType.BUG_FIX, TaskType.REFACTOR]),
                description=f"压力测试 #{i}",
                target_files=random.sample(test_files, min(3, len(test_files))),
                priority=random.randint(1, 5)
            )
            tid = agent.executor.submit_task(task)
            task_ids.append(tid)
        except Exception as e:
            errors.append(f"提交任务{i}: {e}")
    
    submit_time = time.time() - start
    print(f"  提交: {len(task_ids)}/100 成功, 耗时 {submit_time:.2f}s")
    
    # 等待处理
    time.sleep(15)
    
    status = agent.executor.get_status()
    stats = status["stats"]
    print(f"  统计: 完成={stats['completed']}, 失败={stats['failed']}, 超时={stats['timeout']}")
    print(f"  队列: {status['queue_size']}, 活跃: {status['active_tasks']}")
    
    success_rate = stats['completed'] / max(stats['completed'] + stats['failed'] + stats['timeout'], 1) * 100
    print(f"  成功率: {success_rate:.1f}%")
    
    if success_rate < 90:
        errors.append(f"高并发成功率过低: {success_rate:.1f}%")
    
    # ===== 测试2: 知识图谱压力 =====
    print("\n--- 测试2: 知识图谱压力 (200条经验) ---")
    start = time.time()
    added = 0
    for i in range(200):
        try:
            exp = Experience(
                id=f"kg_stress_{i}_{random.randint(1000,9999)}",
                task_type=random.choice(["bug_fix", "refactor", "analyze", "implement"]),
                context=f"压力测试上下文 {i}",
                problem=f"测试问题 {i}: 空指针异常导致程序崩溃",
                solution=f"解决方案 {i}: 添加空值检查并处理边界条件",
                outcome=random.choice(["success", "success", "success", "failure"]),
                lessons=f"经验教训 {i}",
                code_snippets=[],
                tags=[f"tag_{j}" for j in range(3)],
                created_at=datetime.now().isoformat()
            )
            agent.knowledge_graph.add_experience(exp)
            added += 1
        except Exception as e:
            errors.append(f"添加经验{i}: {e}")
    
    add_time = time.time() - start
    print(f"  写入: {added}/200 成功, 耗时 {add_time:.2f}s")
    
    # 搜索性能
    start = time.time()
    for i in range(50):
        agent.knowledge_graph.search_experiences(f"空指针异常", limit=10)
    search_time = time.time() - start
    print(f"  搜索: 50次耗时 {search_time:.2f}s (平均 {search_time/50*1000:.1f}ms/次)")
    
    # 中文检索
    similar = agent.knowledge_graph.get_similar_experiences("空指针崩溃")
    print(f"  中文相似检索: 找到 {len(similar)} 条")
    
    # ===== 测试3: 快速启停 =====
    print("\n--- 测试3: 快速启停 (5轮) ---")
    for i in range(5):
        try:
            agent.executor.stop()
            time.sleep(0.5)
            agent.executor.start(num_workers=2)
            time.sleep(0.5)
            print(f"  第{i+1}轮: ✓")
        except Exception as e:
            errors.append(f"启停第{i+1}轮: {e}")
            print(f"  第{i+1}轮: ✗ {e}")
    
    # ===== 测试4: 内存检查 =====
    print("\n--- 测试4: 内存检查 ---")
    try:
        import psutil
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / 1024 / 1024
        print(f"  当前内存: {mem_mb:.1f} MB")
        if mem_mb > 500:
            errors.append(f"内存使用过高: {mem_mb:.1f}MB")
            print(f"  ⚠️ 内存过高!")
        else:
            print(f"  ✓ 内存正常")
    except ImportError:
        print("  (psutil未安装, 跳过)")
    
    # ===== 测试5: 边界条件 =====
    print("\n--- 测试5: 边界条件 ---")
    
    # 空文件列表
    try:
        task = Task(
            id="edge_empty", task_type=TaskType.ANALYZE,
            description="空文件测试", target_files=[], priority=1
        )
        agent.executor.submit_task(task)
        time.sleep(2)
        print("  ✓ 空文件列表处理正常")
    except Exception as e:
        errors.append(f"空文件列表: {e}")
        print(f"  ✗ 空文件列表: {e}")
    
    # 不存在的文件
    try:
        task = Task(
            id="edge_missing", task_type=TaskType.ANALYZE,
            description="不存在文件测试", target_files=["/tmp/nonexistent_file_xyz.py"], priority=1
        )
        agent.executor.submit_task(task)
        time.sleep(2)
        print("  ✓ 不存在文件处理正常")
    except Exception as e:
        errors.append(f"不存在文件: {e}")
        print(f"  ✗ 不存在文件: {e}")
    
    # ===== 最终状态 =====
    agent.executor.stop()
    
    print("\n" + "=" * 60)
    print("压力测试报告")
    print("=" * 60)
    print(f"错误数: {len(errors)}")
    if errors:
        print("发现的问题:")
        for e in errors:
            print(f"  - {e[:100]}")
    else:
        print("✓ 所有测试通过，未发现新问题!")
    
    kg_stats = agent.knowledge_graph.get_statistics()
    print(f"\n知识库: {kg_stats['total_experiences']} 条经验, {kg_stats['total_patterns']} 个模式")
    
    return len(errors) == 0


if __name__ == "__main__":
    success = run_stress_test()
    sys.exit(0 if success else 1)
