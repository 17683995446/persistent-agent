"""
测试脚本 - 验证Agent各模块功能
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from core.knowledge_graph import KnowledgeGraph, Experience, Pattern, get_knowledge_graph
from core.executor import TaskExecutor, Task, TaskType, CodeAnalyzer
from core.optimizer import Optimizer
from core.watchdog import Watchdog


def test_knowledge_graph():
    """测试知识图谱"""
    print("\n=== 测试知识图谱 ===")
    
    # 使用测试数据库
    kg = KnowledgeGraph(":memory:")
    
    # 添加经验
    exp = Experience(
        id="test_exp_001",
        task_type="bug_fix",
        context="测试上下文",
        problem="空指针异常",
        solution="添加空值检查",
        outcome="success",
        lessons="使用 is None 检查",
        code_snippets=["if value is not None:"],
        tags=["python", "null-check"],
        created_at=datetime.now().isoformat()
    )
    
    exp_id = kg.add_experience(exp)
    print(f"✓ 添加经验: {exp_id}")
    
    # 搜索经验
    results = kg.search_experiences("空指针")
    print(f"✓ 搜索经验: 找到 {len(results)} 条")
    
    # 添加模式
    pattern = Pattern(
        id="test_pattern_001",
        pattern_type="best_practice",
        name="空值检查",
        description="检查变量是否为空",
        code_example="if x is not None:",
        when_to_use="处理可能为空的变量",
        effectiveness=0.9,
        created_at=datetime.now().isoformat()
    )
    
    pattern_id = kg.add_pattern(pattern)
    print(f"✓ 添加模式: {pattern_id}")
    
    # 获取统计
    stats = kg.get_statistics()
    print(f"✓ 统计: {stats}")
    
    return True


def test_code_analyzer():
    """测试代码分析器"""
    print("\n=== 测试代码分析器 ===")
    
    # 创建测试文件
    test_file = "/tmp/test_analyze.py"
    with open(test_file, 'w') as f:
        f.write('''
def hello(name):
    """打招呼"""
    return f"Hello, {name}!"

class Calculator:
    def add(self, a, b):
        return a + b
    
    def divide(self, a, b):
        return a / b  # 可能除零

# TODO: 添加更多功能
''')
    
    # 分析文件
    result = CodeAnalyzer.analyze_file(test_file)
    print(f"✓ 分析文件: {result['file_path']}")
    print(f"  - 行数: {result['lines']}")
    print(f"  - 函数: {len(result['functions'])}")
    print(f"  - 类: {len(result['classes'])}")
    print(f"  - 复杂度: {result['complexity']}")
    
    # 检测Bug
    bugs = CodeAnalyzer.detect_bugs(test_file)
    print(f"✓ 检测Bug: 发现 {len(bugs)} 个潜在问题")
    
    return True


def test_executor():
    """测试执行引擎"""
    print("\n=== 测试执行引擎 ===")
    
    # 使用内存数据库
    kg = KnowledgeGraph(":memory:")
    executor = TaskExecutor(kg, "/tmp")
    
    # 创建测试任务
    task = Task(
        id="test_task_001",
        task_type=TaskType.ANALYZE,
        description="测试分析任务",
        target_files=["/tmp/test_analyze.py"],
        priority=3
    )
    
    # 提交任务
    task_id = executor.submit_task(task)
    print(f"✓ 提交任务: {task_id}")
    
    # 获取状态
    status = executor.get_status()
    print(f"✓ 执行器状态: {status}")
    
    return True


def test_optimizer():
    """测试优化器"""
    print("\n=== 测试优化器 ===")
    
    kg = KnowledgeGraph(":memory:")
    executor = TaskExecutor(kg, "/tmp")
    optimizer = Optimizer(kg, executor)
    
    # 获取策略
    strategy = optimizer.get_best_strategy("analyze", {"file_count": 5})
    if strategy:
        print(f"✓ 最佳策略: {strategy.name}")
    
    # 获取优化报告
    report = optimizer.get_optimization_report()
    print(f"✓ 优化报告: 策略数 {report['strategy_count']}")
    
    # 获取改进建议
    suggestions = optimizer.suggest_improvements()
    print(f"✓ 改进建议: {len(suggestions)} 条")
    
    return True


def test_watchdog():
    """测试保活模块"""
    print("\n=== 测试保活模块 ===")
    
    watchdog = Watchdog()
    
    # 获取状态
    status = watchdog.get_status()
    print(f"✓ 保活状态: running={status['running']}")
    
    # 模拟崩溃记录
    try:
        raise ValueError("测试崩溃")
    except Exception as e:
        record = watchdog.record_crash(e)
        print(f"✓ 记录崩溃: {record.error_message[:30]}...")
    
    return True


def test_integration():
    """集成测试"""
    print("\n=== 集成测试 ===")
    
    from main import PersistentAgent
    
    # 创建Agent实例
    agent = PersistentAgent(work_dir="/tmp")
    print("✓ Agent创建成功")
    
    # 获取状态
    status = agent.get_status()
    print(f"✓ Agent状态: {status['running']}")
    
    # 提交测试任务
    task_id = agent.analyze_code(["/tmp/test_analyze.py"], "集成测试分析")
    print(f"✓ 提交分析任务: {task_id}")
    
    return True


def main():
    """运行所有测试"""
    print("=" * 50)
    print("极简全能持续运行Agent - 测试套件")
    print("=" * 50)
    
    tests = [
        ("知识图谱", test_knowledge_graph),
        ("代码分析器", test_code_analyzer),
        ("执行引擎", test_executor),
        ("优化器", test_optimizer),
        ("保活模块", test_watchdog),
        ("集成测试", test_integration),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, "✓ 通过" if success else "✗ 失败"))
        except Exception as e:
            results.append((name, f"✗ 错误: {e}"))
    
    print("\n" + "=" * 50)
    print("测试结果汇总:")
    print("=" * 50)
    for name, result in results:
        print(f"  {name}: {result}")
    
    passed = sum(1 for _, r in results if "✓" in r)
    print(f"\n总计: {passed}/{len(results)} 通过")


if __name__ == "__main__":
    main()
