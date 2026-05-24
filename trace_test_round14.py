"""
Round 14 - 细粒度日志追踪测试
通过全面日志追踪发现隐藏Bug

测试场景：
1. 多线程并发提交+执行+取消混合操作
2. 异常注入（DB错误、超时、内存压力）
3. 快速启停循环
4. 大规模任务风暴（1000+任务）
5. 知识图谱高并发读写
6. 状态机边界条件测试
7. 资源竞争（锁争用、连接池压力）
"""
import os
import sys
import time
import json
import random
import string
import threading
import logging
import traceback
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# 设置日志 - DEBUG级别以捕获所有TRACE日志
LOG_DIR = tempfile.mkdtemp(prefix="round14_logs_")
LOG_FILE = os.path.join(LOG_DIR, "test_trace.log")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)-8s [%(threadName)-15s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
# 降低第三方库日志级别
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger("Round14Test")

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.executor_v2 import TaskExecutorV2, Task, TaskType, TaskStatus
from core.knowledge_graph import KnowledgeGraph, Experience, Pattern
from core.optimizer import Optimizer
from core.watchdog import Watchdog, FileWatcher


class TestContext:
    """测试上下文"""
    def __init__(self):
        self.work_dir = tempfile.mkdtemp(prefix="round14_work_")
        self.db_path = os.path.join(self.work_dir, "test_knowledge.db")
        self.errors = []
        self.lock = threading.Lock()
        self.stats = defaultdict(int)

    def record_error(self, test_name, error):
        with self.lock:
            self.errors.append((test_name, str(error), traceback.format_exc()))
            self.stats["total_errors"] += 1

    def record_success(self, test_name):
        with self.lock:
            self.stats["total_success"] += 1

    def cleanup(self):
        try:
            shutil.rmtree(self.work_dir, ignore_errors=True)
        except Exception:
            pass


def test_1_concurrent_mixed_operations(ctx: TestContext):
    """测试1: 多线程并发提交+执行+取消混合操作"""
    test_name = "concurrent_mixed_operations"
    logger.info(f"\n{'='*60}\n测试1: 多线程并发混合操作\n{'='*60}")

    kg = KnowledgeGraph(ctx.db_path)
    executor = TaskExecutorV2(kg, ctx.work_dir, auto_start=True)

    errors = []
    submitted_ids = []

    def submit_tasks(count, prefix):
        for i in range(count):
            try:
                task = Task(
                    id=f"{prefix}_{i}_{random.randint(1000,9999)}",
                    task_type=random.choice(list(TaskType)),
                    description=f"测试任务 {prefix}_{i}",
                    target_files=[],
                    priority=random.randint(1, 10)
                )
                tid = executor.submit_task(task)
                submitted_ids.append(tid)
            except Exception as e:
                errors.append(e)

    def cancel_random_tasks():
        time.sleep(0.5)
        for _ in range(20):
            try:
                if submitted_ids:
                    tid = random.choice(submitted_ids)
                    with executor._lock:
                        task = executor.active_tasks.get(tid)
                        if task and task.status == TaskStatus.PENDING:
                            task.status = TaskStatus.CANCELLED
            except Exception as e:
                errors.append(e)
            time.sleep(0.05)

    threads = [
        threading.Thread(target=submit_tasks, args=(100, "batch_a")),
        threading.Thread(target=submit_tasks, args=(100, "batch_b")),
        threading.Thread(target=submit_tasks, args=(100, "batch_c")),
        threading.Thread(target=cancel_random_tasks),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    # 等待任务处理
    time.sleep(5)

    executor.stop()
    kg_stats = kg.get_statistics()

    logger.info(f"  提交任务数: {len(submitted_ids)}")
    logger.info(f"  执行器统计: {executor.stats}")
    logger.info(f"  知识库统计: {kg_stats}")
    logger.info(f"  错误数: {len(errors)}")

    if errors:
        ctx.record_error(test_name, errors[0])
    else:
        ctx.record_success(test_name)


def test_2_exception_injection(ctx: TestContext):
    """测试2: 异常注入（DB错误、超时、内存压力）"""
    test_name = "exception_injection"
    logger.info(f"\n{'='*60}\n测试2: 异常注入测试\n{'='*60}")

    kg = KnowledgeGraph(ctx.db_path)
    executor = TaskExecutorV2(kg, ctx.work_dir, auto_start=True)

    errors = []

    # 2a: 提交大量任务制造DB压力
    logger.info("  2a: DB压力测试 - 快速提交500个任务")
    tasks = []
    for i in range(500):
        task = Task(
            id=f"db_stress_{i}",
            task_type=TaskType.ANALYZE,
            description=f"DB压力测试任务 {i}",
            target_files=[],
            priority=random.randint(1, 10)
        )
        tasks.append(task)

    start = time.time()
    for task in tasks:
        try:
            executor.submit_task(task)
        except Exception as e:
            errors.append(e)
    elapsed = time.time() - start
    logger.info(f"  500个任务提交耗时: {elapsed:.2f}s, 错误: {len(errors)}")

    # 2b: 超时任务
    logger.info("  2b: 超时任务测试")
    timeout_task = Task(
        id="timeout_test_1",
        task_type=TaskType.IMPLEMENT,
        description="超时测试任务",
        target_files=[],
        timeout_seconds=1
    )
    executor.submit_task(timeout_task)
    time.sleep(3)

    # 2c: 知识图谱高并发读写
    logger.info("  2c: 知识图谱高并发读写")
    kg_errors = []

    def kg_writer(n):
        for i in range(n):
            try:
                exp = Experience(
                    id=f"exp_inject_{threading.current_thread().name}_{i}",
                    task_type="analyze",
                    context="",
                    problem=f"注入测试问题 {i}",
                    solution=f"注入测试方案 {i}",
                    outcome="success" if random.random() > 0.1 else "failure",
                    lessons="测试",
                    code_snippets=[],
                    tags=["inject", f"tag_{i % 10}"],
                    created_at=datetime.now().isoformat(),
                    success_rate=0.8,
                    usage_count=0
                )
                kg.add_experience(exp)
            except Exception as e:
                kg_errors.append(e)

    def kg_reader(n):
        for i in range(n):
            try:
                kg.get_similar_experiences(f"查询测试 {i}", limit=5)
                kg.search_experiences(f"搜索测试 {i}", limit=10)
                kg.get_statistics()
            except Exception as e:
                kg_errors.append(e)

    threads = []
    for i in range(10):
        threads.append(threading.Thread(target=kg_writer, args=(50,)))
        threads.append(threading.Thread(target=kg_reader, args=(50,)))

    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    elapsed = time.time() - start

    logger.info(f"  知识图谱并发测试: 20线程×50操作, 耗时: {elapsed:.2f}s, 错误: {len(kg_errors)}")

    time.sleep(3)
    executor.stop()

    all_errors = errors + kg_errors
    if all_errors:
        ctx.record_error(test_name, all_errors[0])
    else:
        ctx.record_success(test_name)


def test_3_rapid_start_stop(ctx: TestContext):
    """测试3: 快速启停循环"""
    test_name = "rapid_start_stop"
    logger.info(f"\n{'='*60}\n测试3: 快速启停循环 (30次)\n{'='*60}")

    kg = KnowledgeGraph(ctx.db_path)
    errors = []

    for i in range(30):
        try:
            executor = TaskExecutorV2(kg, ctx.work_dir, auto_start=False)
            executor.start(3)

            # 提交几个任务
            for j in range(5):
                task = Task(
                    id=f"startstop_{i}_{j}",
                    task_type=TaskType.TEST,
                    description=f"启停测试 {i}_{j}",
                    target_files=[]
                )
                executor.submit_task(task)

            time.sleep(0.3)
            executor.stop()
        except Exception as e:
            errors.append(e)
            logger.error(f"  第{i}次启停失败: {e}")

    logger.info(f"  30次启停循环完成, 错误: {len(errors)}")

    if errors:
        ctx.record_error(test_name, errors[0])
    else:
        ctx.record_success(test_name)


def test_4_task_state_machine(ctx: TestContext):
    """测试4: 状态机边界条件测试"""
    test_name = "task_state_machine"
    logger.info(f"\n{'='*60}\n测试4: 状态机边界条件\n{'='*60}")

    kg = KnowledgeGraph(ctx.db_path)
    executor = TaskExecutorV2(kg, ctx.work_dir, auto_start=True)
    errors = []

    # 4a: 正常生命周期 PENDING -> RUNNING -> COMPLETED
    logger.info("  4a: 正常生命周期")
    task = Task(
        id="lifecycle_normal",
        task_type=TaskType.TEST,
        description="正常生命周期测试",
        target_files=[]
    )
    executor.submit_task(task)
    time.sleep(2)
    result = executor.get_task_result("lifecycle_normal")
    logger.info(f"  正常任务结果: {result['status'] if result else 'NOT FOUND'}")

    # 4b: 重试机制 - 提交会失败的任务
    logger.info("  4b: 重试机制测试")
    # 创建一个不存在的文件来触发失败
    fail_task = Task(
        id="retry_test_1",
        task_type=TaskType.ANALYZE,
        description="重试测试",
        target_files=["/nonexistent/file.py"],
        max_retries=3
    )
    executor.submit_task(fail_task)
    time.sleep(5)
    result = executor.get_task_result("retry_test_1")
    if result:
        logger.info(f"  重试任务结果: status={result['status']}, error={result.get('error', '')[:100]}")

    # 4c: 并发修改同一任务状态
    logger.info("  4c: 并发状态修改测试")
    state_errors = []

    def modify_task_state(task_id, target_status):
        try:
            for _ in range(10):
                with executor._lock:
                    t = executor.active_tasks.get(task_id)
                    if t:
                        t.status = target_status
                time.sleep(0.01)
        except Exception as e:
            state_errors.append(e)

    concurrent_task = Task(
        id="concurrent_state",
        task_type=TaskType.TEST,
        description="并发状态修改测试",
        target_files=[]
    )
    executor.submit_task(concurrent_task)

    threads = [
        threading.Thread(target=modify_task_state, args=("concurrent_state", TaskStatus.RUNNING)),
        threading.Thread(target=modify_task_state, args=("concurrent_state", TaskStatus.PENDING)),
        threading.Thread(target=modify_task_state, args=("concurrent_state", TaskStatus.FAILED)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    time.sleep(2)
    executor.stop()

    all_errors = errors + state_errors
    if all_errors:
        ctx.record_error(test_name, all_errors[0])
    else:
        ctx.record_success(test_name)


def test_5_knowledge_graph_edge_cases(ctx: TestContext):
    """测试5: 知识图谱边界条件"""
    test_name = "kg_edge_cases"
    logger.info(f"\n{'='*60}\n测试5: 知识图谱边界条件\n{'='*60}")

    kg = KnowledgeGraph(ctx.db_path)
    errors = []

    # 5a: 空查询
    logger.info("  5a: 空查询测试")
    try:
        result = kg.get_similar_experiences("")
        assert result == [], f"空查询应返回空列表，实际返回: {result}"
        result = kg.get_similar_experiences(None)
        assert result == [], f"None查询应返回空列表，实际返回: {result}"
        result = kg.get_similar_experiences("   ")
        assert result == [], f"空白查询应返回空列表，实际返回: {result}"
        logger.info("  5a: 空查询测试通过")
    except Exception as e:
        errors.append(e)
        logger.error(f"  5a 失败: {e}")

    # 5b: 重复添加相同ID的经验（ON CONFLICT DO UPDATE）
    logger.info("  5b: 重复添加相同ID经验")
    try:
        exp = Experience(
            id="duplicate_exp_test",
            task_type="analyze",
            context="初始上下文",
            problem="初始问题",
            solution="初始方案",
            outcome="success",
            lessons="初始教训",
            code_snippets=[],
            tags=["tag1", "tag2"],
            created_at=datetime.now().isoformat(),
            success_rate=0.8,
            usage_count=1
        )
        kg.add_experience(exp)

        # 修改后重新添加
        exp.context = "更新上下文"
        exp.tags = ["tag1", "tag3"]  # tag2被移除，tag3被添加
        exp.usage_count = 5
        kg.add_experience(exp)

        # 验证更新
        results = kg.search_experiences("初始问题", limit=1)
        if results:
            assert results[0].context == "更新上下文", f"上下文未更新: {results[0].context}"
            assert "tag3" in results[0].tags, f"tag3未添加: {results[0].tags}"
            assert "tag2" not in results[0].tags, f"tag2未移除: {results[0].tags}"
            logger.info("  5b: 重复添加测试通过")
        else:
            logger.warning("  5b: 未找到经验记录")
    except Exception as e:
        errors.append(e)
        logger.error(f"  5b 失败: {e}")

    # 5c: 更新不存在的经验使用统计
    logger.info("  5c: 更新不存在的经验")
    try:
        kg.update_experience_usage("nonexistent_exp_id", True)
        logger.info("  5c: 更新不存在经验（应静默跳过）")
    except Exception as e:
        errors.append(e)
        logger.error(f"  5c 失败: {e}")

    # 5d: 大量标签索引操作
    logger.info("  5d: 大量标签索引操作")
    try:
        for i in range(200):
            exp = Experience(
                id=f"tag_test_{i}",
                task_type="analyze",
                context="",
                problem=f"标签测试问题 {i}",
                solution=f"标签测试方案 {i}",
                outcome="success",
                lessons="",
                code_snippets=[],
                tags=[f"tag_a_{i%20}", f"tag_b_{i%15}", f"tag_c_{i%10}"],
                created_at=datetime.now().isoformat(),
                success_rate=0.9,
                usage_count=0
            )
            kg.add_experience(exp)
        logger.info("  5d: 200条带标签经验添加完成")
    except Exception as e:
        errors.append(e)
        logger.error(f"  5d 失败: {e}")

    # 5e: 模式添加和更新
    logger.info("  5e: 模式添加和更新")
    try:
        pattern = Pattern(
            id="pattern_test_update",
            pattern_type="best_practice",
            name="测试模式",
            description="初始描述",
            code_example="",
            when_to_use="测试用",
            effectiveness=0.5,
            created_at=datetime.now().isoformat()
        )
        kg.add_pattern(pattern)

        # 更新
        pattern.description = "更新描述"
        pattern.effectiveness = 0.9
        kg.add_pattern(pattern)

        patterns = kg.get_patterns_by_type("best_practice")
        found = [p for p in patterns if p.id == "pattern_test_update"]
        if found:
            assert found[0].effectiveness == 0.9, f"effectiveness未更新: {found[0].effectiveness}"
            logger.info("  5e: 模式更新测试通过")
        else:
            logger.warning("  5e: 未找到模式记录")
    except Exception as e:
        errors.append(e)
        logger.error(f"  5e 失败: {e}")

    # 5f: 导出知识库
    logger.info("  5f: 导出知识库")
    try:
        export_path = os.path.join(ctx.work_dir, "export_test.json")
        kg.export_knowledge(export_path)
        assert os.path.exists(export_path), "导出文件不存在"
        with open(export_path, 'r') as f:
            data = json.load(f)
        assert "experiences" in data, "导出数据缺少experiences"
        assert "patterns" in data, "导出数据缺少patterns"
        logger.info(f"  5f: 导出成功, 经验数: {len(data['experiences'])}, 模式数: {len(data['patterns'])}")
    except Exception as e:
        errors.append(e)
        logger.error(f"  5f 失败: {e}")

    if errors:
        ctx.record_error(test_name, errors[0])
    else:
        ctx.record_success(test_name)


def test_6_optimizer_under_load(ctx: TestContext):
    """测试6: 优化器负载测试"""
    test_name = "optimizer_under_load"
    logger.info(f"\n{'='*60}\n测试6: 优化器负载测试\n{'='*60}")

    kg = KnowledgeGraph(ctx.db_path)
    executor = TaskExecutorV2(kg, ctx.work_dir, auto_start=True)
    optimizer = Optimizer(kg, executor)
    errors = []

    # 预填充经验数据
    logger.info("  预填充经验数据...")
    for i in range(100):
        exp = Experience(
            id=f"opt_exp_{i}",
            task_type=random.choice(["analyze", "bug_fix", "refactor", "implement"]),
            context="",
            problem=f"优化测试问题 {i}",
            solution=f"优化测试方案 {i}",
            outcome="success" if random.random() > 0.2 else "failure",
            lessons="",
            code_snippets=[],
            tags=[f"opt_tag_{i%10}"],
            created_at=datetime.now().isoformat(),
            success_rate=random.uniform(0.3, 1.0),
            usage_count=random.randint(0, 20)
        )
        kg.add_experience(exp)

    # 启动优化器
    optimizer.start()

    # 持续记录执行结果
    logger.info("  持续记录执行结果...")
    for i in range(500):
        try:
            task_type = random.choice(["analyze", "bug_fix", "refactor", "implement"])
            success = random.random() > 0.3
            exec_time = random.uniform(0.01, 2.0)
            optimizer.record_execution_result(task_type, success, exec_time)
        except Exception as e:
            errors.append(e)

    # 获取最佳策略
    logger.info("  获取最佳策略...")
    for task_type in ["analyze", "bug_fix", "refactor", "implement"]:
        try:
            strategy = optimizer.get_best_strategy(task_type, {"file_count": 5})
            if strategy:
                logger.info(f"    {task_type}: {strategy.name} (rate={strategy.success_rate:.3f})")
        except Exception as e:
            errors.append(e)

    # 获取优化报告
    try:
        report = optimizer.get_optimization_report()
        logger.info(f"  优化报告: strategies={report['strategy_count']}")
    except Exception as e:
        errors.append(e)

    # 获取改进建议
    try:
        suggestions = optimizer.suggest_improvements()
        logger.info(f"  改进建议: {len(suggestions)}条")
    except Exception as e:
        errors.append(e)

    time.sleep(2)
    optimizer.stop()
    executor.stop()

    if errors:
        ctx.record_error(test_name, errors[0])
    else:
        ctx.record_success(test_name)


def test_7_watchdog_file_watcher(ctx: TestContext):
    """测试7: Watchdog和FileWatcher测试"""
    test_name = "watchdog_file_watcher"
    logger.info(f"\n{'='*60}\n测试7: Watchdog和FileWatcher测试\n{'='*60}")

    errors = []
    events = []
    event_lock = threading.Lock()

    def file_callback(path, event_type):
        with event_lock:
            events.append((path, event_type))

    # 创建监控目录
    watch_dir = os.path.join(ctx.work_dir, "watch_test")
    os.makedirs(watch_dir, exist_ok=True)

    # 创建一些初始文件
    for i in range(5):
        with open(os.path.join(watch_dir, f"initial_{i}.py"), 'w') as f:
            f.write(f"# 初始文件 {i}\n")

    watcher = FileWatcher(watch_dir, file_callback)
    watcher.start()
    time.sleep(2)  # 等待初始扫描

    # 7a: 创建新文件
    logger.info("  7a: 创建新文件")
    for i in range(10):
        with open(os.path.join(watch_dir, f"new_{i}.py"), 'w') as f:
            f.write(f"# 新文件 {i}\n")
    time.sleep(8)  # 等待扫描检测

    # 7b: 修改文件
    logger.info("  7b: 修改文件")
    for i in range(5):
        with open(os.path.join(watch_dir, f"new_{i}.py"), 'a') as f:
            f.write(f"# 追加内容 {i}\n")
    time.sleep(8)

    # 7c: 删除文件
    logger.info("  7c: 删除文件")
    for i in range(3):
        try:
            os.remove(os.path.join(watch_dir, f"new_{i}.py"))
        except Exception as e:
            errors.append(e)
    time.sleep(20)  # 等待缺失阈值

    watcher.stop()

    with event_lock:
        created = sum(1 for _, t in events if t == "created")
        modified = sum(1 for _, t in events if t == "modified")
        deleted = sum(1 for _, t in events if t == "deleted")
        logger.info(f"  文件事件: created={created}, modified={modified}, deleted={deleted}")
        logger.info(f"  总事件数: {len(events)}")

    # 7d: Watchdog测试
    logger.info("  7d: Watchdog测试")
    try:
        wd = Watchdog()
        wd.start()
        time.sleep(3)

        # 记录一些崩溃
        for i in range(2):
            wd.record_crash(Exception(f"测试崩溃 {i}"))

        status = wd.get_status()
        logger.info(f"  Watchdog状态: running={status['running']}, crashes={status['crash_count']}, safe_mode={status['safe_mode']}")

        wd.stop()
    except Exception as e:
        errors.append(e)
        logger.error(f"  7d 失败: {e}")

    if errors:
        ctx.record_error(test_name, errors[0])
    else:
        ctx.record_success(test_name)


def test_8_massive_task_storm(ctx: TestContext):
    """测试8: 大规模任务风暴"""
    test_name = "massive_task_storm"
    logger.info(f"\n{'='*60}\n测试8: 大规模任务风暴 (2000任务)\n{'='*60}")

    kg = KnowledgeGraph(ctx.db_path)
    executor = TaskExecutorV2(kg, ctx.work_dir, auto_start=True)
    errors = []

    # 快速提交2000个任务
    logger.info("  提交2000个任务...")
    start = time.time()
    for i in range(2000):
        try:
            task = Task(
                id=f"storm_{i}",
                task_type=random.choice(list(TaskType)),
                description=f"风暴测试任务 {i}",
                target_files=[],
                priority=random.randint(1, 10)
            )
            executor.submit_task(task)
        except Exception as e:
            errors.append(e)

    submit_time = time.time() - start
    logger.info(f"  2000个任务提交耗时: {submit_time:.2f}s, 错误: {len(errors)}")

    # 等待处理完成
    logger.info("  等待任务处理...")
    time.sleep(30)

    status = executor.get_status()
    logger.info(f"  执行器状态: {status}")
    logger.info(f"  统计: {executor.stats}")

    executor.stop()

    if errors:
        ctx.record_error(test_name, errors[0])
    else:
        ctx.record_success(test_name)


def run_all_tests():
    """运行所有测试"""
    ctx = TestContext()

    logger.info("=" * 60)
    logger.info("Round 14 - 细粒度日志追踪测试")
    logger.info(f"日志文件: {LOG_FILE}")
    logger.info(f"工作目录: {ctx.work_dir}")
    logger.info("=" * 60)

    tests = [
        ("测试1: 多线程并发混合操作", test_1_concurrent_mixed_operations),
        ("测试2: 异常注入", test_2_exception_injection),
        ("测试3: 快速启停循环", test_3_rapid_start_stop),
        ("测试4: 状态机边界条件", test_4_task_state_machine),
        ("测试5: 知识图谱边界条件", test_5_knowledge_graph_edge_cases),
        ("测试6: 优化器负载测试", test_6_optimizer_under_load),
        ("测试7: Watchdog和FileWatcher", test_7_watchdog_file_watcher),
        ("测试8: 大规模任务风暴", test_8_massive_task_storm),
    ]

    total_start = time.time()
    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            test_func(ctx)
            if not any(e[0] == name.split(":")[0].strip() for e in ctx.errors):
                passed += 1
                logger.info(f"  ✅ {name} 通过")
            else:
                failed += 1
                logger.error(f"  ❌ {name} 失败")
        except Exception as e:
            failed += 1
            ctx.record_error(name, e)
            logger.error(f"  ❌ {name} 异常: {e}\n{traceback.format_exc()}")

    total_time = time.time() - total_start

    # 打印汇总
    logger.info("\n" + "=" * 60)
    logger.info("测试汇总")
    logger.info("=" * 60)
    logger.info(f"  总耗时: {total_time:.1f}s")
    logger.info(f"  通过: {passed}/{len(tests)}")
    logger.info(f"  失败: {failed}/{len(tests)}")
    logger.info(f"  总错误: {ctx.stats['total_errors']}")
    logger.info(f"  日志文件: {LOG_FILE}")

    if ctx.errors:
        logger.info("\n  错误详情:")
        for name, err, tb in ctx.errors:
            logger.info(f"    [{name}] {err[:200]}")

    # 运行日志分析器
    logger.info("\n" + "=" * 60)
    logger.info("运行日志分析器...")
    logger.info("=" * 60)

    ctx.cleanup()

    return LOG_FILE, failed == 0


if __name__ == "__main__":
    log_file, success = run_all_tests()

    print(f"\n\n日志分析器命令:")
    print(f"  python3 log_analyzer_round14.py {log_file}")

    # 自动运行日志分析器
    print("\n自动运行日志分析器...")
    os.system(f"cd /workspace/persistent_agent && python3 log_analyzer_round14.py {log_file}")

    sys.exit(0 if success else 1)
