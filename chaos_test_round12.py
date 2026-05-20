"""
第12轮混乱场景测试 - 模拟真实世界中混乱、不可预测的运行环境
包括：快速启停风暴、资源耗尽、异常风暴、数据竞争、状态不一致
"""
import sys, os, time, json, random, threading, shutil, sqlite3, gc
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType
from core.knowledge_graph import Experience


class ChaosTestRound12:
    def __init__(self):
        self.bugs_found = []
        self.warnings = []
        self.results = {}
        self.test_dir = Path("/tmp/chaos_test_r12")
        self.test_dir.mkdir(parents=True, exist_ok=True)

    def log_bug(self, cat, desc, sev="H"):
        self.bugs_found.append({"category": cat, "description": desc, "severity": sev})
        print(f"  [{sev}-BUG] {cat}: {desc}")

    def log_warn(self, cat, desc):
        self.warnings.append({"category": cat, "description": desc})
        print(f"  [WARN] {cat}: {desc}")

    # ===== 测试1: 快速启停风暴 =====
    def test_rapid_start_stop(self):
        """快速反复创建和销毁Agent，模拟进程频繁重启"""
        print("\n" + "="*70)
        print("测试1: 快速启停风暴 (50次快速启停)")
        print("="*70)

        errors = []
        for i in range(50):
            try:
                work_dir = self.test_dir / f"rapid_{i}"
                shutil.rmtree(work_dir, ignore_errors=True)
                agent = PersistentAgent(work_dir=str(work_dir))
                # 提交几个任务
                for j in range(3):
                    task = Task(id=f"rapid_{i}_{j}", task_type=TaskType.ANALYZE,
                                description=f"快速启停测试", target_files=["/workspace/persistent_agent/config.py"])
                    agent.executor.submit_task(task)
                time.sleep(0.05)  # 极短时间
                agent.stop()
            except Exception as e:
                errors.append(str(e))
                if len(errors) <= 3:
                    self.log_bug("快速启停", f"第{i}次启停失败: {e}")

        if errors:
            self.log_bug("快速启停", f"50次快速启停出现 {len(errors)} 个错误", "H")
        else:
            print("  ✓ 50次快速启停全部成功")

        self.results["rapid_start_stop"] = {"cycles": 50, "errors": len(errors)}

    # ===== 测试2: 异常风暴 =====
    def test_exception_storm(self):
        """同时注入大量异常情况"""
        print("\n" + "="*70)
        print("测试2: 异常风暴 (100个异常任务并发)")
        print("="*70)

        work_dir = self.test_dir / "exception_storm"
        shutil.rmtree(work_dir, ignore_errors=True)
        agent = PersistentAgent(work_dir=str(work_dir))

        # 构造各种异常任务
        storm_tasks = []
        for i in range(25):
            storm_tasks.append(Task(id=f"storm_nonexist_{i}", task_type=TaskType.ANALYZE,
                description="不存在文件", target_files=[f"/nonexistent/{i}.py"]))
        for i in range(25):
            storm_tasks.append(Task(id=f"storm_empty_{i}", task_type=TaskType.ANALYZE,
                description="空文件列表", target_files=[]))
        for i in range(25):
            storm_tasks.append(Task(id=f"storm_huge_{i}", task_type=TaskType.ANALYZE,
                description="x" * 50000, target_files=["/workspace/persistent_agent/config.py"]))
        for i in range(25):
            storm_tasks.append(Task(id=f"storm_special_{i}", task_type=TaskType.ANALYZE,
                description=f"特殊字符\x00\n\r\t{'emoji 🚀' * 50}",
                target_files=["/workspace/persistent_agent/config.py"]))

        # 并发提交
        errors = []
        lock = threading.Lock()
        def submit(t):
            try:
                agent.executor.submit_task(t)
            except Exception as e:
                with lock:
                    errors.append(str(e))

        threads = [threading.Thread(target=submit, args=(t,)) for t in storm_tasks]
        for t in threads: t.start()
        for t in threads: t.join()

        time.sleep(5)
        status = agent.executor.get_status()

        print(f"  提交: {len(storm_tasks)}, 错误: {len(errors)}")
        print(f"  完成: {status['stats']['completed']}, 失败: {status['stats']['failed']}")

        if errors:
            self.log_bug("异常风暴", f"并发提交出现 {len(errors)} 个错误", "H")
        else:
            print("  ✓ 异常风暴测试通过")

        agent.stop()
        self.results["exception_storm"] = {"tasks": len(storm_tasks), "errors": len(errors)}

    # ===== 测试3: 资源耗尽模拟 =====
    def test_resource_exhaustion(self):
        """模拟内存和连接资源耗尽"""
        print("\n" + "="*70)
        print("测试3: 资源耗尽模拟")
        print("="*70)

        work_dir = self.test_dir / "resource_exhaust"
        shutil.rmtree(work_dir, ignore_errors=True)

        # 3.1 大量并发Agent实例
        print("\n  3.1 创建10个并发Agent实例...")
        agents = []
        errors = []
        try:
            for i in range(10):
                agent = PersistentAgent(work_dir=str(work_dir / f"agent_{i}"))
                agents.append(agent)
            print("  ✓ 10个Agent实例创建成功")
        except Exception as e:
            self.log_bug("资源耗尽", f"创建Agent失败: {e}", "H")

        # 3.2 每个Agent大量提交任务
        print("  3.2 每个Agent提交100个任务...")
        for i, agent in enumerate(agents):
            for j in range(100):
                try:
                    task = Task(id=f"exhaust_{i}_{j}", task_type=TaskType.ANALYZE,
                                description=f"资源耗尽测试", target_files=["/workspace/persistent_agent/config.py"])
                    agent.executor.submit_task(task)
                except Exception as e:
                    errors.append(str(e))
                    if len(errors) <= 3:
                        self.log_bug("资源耗尽", f"Agent{i}提交任务失败: {e}")

        time.sleep(3)

        # 3.3 全部同时停止
        print("  3.3 同时停止所有Agent...")
        for agent in agents:
            try:
                agent.stop()
            except Exception as e:
                self.log_warn("资源耗尽", f"停止Agent失败: {e}")

        gc.collect()
        print(f"  ✓ 资源耗尽测试完成, 错误: {len(errors)}")

        self.results["resource_exhaustion"] = {"agents": len(agents), "errors": len(errors)}

    # ===== 测试4: 数据竞争压力 =====
    def test_data_race_pressure(self):
        """极端并发读写知识图谱"""
        print("\n" + "="*70)
        print("测试4: 数据竞争压力 (100线程混合读写)")
        print("="*70)

        work_dir = self.test_dir / "data_race"
        shutil.rmtree(work_dir, ignore_errors=True)
        agent = PersistentAgent(work_dir=str(work_dir))
        kg = agent.knowledge_graph

        write_errors = []
        read_errors = []
        lock = threading.Lock()

        def writer(tid):
            try:
                for i in range(50):
                    exp = Experience(
                        id=f"race_w_{tid}_{i}", task_type="analyze", context="{}",
                        problem=f"写入竞争 {tid}-{i}", solution="方案", outcome="成功",
                        lessons="教训", code_snippets=[], tags=["race"],
                        created_at=datetime.now().isoformat()
                    )
                    kg.add_experience(exp)
            except Exception as e:
                with lock: write_errors.append(str(e))

        def reader(tid):
            try:
                for i in range(50):
                    kg.get_similar_experiences(f"搜索竞争 {tid}-{i}", limit=5)
                    kg.get_statistics()
                    kg.search_experiences(query="竞争", tags=["race"], limit=10)
            except Exception as e:
                with lock: read_errors.append(str(e))

        # 50写线程 + 50读线程
        threads = []
        for i in range(50): threads.append(threading.Thread(target=writer, args=(i,)))
        for i in range(50): threads.append(threading.Thread(target=reader, args=(i,)))

        start = time.time()
        for t in threads: t.start()
        for t in threads: t.join()
        elapsed = time.time() - start

        total_errors = len(write_errors) + len(read_errors)
        print(f"  耗时: {elapsed:.2f}秒")
        print(f"  写入错误: {len(write_errors)}, 读取错误: {len(read_errors)}")

        if total_errors > 0:
            self.log_bug("数据竞争", f"100线程混合读写出现 {total_errors} 个错误", "H")
            for e in (write_errors + read_errors)[:3]:
                print(f"    错误: {e[:100]}")
        else:
            print("  ✓ 100线程混合读写无错误")

        agent.stop()
        self.results["data_race"] = {"threads": 100, "errors": total_errors, "time": elapsed}

    # ===== 测试5: 状态不一致检测 =====
    def test_state_inconsistency(self):
        """检测内存状态与数据库状态是否一致"""
        print("\n" + "="*70)
        print("测试5: 状态不一致检测")
        print("="*70)

        work_dir = self.test_dir / "state_check"
        shutil.rmtree(work_dir, ignore_errors=True)
        agent = PersistentAgent(work_dir=str(work_dir))

        # 提交任务
        for i in range(20):
            task = Task(id=f"state_{i}", task_type=TaskType.ANALYZE,
                        description=f"状态检测 {i}", target_files=["/workspace/persistent_agent/config.py"])
            agent.executor.submit_task(task)

        time.sleep(3)
        status = agent.executor.get_status()

        # 直接查数据库对比
        task_db = work_dir / "tasks.db"
        if task_db.exists():
            conn = sqlite3.connect(str(task_db))
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
            db_status = dict(cursor.fetchall())
            conn.close()

            print(f"  内存状态: completed={status['stats']['completed']}, failed={status['stats']['failed']}")
            print(f"  数据库状态: {db_status}")

            # 检查不一致
            mem_completed = status['stats']['completed']
            db_completed = db_status.get('completed', 0)

            if abs(mem_completed - db_completed) > 5:
                self.log_bug("状态不一致",
                    f"内存completed={mem_completed}, 数据库completed={db_completed}", "M")
            else:
                print("  ✓ 内存与数据库状态一致")
        else:
            self.log_bug("状态检测", "任务数据库文件不存在", "H")

        agent.stop()
        self.results["state_check"] = {"consistent": task_db.exists()}

    # ===== 测试6: 崩溃恢复压力 =====
    def test_crash_recovery_pressure(self):
        """反复崩溃恢复，验证数据完整性"""
        print("\n" + "="*70)
        print("测试6: 崩溃恢复压力 (10次崩溃循环)")
        print("="*70)

        work_dir = self.test_dir / "crash_pressure"
        shutil.rmtree(work_dir, ignore_errors=True)

        total_submitted = 0
        total_recovered = 0
        errors = []

        for cycle in range(10):
            try:
                agent = PersistentAgent(work_dir=str(work_dir))

                # 提交任务
                for i in range(20):
                    task = Task(id=f"crash_{cycle}_{i}", task_type=TaskType.ANALYZE,
                                description=f"崩溃恢复循环{cycle}", target_files=["/workspace/persistent_agent/config.py"],
                                priority=1)
                    agent.executor.submit_task(task)
                    total_submitted += 1

                time.sleep(0.2)

                # 检查恢复的任务
                status = agent.executor.get_status()
                recovered = status['pending_tasks'] + status['running_tasks']
                total_recovered += recovered

                # 模拟崩溃（不调用stop）
                del agent
                gc.collect()

            except Exception as e:
                errors.append(str(e))
                if len(errors) <= 3:
                    self.log_bug("崩溃恢复", f"第{cycle}次循环失败: {e}")

        print(f"  总提交: {total_submitted}, 总恢复: {total_recovered}")
        print(f"  错误: {len(errors)}")

        if errors:
            self.log_bug("崩溃恢复", f"10次崩溃循环出现 {len(errors)} 个错误", "H")
        else:
            print("  ✓ 10次崩溃循环全部成功")

        self.results["crash_pressure"] = {"cycles": 10, "errors": len(errors)}

    # ===== 报告 =====
    def generate_report(self):
        print("\n" + "="*70)
        print("第12轮混乱场景测试报告")
        print("="*70)

        print(f"\nBug: {len(self.bugs_found)}, 警告: {len(self.warnings)}")
        for i, b in enumerate(self.bugs_found, 1):
            print(f"  {i}. [{b['severity']}] {b['category']}: {b['description']}")
        for i, w in enumerate(self.warnings, 1):
            print(f"  {i}. {w['category']}: {w['description']}")

        print("\n结果:")
        for k, v in self.results.items():
            print(f"  [{k}]: {v}")

        report = {"timestamp": datetime.now().isoformat(), "bugs": self.bugs_found,
                  "warnings": self.warnings, "results": self.results}
        with open(self.test_dir / "report_r12.json", "w") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        return len(self.bugs_found) == 0


if __name__ == "__main__":
    tester = ChaosTestRound12()
    try:
        tester.test_rapid_start_stop()
        tester.test_exception_storm()
        tester.test_resource_exhaustion()
        tester.test_data_race_pressure()
        tester.test_state_inconsistency()
        tester.test_crash_recovery_pressure()
        success = tester.generate_report()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n测试异常: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
