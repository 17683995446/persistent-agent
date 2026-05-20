"""
第11轮真实完整场景测试 - 模拟生产环境的完整工作流程
包括：多模块协同、异常注入、故障恢复、真实代码分析
"""
import sys
import os
import time
import json
import random
import threading
import shutil
import sqlite3
import signal
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

sys.path.insert(0, str(Path(__file__).parent))

from main import PersistentAgent
from core.executor_v2 import Task, TaskType, TaskStatus
from core.knowledge_graph import Experience, Pattern
from core.optimizer import Optimizer
from core.watchdog import FileWatcher


class RealWorldTestRound11:
    """第11轮真实完整场景测试"""
    
    def __init__(self):
        self.bugs_found = []
        self.warnings = []
        self.results = {}
        self.test_dir = Path("/tmp/real_world_test_r11")
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

    # ==================== 测试1: 真实代码分析工作流 ====================
    def test_real_code_analysis_workflow(self):
        """测试1: 真实代码分析工作流 - 分析实际项目代码"""
        print("\n" + "=" * 70)
        print("测试1: 真实代码分析工作流")
        print("=" * 70)
        
        work_dir = self.test_dir / "code_analysis"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 分析真实项目文件
        real_files = [
            "/workspace/persistent_agent/main.py",
            "/workspace/persistent_agent/core/executor_v2.py",
            "/workspace/persistent_agent/core/knowledge_graph.py",
            "/workspace/persistent_agent/config.py",
        ]
        
        print(f"\n  分析 {len(real_files)} 个真实代码文件...")
        
        for i, file_path in enumerate(real_files):
            if Path(file_path).exists():
                task = Task(
                    id=f"analyze_real_{i}",
                    task_type=TaskType.ANALYZE,
                    description=f"分析代码文件: {Path(file_path).name}",
                    target_files=[file_path],
                    priority=3
                )
                agent.executor.submit_task(task)
            else:
                self.log_warning("文件缺失", f"文件不存在: {file_path}")
        
        # 等待分析完成
        time.sleep(5)
        
        status = agent.executor.get_status()
        print(f"  分析完成: {status['stats']['completed']} 个任务")
        
        # 检查是否生成了经验
        kg_stats = agent.knowledge_graph.get_statistics()
        print(f"  知识库经验: {kg_stats['total_experiences']} 条")
        
        if kg_stats['total_experiences'] > 0:
            print("  ✓ 代码分析工作流正常")
        else:
            self.log_warning("经验生成", "代码分析后未生成经验")
        
        agent.stop()
        
        self.results["code_analysis"] = {
            "files_analyzed": len(real_files),
            "tasks_completed": status['stats']['completed'],
            "experiences": kg_stats['total_experiences']
        }

    # ==================== 测试2: 多模块协同工作流 ====================
    def test_multi_module_workflow(self):
        """测试2: 多模块协同工作流 - 分析→修复→优化完整流程"""
        print("\n" + "=" * 70)
        print("测试2: 多模块协同工作流")
        print("=" * 70)
        
        work_dir = self.test_dir / "multi_module"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        # 创建一个包含Bug的测试文件
        test_file = work_dir / "buggy_code.py"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text('''
def bad_function(x=[]):
    """有bug的函数"""
    x.append(1)
    return x

def no_docstring(a, b):
    return a + b

try:
    result = 1 / 0
except:
    pass
''')
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 步骤1: 分析代码
        print("\n  步骤1: 分析代码...")
        analyze_task = Task(
            id="workflow_analyze",
            task_type=TaskType.ANALYZE,
            description="分析代码中的问题",
            target_files=[str(test_file)],
            priority=3
        )
        agent.executor.submit_task(analyze_task)
        time.sleep(2)
        
        # 步骤2: 修复Bug
        print("  步骤2: 修复Bug...")
        fix_task = Task(
            id="workflow_fix",
            task_type=TaskType.BUG_FIX,
            description="修复检测到的Bug",
            target_files=[str(test_file)],
            priority=2
        )
        agent.executor.submit_task(fix_task)
        time.sleep(2)
        
        # 步骤3: 重构代码
        print("  步骤3: 重构代码...")
        refactor_task = Task(
            id="workflow_refactor",
            task_type=TaskType.REFACTOR,
            description="重构代码提高质量",
            target_files=[str(test_file)],
            priority=1
        )
        agent.executor.submit_task(refactor_task)
        time.sleep(2)
        
        # 等待所有任务完成
        time.sleep(3)
        
        status = agent.executor.get_status()
        print(f"\n  工作流完成: {status['stats']['completed']} 个任务完成")
        
        # 检查任务结果
        completed = status['stats']['completed']
        if completed >= 3:
            print("  ✓ 多模块协同工作流正常")
        else:
            self.log_warning("工作流", f"只有 {completed}/3 个任务完成")
        
        agent.stop()
        
        self.results["multi_module"] = {
            "tasks_completed": completed,
            "workflow_steps": 3
        }

    # ==================== 测试3: 异常注入测试 ====================
    def test_exception_injection(self):
        """测试3: 异常注入测试 - 模拟各种异常情况"""
        print("\n" + "=" * 70)
        print("测试3: 异常注入测试")
        print("=" * 70)
        
        work_dir = self.test_dir / "exception_injection"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        # 3.1 注入不存在的文件
        print("\n  3.1 测试不存在的文件...")
        task = Task(
            id="exception_nonexistent",
            task_type=TaskType.ANALYZE,
            description="分析不存在的文件",
            target_files=["/nonexistent/path/file.py"],
            priority=3
        )
        agent.executor.submit_task(task)
        time.sleep(1)
        
        # 3.2 注入空文件列表
        print("  3.2 测试空文件列表...")
        task = Task(
            id="exception_empty_files",
            task_type=TaskType.ANALYZE,
            description="分析空文件列表",
            target_files=[],
            priority=3
        )
        agent.executor.submit_task(task)
        time.sleep(1)
        
        # 3.3 注入超大描述
        print("  3.3 测试超大描述...")
        task = Task(
            id="exception_long_desc",
            task_type=TaskType.ANALYZE,
            description="x" * 10000,  # 10KB描述
            target_files=["/workspace/persistent_agent/config.py"],
            priority=3
        )
        agent.executor.submit_task(task)
        time.sleep(1)
        
        # 3.4 注入特殊字符
        print("  3.4 测试特殊字符...")
        special_descs = [
            "测试中文描述",
            "Test with emoji 🚀 🎉",
            "Test with 'quotes' and \"double quotes\"",
            "Test with \n newlines \r\n and \t tabs",
        ]
        for i, desc in enumerate(special_descs):
            task = Task(
                id=f"exception_special_{i}",
                task_type=TaskType.ANALYZE,
                description=desc,
                target_files=["/workspace/persistent_agent/config.py"],
                priority=3
            )
            agent.executor.submit_task(task)
        
        time.sleep(3)
        
        status = agent.executor.get_status()
        print(f"\n  异常注入测试完成: {status['stats']['completed']} 成功, {status['stats']['failed']} 失败")
        
        # 系统应该能处理异常而不崩溃
        print("  ✓ 异常注入测试完成，系统未崩溃")
        
        agent.stop()
        
        self.results["exception_injection"] = {
            "tests": 7,
            "completed": status['stats']['completed'],
            "failed": status['stats']['failed']
        }

    # ==================== 测试4: 故障恢复能力 ====================
    def test_fault_recovery(self):
        """测试4: 故障恢复能力 - 模拟各种故障场景"""
        print("\n" + "=" * 70)
        print("测试4: 故障恢复能力测试")
        print("=" * 70)
        
        # 4.1 模拟崩溃后恢复
        print("\n  4.1 模拟崩溃后恢复...")
        
        work_dir = self.test_dir / "fault_recovery"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        # 创建Agent并提交任务
        agent = PersistentAgent(work_dir=str(work_dir))
        
        for i in range(10):
            task = Task(
                id=f"recovery_task_{i}",
                task_type=TaskType.ANALYZE,
                description=f"恢复测试任务 {i}",
                target_files=["/workspace/persistent_agent/config.py"],
                priority=2
            )
            agent.executor.submit_task(task)
        
        time.sleep(1)
        
        # 不调用stop()直接删除引用模拟崩溃
        del agent
        
        # 重新创建Agent恢复
        try:
            agent2 = PersistentAgent(work_dir=str(work_dir))
            status = agent2.executor.get_status()
            print(f"    恢复后状态: pending={status['pending_tasks']}, running={status['running_tasks']}")
            
            # 等待恢复的任务执行
            time.sleep(3)
            status = agent2.executor.get_status()
            print(f"    3秒后: completed={status['stats']['completed']}")
            
            if status['stats']['completed'] > 0 or status['pending_tasks'] > 0:
                print("  ✓ 崩溃后恢复成功")
            else:
                self.log_warning("恢复能力", "崩溃后任务未恢复")
            
            agent2.stop()
        except Exception as e:
            self.log_bug("恢复能力", f"崩溃后恢复失败: {e}", "H")
        
        # 4.2 数据库损坏后恢复
        print("\n  4.2 数据库损坏后恢复...")
        
        work_dir2 = self.test_dir / "db_corruption"
        shutil.rmtree(work_dir2, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir2))
        
        # 添加正常数据
        for i in range(5):
            task = Task(
                id=f"normal_{i}",
                task_type=TaskType.ANALYZE,
                description=f"正常任务 {i}",
                target_files=["/workspace/persistent_agent/config.py"]
            )
            agent.executor.submit_task(task)
        
        time.sleep(1)
        agent.stop()
        
        # 模拟数据库损坏
        task_db = work_dir2 / "tasks.db"
        if task_db.exists():
            conn = sqlite3.connect(str(task_db))
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO tasks 
                    (id, task_type, description, target_files, status, retry_count)
                    VALUES ('corrupted', 'analyze', '损坏任务', '{invalid json', 'pending', 0)
                """)
                conn.commit()
            except:
                pass
            conn.close()
        
        # 尝试恢复
        try:
            agent2 = PersistentAgent(work_dir=str(work_dir2))
            status = agent2.executor.get_status()
            print(f"    数据库损坏后恢复: pending={status['pending_tasks']}")
            print("  ✓ 数据库损坏后恢复成功")
            agent2.stop()
        except Exception as e:
            self.log_bug("恢复能力", f"数据库损坏后恢复失败: {e}", "H")

    # ==================== 测试5: 长时间压力测试 ====================
    def test_sustained_pressure(self):
        """测试5: 长时间压力测试 - 持续高负载"""
        print("\n" + "=" * 70)
        print("测试5: 长时间压力测试")
        print("=" * 70)
        
        work_dir = self.test_dir / "sustained_pressure"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        duration = 60  # 1分钟持续压力
        print(f"\n  持续压力测试 {duration} 秒...")
        
        start_time = time.time()
        task_count = 0
        errors = []
        
        while time.time() - start_time < duration:
            try:
                task = Task(
                    id=f"pressure_{task_count}_{int(time.time()*1000)%10000}",
                    task_type=random.choice(list(TaskType)),
                    description=f"压力测试任务 {task_count}",
                    target_files=["/workspace/persistent_agent/config.py"],
                    priority=random.randint(1, 5)
                )
                agent.executor.submit_task(task)
                task_count += 1
                
                if task_count % 100 == 0:
                    status = agent.executor.get_status()
                    print(f"    已提交 {task_count} 任务, 完成 {status['stats']['completed']}")
                
                time.sleep(0.05)  # 每秒约20个任务
            except Exception as e:
                errors.append(str(e))
                if len(errors) <= 3:
                    self.log_bug("压力测试", f"提交任务失败: {e}", "H")
        
        # 等待剩余任务完成
        time.sleep(5)
        
        status = agent.executor.get_status()
        print(f"\n  压力测试完成:")
        print(f"    提交任务: {task_count}")
        print(f"    完成任务: {status['stats']['completed']}")
        print(f"    失败任务: {status['stats']['failed']}")
        print(f"    错误数: {len(errors)}")
        
        if len(errors) == 0:
            print("  ✓ 长时间压力测试通过")
        else:
            self.log_warning("压力测试", f"出现 {len(errors)} 个错误")
        
        agent.stop()
        
        self.results["sustained_pressure"] = {
            "duration": duration,
            "tasks_submitted": task_count,
            "errors": len(errors)
        }

    # ==================== 测试6: 并发安全测试 ====================
    def test_concurrency_safety(self):
        """测试6: 并发安全测试 - 多线程并发访问"""
        print("\n" + "=" * 70)
        print("测试6: 并发安全测试")
        print("=" * 70)
        
        work_dir = self.test_dir / "concurrency"
        shutil.rmtree(work_dir, ignore_errors=True)
        
        agent = PersistentAgent(work_dir=str(work_dir))
        
        errors = []
        lock = threading.Lock()
        
        # 6.1 并发任务提交
        print("\n  6.1 并发任务提交...")
        
        def submit_tasks(thread_id):
            try:
                for i in range(20):
                    task = Task(
                        id=f"concurrent_{thread_id}_{i}",
                        task_type=TaskType.ANALYZE,
                        description=f"并发任务 {thread_id}-{i}",
                        target_files=["/workspace/persistent_agent/config.py"]
                    )
                    agent.executor.submit_task(task)
            except Exception as e:
                with lock:
                    errors.append(f"提交-{thread_id}: {e}")
        
        threads = [threading.Thread(target=submit_tasks, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        if errors:
            self.log_bug("并发安全", f"并发提交出现 {len(errors)} 个错误", "H")
        else:
            print("  ✓ 20线程并发提交400任务成功")
        
        # 6.2 并发知识图谱访问
        print("\n  6.2 并发知识图谱访问...")
        
        kg_errors = []
        
        def access_kg(thread_id):
            try:
                for i in range(10):
                    exp = Experience(
                        id=f"kg_concurrent_{thread_id}_{i}",
                        task_type="analyze",
                        context="{}",
                        problem=f"并发KG测试 {thread_id}-{i}",
                        solution="解决方案",
                        outcome="成功",
                        lessons="教训",
                        code_snippets=[],
                        tags=["concurrent"],
                        created_at=datetime.now().isoformat()
                    )
                    agent.knowledge_graph.add_experience(exp)
                    
                    # 并发读取
                    agent.knowledge_graph.get_similar_experiences(f"并发KG测试 {thread_id}", limit=3)
            except Exception as e:
                with lock:
                    kg_errors.append(f"KG-{thread_id}: {e}")
        
        threads = [threading.Thread(target=access_kg, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        if kg_errors:
            self.log_bug("并发安全", f"知识图谱并发访问出现 {len(kg_errors)} 个错误", "H")
        else:
            print("  ✓ 10线程并发知识图谱访问成功")
        
        agent.stop()
        
        self.results["concurrency"] = {
            "submit_errors": len(errors),
            "kg_errors": len(kg_errors)
        }

    # ==================== 生成报告 ====================
    def generate_report(self):
        """生成测试报告"""
        print("\n" + "=" * 70)
        print("第11轮真实完整场景测试报告")
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
        report_path = self.test_dir / "report_round11.json"
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
    tester = RealWorldTestRound11()
    
    try:
        # 执行所有测试
        tester.test_real_code_analysis_workflow()
        tester.test_multi_module_workflow()
        tester.test_exception_injection()
        tester.test_fault_recovery()
        tester.test_sustained_pressure()
        tester.test_concurrency_safety()
        
        # 生成报告
        success = tester.generate_report()
        
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n测试执行异常: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
