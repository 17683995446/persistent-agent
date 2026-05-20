"""
极简全能持续运行Agent - 主控制器 V2
协调所有模块，实现持续运行、自动优化、经验积累
"""
import os
import sys
import json
import time
import uuid
import threading
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from config import *
from core.knowledge_graph import KnowledgeGraph, Experience, Pattern, get_knowledge_graph
from core.executor_v2 import TaskExecutorV2, Task, TaskType, TaskStatus
from core.optimizer import Optimizer
from core.watchdog import Watchdog, FileWatcher


class PersistentAgent:
    """
    持续运行Agent主控制器
    
    核心功能:
    1. 持续执行任务
    2. 自动优化方法
    3. 积累经验知识
    4. 崩溃自动恢复
    """
    
    def __init__(self, work_dir: str = None):
        self.work_dir = Path(work_dir or WORK_DIR)
        self._stop_event = threading.Event()  # 替代布尔标志，线程安全
        self._start_time: Optional[datetime] = None
        
        # 初始化日志
        self._setup_logging()
        
        # 初始化核心模块 (使用V2执行器)
        self.knowledge_graph = get_knowledge_graph()
        self.executor = TaskExecutorV2(self.knowledge_graph, str(self.work_dir), auto_start=True)
        self.optimizer = Optimizer(self.knowledge_graph, self.executor)
        # 传入安全模式回调
        self.watchdog = Watchdog(main_process_callback=self._on_watchdog_event)
        self.file_watcher: Optional[FileWatcher] = None
        
        # 统计
        self.stats = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "experiences_recorded": 0,
            "optimizations_run": 0
        }
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"PersistentAgent 初始化完成, 工作目录: {self.work_dir}")
    
    def _on_watchdog_event(self, event: Dict[str, Any]):
        """保活模块事件回调"""
        event_type = event.get("type", "")
        if event_type == "safe_mode":
            self.logger.warning("收到安全模式通知，暂停接受新任务")
            # 可以在这里实现暂停逻辑
        elif event_type == "crash":
            self.logger.error(f"崩溃事件: {event.get('message', '')}")
    
    def _setup_logging(self):
        """配置日志"""
        log_file = LOG_DIR / f"agent_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format=LOG_FORMAT,
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def start(self, watch_files: bool = True):
        """启动Agent"""
        self._stop_event.clear()
        self._start_time = datetime.now()
        
        self.logger.info("=" * 50)
        self.logger.info("极简全能持续运行Agent 启动中...")
        self.logger.info(f"工作目录: {self.work_dir}")
        self.logger.info(f"知识库: {KNOWLEDGE_GRAPH_DB}")
        self.logger.info("=" * 50)
        
        # 加载历史经验
        self._load_initial_knowledge()
        
        # 启动各模块（executor已在__init__中auto_start）
        self.optimizer.start()
        self.watchdog.start()
        
        # 启动文件监控
        if watch_files and self.work_dir.exists():
            self.file_watcher = FileWatcher(
                str(self.work_dir),
                self._on_file_change
            )
            self.file_watcher.start()
        
        # 启动主循环
        main_thread = threading.Thread(target=self._main_loop, name="MainLoop")
        main_thread.daemon = True
        main_thread.start()
        
        self.logger.info("Agent 已启动, 进入持续运行模式")
        
        # 保持主线程运行
        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("收到中断信号, 正在停止...")
            self.stop()
    
    def stop(self):
        """停止Agent"""
        self._stop_event.set()
        
        self.logger.info("正在停止各模块...")
        
        if self.file_watcher:
            self.file_watcher.stop()
        
        self.optimizer.stop()
        self.executor.stop()
        self.watchdog.stop()
        
        # 保存状态
        self._save_state()
        
        self.logger.info("Agent 已完全停止")
    
    def _main_loop(self):
        """主循环 - 持续运行核心"""
        loop_interval = 5  # 秒
        save_counter = 0
        
        while not self._stop_event.is_set():
            try:
                # 1. 检查执行状态并更新统计
                self._check_execution_status()
                
                # 2. 更新统计
                self._update_stats()
                
                # 3. 定期保存状态（每30次循环 ≈ 150秒）
                save_counter += 1
                if save_counter >= 30:
                    self._save_state()
                    save_counter = 0
                
                # 4. 检查安全模式
                if hasattr(self.watchdog, 'in_safe_mode') and self.watchdog.in_safe_mode:
                    self.logger.warning("系统处于安全模式，等待恢复...")
                
                self._stop_event.wait(loop_interval)
                
            except Exception as e:
                self.logger.error(f"主循环异常: {e}")
                self.watchdog.record_crash(e)
    
    def _load_initial_knowledge(self):
        """加载初始知识"""
        stats = self.knowledge_graph.get_statistics()
        self.logger.info(f"知识库状态: {stats['total_experiences']} 条经验, {stats['total_patterns']} 个模式")
        
        # 如果知识库为空,添加一些基础模式
        if stats['total_patterns'] == 0:
            self._init_default_patterns()
    
    def _init_default_patterns(self):
        """初始化默认模式"""
        default_patterns = [
            Pattern(
                id="pattern_bug_null_check",
                pattern_type="bug_pattern",
                name="空值检查缺失",
                description="未检查变量是否为None/null就使用",
                code_example="if value is not None:\n    process(value)",
                when_to_use="处理可能为空的变量时",
                effectiveness=0.9,
                created_at=datetime.now().isoformat()
            ),
            Pattern(
                id="pattern_exception_handling",
                pattern_type="best_practice",
                name="异常处理最佳实践",
                description="使用具体的异常类型而非裸except",
                code_example="try:\n    operation()\nexcept SpecificError as e:\n    handle(e)",
                when_to_use="所有需要异常处理的场景",
                effectiveness=0.85,
                created_at=datetime.now().isoformat()
            ),
            Pattern(
                id="pattern_function_docstring",
                pattern_type="best_practice",
                name="函数文档字符串",
                description="为函数添加清晰的文档说明",
                code_example="def process(data):\n    \"\"\"处理输入数据并返回结果。\n    \n    Args:\n        data: 输入数据\n    Returns:\n        处理后的结果\n    \"\"\"",
                when_to_use="所有公共函数",
                effectiveness=0.8,
                created_at=datetime.now().isoformat()
            )
        ]
        
        for pattern in default_patterns:
            self.knowledge_graph.add_pattern(pattern)
        
        self.logger.info(f"已初始化 {len(default_patterns)} 个默认模式")
    
    def _on_file_change(self, file_path: str, event_type: str):
        """文件变化回调"""
        self.logger.info(f"文件事件: {event_type} - {file_path}")
        
        if event_type in ("created", "modified"):
            # 自动创建分析任务
            task = Task(
                id=self._generate_task_id(),
                task_type=TaskType.ANALYZE,
                description=f"分析文件: {Path(file_path).name}",
                target_files=[file_path],
                priority=2
            )
            self.submit_task(task)
    
    def submit_task(self, task: Task) -> str:
        """提交任务"""
        # I/O操作在锁外执行
        try:
            similar = self.knowledge_graph.get_similar_experiences(task.description, limit=3)
            if similar:
                task.context["related_experiences"] = [
                    {"id": exp.id, "solution": exp.solution[:100]} 
                    for exp, _ in similar
                ]
                self.logger.info(f"找到 {len(similar)} 条相关经验")
        except Exception as e:
            self.logger.warning(f"查找相关经验失败: {e}")
        
        return self.executor.submit_task(task)
    
    def _check_execution_status(self):
        """检查执行状态"""
        try:
            status = self.executor.get_status()
            executor_stats = status.get("stats", {})
            self.stats["tasks_completed"] = executor_stats.get("completed", 0)
            self.stats["tasks_failed"] = executor_stats.get("failed", 0)
        except Exception as e:
            self.logger.warning(f"获取执行状态失败: {e}")
    
    def _update_stats(self):
        """更新统计"""
        try:
            kg_stats = self.knowledge_graph.get_statistics()
            self.stats["experiences_recorded"] = kg_stats["total_experiences"]
        except Exception as e:
            self.logger.warning(f"更新统计失败: {e}")
    
    def _save_state(self):
        """保存当前状态"""
        try:
            state = {
                "timestamp": datetime.now().isoformat(),
                "work_dir": str(self.work_dir),
                "stats": self.stats,
                "executor_status": self.executor.get_status(),
                "optimizer_report": self.optimizer.get_optimization_report(),
                "watchdog_status": self.watchdog.get_status()
            }
            
            state_file = LOG_DIR / "agent_state.json"
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"保存状态失败: {e}")
    
    def _generate_task_id(self) -> str:
        """生成任务ID"""
        return f"task_{uuid.uuid4().hex[:8]}"
    
    # ==================== 公共API ====================
    
    def analyze_code(self, file_paths: List[str], description: str = "") -> str:
        """分析代码"""
        task = Task(
            id=self._generate_task_id(),
            task_type=TaskType.ANALYZE,
            description=description or "代码分析",
            target_files=file_paths,
            priority=3
        )
        return self.submit_task(task)
    
    def fix_bug(self, file_path: str, bug_description: str) -> str:
        """修复Bug"""
        task = Task(
            id=self._generate_task_id(),
            task_type=TaskType.BUG_FIX,
            description=bug_description,
            target_files=[file_path],
            priority=4
        )
        return self.submit_task(task)
    
    def refactor_code(self, file_paths: List[str], refactor_goal: str) -> str:
        """重构代码"""
        task = Task(
            id=self._generate_task_id(),
            task_type=TaskType.REFACTOR,
            description=refactor_goal,
            target_files=file_paths,
            priority=2
        )
        return self.submit_task(task)
    
    def implement_feature(self, description: str, target_files: List[str] = None) -> str:
        """实现功能"""
        task = Task(
            id=self._generate_task_id(),
            task_type=TaskType.IMPLEMENT,
            description=description,
            target_files=target_files or [],
            priority=3
        )
        return self.submit_task(task)
    
    def get_status(self) -> Dict[str, Any]:
        """获取Agent状态"""
        return {
            "running": not self._stop_event.is_set(),
            "uptime": str(datetime.now() - self._start_time) if self._start_time else "0:00:00",
            "work_dir": str(self.work_dir),
            "stats": self.stats,
            "executor": self.executor.get_status(),
            "optimizer": self.optimizer.get_optimization_report(),
            "watchdog": self.watchdog.get_status(),
            "knowledge": self.knowledge_graph.get_statistics()
        }
    
    def get_improvement_suggestions(self) -> List[Dict[str, Any]]:
        """获取改进建议"""
        return self.optimizer.suggest_improvements()
    
    def export_knowledge(self, output_path: str = None):
        """导出知识库"""
        output_path = output_path or str(LOG_DIR / "knowledge_export.json")
        self.knowledge_graph.export_knowledge(output_path)
        return output_path


def main():
    """主入口"""
    parser = argparse.ArgumentParser(description="极简全能持续运行Agent")
    parser.add_argument("--work-dir", "-w", default=".", help="工作目录")
    parser.add_argument("--no-watch", action="store_true", help="禁用文件监控")
    parser.add_argument("--status", action="store_true", help="显示状态后退出")
    parser.add_argument("--export", action="store_true", help="导出知识库后退出")
    
    args = parser.parse_args()
    
    agent = PersistentAgent(work_dir=args.work_dir)
    
    if args.status:
        print(json.dumps(agent.get_status(), indent=2, ensure_ascii=False, default=str))
        return
    
    if args.export:
        output = agent.export_knowledge()
        print(f"知识库已导出到: {output}")
        return
    
    # 正常启动
    agent.start(watch_files=not args.no_watch)


if __name__ == "__main__":
    main()
