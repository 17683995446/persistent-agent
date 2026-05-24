"""
执行引擎模块 - 任务执行核心
支持：需求实现、代码重构、代码分析、Bug修复
"""
import os
import re
import ast
import json
import subprocess
import threading
import queue
import traceback
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import logging
from collections import deque
from core.knowledge_graph import Experience

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """任务类型"""
    IMPLEMENT = "implement"      # 实现需求
    REFACTOR = "refactor"        # 重构代码
    ANALYZE = "analyze"          # 分析代码
    BUG_FIX = "bug_fix"          # 修复Bug
    OPTIMIZE = "optimize"        # 优化代码
    TEST = "test"                # 测试代码


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """任务定义"""
    id: str
    task_type: TaskType
    description: str
    target_files: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    priority: int = 1  # 1-5, 5最高
    status: TaskStatus = TaskStatus.PENDING
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0
    related_experiences: List[str] = field(default_factory=list)
    max_retries: int = 3  # 最大重试次数
    timeout_seconds: int = 300  # 任务超时时间(秒)


@dataclass
class ExecutionResult:
    """执行结果"""
    success: bool
    task_id: str
    output: str
    changes: List[Dict[str, Any]]
    issues_found: List[Dict[str, Any]]
    suggestions: List[str]
    execution_time: float
    experience_id: Optional[str] = None


class CodeAnalyzer:
    """代码分析器"""
    
    @staticmethod
    def analyze_file(file_path: str) -> Dict[str, Any]:
        """分析单个文件"""
        result = {
            "file_path": file_path,
            "exists": os.path.exists(file_path),
            "size": 0,
            "lines": 0,
            "functions": [],
            "classes": [],
            "imports": [],
            "issues": [],
            "complexity": 0
        }
        
        if not result["exists"]:
            return result
        
        # 尝试多种编码读取文件
        content = None
        encodings = ['utf-8', 'gbk', 'latin-1']
        try:
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
        except Exception as e:
            result["issues"].append(f"分析错误: {str(e)}")
            return result

        if content is None:
            result["issues"].append("分析错误: 无法识别文件编码")
            return result

        try:
            result["size"] = len(content)
            result["lines"] = content.count('\n') + 1

            # Python文件AST分析
            if file_path.endswith('.py'):
                result.update(CodeAnalyzer._analyze_python(content))
            # JavaScript/TypeScript分析
            elif file_path.endswith(('.js', '.ts', '.jsx', '.tsx')):
                result.update(CodeAnalyzer._analyze_javascript(content))

        except Exception as e:
            result["issues"].append(f"分析错误: {str(e)}")
        
        return result
    
    @staticmethod
    def _analyze_python(content: str) -> Dict[str, Any]:
        """分析Python代码"""
        result = {
            "functions": [],
            "classes": [],
            "imports": [],
            "issues": [],
            "complexity": 0
        }
        
        try:
            tree = ast.parse(content)

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    result["functions"].append({
                        "name": node.name,
                        "line": node.lineno,
                        "args": [arg.arg for arg in node.args.args],
                        "docstring": ast.get_docstring(node) or ""
                    })
                    result["complexity"] += 1

                elif isinstance(node, ast.ClassDef):
                    methods = [n.name for n in node.body if isinstance(n, ast.FunctionDef)]
                    result["classes"].append({
                        "name": node.name,
                        "line": node.lineno,
                        "methods": methods
                    })
                    result["complexity"] += len(methods)

                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            result["imports"].append(alias.name)
                    else:
                        module = node.module or ""
                        for alias in node.names:
                            result["imports"].append(f"{module}.{alias.name}" if module else alias.name)

            # 检测常见问题
            result["issues"].extend(CodeAnalyzer._detect_python_issues(content, tree))

        except SyntaxError as e:
            result["issues"].append(f"语法错误: {e.msg} (行 {e.lineno})")
        except MemoryError:
            result["issues"].append("解析错误: 文件过大导致内存不足")
        except RecursionError:
            result["issues"].append("解析错误: 代码结构过于复杂导致递归深度超限")
        except Exception as e:
            result["issues"].append(f"解析错误: {str(e)}")
        
        return result
    
    @staticmethod
    def _detect_python_issues(content: str, tree: ast.AST) -> List[str]:
        """检测Python代码问题"""
        issues = []
        
        # 检测长函数
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_lines = node.end_lineno - node.lineno if hasattr(node, 'end_lineno') else 0
                if func_lines > 50:
                    issues.append(f"函数 {node.name} 过长 ({func_lines} 行), 建议拆分")
        
        # 检测TODO/FIXME
        for i, line in enumerate(content.split('\n'), 1):
            if 'TODO' in line or 'FIXME' in line:
                issues.append(f"行 {i}: 发现待办事项 - {line.strip()}")
        
        return issues
    
    @staticmethod
    def _analyze_javascript(content: str) -> Dict[str, Any]:
        """分析JavaScript/TypeScript代码"""
        result = {
            "functions": [],
            "classes": [],
            "imports": [],
            "issues": [],
            "complexity": 0
        }
        
        # 简单的正则分析
        # 函数检测
        func_pattern = r'(?:function\s+(\w+)|(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s*)?\(?[^)]*\)?\s*=>)'
        for match in re.finditer(func_pattern, content):
            name = match.group(1) or match.group(2)
            if name:
                result["functions"].append({"name": name})
                result["complexity"] += 1
        
        # 类检测
        class_pattern = r'class\s+(\w+)'
        for match in re.finditer(class_pattern, content):
            result["classes"].append({"name": match.group(1)})
        
        # import检测
        import_pattern = r'import\s+.*?from\s+[\'"]([^\'"]+)[\'"]'
        for match in re.finditer(import_pattern, content):
            result["imports"].append(match.group(1))
        
        return result
    
    @staticmethod
    def detect_bugs(file_path: str) -> List[Dict[str, Any]]:
        """检测潜在Bug"""
        bugs = []

        if not os.path.exists(file_path):
            return bugs

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            lines = content.split('\n')

        # Python常见Bug模式
        if file_path.endswith('.py'):
            for i, line in enumerate(lines, 1):
                # 可变默认参数
                if re.search(r'def\s+\w+\([^)]*=\s*\[\]', line):
                    bugs.append({
                        "line": i,
                        "type": "mutable_default_arg",
                        "message": "可变默认参数可能导致意外行为",
                        "severity": "medium"
                    })

                # 裸except
                if re.search(r'except\s*:', line):
                    bugs.append({
                        "line": i,
                        "type": "bare_except",
                        "message": "裸except会捕获所有异常包括KeyboardInterrupt",
                        "severity": "medium"
                    })

                # == None 而非 is None
                if '== None' in line or '!= None' in line:
                    bugs.append({
                        "line": i,
                        "type": "equality_none",
                        "message": "应使用 'is None' 而非 '== None'",
                        "severity": "low"
                    })

            # 资源泄漏检测：open() 未在 with 中使用且未显式 close()
            bugs.extend(CodeAnalyzer._detect_resource_leaks(content, lines))

            # 空指针风险检测：参数可能为None但直接调用方法
            bugs.extend(CodeAnalyzer._detect_potential_none(content))

            # 未使用的导入检测
            bugs.extend(CodeAnalyzer._detect_unused_imports(content))

        return bugs

    @staticmethod
    def _detect_resource_leaks(content: str, lines: List[str]) -> List[Dict[str, Any]]:
        """
        资源泄漏检测
        检测 open() 调用但未在 with 语句中使用且未显式 close() 的情况
        """
        bugs = []

        # 检查当前行是否在 with 语句块内
        def is_inside_with_block(target_line_idx: int) -> bool:
            """判断目标行是否在 with 语句块内"""
            for j in range(target_line_idx):
                stripped = lines[j].strip()
                # 检查 with 语句（可能跨多行）
                if re.match(r'with\s+', stripped) or re.match(r'with\s*\(', stripped):
                    # 向下查找 with 块的结束位置
                    indent = len(lines[j]) - len(lines[j].lstrip())
                    for k in range(j + 1, len(lines)):
                        if lines[k].strip() == '':
                            continue
                        curr_indent = len(lines[k]) - len(lines[k].lstrip())
                        if curr_indent <= indent:
                            break
                        if k == target_line_idx:
                            return True
            return False

        # 检查是否是 with open(...) as xxx: 模式的一部分
        def is_with_open_statement(line_text: str) -> bool:
            """检查行是否是 with open(...) as xxx: 语句"""
            stripped = line_text.strip()
            return bool(re.match(r'with\s+.*open\s*\(', stripped))

        # 查找所有 open() 赋值语句
        for i, line in enumerate(lines):
            stripped = line.strip()
            # 匹配 xxx = open(...) 模式
            match = re.match(r'(\w+)\s*=\s*open\s*\(', stripped)
            if match:
                var_name = match.group(1)
                # 检查该行是否是 with open(...) as xxx: 模式
                if is_with_open_statement(line):
                    continue
                # 检查该行是否在 with 块内
                if is_inside_with_block(i):
                    continue
                # 获取当前行的缩进级别，用于限制 close() 搜索范围在同一代码块内
                open_indent = len(line) - len(line.lstrip())
                # 检查后续代码中是否有 var_name.close() 调用（仅在同一代码块内）
                has_close = False
                for j in range(i + 1, len(lines)):
                    curr_stripped = lines[j].strip()
                    if curr_stripped == '':
                        continue
                    curr_indent = len(lines[j]) - len(lines[j].lstrip())
                    # 如果缩进比 open() 更外层，或者遇到新的顶级定义，说明离开了当前代码块
                    if curr_indent < open_indent:
                        break
                    # 遇到同级的 def/class 说明离开了当前函数
                    if curr_indent == open_indent and re.match(r'(def|class)\s+', curr_stripped):
                        break
                    if re.search(rf'{re.escape(var_name)}\.close\s*\(\)', lines[j]):
                        has_close = True
                        break
                if not has_close:
                    bugs.append({
                        "line": i + 1,
                        "type": "resource_leak",
                        "message": f"变量 '{var_name}' 由 open() 赋值但未在 with 语句中使用且未显式 close()，可能导致资源泄漏",
                        "severity": "high"
                    })

        return bugs

    @staticmethod
    def _detect_potential_none(content: str) -> List[Dict[str, Any]]:
        """
        空指针风险检测
        检测函数参数可能为None但直接调用 .strip(), .split(), .lower() 等方法的情况
        """
        bugs = []

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return bugs

        # 需要检测的方法列表（可能因None而抛出AttributeError的常见字符串/列表方法）
        risky_methods = {
            'strip', 'lstrip', 'rstrip', 'split', 'rsplit', 'splitlines',
            'lower', 'upper', 'capitalize', 'title', 'swapcase',
            'startswith', 'endswith', 'find', 'rfind', 'index', 'rindex',
            'replace', 'format', 'join', 'encode', 'decode',
            'count', 'expandtabs', 'translate', 'zfill',
            'center', 'ljust', 'rjust',
            'append', 'extend', 'pop', 'remove', 'insert', 'clear',
            'sort', 'reverse', 'copy', 'keys', 'values', 'items',
            'get', 'update', 'read', 'write', 'readline', 'readlines',
        }

        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue

            # 收集函数参数名
            param_names = set()
            for arg in node.args.args:
                param_names.add(arg.arg)

            # 检查函数体中是否有对参数的 None 检查
            # 收集所有被检查过 None 的变量
            none_checked_vars = set()
            for body_node in ast.walk(node):
                # 检测 if xxx is None / if xxx is not None / if xxx == None 等模式
                if isinstance(body_node, ast.If):
                    # 检查 if 条件中是否有 None 比较
                    for test_node in ast.walk(body_node.test):
                        if isinstance(test_node, ast.Name) and test_node.id in param_names:
                            # 检查父节点是否是 Compare 且包含 None
                            if isinstance(body_node.test, ast.Compare):
                                for comparator in body_node.test.comparators:
                                    if isinstance(comparator, ast.Constant) and comparator.value is None:
                                        none_checked_vars.add(test_node.id)
                            # 检查是否是 if not xxx 模式
                            if isinstance(body_node.test, ast.UnaryOp) and isinstance(body_node.test.op, ast.Not):
                                if isinstance(body_node.test.operand, ast.Name):
                                    none_checked_vars.add(body_node.test.operand.id)

            # 遍历函数体，查找参数直接调用方法的情况
            for body_node in ast.walk(node):
                if isinstance(body_node, ast.Call):
                    # 检查 func.attr 是否是风险方法
                    if isinstance(body_node.func, ast.Attribute) and body_node.func.attr in risky_methods:
                        # 检查调用对象是否是函数参数
                        if isinstance(body_node.func.value, ast.Name) and body_node.func.value.id in param_names:
                            param_name = body_node.func.value.id
                            # 如果该参数没有被 None 检查过，则报告
                            if param_name not in none_checked_vars:
                                bugs.append({
                                    "line": body_node.lineno,
                                    "type": "potential_none",
                                    "message": f"参数 '{param_name}' 可能为 None 但直接调用了 .{body_node.func.attr}() 方法，建议添加 None 检查",
                                    "severity": "medium"
                                })

        return bugs

    @staticmethod
    def _detect_unused_imports(content: str) -> List[Dict[str, Any]]:
        """
        未使用的导入检测
        检测 import 了但未在代码中使用的模块
        """
        bugs = []

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return bugs

        # 收集所有导入信息：(行号, 导入名称, 是否是 from...import)
        imports_info = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    # 获取实际使用的名称（可能是 as 别名）
                    used_name = alias.asname if alias.asname else alias.name.split('.')[0]
                    imports_info.append((node.lineno, used_name, alias.name))
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    for alias in node.names:
                        used_name = alias.asname if alias.asname else alias.name
                        imports_info.append((node.lineno, used_name, f"{node.module}.{alias.name}"))

        # 收集代码中所有使用的名称
        used_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                used_names.add(node.id)
            elif isinstance(node, ast.Attribute):
                # 对于属性访问，记录属性名和值的名
                if isinstance(node.value, ast.Name):
                    used_names.add(node.value.id)

        # 检查每个导入是否被使用
        for line_no, used_name, full_name in imports_info:
            if used_name not in used_names:
                # 只排除 from __future__ 导入（这些是编译器指令，不需要在代码中显式使用）
                if full_name.startswith("__future__."):
                    continue
                bugs.append({
                    "line": line_no,
                    "type": "unused_import",
                    "message": f"导入了 '{full_name}' 但未在代码中使用",
                    "severity": "low"
                })

        return bugs


class TaskExecutor:
    """
    任务执行器
    负责执行各类任务并记录经验
    """
    
    def __init__(self, knowledge_graph, work_dir: str):
        self.kg = knowledge_graph
        self.work_dir = Path(work_dir)
        self.task_queue = queue.PriorityQueue()
        self.active_tasks: Dict[str, Task] = {}
        self.completed_tasks: deque = deque(maxlen=1000)
        self._lock = threading.RLock()
        self._running = threading.Event()
        self._workers: List[threading.Thread] = []
        
        # 注册任务处理器
        self.handlers: Dict[TaskType, Callable] = {
            TaskType.ANALYZE: self._handle_analyze,
            TaskType.BUG_FIX: self._handle_bug_fix,
            TaskType.REFACTOR: self._handle_refactor,
            TaskType.IMPLEMENT: self._handle_implement,
            TaskType.OPTIMIZE: self._handle_optimize,
            TaskType.TEST: self._handle_test,
        }
    
    def _resolve_safe_path(self, file_path: str):
        """安全地解析文件路径，防止路径遍历攻击"""
        try:
            if os.path.isabs(file_path):
                full_path = Path(file_path).resolve()
            else:
                full_path = (self.work_dir / file_path).resolve()
            work_dir_resolved = self.work_dir.resolve()
            try:
                full_path.relative_to(work_dir_resolved)
                return str(full_path)
            except ValueError:
                logger.warning(f"路径遍历尝试被阻止: {file_path}")
                return None
        except (OSError, ValueError) as e:
            logger.warning(f"路径解析失败: {file_path}, 错误: {e}")
            return None

    def submit_task(self, task: Task) -> str:
        """提交任务"""
        with self._lock:
            # 查找相关经验
            similar = self.kg.get_similar_experiences(task.description, limit=3)
            task.related_experiences = [exp.id for exp, _ in similar]
            
            # 加入队列(优先级取负,数值大的优先)
            self.task_queue.put((-task.priority, task.created_at, task.id))
            self.active_tasks[task.id] = task
            
            logger.info(f"任务已提交: {task.id} - {task.task_type.value}")
        
        return task.id
    
    def start(self, num_workers: int = 2):
        """启动执行器"""
        with self._lock:
            if self._running.is_set():
                logger.warning("执行器已在运行")
                return
            self._running.set()
        for i in range(num_workers):
            worker = threading.Thread(target=self._worker_loop, name=f"Worker-{i}")
            worker.daemon = True
            worker.start()
            self._workers.append(worker)
        logger.info(f"执行器已启动, {num_workers}个工作线程")
    
    def stop(self):
        """停止执行器"""
        with self._lock:
            if not self._running.is_set():
                logger.warning("执行器未在运行")
                return
            self._running.clear()

        # 等待工作线程结束
        for worker in self._workers:
            worker.join(timeout=5)

        with self._lock:
            # 清空队列
            while not self.task_queue.empty():
                try:
                    self.task_queue.get_nowait()
                    self.task_queue.task_done()
                except queue.Empty:
                    break

            # 将未执行任务标记为取消，将RUNNING任务标记为FAILED
            for task_id, task in list(self.active_tasks.items()):
                if task.status == TaskStatus.RUNNING:
                    task.status = TaskStatus.FAILED
                    task.error = "执行器停止，任务被强制终止"
                elif task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.FAILED
                    task.error = "执行器停止，任务被取消"
                self.completed_tasks.append(task)

            self.active_tasks.clear()
            self._workers.clear()

        logger.info("执行器已停止")
    
    def _worker_loop(self):
        """工作线程主循环"""
        while self._running.is_set():
            try:
                # 获取任务(带超时)
                try:
                    _, _, task_id = self.task_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                try:
                    with self._lock:
                        task = self.active_tasks.get(task_id)
                        if not task:
                            continue
                        
                        task.status = TaskStatus.RUNNING
                        task.started_at = datetime.now().isoformat()
                    
                    # 执行任务
                    result = self._execute_task(task)
                    
                    # 更新任务状态
                    with self._lock:
                        task.status = TaskStatus.COMPLETED if result.success else TaskStatus.FAILED
                        task.completed_at = datetime.now().isoformat()
                        task.result = result.__dict__
                        
                        if not result.success:
                            task.error = result.output
                        
                        self.completed_tasks.append(task)
                        if task.id in self.active_tasks:
                            del self.active_tasks[task.id]
                finally:
                    self.task_queue.task_done()
                
            except Exception as e:
                logger.error(f"工作线程异常: {traceback.format_exc()}")
    
    def _execute_task(self, task: Task) -> ExecutionResult:
        """执行单个任务"""
        start_time = datetime.now()
        
        try:
            handler = self.handlers.get(task.task_type)
            if not handler:
                return ExecutionResult(
                    success=False,
                    task_id=task.id,
                    output=f"未知的任务类型: {task.task_type}",
                    changes=[],
                    issues_found=[],
                    suggestions=[],
                    execution_time=0
                )
            
            # 执行处理器
            result = handler(task)
            
            # 计算执行时间
            execution_time = (datetime.now() - start_time).total_seconds()
            result.execution_time = execution_time
            
            # 记录经验
            experience_id = self._record_experience(task, result)
            result.experience_id = experience_id
            
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output=f"执行异常: {str(e)}\n{traceback.format_exc()}",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=execution_time
            )
    
    def _handle_analyze(self, task: Task) -> ExecutionResult:
        """处理代码分析任务"""
        if not task.target_files:
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="未指定目标文件",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=0
            )

        results = []
        issues = []
        suggestions = []
        
        for file_path in task.target_files:
            full_path = self._resolve_safe_path(file_path)
            if full_path is None:
                issues.append({"file": file_path, "type": "path_traversal", "message": f"路径遍历尝试被阻止: {file_path}", "severity": "high"})
                continue

            # 分析文件
            analysis = CodeAnalyzer.analyze_file(full_path)
            results.append(analysis)

            # 检测Bug
            bugs = CodeAnalyzer.detect_bugs(full_path)
            issues.extend(bugs)
            
            # 生成建议
            if analysis.get("complexity", 0) > 20:
                suggestions.append(f"{file_path}: 复杂度过高, 建议拆分模块")
            
            if len(analysis.get("functions", [])) > 15:
                suggestions.append(f"{file_path}: 函数过多, 建议按职责拆分文件")
        
        output = f"分析了 {len(task.target_files)} 个文件\n"
        output += f"发现 {len(issues)} 个潜在问题\n"
        output += f"生成 {len(suggestions)} 条建议"
        
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=output,
            changes=[],
            issues_found=issues,
            suggestions=suggestions,
            execution_time=0
        )
    
    def _handle_bug_fix(self, task: Task) -> ExecutionResult:
        """处理Bug修复任务"""
        if not task.target_files:
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="未指定目标文件",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=0
            )

        # 先分析问题
        issues = []
        changes = []
        files_processed = 0
        
        for file_path in task.target_files:
            full_path = self._resolve_safe_path(file_path)
            if full_path is None:
                issues.append({"file": file_path, "type": "path_traversal", "message": f"路径遍历尝试被阻止: {file_path}", "severity": "high"})
                continue

            if not os.path.exists(full_path):
                continue

            # 检测Bug
            bugs = CodeAnalyzer.detect_bugs(full_path)
            issues.extend(bugs)
            files_processed += 1
            
            # 查找相关经验
            similar = self.kg.get_similar_experiences(task.description)
            if similar:
                logger.info(f"找到 {len(similar)} 条相关经验")
        
        # 生成修复建议
        suggestions = []
        for issue in issues:
            suggestions.append(f"行 {issue['line']}: {issue['message']}")
        
        output = f"检测到 {len(issues)} 个潜在Bug\n"
        output += "建议检查以下问题:\n" + "\n".join(suggestions) if suggestions else "未发现明显Bug模式"
        
        if files_processed == 0:
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="所有目标文件均不存在",
                changes=changes,
                issues_found=issues,
                suggestions=suggestions,
                execution_time=0
            )
        
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=output,
            changes=changes,
            issues_found=issues,
            suggestions=suggestions,
            execution_time=0
        )
    
    def _handle_refactor(self, task: Task) -> ExecutionResult:
        """处理重构任务"""
        if not task.target_files:
            return ExecutionResult(
                success=False,
                task_id=task.id,
                output="未指定目标文件",
                changes=[],
                issues_found=[],
                suggestions=[],
                execution_time=0
            )

        suggestions = []
        changes = []
        
        for file_path in task.target_files:
            full_path = self._resolve_safe_path(file_path)
            if full_path is None:
                suggestions.append(f"{file_path}: 路径遍历尝试被阻止")
                continue
            analysis = CodeAnalyzer.analyze_file(full_path)
            
            # 重构建议
            if analysis.get("complexity", 0) > 15:
                suggestions.append(f"{file_path}: 考虑将复杂函数拆分为更小的函数")
            
            # 检查函数长度
            for func in analysis.get("functions", []):
                if func.get("docstring") == "":
                    suggestions.append(f"{file_path}: 函数 {func['name']} 缺少文档字符串")
        
        output = f"生成了 {len(suggestions)} 条重构建议"
        
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=output,
            changes=changes,
            issues_found=[],
            suggestions=suggestions,
            execution_time=0
        )
    
    def _handle_implement(self, task: Task) -> ExecutionResult:
        """处理实现任务"""
        suggestions = []
        similar = []

        # 知识图谱查询是可选依赖，失败时不应导致任务失败
        try:
            similar = self.kg.get_similar_experiences(task.description)
            if similar:
                for exp, score in similar[:3]:
                    suggestions.append(f"参考经验 {exp.id}: {(exp.solution or '')[:100]}...")
        except Exception as e:
            # 知识图谱查询失败，记录警告但继续执行任务
            logger.warning(f"知识图谱查询失败: {str(e)}")
            suggestions.append(f"[警告] 知识图谱查询失败: {str(e)}")

        output = f"实现任务: {task.description}\n"
        output += f"找到 {len(similar)} 条相关经验\n"
        if suggestions:
            output += "参考建议:\n" + "\n".join(suggestions)

        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=output,
            changes=[],
            issues_found=[],
            suggestions=suggestions,
            execution_time=0
        )
    
    def _handle_optimize(self, task: Task) -> ExecutionResult:
        """处理优化任务"""
        suggestions = []
        
        for file_path in task.target_files:
            full_path = self._resolve_safe_path(file_path)
            if full_path is None:
                suggestions.append(f"{file_path}: 路径遍历尝试被阻止")
                continue
            analysis = CodeAnalyzer.analyze_file(full_path)
            
            # 性能优化建议
            if file_path.endswith('.py'):
                suggestions.append("考虑使用性能分析工具 (cProfile, line_profiler)")
            
            if analysis.get("lines", 0) > 500:
                suggestions.append(f"{file_path}: 文件过大, 建议模块化拆分")
        
        return ExecutionResult(
            success=True,
            task_id=task.id,
            output=f"生成了 {len(suggestions)} 条优化建议",
            changes=[],
            issues_found=[],
            suggestions=suggestions,
            execution_time=0
        )
    
    def _handle_test(self, task: Task) -> ExecutionResult:
        """处理测试任务"""
        results = []
        
        for file_path in task.target_files:
            full_path = self._resolve_safe_path(file_path)
            if full_path is None:
                results.append({
                    "file": file_path,
                    "test_file": None,
                    "passed": None,
                    "output": "路径遍历尝试被阻止"
                })
                continue
            
            # 检查是否存在测试文件
            test_file = self._find_test_file(file_path)
            
            if test_file and os.path.exists(test_file):
                # 运行测试
                try:
                    result = subprocess.run(
                        ['python', '-m', 'pytest', test_file, '-v', '--tb=short'],
                        capture_output=True,
                        text=True,
                        timeout=60,
                        cwd=str(self.work_dir)
                    )
                    results.append({
                        "file": file_path,
                        "test_file": test_file,
                        "passed": result.returncode == 0,
                        "output": result.stdout[-500:] if len(result.stdout) > 500 else result.stdout
                    })
                except subprocess.TimeoutExpired:
                    results.append({
                        "file": file_path,
                        "test_file": test_file,
                        "passed": False,
                        "output": "测试超时"
                    })
            else:
                results.append({
                    "file": file_path,
                    "test_file": None,
                    "passed": None,
                    "output": "未找到测试文件"
                })
        
        passed = sum(1 for r in results if r.get("passed") is True)
        output = f"测试结果: {passed}/{len(results)} 通过"
        
        return ExecutionResult(
            success=passed == len(results),
            task_id=task.id,
            output=output,
            changes=[],
            issues_found=[],
            suggestions=[],
            execution_time=0
        )
    
    def _find_test_file(self, source_file: str) -> Optional[str]:
        """查找对应的测试文件"""
        base = Path(source_file).stem
        possible_names = [
            f"test_{base}.py",
            f"{base}_test.py",
            f"tests/test_{base}.py",
            f"tests/{base}_test.py"
        ]
        
        for name in possible_names:
            test_path = self.work_dir / name
            if test_path.exists():
                return str(test_path)
        
        return None
    
    def _record_experience(self, task: Task, result: ExecutionResult) -> str:
        """记录经验到知识图谱"""
        experience = Experience(
            id=self._generate_exp_id(task),
            task_type=task.task_type.value,
            context=json.dumps(task.context, ensure_ascii=False),
            problem=task.description,
            solution=result.output[:500] if result.output else "",
            outcome="success" if result.success else "failure",
            lessons=self._extract_lessons(result),
            code_snippets=[],
            tags=[task.task_type.value] + [Path(f).stem for f in task.target_files[:3]],
            created_at=datetime.now().isoformat(),
            success_rate=1.0 if result.success else 0.0,
            usage_count=1
        )
        
        return self.kg.add_experience(experience)
    
    def _generate_exp_id(self, task: Task) -> str:
        """生成经验ID"""
        import hashlib
        content = f"{task.task_type.value}{task.description}{datetime.now().isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    def _extract_lessons(self, result: ExecutionResult) -> str:
        """从执行结果提取经验教训"""
        lessons = []
        
        if result.success:
            lessons.append("成功完成")
            if result.suggestions:
                lessons.append(f"关键建议: {result.suggestions[0]}")
        else:
            lessons.append(f"失败原因: {result.output[:200] if result.output else '未知'}")
        
        return "; ".join(lessons)
    
    def get_status(self) -> Dict[str, Any]:
        """获取执行器状态"""
        with self._lock:
            return {
                "running": self._running.is_set(),
                "active_tasks": len(self.active_tasks),
                "queued_tasks": self.task_queue.qsize(),
                "completed_tasks": len(self.completed_tasks),
                "workers": len(self._workers)
            }
