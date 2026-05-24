"""
优化器模块 - 自我改进和学习
支持：方法优化、模式提取、策略调整、性能监控

R25修复:
- [R25-Bug#209] stop() 延长join超时并清理线程引用

R26修复:
- [R26-Bug#213] error_patterns添加大小限制和清理机制
"""
import json
import math
import os
import random
import re
import string
import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import logging

logger = logging.getLogger(__name__)

# 尝试在模块级别导入运行时依赖，避免在方法内部延迟导入
try:
    from config import OPTIMIZATION_INTERVAL, MIN_EXPERIENCE_FOR_OPTIMIZATION
except ImportError:
    # 模块加载时 config 可能尚不可用，提供默认值；运行时会再次尝试导入
    OPTIMIZATION_INTERVAL = 300
    MIN_EXPERIENCE_FOR_OPTIMIZATION = 10

try:
    from core.knowledge_graph import Pattern, KnowledgeGraph
except ImportError:
    Pattern = None  # 运行时会再次尝试导入
    KnowledgeGraph = None


@dataclass
class OptimizationResult:
    """优化结果"""
    optimization_type: str
    before_state: Dict[str, Any]
    after_state: Dict[str, Any]
    improvement: float
    timestamp: str
    details: str


@dataclass
class Strategy:
    """执行策略"""
    name: str
    task_type: str
    priority: int
    conditions: List[Dict[str, Any]]
    actions: List[Dict[str, Any]]
    success_rate: float
    usage_count: int
    last_used: Optional[str]


class Optimizer:
    """
    优化器
    持续分析执行结果，改进方法，提取模式
    """

    # 策略优先级的合法范围
    PRIORITY_MIN = 1
    PRIORITY_MAX = 10

    def __init__(self, knowledge_graph, executor):
        self.kg = knowledge_graph
        # executor 保留参数以维持公共API兼容性，当前版本未直接使用
        self._executor = executor

        self._lock = threading.RLock()
        self._running = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # 统计数据
        self.stats = {
            "total_optimizations": 0,
            "patterns_extracted": 0,
            "strategies_updated": 0,
            "last_optimization": None
        }

        # 策略库
        self.strategies: Dict[str, List[Strategy]] = defaultdict(list)

        # 性能指标
        # task_success_rate: 存储单次执行结果(0.0/1.0)，用于实时统计
        # task_success_trend: 存储聚合趋势(0.0~1.0)，用于趋势分析
        # execution_times: 存储每次执行耗时
        # error_patterns: 存储错误模式计数
        # 所有列表均使用 deque 限制最大长度，防止无限增长
        # [R26-Bug#213修复] error_patterns使用带大小限制的Counter替代
        self.metrics = {
            "task_success_rate": defaultdict(lambda: deque(maxlen=500)),
            "task_success_trend": defaultdict(lambda: deque(maxlen=500)),
            "execution_times": defaultdict(lambda: deque(maxlen=500)),
            "error_patterns": Counter(),
        }
        
        # [R26-Bug#213修复] error_patterns最大键数量
        self._max_error_patterns = 100

        # 初始化默认策略 - 在锁内完成，确保线程安全
        with self._lock:
            self._init_default_strategies()

        logger.debug(f"[TRACE] __init__ completed, default strategies count: {sum(len(v) for v in self.strategies.values())}")

    def _validate_priority(self, priority: int) -> int:
        """验证并修正策略优先级到合法范围"""
        logger.debug(f"[TRACE] _validate_priority input: priority={priority} (type={type(priority).__name__})")
        if not isinstance(priority, int):
            logger.warning(f"策略优先级应为整数，收到 {type(priority).__name__}，已修正为 {self.PRIORITY_MIN}")
            priority = self.PRIORITY_MIN
        validated = max(self.PRIORITY_MIN, min(self.PRIORITY_MAX, priority))
        logger.debug(f"[TRACE] _validate_priority output: {validated} (clamped from {priority})")
        return validated

    def _init_default_strategies(self):
        """初始化默认执行策略"""
        default_strategies = [
            Strategy(
                name="fast_analysis",
                task_type="analyze",
                priority=self._validate_priority(1),
                conditions=[{"file_count": {"<": 10}}],
                actions=[{"parallel": True, "depth": "quick"}],
                success_rate=0.9,
                usage_count=0,
                last_used=None
            ),
            Strategy(
                name="thorough_bug_fix",
                task_type="bug_fix",
                priority=self._validate_priority(2),
                conditions=[{"severity": "high"}],
                actions=[{"check_related": True, "run_tests": True}],
                success_rate=0.85,
                usage_count=0,
                last_used=None
            ),
            Strategy(
                name="safe_refactor",
                task_type="refactor",
                priority=self._validate_priority(1),
                conditions=[{"test_coverage": {">": 0.8}}],
                actions=[{"preserve_behavior": True, "incremental": True}],
                success_rate=0.95,
                usage_count=0,
                last_used=None
            )
        ]

        for strategy in default_strategies:
            logger.debug(f"[TRACE] _init_default_strategies: adding strategy '{strategy.name}' for task_type='{strategy.task_type}', priority={strategy.priority}")
            self.strategies[strategy.task_type].append(strategy)

        logger.debug(f"[TRACE] _init_default_strategies completed, total strategies: {sum(len(v) for v in self.strategies.values())}")

    def start(self):
        """[H-Bug修复] 启动优化器 - 防止重复调用导致双线程并发"""
        logger.debug("[TRACE][LOCK] acquire _lock in start")
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in start")
            if self._running.is_set() and self._thread and self._thread.is_alive():
                logger.debug("[TRACE] start: optimizer already running, skipping")
                logger.warning("优化器已在运行")
                return
            self._running.set()
            self._thread = threading.Thread(target=self._optimization_loop, name="Optimizer")
            self._thread.daemon = True
            self._thread.start()
            logger.debug(f"[TRACE] start: thread created and started, thread={self._thread.name}")
            logger.info("优化器已启动")
        logger.debug("[TRACE][LOCK] release _lock in start")

    def stop(self):
        """停止优化器
        
        R25-Bug#209修复: 延长join超时，确保优化循环有足够时间退出
        """
        logger.debug(f"[TRACE][LOCK] acquire _lock in stop")
        with self._lock:
            logger.debug(f"[TRACE][LOCK] acquired _lock in stop")
            logger.debug(f"[TRACE] stop: setting _running=False, current thread={self._thread.name if self._thread else None}")
            self._running.clear()
            thread = self._thread
        logger.debug(f"[TRACE][LOCK] release _lock in stop")
        if thread:
            # [R25-Bug#209修复] 延长超时到10秒，给优化循环更多时间
            join_timeout = 10
            logger.debug(f"[TRACE] stop: waiting for thread {thread.name} to join (timeout={join_timeout}s)")
            thread.join(timeout=join_timeout)
            is_alive = thread.is_alive()
            logger.debug(f"[TRACE] stop: thread join completed, is_alive={is_alive}")
            if is_alive:
                logger.warning(f"优化器线程在{join_timeout}秒内未退出，可能存在资源泄漏")
        
        # [R25-Bug#209修复] 清理线程引用
        with self._lock:
            self._thread = None
        
        logger.info("优化器已停止")

    def _cleanup_error_patterns(self):
        """[R26-Bug#213修复] 清理过多的error_patterns键，只保留高频模式"""
        if len(self.metrics["error_patterns"]) <= self._max_error_patterns:
            return
        
        # 只保留出现次数>=2的模式，最多保留_max_error_patterns个
        most_common = self.metrics["error_patterns"].most_common(self._max_error_patterns)
        # 过滤掉只出现1次的模式
        filtered = {k: v for k, v in most_common if v >= 2}
        
        old_count = len(self.metrics["error_patterns"])
        self.metrics["error_patterns"] = Counter(filtered)
        new_count = len(self.metrics["error_patterns"])
        
        logger.debug(f"[TRACE] _cleanup_error_patterns: {old_count} -> {new_count} patterns")

    def _optimization_loop(self):
        """优化主循环 - 添加指数退避机制处理连续异常"""
        # 运行时再次尝试导入，以获取实际配置值
        try:
            from config import OPTIMIZATION_INTERVAL as _interval, MIN_EXPERIENCE_FOR_OPTIMIZATION as _min_exp
            interval = _interval
            min_exp = _min_exp
        except ImportError:
            interval = OPTIMIZATION_INTERVAL
            min_exp = MIN_EXPERIENCE_FOR_OPTIMIZATION

        # 指数退避相关变量
        consecutive_failures = 0
        max_backoff_multiplier = 16  # 最大退避倍数
        base_interval = interval

        while self._running.is_set():
            try:
                logger.debug(f"[TRACE] _optimization_loop: sleeping for {interval}s")
                # [R19修复] 使用短间隔轮询替代Event.wait，因为Event.wait不响应clear()
                for _ in range(int(interval * 10)):
                    if not self._running.is_set():
                        break
                    time.sleep(0.1)

                logger.debug("[TRACE] _optimization_loop: woke up, checking experience count")
                # 检查是否有足够的新经验
                stats = self.kg.get_statistics()
                logger.debug(f"[TRACE] _optimization_loop: total_experiences={stats['total_experiences']}, min_exp={min_exp}")
                if stats["total_experiences"] >= min_exp:
                    logger.debug("[TRACE] _optimization_loop: calling _run_optimization_cycle")
                    self._run_optimization_cycle()
                else:
                    logger.debug(f"[TRACE] _optimization_loop: not enough experiences ({stats['total_experiences']} < {min_exp}), skipping")

                # 成功执行后重置失败计数和间隔
                if consecutive_failures > 0:
                    logger.debug(f"[TRACE] _optimization_loop: resetting consecutive_failures from {consecutive_failures} to 0")
                    consecutive_failures = 0
                    interval = base_interval

            except Exception as e:
                consecutive_failures += 1
                # 计算退避倍数，但不超过最大值
                backoff_multiplier = min(2 ** (consecutive_failures - 1), max_backoff_multiplier)
                interval = base_interval * backoff_multiplier
                logger.error(f"优化循环异常 (连续失败 {consecutive_failures} 次, 下次间隔 {interval}s): {e}")

    def _run_optimization_cycle(self):
        """执行一轮优化"""
        logger.info("开始优化周期...")
        logger.debug("[TRACE] _run_optimization_cycle: start")

        optimizations = []

        # 1. 分析成功率趋势
        logger.debug("[TRACE] _run_optimization_cycle: step 1 - _optimize_success_rates")
        success_opt = self._optimize_success_rates()
        logger.debug(f"[TRACE] _run_optimization_cycle: step 1 result: {success_opt is not None}")
        if success_opt:
            optimizations.append(success_opt)

        # 2. 提取新模式
        logger.debug("[TRACE] _run_optimization_cycle: step 2 - _extract_patterns")
        pattern_opt = self._extract_patterns()
        logger.debug(f"[TRACE] _run_optimization_cycle: step 2 result: {pattern_opt is not None}")
        if pattern_opt:
            optimizations.append(pattern_opt)

        # 3. 更新策略
        logger.debug("[TRACE] _run_optimization_cycle: step 3 - _update_strategies")
        strategy_opt = self._update_strategies()
        logger.debug(f"[TRACE] _run_optimization_cycle: step 3 result: {strategy_opt is not None}")
        if strategy_opt:
            optimizations.append(strategy_opt)

        # 4. 清理低效经验
        logger.debug("[TRACE] _run_optimization_cycle: step 4 - _cleanup_ineffective_experiences")
        cleanup_opt = self._cleanup_ineffective_experiences()
        logger.debug(f"[TRACE] _run_optimization_cycle: step 4 result: {cleanup_opt is not None}")
        if cleanup_opt:
            optimizations.append(cleanup_opt)

        logger.debug("[TRACE][LOCK] acquire _lock in _run_optimization_cycle")
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in _run_optimization_cycle")
            self.stats["total_optimizations"] += len(optimizations)
            self.stats["last_optimization"] = datetime.now().isoformat()
        logger.debug("[TRACE][LOCK] release _lock in _run_optimization_cycle")

        logger.info(f"优化周期完成, 执行了 {len(optimizations)} 项优化")
        logger.debug(f"[TRACE] _run_optimization_cycle: completed, total optimizations executed: {len(optimizations)}")

    def _optimize_success_rates(self) -> Optional[OptimizationResult]:
        """优化成功率分析"""
        logger.debug("[TRACE] _optimize_success_rates: start")
        # 分析各类型任务的成功率
        stats = self.kg.get_statistics()

        improvements = []
        for task_type, count in stats.get("by_task_type", {}).items():
            logger.debug(f"[TRACE] _optimize_success_rates: analyzing task_type='{task_type}', count={count}")
            # 获取该类型的经验
            experiences = self.kg.search_experiences("", task_type=task_type, limit=100)

            if len(experiences) >= 5:
                success_count = sum(1 for e in experiences if e.outcome == "success")
                rate = success_count / len(experiences)
                logger.debug(f"[TRACE] _optimize_success_rates: task_type='{task_type}', experiences={len(experiences)}, success_count={success_count}, rate={rate:.4f}")

                # [H-逻辑错误修复] 将聚合趋势写入 task_success_trend，而非 task_success_rate
                # task_success_rate 仅存储单次执行结果(0/1)，由 record_execution_result 写入
                logger.debug("[TRACE][LOCK] acquire _lock in _optimize_success_rates")
                with self._lock:
                    logger.debug("[TRACE][LOCK] acquired _lock in _optimize_success_rates")
                    self.metrics["task_success_trend"][task_type].append(rate)

                    # 如果成功率下降,生成改进建议
                    trend_data = self.metrics["task_success_trend"][task_type]
                    if len(trend_data) >= 3:
                        recent = list(trend_data)[-3:]
                        logger.debug(f"[TRACE] _optimize_success_rates: task_type='{task_type}', trend_data_len={len(trend_data)}, recent_3={recent}")
                        if recent[-1] < recent[0]:
                            logger.debug(f"[TRACE] _optimize_success_rates: task_type='{task_type}' trend declining ({recent[0]:.4f} -> {recent[-1]:.4f})")
                            improvements.append({
                                "task_type": task_type,
                                "trend": "declining",
                                "current_rate": rate,
                                "suggestion": f"任务类型 {task_type} 成功率下降, 需要检查方法"
                            })
                logger.debug("[TRACE][LOCK] release _lock in _optimize_success_rates")

        logger.debug(f"[TRACE] _optimize_success_rates: completed, improvements found: {len(improvements)}")

        if improvements:
            return OptimizationResult(
                optimization_type="success_rate_analysis",
                before_state={"analyzed_types": list(stats.get("by_task_type", {}).keys())},
                after_state={"improvements": improvements},
                improvement=len(improvements),
                timestamp=datetime.now().isoformat(),
                details=f"发现 {len(improvements)} 个需要关注的任务类型"
            )

        return None

    def _extract_patterns(self) -> Optional[OptimizationResult]:
        """从经验中提取模式"""
        logger.debug("[TRACE] _extract_patterns: start")
        # [H-Bug#78修复] 使用局部变量而非修改模块级Pattern，避免Python作用域问题
        # 模块级 Pattern = None，方法内 Pattern = _Pattern 会让Python将整个方法的Pattern视为局部变量
        # 导致 "local variable 'Pattern' referenced before assignment" 错误
        _PatternClass = Pattern
        if _PatternClass is None:
            try:
                from core.knowledge_graph import Pattern as _PatternClass
            except ImportError:
                logger.warning("无法导入 Pattern 类，跳过模式提取")
                return None

        # 获取成功经验
        successful = self.kg.search_experiences("", limit=50)
        successful = [e for e in successful if e.outcome == "success"]
        logger.debug(f"[TRACE] _extract_patterns: successful experiences count={len(successful)}")

        if len(successful) < 5:
            logger.debug(f"[TRACE] _extract_patterns: not enough successful experiences ({len(successful)} < 5), returning None")
            return None

        # 分析共同特征
        tag_frequency = Counter()
        solution_keywords = Counter()

        # [OPT-020修复] 使用与knowledge_graph相同的分词方法进行中文分词
        def _tokenize_for_optimizer(text: str) -> set:
            """分词：支持中英文混合文本"""
            if text is None:
                return set()
            tokens = set()
            # 英文分词：按空格和标点分割
            words = re.findall(r'[a-zA-Z0-9]+', text.lower())
            tokens.update(w for w in words if len(w) > 1)

            # 提取中文字符，生成字符级bigram和unigram
            chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
            if chinese_chars:
                # unigram
                tokens.update(chinese_chars)
                # bigram
                for i in range(len(chinese_chars) - 1):
                    tokens.add(chinese_chars[i] + chinese_chars[i + 1])

            return tokens

        for exp in successful:
            tag_frequency.update(exp.tags)
            # [OPT-020修复] 使用改进的分词方法替代简单的空格分割
            tokens = _tokenize_for_optimizer(exp.solution)
            solution_keywords.update(w for w in tokens if len(w) > 2)

        # 提取高频模式
        top_tags = tag_frequency.most_common(5)
        top_keywords = solution_keywords.most_common(10)
        logger.debug(f"[TRACE] _extract_patterns: top_tags={top_tags}, top_keywords_count={len(top_keywords)}")

        # 创建新模式
        new_patterns = []
        for tag, count in top_tags:
            if count >= 3:
                # [M-模式ID修复] 加入随机后缀，防止同一秒内创建多个模式时ID重复
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                pattern = _PatternClass(
                    id=f"pattern_{tag}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random_suffix}",
                    pattern_type="best_practice",
                    name=f"高频标签模式: {tag}",
                    description=f"在成功经验中出现 {count} 次",
                    code_example="",
                    when_to_use=f"当任务涉及 {tag} 时优先考虑",
                    effectiveness=min(count / len(successful), 1.0),
                    created_at=datetime.now().isoformat()
                )
                self.kg.add_pattern(pattern)
                new_patterns.append(pattern.id)
                logger.debug(f"[TRACE] _extract_patterns: created pattern id='{pattern.id}', tag='{tag}', count={count}, effectiveness={pattern.effectiveness:.4f}")

        if new_patterns:
            logger.debug("[TRACE][LOCK] acquire _lock in _extract_patterns")
            with self._lock:
                logger.debug("[TRACE][LOCK] acquired _lock in _extract_patterns")
                self.stats["patterns_extracted"] += len(new_patterns)
            logger.debug("[TRACE][LOCK] release _lock in _extract_patterns")

            logger.debug(f"[TRACE] _extract_patterns: completed, new_patterns={new_patterns}")
            return OptimizationResult(
                optimization_type="pattern_extraction",
                before_state={"experience_count": len(successful)},
                after_state={"new_patterns": new_patterns},
                improvement=len(new_patterns),
                timestamp=datetime.now().isoformat(),
                details=f"从 {len(successful)} 条成功经验中提取了 {len(new_patterns)} 个模式"
            )

        return None

    def _update_strategies(self) -> Optional[OptimizationResult]:
        """更新执行策略"""
        logger.debug("[TRACE] _update_strategies: start")
        updates = []

        # 先在锁外查询数据
        all_experiences = {}
        for task_type in list(self.strategies.keys()):
            all_experiences[task_type] = self.kg.search_experiences("", task_type=task_type, limit=20)

        # 在锁内更新策略
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in _update_strategies")
            for task_type, strategies in self.strategies.items():
                logger.debug(f"[TRACE] _update_strategies: processing task_type='{task_type}', strategy_count={len(strategies)}")
                # 获取该类型的历史经验
                experiences = all_experiences.get(task_type, [])

                if len(experiences) >= 5:
                    # [M-权重修复] 考虑 usage_count 作为样本权重计算加权平均成功率
                    total_weight = 0
                    weighted_success = 0.0
                    for e in experiences:
                        weight = max(e.usage_count, 1)  # 至少权重为1，避免零权重
                        weighted_success += e.success_rate * weight
                        total_weight += weight
                    avg_success = weighted_success / total_weight if total_weight > 0 else 0
                    logger.debug(f"[TRACE] _update_strategies: task_type='{task_type}', avg_success={avg_success:.4f}, experiences={len(experiences)}")

                    # 更新策略成功率
                    for strategy in strategies:
                        old_rate = strategy.success_rate
                        # 滑动平均更新
                        strategy.success_rate = old_rate * 0.7 + avg_success * 0.3
                        logger.debug(f"[TRACE] _update_strategies: strategy '{strategy.name}' (task_type='{task_type}'): {old_rate:.4f} -> {strategy.success_rate:.4f}")

                        if abs(strategy.success_rate - old_rate) > 0.05:
                            updates.append({
                                "strategy": strategy.name,
                                "old_rate": old_rate,
                                "new_rate": strategy.success_rate
                            })
        logger.debug("[TRACE][LOCK] release _lock in _update_strategies")

        if updates:
            with self._lock:
                self.stats["strategies_updated"] += len(updates)

            return OptimizationResult(
                optimization_type="strategy_update",
                before_state={},
                after_state={"updates": updates},
                improvement=len(updates),
                timestamp=datetime.now().isoformat(),
                details=f"更新了 {len(updates)} 个策略的成功率"
            )

        return None

    def _cleanup_ineffective_experiences(self) -> Optional[OptimizationResult]:
        """清理低效经验"""
        logger.debug("[TRACE] _cleanup_ineffective_experiences: start")
        # 找出成功率极低且使用次数少的经验
        experiences = self.kg.search_experiences("", limit=100)
        logger.debug(f"[TRACE] _cleanup_ineffective_experiences: scanned experiences count={len(experiences)}")

        to_deprecate = []
        for exp in experiences:
            if exp.success_rate < 0.3 and exp.usage_count < 2:
                to_deprecate.append(exp.id)
        logger.debug(f"[TRACE] _cleanup_ineffective_experiences: ineffective experiences count={len(to_deprecate)}")

        # 这里只是标记,实际删除需要谨慎
        if to_deprecate:
            logger.info(f"发现 {len(to_deprecate)} 条低效经验, 建议人工审核")

            return OptimizationResult(
                optimization_type="experience_cleanup",
                before_state={"total": len(experiences)},
                after_state={"to_review": to_deprecate},
                improvement=len(to_deprecate),
                timestamp=datetime.now().isoformat(),
                details=f"标记了 {len(to_deprecate)} 条低效经验待审核"
            )

        return None

    def get_best_strategy(self, task_type: str, context: Dict[str, Any] = None) -> Optional[Strategy]:
        """获取最佳策略"""
        logger.debug(f"[TRACE] get_best_strategy: task_type='{task_type}', context={context}")
        logger.debug("[TRACE][LOCK] acquire _lock in get_best_strategy")
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in get_best_strategy")
            strategies = list(self.strategies.get(task_type, []))
        logger.debug("[TRACE][LOCK] release _lock in get_best_strategy")

        if not strategies:
            logger.debug(f"[TRACE] get_best_strategy: no strategies found for task_type='{task_type}', returning None")
            return None

        logger.debug(f"[TRACE] get_best_strategy: found {len(strategies)} strategies for task_type='{task_type}'")
        # 按成功率和优先级排序
        valid_strategies = []
        for s in strategies:
            # 检查条件是否满足
            conditions_met = True
            for cond in s.conditions:
                for key, constraint in cond.items():
                    if context and key in context:
                        value = context[key]
                        for op, threshold in constraint.items():
                            if op == "<" and not (value < threshold):
                                conditions_met = False
                            elif op == ">" and not (value > threshold):
                                conditions_met = False
                            elif op == "==" and not (value == threshold):
                                conditions_met = False
            score = s.success_rate * 0.7 + (s.priority / 10) * 0.3
            logger.debug(f"[TRACE] get_best_strategy: strategy '{s.name}' conditions_met={conditions_met}, score={score:.4f} (rate={s.success_rate:.4f}, priority={s.priority})")

            if conditions_met:
                valid_strategies.append(s)

        if valid_strategies:
            # [M-Bug修复] 使用加权评分而非简单乘法，避免高优先级低成功率策略胜出
            # score = success_rate * 0.7 + (priority / 10) * 0.3
            best = max(valid_strategies, key=lambda s: s.success_rate * 0.7 + (s.priority / 10) * 0.3)
            logger.debug(f"[TRACE] get_best_strategy: returning valid strategy '{best.name}' (score={best.success_rate * 0.7 + (best.priority / 10) * 0.3:.4f})")
            return best

        # 返回默认策略：优先考虑成功率，其次考虑优先级
        best = max(strategies, key=lambda s: s.success_rate * 0.7 + (s.priority / 10) * 0.3)
        logger.debug(f"[TRACE] get_best_strategy: no valid strategies, returning fallback '{best.name}' (score={best.success_rate * 0.7 + (best.priority / 10) * 0.3:.4f})")
        return best

    def _quick_update_strategy(self, task_type: str):
        """
        [H-Bug修复] 轻量级策略更新 - 加锁保护，防止与优化循环线程竞态
        """
        logger.debug(f"[TRACE] _quick_update_strategy: task_type='{task_type}'")
        logger.debug("[TRACE][LOCK] acquire _lock in _quick_update_strategy")
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in _quick_update_strategy")
            # 获取该 task_type 的最近执行结果
            rate_deque = self.metrics["task_success_rate"].get(task_type)
            if not rate_deque:
                logger.debug(f"[TRACE] _quick_update_strategy: no rate_deque for task_type='{task_type}', returning")
                return

            # 取最近 N 次结果计算平均成功率
            recent_results = list(rate_deque)[-20:]
            logger.debug(f"[TRACE] _quick_update_strategy: task_type='{task_type}', rate_deque_len={len(rate_deque)}, recent_results_len={len(recent_results)}")
            if not recent_results:
                return

            new_avg = sum(recent_results) / len(recent_results)
            logger.debug(f"[TRACE] _quick_update_strategy: task_type='{task_type}', new_avg={new_avg:.4f}")

            # 用指数移动平均更新该 task_type 下所有策略的 success_rate
            alpha = 0.3
            for strategy in self.strategies.get(task_type, []):
                old_rate = strategy.success_rate
                strategy.success_rate = old_rate * (1 - alpha) + new_avg * alpha
                logger.debug(f"[TRACE] _quick_update_strategy: strategy '{strategy.name}' (task_type='{task_type}'): {old_rate:.4f} -> {strategy.success_rate:.4f}")
                logger.debug(f"快速更新策略 {strategy.name} 成功率: {old_rate:.4f} -> {strategy.success_rate:.4f}")
        logger.debug("[TRACE][LOCK] release _lock in _quick_update_strategy")

    def record_execution_result(self, task_type: str, success: bool, execution_time: float):
        """记录执行结果用于优化分析"""
        # [H-Bug#123修复] 校验 execution_time，防止非法值污染统计数据
        if execution_time is None or execution_time < 0 or math.isnan(execution_time) or math.isinf(execution_time):
            execution_time = 0.0

        # [OPT-006修复] 验证 task_type 不为 None 且为字符串
        if task_type is None:
            logger.warning("[TRACE] record_execution_result: task_type is None, skipping record")
            return
        if not isinstance(task_type, str):
            logger.warning(f"[TRACE] record_execution_result: task_type must be a string, got {type(task_type).__name__}, skipping record")
            return
        if not task_type.strip():
            logger.warning("[TRACE] record_execution_result: task_type is empty string, skipping record")
            return

        logger.debug(f"[TRACE] record_execution_result: task_type='{task_type}', success={success}, execution_time={execution_time:.4f}")
        logger.debug("[TRACE][LOCK] acquire _lock in record_execution_result")
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in record_execution_result")
            # [H-逻辑错误修复] task_success_rate 仅存储单次执行结果(0.0/1.0)
            self.metrics["task_success_rate"][task_type].append(1.0 if success else 0.0)
            self.metrics["execution_times"][task_type].append(execution_time)

            if not success:
                self.metrics["error_patterns"][task_type] += 1
                logger.debug(f"[TRACE] record_execution_result: error_pattern count for '{task_type}' incremented to {self.metrics['error_patterns'][task_type]}")
                
                # [R26-Bug#213修复] 检查并清理过多的error_patterns键
                if len(self.metrics["error_patterns"]) > self._max_error_patterns:
                    # 只保留出现次数>=2的模式，最多保留_max_error_patterns个
                    self._cleanup_error_patterns()
        logger.debug("[TRACE][LOCK] release _lock in record_execution_result")

        # 记录结果后立即触发轻量级策略更新，无需等待完整优化周期
        logger.debug(f"[TRACE] record_execution_result: calling _quick_update_strategy for task_type='{task_type}'")
        self._quick_update_strategy(task_type)

    def get_optimization_report(self) -> Dict[str, Any]:
        """获取优化报告"""
        logger.debug("[TRACE] get_optimization_report: start")
        # [M-Bug#125修复] 先在锁外获取 kg 统计数据，避免在锁内调用 kg.get_statistics() 导致潜在死锁
        knowledge_stats = self.kg.get_statistics()
        logger.debug("[TRACE][LOCK] acquire _lock in get_optimization_report")
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in get_optimization_report")
            # 计算平均成功率（基于单次执行结果）
            avg_rates = {}
            for task_type, rates in self.metrics["task_success_rate"].items():
                rate_list = list(rates)
                if rate_list:
                    avg_rates[task_type] = sum(rate_list[-10:]) / len(rate_list[-10:])

            # 计算平均执行时间
            avg_times = {}
            for task_type, times in self.metrics["execution_times"].items():
                time_list = list(times)
                if time_list:
                    avg_times[task_type] = sum(time_list[-10:]) / len(time_list[-10:])

            report = {
                "statistics": self.stats.copy(),
                "average_success_rates": avg_rates,
                "average_execution_times": avg_times,
                "top_error_patterns": dict(self.metrics["error_patterns"].most_common(5)),
                "strategy_count": sum(len(s) for s in self.strategies.values()),
                "knowledge_stats": knowledge_stats
            }
        logger.debug("[TRACE][LOCK] release _lock in get_optimization_report")
        logger.debug(f"[TRACE] get_optimization_report: completed, strategy_count={report['strategy_count']}, task_types_in_rates={list(avg_rates.keys())}")
        return report

    def suggest_improvements(self) -> List[Dict[str, Any]]:
        """生成改进建议"""
        logger.debug("[TRACE] suggest_improvements: start")
        suggestions = []

        # [M-线程安全修复] 读取共享状态时加锁
        logger.debug("[TRACE][LOCK] acquire _lock in suggest_improvements")
        with self._lock:
            logger.debug("[TRACE][LOCK] acquired _lock in suggest_improvements")
            # 分析成功率低的任务类型（基于单次执行结果）
            for task_type, rates in self.metrics["task_success_rate"].items():
                rate_list = list(rates)
                if rate_list and len(rate_list) >= 5:
                    recent_rate = sum(rate_list[-5:]) / 5
                    if recent_rate < 0.7:
                        suggestion = {
                            "type": "low_success_rate",
                            "task_type": task_type,
                            "rate": recent_rate,
                            "suggestion": f"任务类型 '{task_type}' 成功率较低({recent_rate:.1%}), 建议检查执行方法或增加相关训练"
                        }
                        suggestions.append(suggestion)
                        logger.debug(f"[TRACE] suggest_improvements: generated suggestion type='low_success_rate', task_type='{task_type}', rate={recent_rate:.4f}")

            # 分析执行时间过长的任务
            for task_type, times in self.metrics["execution_times"].items():
                time_list = list(times)
                if time_list and len(time_list) >= 5:
                    avg_time = sum(time_list[-5:]) / 5
                    if avg_time > 60:  # 超过60秒
                        suggestion = {
                            "type": "slow_execution",
                            "task_type": task_type,
                            "avg_time": avg_time,
                            "suggestion": f"任务类型 '{task_type}' 执行时间过长({avg_time:.1f}s), 建议优化执行流程"
                        }
                        suggestions.append(suggestion)
                        logger.debug(f"[TRACE] suggest_improvements: generated suggestion type='slow_execution', task_type='{task_type}', avg_time={avg_time:.4f}")

            # 分析错误模式
            for task_type, count in self.metrics["error_patterns"].most_common(3):
                if count >= 3:
                    suggestion = {
                        "type": "frequent_errors",
                        "task_type": task_type,
                        "error_count": count,
                        "suggestion": f"任务类型 '{task_type}' 频繁出错({count}次), 建议检查常见错误模式并添加预防措施"
                    }
                    suggestions.append(suggestion)
                    logger.debug(f"[TRACE] suggest_improvements: generated suggestion type='frequent_errors', task_type='{task_type}', error_count={count}")
        logger.debug("[TRACE][LOCK] release _lock in suggest_improvements")

        logger.debug(f"[TRACE] suggest_improvements: completed, total suggestions={len(suggestions)}")
        return suggestions
