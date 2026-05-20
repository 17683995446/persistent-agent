"""
优化器模块 - 自我改进和学习
支持：方法优化、模式提取、策略调整、性能监控
"""
import json
import os
import random
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
    from core.knowledge_graph import Pattern
except ImportError:
    Pattern = None  # 运行时会再次尝试导入


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
        self._running = False
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
        # 所有列表均使用 deque 限制最大长度，防止无限增长
        self.metrics = {
            "task_success_rate": defaultdict(lambda: deque(maxlen=500)),
            "task_success_trend": defaultdict(lambda: deque(maxlen=500)),
            "execution_times": defaultdict(lambda: deque(maxlen=500)),
            "error_patterns": Counter(),
        }

        # 初始化默认策略
        self._init_default_strategies()

    def _validate_priority(self, priority: int) -> int:
        """验证并修正策略优先级到合法范围"""
        if not isinstance(priority, int):
            logger.warning(f"策略优先级应为整数，收到 {type(priority).__name__}，已修正为 {self.PRIORITY_MIN}")
            priority = self.PRIORITY_MIN
        return max(self.PRIORITY_MIN, min(self.PRIORITY_MAX, priority))

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
            self.strategies[strategy.task_type].append(strategy)

    def start(self):
        """[H-Bug修复] 启动优化器 - 防止重复调用导致双线程并发"""
        with self._lock:
            if self._running and self._thread and self._thread.is_alive():
                logger.warning("优化器已在运行")
                return
            self._running = True
            self._thread = threading.Thread(target=self._optimization_loop, name="Optimizer")
            self._thread.daemon = True
            self._thread.start()
            logger.info("优化器已启动")

    def stop(self):
        """停止优化器"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("优化器已停止")

    def _optimization_loop(self):
        """优化主循环"""
        # 运行时再次尝试导入，以获取实际配置值
        try:
            from config import OPTIMIZATION_INTERVAL as _interval, MIN_EXPERIENCE_FOR_OPTIMIZATION as _min_exp
            interval = _interval
            min_exp = _min_exp
        except ImportError:
            interval = OPTIMIZATION_INTERVAL
            min_exp = MIN_EXPERIENCE_FOR_OPTIMIZATION

        while self._running:
            try:
                time.sleep(interval)

                # 检查是否有足够的新经验
                stats = self.kg.get_statistics()
                if stats["total_experiences"] >= min_exp:
                    self._run_optimization_cycle()

            except Exception as e:
                logger.error(f"优化循环异常: {e}")

    def _run_optimization_cycle(self):
        """执行一轮优化"""
        logger.info("开始优化周期...")

        optimizations = []

        # 1. 分析成功率趋势
        success_opt = self._optimize_success_rates()
        if success_opt:
            optimizations.append(success_opt)

        # 2. 提取新模式
        pattern_opt = self._extract_patterns()
        if pattern_opt:
            optimizations.append(pattern_opt)

        # 3. 更新策略
        strategy_opt = self._update_strategies()
        if strategy_opt:
            optimizations.append(strategy_opt)

        # 4. 清理低效经验
        cleanup_opt = self._cleanup_ineffective_experiences()
        if cleanup_opt:
            optimizations.append(cleanup_opt)

        with self._lock:
            self.stats["total_optimizations"] += len(optimizations)
            self.stats["last_optimization"] = datetime.now().isoformat()

        logger.info(f"优化周期完成, 执行了 {len(optimizations)} 项优化")

    def _optimize_success_rates(self) -> Optional[OptimizationResult]:
        """优化成功率分析"""
        # 分析各类型任务的成功率
        stats = self.kg.get_statistics()

        improvements = []
        for task_type, count in stats.get("by_task_type", {}).items():
            # 获取该类型的经验
            experiences = self.kg.search_experiences("", task_type=task_type, limit=100)

            if len(experiences) >= 5:
                success_count = sum(1 for e in experiences if e.outcome == "success")
                rate = success_count / len(experiences)

                # [H-逻辑错误修复] 将聚合趋势写入 task_success_trend，而非 task_success_rate
                # task_success_rate 仅存储单次执行结果(0/1)，由 record_execution_result 写入
                with self._lock:
                    self.metrics["task_success_trend"][task_type].append(rate)

                    # 如果成功率下降,生成改进建议
                    trend_data = self.metrics["task_success_trend"][task_type]
                    if len(trend_data) >= 3:
                        recent = list(trend_data)[-3:]
                        if recent[-1] < recent[0]:
                            improvements.append({
                                "task_type": task_type,
                                "trend": "declining",
                                "current_rate": rate,
                                "suggestion": f"任务类型 {task_type} 成功率下降, 需要检查方法"
                            })

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
        # 运行时再次尝试导入 Pattern，确保可用
        if Pattern is None:
            try:
                from core.knowledge_graph import Pattern as _Pattern
                Pattern = _Pattern
            except ImportError:
                logger.warning("无法导入 Pattern 类，跳过模式提取")
                return None

        # 获取成功经验
        successful = self.kg.search_experiences("", limit=50)
        successful = [e for e in successful if e.outcome == "success"]

        if len(successful) < 5:
            return None

        # 分析共同特征
        tag_frequency = Counter()
        solution_keywords = Counter()

        for exp in successful:
            tag_frequency.update(exp.tags)
            # 简单的关键词提取
            words = exp.solution.lower().split()
            solution_keywords.update(w for w in words if len(w) > 4)

        # 提取高频模式
        top_tags = tag_frequency.most_common(5)
        top_keywords = solution_keywords.most_common(10)

        # 创建新模式
        new_patterns = []
        for tag, count in top_tags:
            if count >= 3:
                # [M-模式ID修复] 加入随机后缀，防止同一秒内创建多个模式时ID重复
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
                pattern = Pattern(
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

        if new_patterns:
            with self._lock:
                self.stats["patterns_extracted"] += len(new_patterns)

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
        updates = []

        # [H-线程安全修复] 修改 strategies 共享状态时加锁
        with self._lock:
            for task_type, strategies in self.strategies.items():
                # 获取该类型的历史经验
                experiences = self.kg.search_experiences("", task_type=task_type, limit=20)

                if len(experiences) >= 5:
                    # [M-权重修复] 考虑 usage_count 作为样本权重计算加权平均成功率
                    total_weight = 0
                    weighted_success = 0.0
                    for e in experiences:
                        weight = max(e.usage_count, 1)  # 至少权重为1，避免零权重
                        weighted_success += e.success_rate * weight
                        total_weight += weight
                    avg_success = weighted_success / total_weight if total_weight > 0 else 0

                    # 更新策略成功率
                    for strategy in strategies:
                        old_rate = strategy.success_rate
                        # 滑动平均更新
                        strategy.success_rate = old_rate * 0.7 + avg_success * 0.3

                        if abs(strategy.success_rate - old_rate) > 0.05:
                            updates.append({
                                "strategy": strategy.name,
                                "old_rate": old_rate,
                                "new_rate": strategy.success_rate
                            })

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
        # 找出成功率极低且使用次数少的经验
        experiences = self.kg.search_experiences("", limit=100)

        to_deprecate = []
        for exp in experiences:
            if exp.success_rate < 0.3 and exp.usage_count < 2:
                to_deprecate.append(exp.id)

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
        with self._lock:
            strategies = list(self.strategies.get(task_type, []))

        if not strategies:
            return None

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

            if conditions_met:
                valid_strategies.append(s)

        if valid_strategies:
            # [M-Bug修复] 使用加权评分而非简单乘法，避免高优先级低成功率策略胜出
            # score = success_rate * 0.7 + (priority / 10) * 0.3
            return max(valid_strategies, key=lambda s: s.success_rate * 0.7 + (s.priority / 10) * 0.3)

        # 返回默认策略：优先考虑成功率，其次考虑优先级
        return max(strategies, key=lambda s: s.success_rate * 0.7 + (s.priority / 10) * 0.3)

    def _quick_update_strategy(self, task_type: str):
        """
        [H-Bug修复] 轻量级策略更新 - 加锁保护，防止与优化循环线程竞态
        """
        with self._lock:
            # 获取该 task_type 的最近执行结果
            rate_deque = self.metrics["task_success_rate"].get(task_type)
            if not rate_deque:
                return

            # 取最近 N 次结果计算平均成功率
            recent_results = list(rate_deque)[-20:]
            if not recent_results:
                return

            new_avg = sum(recent_results) / len(recent_results)

            # 用指数移动平均更新该 task_type 下所有策略的 success_rate
            alpha = 0.3
            for strategy in self.strategies.get(task_type, []):
                old_rate = strategy.success_rate
                strategy.success_rate = old_rate * (1 - alpha) + new_avg * alpha
                logger.debug(f"快速更新策略 {strategy.name} 成功率: {old_rate:.4f} -> {strategy.success_rate:.4f}")

    def record_execution_result(self, task_type: str, success: bool, execution_time: float):
        """记录执行结果用于优化分析"""
        with self._lock:
            # [H-逻辑错误修复] task_success_rate 仅存储单次执行结果(0.0/1.0)
            self.metrics["task_success_rate"][task_type].append(1.0 if success else 0.0)
            self.metrics["execution_times"][task_type].append(execution_time)

            if not success:
                self.metrics["error_patterns"][task_type] += 1

        # 记录结果后立即触发轻量级策略更新，无需等待完整优化周期
        self._quick_update_strategy(task_type)

    def get_optimization_report(self) -> Dict[str, Any]:
        """获取优化报告"""
        with self._lock:
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

            return {
                "statistics": self.stats.copy(),
                "average_success_rates": avg_rates,
                "average_execution_times": avg_times,
                "top_error_patterns": dict(self.metrics["error_patterns"].most_common(5)),
                "strategy_count": sum(len(s) for s in self.strategies.values()),
                "knowledge_stats": self.kg.get_statistics()
            }

    def suggest_improvements(self) -> List[Dict[str, Any]]:
        """生成改进建议"""
        suggestions = []

        # [M-线程安全修复] 读取共享状态时加锁
        with self._lock:
            # 分析成功率低的任务类型（基于单次执行结果）
            for task_type, rates in self.metrics["task_success_rate"].items():
                rate_list = list(rates)
                if rate_list and len(rate_list) >= 5:
                    recent_rate = sum(rate_list[-5:]) / 5
                    if recent_rate < 0.7:
                        suggestions.append({
                            "type": "low_success_rate",
                            "task_type": task_type,
                            "rate": recent_rate,
                            "suggestion": f"任务类型 '{task_type}' 成功率较低({recent_rate:.1%}), 建议检查执行方法或增加相关训练"
                        })

            # 分析执行时间过长的任务
            for task_type, times in self.metrics["execution_times"].items():
                time_list = list(times)
                if time_list and len(time_list) >= 5:
                    avg_time = sum(time_list[-5:]) / 5
                    if avg_time > 60:  # 超过60秒
                        suggestions.append({
                            "type": "slow_execution",
                            "task_type": task_type,
                            "avg_time": avg_time,
                            "suggestion": f"任务类型 '{task_type}' 执行时间过长({avg_time:.1f}s), 建议优化执行流程"
                        })

            # 分析错误模式
            for task_type, count in self.metrics["error_patterns"].most_common(3):
                if count >= 3:
                    suggestions.append({
                        "type": "frequent_errors",
                        "task_type": task_type,
                        "error_count": count,
                        "suggestion": f"任务类型 '{task_type}' 频繁出错({count}次), 建议检查常见错误模式并添加预防措施"
                    })

        return suggestions
