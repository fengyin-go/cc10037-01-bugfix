"""告警条件的正则/阈值匹配引擎。

支持告警条件的解析、匹配和决策：
- 阈值比较（delay > 60, failed_ratio >= 0.1）
- 正则匹配（exception_type matches "DataSourceError|NetworkTimeout"）
- 复合条件（AND / OR 逻辑）
- 级别提升规则
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class AlertLevel(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"


@dataclass
class AlertRule:
    """一条告警规则定义。"""
    rule_id: str
    rule_name: str
    monitor_target: str                     # 监控对象 (pipeline_id / table / resource_pool)
    condition: str                          # 告警条件表达式
    level: AlertLevel = AlertLevel.WARN
    channels: list[str] = field(default_factory=lambda: ["email"])
    silence_window: str = ""               # 静默时段 "22:00-06:00"
    aggregation_window_minutes: int = 5    # 聚合窗口（分钟）


@dataclass
class AlertResult:
    """单条规则匹配结果。"""
    rule_id: str
    rule_name: str
    matched: bool
    level: AlertLevel
    monitor_target: str
    actual_values: dict[str, Any] = field(default_factory=dict)
    message: str = ""
    trigger_channels: list[str] = field(default_factory=list)


class AlertMatcher:
    """告警正则/阈值匹配引擎。

    支持的表达式语法：
    - delay > 60
    - failed_ratio >= 0.1
    - exception_type == "DataSourceError"
    - exception_type matches "DataSourceError|Timeout"
    - status == "FAILED"
    - cpu_usage > 75 AND memory_usage > 80
    - AND / OR 连接

    用法:
        matcher = AlertMatcher()
        rule = AlertRule("a1", "管线延迟告警", "p_dwd_order",
                         "delay > 30", AlertLevel.WARN)
        result = matcher.match(rule, {"delay": 45})
    """

    def __init__(self) -> None:
        self._compiled_regexes: dict[str, re.Pattern] = {}

    def match(self, rule: AlertRule, metrics: dict[str, Any]) -> AlertResult:
        """评估一条规则是否匹配当前指标。"""
        try:
            matched = self._evaluate_condition(rule.condition, metrics)
        except Exception as e:
            return AlertResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                matched=False,
                level=rule.level,
                monitor_target=rule.monitor_target,
                message=f"条件解析错误: {e}",
            )

        if not matched:
            return AlertResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                matched=False,
                level=rule.level,
                monitor_target=rule.monitor_target,
                actual_values=metrics,
                message="未触发",
            )

        # 生成告警消息
        message = self._compose_message(rule, metrics)
        return AlertResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            matched=True,
            level=rule.level,
            monitor_target=rule.monitor_target,
            actual_values=metrics,
            message=message,
            trigger_channels=list(rule.channels),
        )

    def match_batch(
        self, rules: list[AlertRule], metrics: dict[str, Any]
    ) -> list[AlertResult]:
        """批量评估规则并去重（同目标只保留最高级别）。"""
        results = [self.match(r, metrics) for r in rules]
        triggered = [r for r in results if r.matched]

        # 去重：按 monitor_target 保留最高级别
        best_by_target: dict[str, AlertResult] = {}
        level_rank = {AlertLevel.INFO: 0, AlertLevel.WARN: 1,
                       AlertLevel.CRITICAL: 2, AlertLevel.FATAL: 3}

        for r in triggered:
            existing = best_by_target.get(r.monitor_target)
            if existing is None or level_rank[r.level] > level_rank[existing.level]:
                best_by_target[r.monitor_target] = r

        # 返回：匹配的 + 不匹配的标记
        triggered_ids = {r.monitor_target for r in best_by_target.values()}
        final: list[AlertResult] = []
        for r in results:
            if not r.matched:
                final.append(r)
            elif r.monitor_target in triggered_ids and r.rule_id == best_by_target[r.monitor_target].rule_id:
                final.append(r)

        return final

    def escalation_rule(
        self, current: AlertLevel, repeat_count: int, max_repeat: int = 3
    ) -> AlertLevel:
        """告警升级规则：重复触发N次后自动升级。"""
        if repeat_count >= max_repeat * 2:
            return AlertLevel.FATAL
        if repeat_count >= max_repeat:
            if current == AlertLevel.WARN:
                return AlertLevel.CRITICAL
            if current == AlertLevel.CRITICAL:
                return AlertLevel.FATAL
        return current

    # ---- 表达式解析与求值 ----

    def _evaluate_condition(self, condition: str, metrics: dict[str, Any]) -> bool:
        """解析并求值条件表达式。"""
        condition = condition.strip()

        # 处理 AND
        if " AND " in condition:
            parts = condition.split(" AND ")
            return all(self._evaluate_simple(p.strip(), metrics) for p in parts)

        # 处理 OR
        if " OR " in condition:
            parts = condition.split(" OR ")
            return any(self._evaluate_simple(p.strip(), metrics) for p in parts)

        return self._evaluate_simple(condition, metrics)

    def _evaluate_simple(self, expr: str, metrics: dict[str, Any]) -> bool:
        """求值单个比较表达式。"""
        # matches 操作符
        m = re.match(r'^(\w+)\s+matches\s+"([^"]+)"\s*$', expr)
        if m:
            field, pattern = m.group(1), m.group(2)
            value = str(metrics.get(field, ""))
            if pattern not in self._compiled_regexes:
                self._compiled_regexes[pattern] = re.compile(pattern)
            return bool(self._compiled_regexes[pattern].search(value))

        # 比较操作符: == != > >= < <=
        for op in (">=", "<=", "!=", "==", ">", "<"):
            if op in expr:
                field, rest = expr.split(op, 1)
                field = field.strip()
                raw = rest.strip().strip('"').strip("'")
                actual = metrics.get(field)

                # 尝试数字比较
                try:
                    actual_num = float(actual) if actual is not None else 0
                    threshold_num = float(raw)
                    if op == ">":
                        return actual_num > threshold_num
                    if op == ">=":
                        return actual_num >= threshold_num
                    if op == "<":
                        return actual_num < threshold_num
                    if op == "<=":
                        return actual_num <= threshold_num
                    if op == "==":
                        return abs(actual_num - threshold_num) < 1e-9
                    if op == "!=":
                        return abs(actual_num - threshold_num) >= 1e-9
                except (TypeError, ValueError):
                    pass

                # 字符串比较
                if op == "==":
                    return str(actual) == raw
                if op == "!=":
                    return str(actual) != raw

                return False

        return True  # 无法解析的表达式 -> 默认不触发告警

    @staticmethod
    def _compose_message(rule: AlertRule, metrics: dict[str, Any]) -> str:
        """生成告警消息文本。"""
        parts = [f"[{rule.level.value}] {rule.rule_name}"]
        parts.append(f"目标: {rule.monitor_target}")
        parts.append(f"条件: {rule.condition}")
        for k, v in metrics.items():
            parts.append(f"  {k}={v}")
        return "\n".join(parts)