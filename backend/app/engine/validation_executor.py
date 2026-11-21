"""质量规则校验执行引擎。

校验类型：NOT_NULL / UNIQUE / REFERENCE / EXPRESSION / RANGE_VALUE / PATTERN_MATCH
输入：规则定义 + 行级数据抽样 → 输出：通过/失败/阻断决策。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class CheckType(str, Enum):
    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    REFERENCE = "REFERENCE"
    EXPRESSION = "EXPRESSION"
    RANGE_VALUE = "RANGE_VALUE"
    PATTERN_MATCH = "PATTERN_MATCH"


class CheckLevel(str, Enum):
    """校验级别决定失败后的处理方式。"""
    WARN = "WARN"       # 仅告警
    BLOCK = "BLOCK"     # 阻断管线
    FIX = "FIX"         # 自动修复尝试


@dataclass
class QualityRule:
    """一条质量校验规则定义。"""
    rule_id: str
    rule_name: str
    target_table: str
    check_field: str
    check_type: CheckType
    threshold: float = 0.0        # 允许的最大失败比例 (0~1)
    level: CheckLevel = CheckLevel.WARN
    extra: dict[str, Any] = field(default_factory=dict)
    rule_version: str = "v1"       # 规则版本：回执和告警必须引用同一版本
    # extra 可包含：
    #   reference_table, reference_field → REFERENCE
    #   expression → EXPRESSION (Python表达式，字段值通过 {value} 传入)
    #   pattern → PATTERN_MATCH (正则)
    #   min_value, max_value → RANGE_VALUE


@dataclass
class ValidationResult:
    """单条规则对一批数据的校验结果。

    注意：``total_rows`` 是参与该规则校验的样本行数（``sample_size`` 同义），
    不会因为同一批次还有别的规则而被放大。每条规则的坏行率都只按自己的
    样本统计，避免一条规则的合格行稀释另一条规则的坏行率。
    """
    rule_id: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    failed_ratio: float
    threshold_exceeded: bool
    blocked: bool  # 是否触发阻断
    error_samples: list[dict[str, Any]] = field(default_factory=list)
    # 回执需要的上下文：规则版本、适用阈值和样本数必须与校验结果一起流转
    rule_name: str = ""
    rule_version: str = "v1"
    target_table: str = ""
    check_field: str = ""
    check_type: str = ""
    level: str = CheckLevel.WARN.value
    threshold: float = 0.0
    sample_size: int = 0

    @property
    def bad_rows(self) -> int:
        """坏行数（与 failed_rows 同义，供回执/告警面板直接引用）。"""
        return self.failed_rows

    @property
    def bad_ratio(self) -> float:
        return self.failed_ratio

    @property
    def decision(self) -> str:
        """单规则结论：BLOCK（越线且阻断）/ ALERT（越线仅告警）/ PASS。"""
        if self.blocked:
            return "BLOCK"
        if self.threshold_exceeded:
            return "ALERT"
        return "PASS"


class ValidationEngine:
    """质量规则校验引擎。

    用法:
        engine = ValidationEngine()
        rule = QualityRule(rule_id="r1", rule_name="非空检查",
                           target_table="dw.dwd_order", check_field="order_id",
                           check_type=CheckType.NOT_NULL, threshold=0.01)
        result = engine.run(rule, [{"order_id": "123"}, {"order_id": None}])
    """

    _EXPR_BUILTINS: dict[str, Any] = {
        "len": len, "abs": abs, "min": min, "max": max,
        "round": round, "int": int, "float": float, "str": str,
    }

    def __init__(self) -> None:
        self._checkers: dict[CheckType, Callable[..., tuple[bool, str | None]]] = {
            CheckType.NOT_NULL: self._check_not_null,
            CheckType.UNIQUE: self._check_unique,
            CheckType.REFERENCE: self._check_reference,
            CheckType.EXPRESSION: self._check_expression,
            CheckType.RANGE_VALUE: self._check_range,
            CheckType.PATTERN_MATCH: self._check_pattern,
        }
        # 引用数据集（模拟引用完整性校验的查找表）
        self._reference_sets: dict[str, set[Any]] = {}

    def set_reference(self, ref_key: str, values: set[Any]) -> None:
        """预置引用查找集。"""
        self._reference_sets[ref_key] = values

    def run(self, rule: QualityRule, rows: list[dict[str, Any]]) -> ValidationResult:
        context = {
            "rule_name": rule.rule_name,
            "rule_version": rule.rule_version,
            "target_table": rule.target_table,
            "check_field": rule.check_field,
            "check_type": rule.check_type.value,
            "level": rule.level.value,
            "threshold": rule.threshold,
            "sample_size": len(rows),
        }
        checker = self._checkers.get(rule.check_type)
        if checker is None:
            return ValidationResult(
                rule_id=rule.rule_id,
                total_rows=len(rows), passed_rows=0, failed_rows=len(rows),
                failed_ratio=1.0, threshold_exceeded=True, blocked=True,
                error_samples=[{"error": f"未知校验类型 {rule.check_type.value}"}],
                **context,
            )

        passed, failed, samples = checker(rows, rule)

        # 分母必须是该规则自己的样本行数；为空批次按 1 处理避免除零。
        sample_size = len(rows)
        total = sample_size or 1
        ratio = failed / total
        threshold_exceeded = ratio > rule.threshold
        blocked = rule.level == CheckLevel.BLOCK and threshold_exceeded
        return ValidationResult(
            rule_id=rule.rule_id,
            total_rows=sample_size,
            passed_rows=passed,
            failed_rows=failed,
            failed_ratio=ratio,
            threshold_exceeded=threshold_exceeded,
            blocked=blocked,
            error_samples=samples[:5],
            **context,
        )

    # ---- 内置校验器 ----

    @staticmethod
    def _check_not_null(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            if val is not None and str(val).strip() != "":
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({"row_index": i, rule.check_field: val, "reason": "NULL/空值"})
        return passed, failed, samples

    @staticmethod
    def _check_unique(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        seen: set[Any] = set()
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            if val is not None and val not in seen:
                seen.add(val)
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": "重复值" if val in seen else "NULL值",
                    })
        return passed, failed, samples

    def _check_reference(
        self, rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        ref_key = rule.extra.get("reference_table", "")
        ref_set = self._reference_sets.get(ref_key, set())
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            if val is None:
                failed += 1
                if len(samples) < 5:
                    samples.append({"row_index": i, rule.check_field: val, "reason": "NULL值"})
            elif val in ref_set:
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": f"引用缺失: {val} 不在 {ref_key} 中",
                    })
        return passed, failed, samples

    @staticmethod
    def _check_expression(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        expr_src = rule.extra.get("expression", "True")
        # 安全编译表达式
        try:
            expr = compile(expr_src, "<quality_expression>", "eval")
        except SyntaxError:
            return 0, len(rows), [{"error": f"表达式语法错误: {expr_src}"}]

        namespace: dict[str, Any] = {
            "__builtins__": None,
        }
        namespace.update({
            "len": len, "abs": abs, "min": min, "max": max,
            "round": round, "int": int, "float": float, "str": str,
            "re": re,
        })

        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            local = dict(namespace)
            local["value"] = val
            for k, v in row.items():
                if k not in local:
                    local[k] = v
            try:
                ok = bool(eval(expr, {"__builtins__": {}}, local))
            except Exception:
                ok = False
            if ok:
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": f"表达式不满足: {expr_src}",
                    })
        return passed, failed, samples

    @staticmethod
    def _check_range(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        min_val = rule.extra.get("min_value")
        max_val = rule.extra.get("max_value")
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            try:
                num = float(val) if val is not None else None
            except (TypeError, ValueError):
                num = None
            ok = True
            reason = ""
            if num is None:
                ok = False
                reason = "NULL/非数字"
            elif min_val is not None and num < float(min_val):
                ok = False
                reason = f"小于下限 {min_val}"
            elif max_val is not None and num > float(max_val):
                ok = False
                reason = f"大于上限 {max_val}"
            if ok:
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val, "reason": reason,
                    })
        return passed, failed, samples

    @staticmethod
    def _check_pattern(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        pattern = rule.extra.get("pattern", ".*")
        try:
            compiled = re.compile(pattern)
        except re.error:
            return 0, len(rows), [{"error": f"正则错误: {pattern}"}]
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = str(row.get(rule.check_field, ""))
            if compiled.match(val):
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": f"不匹配模式 {pattern}",
                    })
        return passed, failed, samples

    # ---- 批量执行 ----

    def run_batch(
        self, rules: list[QualityRule], rows: list[dict[str, Any]]
    ) -> list[ValidationResult]:
        """对同一批数据执行多条规则，返回结果列表。"""
        return [self.run(rule, rows) for rule in rules]

    def run_pipeline_quality(
        self,
        rules: list[QualityRule],
        rows: list[dict[str, Any]],
        pipeline_id: str,
    ) -> dict[str, Any]:
        """完整的管线质量门禁：执行所有规则 → 汇总 → 通过/阻断决策。"""
        results = self.run_batch(rules, rows)
        summary = self.summarize_pipeline_quality(results, pipeline_id)
        # 逐规则原始结果一并返回，供回执按规则阈值判定落账。
        summary["validation_results"] = results
        return summary

    def summarize_pipeline_quality(
        self,
        results: list[ValidationResult],
        pipeline_id: str,
    ) -> dict[str, Any]:
        """把逐规则结果汇总成门禁结论（不做跨规则坏行率混算）。"""
        any_blocked = any(r.blocked for r in results)
        total_failed = sum(r.failed_rows for r in results)
        total_rows = sum(r.total_rows for r in results) or 1
        return {
            "pipeline_id": pipeline_id,
            "rule_results": [
                {
                    "rule_id": r.rule_id,
                    "rule_name": r.rule_name,
                    "rule_version": r.rule_version,
                    "target_table": r.target_table,
                    "check_field": r.check_field,
                    "check_type": r.check_type,
                    "level": r.level,
                    "total": r.total_rows,
                    "sample_size": r.sample_size,
                    "passed": r.passed_rows,
                    "failed": r.failed_rows,
                    "ratio": round(r.failed_ratio, 4),
                    "threshold": r.threshold,
                    "threshold_exceeded": r.threshold_exceeded,
                    "blocked": r.blocked,
                    "decision": r.decision,
                }
                for r in results
            ],
            "overall_pass": not any_blocked,
            "overall_failed_ratio": round(total_failed / total_rows, 4),
            "decision": "RELEASE" if not any_blocked else "BLOCKED",
        }