"""数据质量规则领域服务：管理校验规则定义、阈值评估和质量统计。

上游：schema_evolution 产出字段变更 → quality_rule 更新校验规则
消费：validation_run 消费质量规则执行校验批次并产出报告
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class QualityRuleService(BaseModuleService):
    MODULE = "quality_rule"
    ENTITY = "校验规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "目标表", "校验字段", "校验类型", "阈值设定", "阀值动作", "校验级别"]
    REQUIRED_FIELDS = ["规则名称", "目标表", "校验字段"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停", "已废弃"]
    ACTION_RULES = {"启用规则": "草稿", "暂停规则": "已启用", "废弃规则": "已暂停"}
    NEGATIVE_ACTIONS = []

    def query_by_table(self, table_name: str) -> list[dict[str, Any]]:
        """按目标表查询质量规则。

        在分区扫描或 Schema 变更后，根据目标表检索关联的校验规则，
        返回规则列表供校验批次编排使用。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if table_name.casefold() in str(row.get("目标表", "")).casefold():
                result.append(dict(row))
        return result

    def evaluate_threshold(self, field_name: str, field_value: Any) -> dict[str, Any]:
        """评估字段值是否满足阈值设定。

        根据校验类型和阈值设定判断数据字段是否符合质量要求，
        产出评估结果包含是否通过、偏差信息。
        """
        rules = store.rows(self.MODULE)
        violations: list[dict[str, Any]] = []
        for rule in rules:
            rule_field = str(rule.get("校验字段", ""))
            if rule_field.casefold() != field_name.casefold():
                continue
            check_type = rule.get("校验类型", "")
            threshold_str = str(rule.get("阈值设定", "0"))
            level = rule.get("校验级别", "普通")
            try:
                threshold = float(threshold_str)
                val = float(field_value) if field_value is not None else 0
            except (ValueError, TypeError):
                threshold = 0
                val = 0

            passed = True
            detail = ""
            if check_type == "上限":
                passed = val <= threshold
                detail = f"值 {val} {'≤' if passed else '>'} 上限 {threshold}"
            elif check_type == "下限":
                passed = val >= threshold
                detail = f"值 {val} {'≥' if passed else '<'} 下限 {threshold}"
            elif check_type == "非空":
                passed = field_value is not None and str(field_value).strip() != ""
                detail = f"字段 {'非空' if passed else '为空'}"
            elif check_type == "唯一":
                passed = True
                detail = "唯一性校验需统计维度评估"

            if not passed:
                violations.append({
                    "rule": rule.get("规则名称"),
                    "field": field_name,
                    "check_type": check_type,
                    "threshold": threshold,
                    "actual_value": field_value,
                    "level": level,
                    "detail": detail,
                })

        return {
            "field": field_name,
            "value": field_value,
            "total_rules": len(rules),
            "violations": violations,
            "passed": len(violations) == 0,
            "evaluated_at": datetime.now().isoformat(),
        }

    def quality_summary(self) -> dict[str, Any]:
        """数据质量汇总统计。

        统计所有已启用规则的覆盖范围和校验级别分布，
        产出质量快照供数据治理面板消费。
        """
        rows = store.rows(self.MODULE)
        enabled = [r for r in rows if r.get("status") == "已启用"]
        paused = [r for r in rows if r.get("status") == "已暂停"]
        by_level: dict[str, int] = {}
        by_type: dict[str, int] = {}
        for r in rows:
            level = str(r.get("校验级别", "普通"))
            by_level[level] = by_level.get(level, 0) + 1
            ctype = str(r.get("校验类型", ""))
            by_type[ctype] = by_type.get(ctype, 0) + 1

        return {
            "total_rules": len(rows),
            "enabled": len(enabled),
            "paused": len(paused),
            "by_level": by_level,
            "by_check_type": by_type,
            "coverage_pct": round(len(enabled) / max(len(rows), 1) * 100, 1),
            "snapshot_at": datetime.now().isoformat(),
        }

    def batch_disable_for_table(self, table_name: str) -> dict[str, Any]:
        """对指定表批量暂停质量规则。

        当 Schema 变更导致表中的字段大量变动时，批量暂停关联规则，
        避免误报，返回受影响规则列表供人工复核。
        """
        disabled: list[str] = []
        for row in store.rows(self.MODULE):
            if table_name.casefold() in str(row.get("目标表", "")).casefold():
                if row.get("status") == "已启用":
                    row["status"] = "已暂停"
                    row["暂停原因"] = f"表 {table_name} schema变更触发的批量暂停"
                    disabled.append(str(row.get("规则名称")))

        return {
            "table": table_name,
            "disabled_rules": disabled,
            "disabled_count": len(disabled),
            "operated_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg