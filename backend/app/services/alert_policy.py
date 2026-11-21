"""告警策略领域服务：管理告警规则配置、事件聚合和通知渠道控制。

上游：validation_run + exception_capture + watermark_tracker 产出异常 → alert_policy 触发告警
消费：通知系统和运维值班面板消费告警事件
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class AlertPolicyService(BaseModuleService):
    MODULE = "alert_policy"
    ENTITY = "告警规则"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["规则名称", "监控对象", "告警条件", "告警级别", "通知渠道", "静默时段", "聚合窗口"]
    REQUIRED_FIELDS = ["规则名称", "监控对象", "告警条件"]
    STATUS_ORDER = ["草稿", "已启用", "已静默", "已停用"]
    ACTION_RULES = {"启用告警": "草稿", "静默告警": "已启用", "停用告警": "已启用"}
    NEGATIVE_ACTIONS = ["停用告警"]

    def query_by_target(self, target: str) -> list[dict[str, Any]]:
        """按监控对象查询告警规则。

        从 store 中筛选指定对象（管线、表、节点）的告警规则，
        返回规则列表供告警面板消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if target.casefold() in str(row.get("监控对象", "")).casefold():
                result.append(dict(row))
        return result

    def evaluate_alert_condition(self, rule_name: str, metric_value: float) -> dict[str, Any]:
        """评估告警条件是否触发。

        根据告警条件的阈值与当前指标值比较，
        判断是否应触发告警和计算告警级别。
        """
        rule = None
        for row in store.rows(self.MODULE):
            if row.get("规则名称") == rule_name:
                rule = row
                break
        if rule is None:
            return {"error": f"告警规则 {rule_name} 不存在"}

        condition = str(rule.get("告警条件", ""))
        level = str(rule.get("告警级别", "普通"))

        triggered = False
        threshold = 0.0
        try:
            # 先匹配双字符操作符，避免 ">=" 被 ">" 抢先切成 "= 0.01"。
            for op in (">=", "<=", "==", "!=", ">", "<"):
                if op in condition:
                    threshold_str = condition.split(op, 1)[1].strip()
                    threshold = float(threshold_str.replace("%", ""))
                    triggered = {
                        ">": metric_value > threshold,
                        ">=": metric_value >= threshold,
                        "<": metric_value < threshold,
                        "<=": metric_value <= threshold,
                        "==": abs(metric_value - threshold) < 1e-9,
                        "!=": abs(metric_value - threshold) >= 1e-9,
                    }[op]
                    break
        except (ValueError, IndexError):
            pass

        return {
            "rule": rule_name,
            "condition": condition,
            "threshold": threshold,
            "current_value": metric_value,
            "triggered": triggered,
            "severity": level,
            "evaluated_at": datetime.now().isoformat(),
        }

    def compute_alert_window(self, rule_name: str) -> dict[str, Any]:
        """计算聚合窗口内的告警事件量。

        根据聚合窗口大小统计 store 中相关模块的告警事件数，
        判断是否达到聚合阈值。
        """
        rule = None
        for row in store.rows(self.MODULE):
            if row.get("规则名称") == rule_name:
                rule = row
                break
        if rule is None:
            return {"error": f"告警规则 {rule_name} 不存在"}

        window_str = str(rule.get("聚合窗口", "60"))
        try:
            window_min = int(window_str.replace("min", "").replace("s", "").strip())
        except ValueError:
            window_min = 60

        alert_level = str(rule.get("告警级别", "普通"))
        target = str(rule.get("监控对象", ""))

        related_events = 0
        ex_rows = store.rows("exception_capture") if "exception_capture" in store._tables else []
        for ex in ex_rows:
            if target.casefold() in str(ex.get("所属管线", "")).casefold():
                related_events += 1

        return {
            "rule": rule_name,
            "window_minutes": window_min,
            "related_events_count": related_events,
            "channel": rule.get("通知渠道", "未配置"),
            "silence_window": rule.get("静默时段", "无"),
            "recommendation": (
                f"在 {window_min} 分钟聚合窗口内已累计 {related_events} 个相关事件"
            ),
            "computed_at": datetime.now().isoformat(),
        }

    def get_active_alerts(self) -> list[dict[str, Any]]:
        """获取当前活跃的告警规则。

        筛选 store 中已启用且不在静默时段的规则，
        返回活跃规则列表供告警中心汇总展示。
        """
        rows = store.rows(self.MODULE)
        active: list[dict[str, Any]] = []
        for row in rows:
            if row.get("status") == "已启用":
                silence = str(row.get("静默时段", ""))
                is_silenced = False
                if silence and silence != "无":
                    try:
                        parts = silence.split("-")
                        if len(parts) == 2:
                            start = int(parts[0].replace(":", ""))
                            end = int(parts[1].replace(":", ""))
                            now_hour = datetime.now().hour
                            is_silenced = start <= now_hour < end
                    except ValueError:
                        pass

                active.append({
                    "rule": row.get("规则名称"),
                    "target": row.get("监控对象"),
                    "level": row.get("告警级别"),
                    "channel": row.get("通知渠道"),
                    "is_silenced": is_silenced,
                })

        return active

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg

    # ---- 质量批次回执 → 告警面板 ----

    def evaluate_receipt_alerts(self, receipt_id: str) -> dict[str, Any]:
        """按质量批次阻断回执的越线规则评估告警。

        告警依据必须来自回执里每条规则自己的阈值、样本数和坏行数，
        不能用批次汇总坏行率——那会把合格行的规则混进来稀释坏行率。
        """
        from app.engine.quality_receipt import quality_ledger

        receipt = quality_ledger.get_receipt(receipt_id)
        if receipt is None:
            return {"error": f"回执 {receipt_id} 不存在"}

        evaluations: list[dict[str, Any]] = []
        for decision in receipt.rule_decisions:
            evaluations.append({
                "rule_id": decision.rule_id,
                "rule_name": decision.rule_name,
                "rule_version": decision.rule_version,
                "threshold": decision.threshold,
                "sample_size": decision.sample_size,
                "bad_rows": decision.failed_rows,
                "bad_ratio": round(decision.failed_ratio, 6),
                "threshold_exceeded": decision.threshold_exceeded,
                "blocked": decision.blocked,
                "decision": decision.decision,
                "triggered": decision.threshold_exceeded,
                "judgement": decision.threshold_judgement,
            })

        return {
            "receipt_id": receipt.receipt_id,
            "batch_id": receipt.batch_id,
            "pipeline_id": receipt.pipeline_id,
            "decision": receipt.decision,
            "triggered_rules": [e for e in evaluations if e["triggered"]],
            "all_rules": evaluations,
            "alert_events": [
                e.as_dict() for e in quality_ledger.list_alerts(receipt_id=receipt_id)
            ],
            "alert_state": quality_ledger.receipt_view(receipt)["alert_state"],
            "evaluated_at": datetime.now().isoformat(),
        }

    def active_quality_alerts(self) -> dict[str, Any]:
        """告警面板：质量批次阻断回执登记给值班组的告警事件。"""
        from app.engine.quality_receipt import quality_ledger

        events = quality_ledger.list_alerts()
        return {
            "total": len(events),
            "receiver_connected": quality_ledger.receiver_connected,
            "pending": sum(1 for e in events if e.status == "PENDING"),
            "delivered": sum(1 for e in events if e.status == "DELIVERED"),
            "items": [e.as_dict() for e in events],
            "snapshot_at": datetime.now().isoformat(),
        }