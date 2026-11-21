"""质量批次阻断回执与告警交接账本。

质量门禁算完之后，结论必须落到一份可追溯的**回执**里：
- 每条规则一行阈值判定（阈值、样本数、坏行数、坏行率、是否越线、是否阻断）；
- 回执一旦生成即"结论已确认"，同一批次重放只返回同一份回执；
- 告警评估只从回执里的**越线规则**读取依据，不读取批次汇总坏行率；
- 告警事件按 (回执, 规则, 规则版本) 生成稳定身份，并发重放不会产生重复告警；
- 接收侧断开时事件保持 PENDING，恢复后按原规则版本和坏行统计补投一次。

该账本只依赖标准库，进程内共享；真实项目可替换为数据库表 + 唯一约束。
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

ON_CALL_GROUP = "数据工程值班组"

# 回执/事件状态
ALERT_PENDING = "PENDING"
ALERT_DELIVERED = "DELIVERED"
ALERT_NONE = "NONE"


def _now() -> str:
    return datetime.now().isoformat()


def _stable_hash(payload: Any, length: int = 12) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:length]


@dataclass
class RuleThresholdDecision:
    """回执中的单规则阈值判定。"""

    rule_id: str
    rule_name: str
    rule_version: str
    target_table: str
    check_field: str
    check_type: str
    level: str
    threshold: float
    sample_size: int
    passed_rows: int
    failed_rows: int
    failed_ratio: float
    threshold_exceeded: bool
    blocked: bool
    decision: str
    error_samples: list[dict[str, Any]] = field(default_factory=list)

    @property
    def threshold_judgement(self) -> str:
        """人类可读的阈值判定，例如 ``0.5000 > 0.0001 → 越线（阻断）``。"""
        op = ">" if self.threshold_exceeded else "<="
        if self.blocked:
            outcome = "越线（阻断）"
        elif self.threshold_exceeded:
            outcome = "越线（告警）"
        else:
            outcome = "未越线（通过）"
        return (
            f"failed_ratio {self.failed_ratio:.4f} {op} threshold "
            f"{self.threshold:.4f} → {outcome}"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "rule_version": self.rule_version,
            "target_table": self.target_table,
            "check_field": self.check_field,
            "check_type": self.check_type,
            "level": self.level,
            "threshold": self.threshold,
            "threshold_exceeded": self.threshold_exceeded,
            "blocked": self.blocked,
            "decision": self.decision,
            "sample_size": self.sample_size,
            "total": self.sample_size,
            "passed_rows": self.passed_rows,
            "failed_rows": self.failed_rows,
            "bad_rows": self.failed_rows,
            "failed_ratio": round(self.failed_ratio, 6),
            "bad_ratio": round(self.failed_ratio, 6),
            "threshold_judgement": self.threshold_judgement,
            "error_samples": self.error_samples,
        }


@dataclass
class AlertEvent:
    """登记给值班组的告警事件（告警面板读取的对象）。"""

    alert_id: str
    receipt_id: str
    batch_id: str
    pipeline_id: str
    rule_id: str
    rule_name: str
    rule_version: str
    target_table: str
    check_field: str
    level: str
    threshold: float
    threshold_judgement: str
    sample_size: int
    passed_rows: int
    failed_rows: int
    failed_ratio: float
    on_call_group: str
    created_at: str
    message: str
    status: str = ALERT_PENDING
    delivery_attempts: int = 0
    last_attempt_at: str | None = None
    delivered_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "receipt_id": self.receipt_id,
            "batch_id": self.batch_id,
            "pipeline_id": self.pipeline_id,
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "rule_version": self.rule_version,
            "target_table": self.target_table,
            "check_field": self.check_field,
            "level": self.level,
            "threshold": self.threshold,
            "threshold_judgement": self.threshold_judgement,
            "sample_size": self.sample_size,
            "total": self.sample_size,
            "passed_rows": self.passed_rows,
            "failed_rows": self.failed_rows,
            "bad_rows": self.failed_rows,
            "failed_ratio": round(self.failed_ratio, 6),
            "bad_ratio": round(self.failed_ratio, 6),
            "on_call_group": self.on_call_group,
            "status": self.status,
            "delivery_attempts": self.delivery_attempts,
            "last_attempt_at": self.last_attempt_at,
            "delivered_at": self.delivered_at,
            "created_at": self.created_at,
            "message": self.message,
        }


@dataclass
class QualityBatchReceipt:
    """质量批次阻断回执。"""

    receipt_id: str
    batch_id: str
    pipeline_id: str
    target_table: str
    partition: str
    created_at: str
    decision: str
    overall_pass: bool
    rule_decisions: list[RuleThresholdDecision]
    fingerprint: str
    replay_count: int = 0
    last_replayed_at: str | None = None
    content_conflict: bool = False

    @property
    def blocking_rules(self) -> list[RuleThresholdDecision]:
        return [d for d in self.rule_decisions if d.blocked]

    @property
    def alerting_rules(self) -> list[RuleThresholdDecision]:
        """越线规则：阻断规则 + 仅越线的告警规则。"""
        return [d for d in self.rule_decisions if d.threshold_exceeded]

    def as_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "batch_id": self.batch_id,
            "pipeline_id": self.pipeline_id,
            "target_table": self.target_table,
            "partition": self.partition,
            "created_at": self.created_at,
            "decision": self.decision,
            "overall_pass": self.overall_pass,
            "fingerprint": self.fingerprint,
            "replay_count": self.replay_count,
            "last_replayed_at": self.last_replayed_at,
            "content_conflict": self.content_conflict,
            "rule_decisions": [d.as_dict() for d in self.rule_decisions],
            "blocking_rules": [d.as_dict() for d in self.blocking_rules],
            "alerting_rules": [d.as_dict() for d in self.alerting_rules],
            "threshold_judgements": [
                {
                    "rule_id": d.rule_id,
                    "rule_name": d.rule_name,
                    "rule_version": d.rule_version,
                    "blocked": d.blocked,
                    "threshold_exceeded": d.threshold_exceeded,
                    "sample_size": d.sample_size,
                    "failed_rows": d.failed_rows,
                    "failed_ratio": round(d.failed_ratio, 6),
                    "threshold": d.threshold,
                    "judgement": d.threshold_judgement,
                }
                for d in self.rule_decisions
            ],
        }


class QualityGateLedger:
    """回执、告警事件和接收侧状态的进程内账本（带锁，支持并发重放）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._receipts: dict[str, QualityBatchReceipt] = {}
        self._receipt_by_batch: dict[str, str] = {}
        self._alerts: dict[str, AlertEvent] = {}
        self._delivery_log: list[dict[str, Any]] = []
        self._receiver_connected = True

    # ---- 回执 ----

    def batch_fingerprint(
        self,
        *,
        pipeline_id: str,
        target_table: str,
        partition: str,
        rules: Iterable[dict[str, Any]],
        sample_data: list[dict[str, Any]],
    ) -> str:
        payload = {
            "pipeline_id": pipeline_id,
            "target_table": target_table,
            "partition": partition,
            "rules": sorted(rules, key=lambda r: str(r.get("rule_id", ""))),
            "sample_data": sample_data,
        }
        return _stable_hash(payload, 16)

    def record_receipt(
        self,
        *,
        pipeline_id: str,
        target_table: str,
        partition: str,
        batch_id: str,
        fingerprint: str,
        validation_results: Iterable[Any],
    ) -> tuple[QualityBatchReceipt, bool]:
        """登记回执；同一 batch_id 的重放返回同一份回执。

        返回 ``(receipt, is_replay)``。回执一旦登记即代表门禁结论已确认，
        调用方不应再对原批重新计算结论。
        """
        decisions = [self._decision_from_validation(r) for r in validation_results]
        blocked = any(d.blocked for d in decisions)
        decision = "BLOCKED" if blocked else "RELEASE"
        # 回执身份只锚定批次（含管线），所以同一批次无论重放多少次、
        # 内容是否变化，都只有一份回执和一个告警身份。
        receipt_id = "QR-" + _stable_hash(
            {"pipeline_id": pipeline_id, "batch_id": batch_id}
        )

        with self._lock:
            existing = self._receipts.get(receipt_id)
            if existing is not None:
                existing.replay_count += 1
                existing.last_replayed_at = _now()
                # 同一批次标识却出现不同内容：结论仍以首次确认的回执为准，
                # 只标记冲突供值班人员复核，不重复登记告警。
                existing.content_conflict = existing.content_conflict or (
                    existing.fingerprint != fingerprint
                )
                return existing, True

            receipt = QualityBatchReceipt(
                receipt_id=receipt_id,
                batch_id=batch_id,
                pipeline_id=pipeline_id,
                target_table=target_table,
                partition=partition,
                created_at=_now(),
                decision=decision,
                overall_pass=not blocked,
                rule_decisions=decisions,
                fingerprint=fingerprint,
            )
            self._receipts[receipt_id] = receipt
            self._receipt_by_batch[batch_id] = receipt_id
            return receipt, False

    @staticmethod
    def _decision_from_validation(result: Any) -> RuleThresholdDecision:
        return RuleThresholdDecision(
            rule_id=str(getattr(result, "rule_id", "")),
            rule_name=str(getattr(result, "rule_name", "")),
            rule_version=str(getattr(result, "rule_version", "v1")),
            target_table=str(getattr(result, "target_table", "")),
            check_field=str(getattr(result, "check_field", "")),
            check_type=str(getattr(result, "check_type", "")),
            level=str(getattr(result, "level", "")),
            threshold=float(getattr(result, "threshold", 0.0)),
            sample_size=int(getattr(result, "sample_size", 0)),
            passed_rows=int(getattr(result, "passed_rows", 0)),
            failed_rows=int(getattr(result, "failed_rows", 0)),
            failed_ratio=float(getattr(result, "failed_ratio", 0.0)),
            threshold_exceeded=bool(getattr(result, "threshold_exceeded", False)),
            blocked=bool(getattr(result, "blocked", False)),
            decision=str(getattr(result, "decision", "PASS")),
            error_samples=list(getattr(result, "error_samples", []) or []),
        )

    def get_receipt(self, receipt_id: str) -> QualityBatchReceipt | None:
        with self._lock:
            return self._receipts.get(receipt_id)

    def receipt_for_batch(self, batch_id: str) -> QualityBatchReceipt | None:
        with self._lock:
            receipt_id = self._receipt_by_batch.get(batch_id)
            return self._receipts.get(receipt_id) if receipt_id else None

    def list_receipts(self) -> list[QualityBatchReceipt]:
        with self._lock:
            return list(self._receipts.values())

    # ---- 告警登记与接收侧 ----

    def register_alerts_for_receipt(self, receipt: QualityBatchReceipt) -> list[AlertEvent]:
        """只按回执中的越线规则登记告警，绝不使用批次汇总坏行率。"""
        with self._lock:
            events: list[AlertEvent] = []
            for decision in receipt.alerting_rules:
                alert_id = "ALT-" + _stable_hash({
                    "receipt_id": receipt.receipt_id,
                    "rule_id": decision.rule_id,
                    "rule_version": decision.rule_version,
                })
                event = self._alerts.get(alert_id)
                if event is None:
                    event = AlertEvent(
                        alert_id=alert_id,
                        receipt_id=receipt.receipt_id,
                        batch_id=receipt.batch_id,
                        pipeline_id=receipt.pipeline_id,
                        rule_id=decision.rule_id,
                        rule_name=decision.rule_name,
                        rule_version=decision.rule_version,
                        target_table=decision.target_table,
                        check_field=decision.check_field,
                        level=self._alert_level(decision),
                        threshold=decision.threshold,
                        threshold_judgement=decision.threshold_judgement,
                        sample_size=decision.sample_size,
                        passed_rows=decision.passed_rows,
                        failed_rows=decision.failed_rows,
                        failed_ratio=decision.failed_ratio,
                        on_call_group=ON_CALL_GROUP,
                        created_at=_now(),
                        message=self._compose_message(receipt, decision),
                    )
                    self._alerts[alert_id] = event
                events.append(event)
                # 每次重放都尝试补投；已投递的事件保持原身份且不重复投递。
                self._attempt_delivery_locked(event)
            return events

    def register_alerts_from_receipt(self, receipt_id: str) -> list[AlertEvent] | None:
        receipt = self.get_receipt(receipt_id)
        if receipt is None:
            return None
        return self.register_alerts_for_receipt(receipt)

    @staticmethod
    def _alert_level(decision: RuleThresholdDecision) -> str:
        # 阻断级规则按 CRITICAL 交接，仅告警级规则按 WARN 交接。
        if decision.blocked or decision.level.upper() == "BLOCK":
            return "CRITICAL"
        if decision.level.upper() == "FIX":
            return "WARN"
        return decision.level.upper() if decision.level else "WARN"

    @staticmethod
    def _compose_message(
        receipt: QualityBatchReceipt, decision: RuleThresholdDecision
    ) -> str:
        return (
            f"[{QualityGateLedger._alert_level(decision)}] 质量规则越线："
            f"{decision.rule_name}（{decision.rule_id}@{decision.rule_version}）\n"
            f"批次: {receipt.batch_id} / 回执: {receipt.receipt_id}\n"
            f"字段: {decision.check_field}，样本: {decision.sample_size} 行，"
            f"坏行: {decision.failed_rows} 行（{decision.failed_ratio:.2%}）\n"
            f"判定: {decision.threshold_judgement}"
        )

    def _attempt_delivery_locked(self, event: AlertEvent) -> bool:
        if event.status == ALERT_DELIVERED:
            return True
        event.delivery_attempts += 1
        event.last_attempt_at = _now()
        if not self._receiver_connected:
            event.status = ALERT_PENDING
            return False
        event.status = ALERT_DELIVERED
        event.delivered_at = _now()
        self._delivery_log.append(event.as_dict())
        return True

    def disconnect_receiver(self) -> None:
        with self._lock:
            self._receiver_connected = False

    def connect_receiver(self, drain: bool = True) -> list[AlertEvent]:
        """恢复接收侧；默认把断连期间积压的告警补投给值班组。"""
        with self._lock:
            self._receiver_connected = True
            return self._drain_locked() if drain else []

    # 语义别名，便于调用侧按"恢复/连接"两种措辞使用。
    recover_receiver = connect_receiver

    def drain_pending(self) -> list[AlertEvent]:
        with self._lock:
            return self._drain_locked()

    def _drain_locked(self) -> list[AlertEvent]:
        delivered: list[AlertEvent] = []
        if not self._receiver_connected:
            return delivered
        for event in sorted(self._alerts.values(), key=lambda e: e.created_at):
            if event.status == ALERT_PENDING and self._attempt_delivery_locked(event):
                delivered.append(event)
        return delivered

    @property
    def receiver_connected(self) -> bool:
        with self._lock:
            return self._receiver_connected

    def list_alerts(
        self,
        *,
        receipt_id: str | None = None,
        batch_id: str | None = None,
        status: str | None = None,
    ) -> list[AlertEvent]:
        with self._lock:
            events = list(self._alerts.values())
        if receipt_id:
            events = [e for e in events if e.receipt_id == receipt_id]
        if batch_id:
            events = [e for e in events if e.batch_id == batch_id]
        if status:
            events = [e for e in events if e.status == status.upper()]
        return sorted(events, key=lambda e: e.created_at)

    def delivered_inbox(self) -> list[dict[str, Any]]:
        """接收侧实际收到的告警（已投递），供值班组核对。"""
        with self._lock:
            return list(self._delivery_log)

    def clear(self) -> None:
        """测试/本地重置账本。"""
        with self._lock:
            self._receipts.clear()
            self._receipt_by_batch.clear()
            self._alerts.clear()
            self._delivery_log.clear()
            self._receiver_connected = True

    # ---- 回执视图 ----

    def receipt_view(self, receipt: QualityBatchReceipt) -> dict[str, Any]:
        """回执 + 告警交接状态，供回执接口和值班面板共同读取。"""
        events = self.list_alerts(receipt_id=receipt.receipt_id)
        if not events:
            alert_state = ALERT_NONE
        elif all(e.status == ALERT_DELIVERED for e in events):
            alert_state = ALERT_DELIVERED
        else:
            alert_state = ALERT_PENDING

        if alert_state == ALERT_PENDING:
            next_action = "HANDOFF_ALERT"
            disposition = "结论已确认，告警待交接"
            operator_hint = (
                "门禁结论已确认，原批无需重新提交；接收侧恢复后补投告警即可。"
            )
        elif alert_state == ALERT_DELIVERED:
            next_action = "NONE"
            disposition = "结论已确认，告警已交接"
            operator_hint = "门禁结论和告警交接均已完成，无需重新提交原批。"
        else:
            next_action = "NONE"
            disposition = "结论已确认，无需告警"
            operator_hint = "门禁结论已确认且无越线规则，无需重新提交原批。"

        view = receipt.as_dict()
        view.update({
            "conclusion_confirmed": True,
            "resubmittable": False,
            "handoff_only": alert_state == ALERT_PENDING,
            "alert_state": alert_state,
            "alert_events": [e.as_dict() for e in events],
            "alert_count": len(events),
            "receiver_connected": self.receiver_connected,
            "next_action": next_action,
            "batch_disposition": disposition,
            "operator_hint": operator_hint,
        })
        return view

    def batch_disposition(self, batch_id: str) -> dict[str, Any]:
        """区分"原批可重新提交"与"结论已确认、只剩告警待交接"。"""
        receipt = self.receipt_for_batch(batch_id)
        if receipt is None:
            return {
                "batch_id": batch_id,
                "receipt_id": None,
                "conclusion_confirmed": False,
                "resubmittable": True,
                "handoff_only": False,
                "alert_state": ALERT_NONE,
                "next_action": "RESUBMIT_BATCH",
                "batch_disposition": "未找到已确认回执，原批可重新提交",
                "operator_hint": "门禁尚未产出回执，原批可重新提交。",
            }
        return self.receipt_view(receipt)


# 进程内共享账本；真实项目替换为带唯一约束的持久化存储。
quality_ledger = QualityGateLedger()
