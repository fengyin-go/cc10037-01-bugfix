"""质量批次阻断回执 / 告警交接回归测试。

不依赖 pytest/fastapi 测试客户端，直接调用领域对象，保证干净环境可跑：
    cd backend && .venv/bin/python -m unittest discover -s tests -v

覆盖用户报告的场景：订单键半数缺失（BLOCK）与地区字段全部合格（PASS）
在同一批次里并存，断开告警接收侧后重放原批，恢复后仍能按订单键规则收到告警。
"""

from __future__ import annotations

import concurrent.futures as cf
import unittest

from app.engine.quality_receipt import QualityGateLedger
from app.engine.validation_executor import CheckLevel, CheckType, QualityRule
from app.orchestrator import ETLOrchestrator


RULES = [
    QualityRule(
        rule_id="R-ORDER-KEY", rule_name="订单主键非空", rule_version="v3",
        target_table="dw.dwd_order_wide", check_field="order_id",
        check_type=CheckType.NOT_NULL, threshold=0.3, level=CheckLevel.BLOCK,
    ),
    QualityRule(
        rule_id="R-REGION", rule_name="地区字段非空", rule_version="v2",
        target_table="dw.dwd_order_wide", check_field="region_code",
        check_type=CheckType.NOT_NULL, threshold=0.3, level=CheckLevel.WARN,
    ),
]
# 10 行：订单键 5 行缺失（50%），地区字段全部合格（0%）。
ROWS = [
    {"order_id": None if i < 5 else f"O{i:03d}", "region_code": f"R{i % 3}"}
    for i in range(10)
]


class ReceiptAlertFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.orchestrator = ETLOrchestrator()
        self.ledger: QualityGateLedger = self.orchestrator.ledger
        self.ledger.clear()

    def tearDown(self) -> None:
        self.ledger.clear()

    def _gate(self, *, batch_id: str = "VAL-ORDER-20261121-001", rows=ROWS):
        return self.orchestrator.run_quality_gate(
            "p_dwd_order", RULES, rows,
            batch_id=batch_id,
            target_table="dw.dwd_order_wide",
            partition="2026-11-20",
        )

    def test_alert_uses_rule_own_bad_ratio_not_diluted_batch_average(self) -> None:
        gate = self._gate()
        self.assertEqual(gate.decision, "BLOCKED")
        # 批次汇总坏行率会被地区规则的合格行稀释到 25%，
        # 但订单键规则自身是 50%，告警必须按 50% 判定为越线。
        self.assertAlmostEqual(gate.failed_ratio, 0.25)
        order_alert = next(a for a in gate.triggered_alerts if a.rule_id == "qalert_R-ORDER-KEY")
        self.assertTrue(order_alert.matched)
        self.assertEqual(order_alert.actual_values["failed_ratio"], 0.5)
        region_alert = next(a for a in gate.triggered_alerts if a.rule_id == "qalert_R-REGION")
        self.assertFalse(region_alert.matched)

    def test_receipt_lists_every_rule_threshold_judgement(self) -> None:
        receipt = self._gate().receipt
        decisions = {d["rule_id"]: d["decision"] for d in receipt["rule_decisions"]}
        self.assertEqual(decisions, {"R-ORDER-KEY": "BLOCK", "R-REGION": "PASS"})
        judgements = {j["rule_id"]: j for j in receipt["threshold_judgements"]}
        self.assertEqual(len(judgements), 2)
        self.assertTrue(judgements["R-ORDER-KEY"]["blocked"])
        self.assertFalse(judgements["R-REGION"]["blocked"])
        self.assertEqual(judgements["R-ORDER-KEY"]["sample_size"], 10)
        self.assertEqual(judgements["R-ORDER-KEY"]["failed_rows"], 5)
        self.assertEqual(judgements["R-ORDER-KEY"]["rule_version"], "v3")

    def test_disconnect_replay_and_recover(self) -> None:
        self.ledger.disconnect_receiver()
        gate = self._gate()
        receipt = gate.receipt
        self.assertEqual(receipt["alert_state"], "PENDING")
        self.assertTrue(receipt["handoff_only"])
        self.assertFalse(receipt["resubmittable"])
        self.assertEqual(receipt["batch_disposition"], "结论已确认，告警待交接")
        self.assertEqual([a["rule_id"] for a in receipt["alert_events"]], ["R-ORDER-KEY"])

        replay = self._gate()
        self.assertEqual(replay.receipt_id, gate.receipt_id)
        self.assertTrue(replay.gateway["receipt_replay"])
        self.assertEqual(
            [e["alert_id"] for e in replay.alert_events],
            [e["alert_id"] for e in gate.alert_events],
        )
        self.assertEqual(replay.receipt["replay_count"], 1)

        pending = self.ledger.list_alerts(batch_id=gate.batch_id)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].status, "PENDING")

        drained = self.ledger.connect_receiver()
        self.assertEqual([e.rule_id for e in drained], ["R-ORDER-KEY"])
        self.assertEqual(drained[0].status, "DELIVERED")
        self.assertEqual(drained[0].rule_version, "v3")
        self.assertEqual((drained[0].sample_size, drained[0].failed_rows), (10, 5))
        self.assertAlmostEqual(drained[0].failed_ratio, 0.5)

        # 恢复后重放不再产生第二条投递记录。
        self._gate()
        self.assertEqual(len(self.ledger.delivered_inbox()), 1)

    def test_concurrent_replay_keeps_single_alert_identity(self) -> None:
        self.ledger.disconnect_receiver()
        with cf.ThreadPoolExecutor(max_workers=8) as pool:
            gates = list(pool.map(lambda _: self._gate(), range(16)))
        self.assertEqual({g.receipt_id for g in gates}, {gates[0].receipt_id})
        identities = {tuple(e["alert_id"] for e in g.alert_events) for g in gates}
        self.assertEqual(len(identities), 1)
        self.assertEqual(len(self.ledger.list_alerts(batch_id=gates[0].batch_id)), 1)

    def test_same_batch_with_changed_content_stays_one_receipt(self) -> None:
        first = self._gate()
        second = self._gate(rows=ROWS[:5])
        self.assertEqual(second.receipt_id, first.receipt_id)
        self.assertTrue(second.receipt["content_conflict"])
        self.assertEqual(len(second.receipt["alert_events"]), 1)

    def test_unconfirmed_batch_is_resubmittable(self) -> None:
        disposition = self.ledger.batch_disposition("VAL-NOT-RUN")
        self.assertTrue(disposition["resubmittable"])
        self.assertEqual(disposition["next_action"], "RESUBMIT_BATCH")


if __name__ == "__main__":
    unittest.main()
