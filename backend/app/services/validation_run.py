"""校验批次领域服务：执行质量校验批次、产出校验报告和异常标记。

上游：quality_rule 产出校验规则 → validation_run 执行校验批次
消费：alert_policy 消费校验结果触发告警
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ValidationRunService(BaseModuleService):
    MODULE = "validation_run"
    ENTITY = "校验批次"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["批次标识", "目标表", "校验分区", "总行数", "通过行数", "失败行数", "校验耗时"]
    REQUIRED_FIELDS = ["批次标识", "目标表", "校验分区"]
    STATUS_ORDER = ["校验中", "已通过", "未通过", "已阻断"]
    ACTION_RULES = {"确认通过": "校验中", "标记阻断": "未通过"}
    NEGATIVE_ACTIONS = []

    def query_by_table(self, table_name: str) -> list[dict[str, Any]]:
        """按目标表查询校验批次记录。

        从 store 中的校验批次数据里筛选指定表的批次，
        返回批次列表供质量趋势分析消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if table_name.casefold() in str(row.get("目标表", "")).casefold():
                result.append(dict(row))
        return result

    def compute_pass_rate(self, batch_id: str) -> dict[str, Any]:
        """计算校验批次的通过率。

        基于批次记录中的总行数和通过行数字段，
        计算通过率、失败率等核心质量指标。
        """
        target = None
        for row in store.rows(self.MODULE):
            if row.get("批次标识") == batch_id:
                target = row
                break
        if target is None:
            return {"error": f"批次 {batch_id} 不存在"}

        total = int(target.get("总行数", "0"))
        passed = int(target.get("通过行数", "0"))
        failed = int(target.get("失败行数", "0"))
        pass_rate = round(passed / max(total, 1) * 100, 2)
        fail_rate = round(failed / max(total, 1) * 100, 2)

        return {
            "batch_id": batch_id,
            "table": target.get("目标表"),
            "partition": target.get("校验分区"),
            "total_rows": total,
            "passed_rows": passed,
            "failed_rows": failed,
            "pass_rate_pct": pass_rate,
            "fail_rate_pct": fail_rate,
            "status": "通过" if pass_rate >= 99.9 else "需关注",
            "computed_at": datetime.now().isoformat(),
        }

    def aggregate_by_table(self) -> dict[str, Any]:
        """按目标表汇总校验批次统计。

        汇总所有校验批次数据，产出各表的质量指标总览，
        包括平均通过率和异常批次数量。
        """
        rows = store.rows(self.MODULE)
        table_stats: dict[str, dict[str, int]] = {}
        for row in rows:
            table = str(row.get("目标表", "未知"))
            if table not in table_stats:
                table_stats[table] = {"批次总数": 0, "通过批次": 0, "失败批次": 0, "阻断批次": 0,
                                       "累计总行": 0, "累计通过": 0, "累计失败": 0}
            stats = table_stats[table]
            stats["批次总数"] += 1
            status = str(row.get("status", ""))
            if status == "已通过":
                stats["通过批次"] += 1
            elif status == "未通过":
                stats["失败批次"] += 1
            elif status == "已阻断":
                stats["阻断批次"] += 1
            stats["累计总行"] += int(row.get("总行数", "0"))
            stats["累计通过"] += int(row.get("通过行数", "0"))
            stats["累计失败"] += int(row.get("失败行数", "0"))

        return {
            "tables": len(table_stats),
            "table_stats": {
                t: {
                    **s,
                    "通过率": round(s["累计通过"] / max(s["累计总行"], 1) * 100, 1),
                }
                for t, s in table_stats.items()
            },
            "snapshot_at": datetime.now().isoformat(),
        }

    def detect_degradation(self, table_name: str, threshold_pct: float = 95.0) -> dict[str, Any]:
        """检测数据质量退化趋势。

        对比最新两个校验批次的通过率，当通过率下降到阈值以下时
        标记质量退化并产出告警建议。
        """
        batches = self.query_by_table(table_name)
        if len(batches) < 2:
            return {"table": table_name, "degradation": "数据不足", "batch_count": len(batches)}

        sorted_batches = sorted(
            batches, key=lambda b: str(b.get("批次标识", "")), reverse=True
        )
        current = sorted_batches[0]
        previous = sorted_batches[1]

        cur_pass = int(current.get("通过行数", "0"))
        cur_total = int(current.get("总行数", "1"))
        prev_pass = int(previous.get("通过行数", "0"))
        prev_total = int(previous.get("总行数", "1"))

        cur_rate = round(cur_pass / max(cur_total, 1) * 100, 2)
        prev_rate = round(prev_pass / max(prev_total, 1) * 100, 2)
        delta = round(cur_rate - prev_rate, 2)

        is_degraded = cur_rate < threshold_pct and delta < 0
        return {
            "table": table_name,
            "current_pass_rate": cur_rate,
            "previous_pass_rate": prev_rate,
            "delta_pct": delta,
            "degraded": is_degraded,
            "threshold_pct": threshold_pct,
            "alert_recommendation": "建议触发质量告警" if is_degraded else "质量正常",
            "computed_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg