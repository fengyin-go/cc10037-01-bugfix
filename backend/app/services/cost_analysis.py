"""成本分析领域服务：管理管线资源消耗快照、费用趋势预测和同比分析。

上游：resource_pool 产出资源使用 → cost_analysis 计算费用
消费：告警策略和容量规划面板消费成本数据
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CostAnalysisService(BaseModuleService):
    MODULE = "cost_analysis"
    ENTITY = "成本快照"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["快照日期", "管线标识", "计算时长", "扫描数据量", "计算费用", "存储费用", "同比变化"]
    REQUIRED_FIELDS = ["快照日期", "管线标识", "计算时长"]
    STATUS_ORDER = ["估算", "已确认", "已结算"]
    ACTION_RULES = {"确认费用": "估算", "完成结算": "已确认"}
    NEGATIVE_ACTIONS = []

    def query_by_pipeline(self, pipeline_name: str) -> list[dict[str, Any]]:
        """按管线标识查询成本快照。

        从 store 中筛选指定管线的费用记录，
        返回成本数据供趋势分析消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if pipeline_name.casefold() in str(row.get("管线标识", "")).casefold():
                result.append(dict(row))
        return result

    def aggregate_pipeline_cost(self, pipeline_name: str) -> dict[str, Any]:
        """汇总管线的历史成本。

        统计指定管线所有快照的计算费用和存储费用总和，
        计算日均和月均费用供预算规划。
        """
        snapshots = self.query_by_pipeline(pipeline_name)
        total_compute = 0.0
        total_storage = 0.0
        total_scan = 0.0

        for snap in snapshots:
            try:
                total_compute += float(snap.get("计算费用", "0"))
                total_storage += float(snap.get("存储费用", "0"))
                total_scan += float(snap.get("扫描数据量", "0"))
            except ValueError:
                pass

        count = max(len(snapshots), 1)
        return {
            "pipeline": pipeline_name,
            "snapshot_count": len(snapshots),
            "total_compute_cost": round(total_compute, 2),
            "total_storage_cost": round(total_storage, 2),
            "total_cost": round(total_compute + total_storage, 2),
            "avg_compute_per_snapshot": round(total_compute / count, 2),
            "avg_storage_per_snapshot": round(total_storage / count, 2),
            "total_scan_data_gb": round(total_scan, 1),
            "snapshot_at": datetime.now().isoformat(),
        }

    def detect_cost_anomalies(self, threshold_factor: float = 2.0) -> list[dict[str, Any]]:
        """检测费用异常波动。

        对比各管线最近两个快照的费用变化，
        当环比增长超过阈值因子时标记为异常。
        """
        rows = store.rows(self.MODULE)
        pipeline_groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            pipeline = str(row.get("管线标识", ""))
            pipeline_groups.setdefault(pipeline, []).append(row)

        anomalies: list[dict[str, Any]] = []
        for pipeline, snaps in pipeline_groups.items():
            if len(snaps) < 2:
                continue
            sorted_snaps = sorted(
                snaps,
                key=lambda s: str(s.get("快照日期", "")),
                reverse=True,
            )
            try:
                latest_cost = float(sorted_snaps[0].get("计算费用", "0")) + \
                              float(sorted_snaps[0].get("存储费用", "0"))
                prev_cost = float(sorted_snaps[1].get("计算费用", "0")) + \
                            float(sorted_snaps[1].get("存储费用", "0"))
            except ValueError:
                continue

            if prev_cost > 0 and latest_cost / prev_cost >= threshold_factor:
                anomalies.append({
                    "pipeline": pipeline,
                    "latest_cost": latest_cost,
                    "previous_cost": prev_cost,
                    "ratio": round(latest_cost / max(prev_cost, 0.01), 2),
                    "change_pct": round((latest_cost - prev_cost) / max(prev_cost, 0.01) * 100, 1),
                    "level": "严重" if latest_cost / max(prev_cost, 0.01) >= 3 else "关注",
                })

        return anomalies

    def forecast_monthly_cost(self) -> dict[str, Any]:
        """预测月度总费用。

        基于所有管线最近快照的日平均费用推估月度成本，
        产出预测数据供预算审批消费。
        """
        rows = store.rows(self.MODULE)
        total_daily = 0.0
        pipeline_count = 0

        for row in rows:
            try:
                total_daily += float(row.get("计算费用", "0"))
                total_daily += float(row.get("存储费用", "0"))
                pipeline_count += 1
            except ValueError:
                pass

        avg_daily = total_daily / max(pipeline_count, 1)
        forecast_monthly = avg_daily * 30

        return {
            "total_pipeline_count": pipeline_count,
            "avg_daily_cost_per_pipeline": round(avg_daily, 2),
            "total_daily_observed": round(total_daily, 2),
            "forecast_monthly_cost": round(forecast_monthly, 2),
            "forecast_quarterly_cost": round(forecast_monthly * 3, 2),
            "forecast_annual_cost": round(forecast_monthly * 12, 2),
            "snapshot_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg