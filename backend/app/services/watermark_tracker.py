"""水位追踪领域服务：管理管线分区水位推进、延迟计算和调度时间记录。

上游：sliding_window 产出窗口水位 → watermark_tracker 追踪水位
消费：alert_policy 消费延迟数据触发告警
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class WatermarkTrackerService(BaseModuleService):
    MODULE = "watermark_tracker"
    ENTITY = "水位记录"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["管线标识", "分区字段", "当前水位", "调度时间", "延迟分钟", "上次推进时间", "水位状态"]
    REQUIRED_FIELDS = ["管线标识", "分区字段", "当前水位"]
    STATUS_ORDER = ["正常", "延迟", "严重延迟", "已暂停"]
    ACTION_RULES = {"标记延迟": "正常", "标记严重": "延迟", "暂停追踪": "延迟"}
    NEGATIVE_ACTIONS = []

    def advance_watermark(self, pipeline_id: str, new_watermark: str) -> dict[str, Any]:
        """推进管线水位。

        更新管线当前水位值和上次推进时间，
        计算延迟分钟并更新水位状态。
        """
        target = None
        for row in store.rows(self.MODULE):
            if row.get("管线标识") == pipeline_id:
                target = row
                break

        if target is None:
            return {"error": f"管线 {pipeline_id} 的水位记录不存在"}

        previous = target.get("当前水位", "")
        target["当前水位"] = new_watermark
        target["上次推进时间"] = datetime.now().isoformat()
        target["调度时间"] = datetime.now().isoformat()

        try:
            prev_dt = datetime.fromisoformat(previous.replace("Z", "+00:00"))
            new_dt = datetime.fromisoformat(new_watermark.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            prev_dt = datetime.now()
            new_dt = datetime.now()

        delay_min = round((new_dt - prev_dt).total_seconds() / 60, 1)
        target["延迟分钟"] = str(max(delay_min, 0))

        if delay_min >= 60:
            target["status"] = "严重延迟"
        elif delay_min >= 15:
            target["status"] = "延迟"
        else:
            target["status"] = "正常"

        return {
            "pipeline": pipeline_id,
            "previous_watermark": previous,
            "new_watermark": new_watermark,
            "delay_minutes": delay_min,
            "status": target["status"],
            "advanced_at": datetime.now().isoformat(),
        }

    def compute_lag_distribution(self) -> dict[str, Any]:
        """计算水位延迟分布。

        统计所有管线记录的延迟分钟分布，
        产出延迟概览供运维调度面板消费。
        """
        rows = store.rows(self.MODULE)
        normal = 0
        delayed = 0
        severe = 0
        total_delay = 0.0

        for row in rows:
            status = row.get("status", "正常")
            if status == "正常":
                normal += 1
            elif status == "严重延迟":
                severe += 1
            else:
                delayed += 1
            try:
                total_delay += float(row.get("延迟分钟", "0"))
            except ValueError:
                pass

        return {
            "total_pipelines": len(rows),
            "normal": normal,
            "delayed": delayed,
            "severe": severe,
            "avg_delay_min": round(total_delay / max(len(rows), 1), 1),
            "max_delay_min": max(
                (float(r.get("延迟分钟", "0")) for r in rows if r.get("延迟分钟", "0")),
                default=0
            ),
            "snapshot_at": datetime.now().isoformat(),
        }

    def find_stalled_pipelines(self, stall_threshold_min: int = 120) -> list[dict[str, Any]]:
        """找出停滞的管线管道。

        检测超过阈值未推进水位的管线，识别管道停滞风险，
        返回停滞管线列表供告警策略消费。
        """
        rows = store.rows(self.MODULE)
        stalled: list[dict[str, Any]] = []
        now = datetime.now()

        for row in rows:
            last_advance = str(row.get("上次推进时间", ""))
            try:
                last_dt = datetime.fromisoformat(last_advance.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            idle_min = (now - last_dt).total_seconds() / 60
            if idle_min >= stall_threshold_min:
                stalled.append({
                    "pipeline": row.get("管线标识"),
                    "current_waterm": row.get("当前水位"),
                    "idle_minutes": round(idle_min, 1),
                    "last_advance": last_advance,
                    "status": row.get("status"),
                })

        return stalled

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg