"""滑动窗口领域服务：管理数据流的分区窗口切分、水位推进和迟到策略。

上游：task_node 产出节点配置 → sliding_window 进行窗口切分
消费：watermark_tracker 消费窗口水位事件推进整体水位
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SlidingWindowService(BaseModuleService):
    MODULE = "sliding_window"
    ENTITY = "窗口配置"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["窗口名称", "目标表", "窗口字段", "窗口大小", "滑动步长", "水印延迟", "迟到策略"]
    REQUIRED_FIELDS = ["窗口名称", "目标表", "窗口字段"]
    STATUS_ORDER = ["草稿", "已启用", "已过期", "已停用"]
    ACTION_RULES = {"启用窗口": "草稿", "标记过期": "已启用", "停用窗口": "已启用"}
    NEGATIVE_ACTIONS = ["停用窗口"]

    def query_by_table(self, table_name: str) -> list[dict[str, Any]]:
        """按目标表查询窗口配置。

        从 store 中筛选指定表的窗口分区配置记录，
        返回窗口列表供分区扫描和数据写入消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if table_name.casefold() in str(row.get("目标表", "")).casefold():
                result.append(dict(row))
        return result

    def advance_watermark(self, window_name: str, event_time: str) -> dict[str, Any]:
        """推进滑动窗口水位。

        根据事件时间推进窗口水位位置，计算水位覆盖范围，
        供 watermark_tracker 记录消费和延迟监控。
        """
        window = None
        for row in store.rows(self.MODULE):
            if row.get("窗口名称") == window_name:
                window = row
                break
        if window is None:
            return {"error": f"窗口 {window_name} 不存在"}

        window_size_str = str(window.get("窗口大小", "3600"))
        step_str = str(window.get("滑动步长", "1800"))
        delay_str = str(window.get("水印延迟", "300"))

        try:
            window_size = int(window_size_str)
            step = int(step_str)
            delay = int(delay_str)
        except ValueError:
            window_size = 3600
            step = 1800
            delay = 300

        try:
            dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            dt = datetime.now()

        waterm_ts = dt - timedelta(seconds=delay)
        window_end = waterm_ts + timedelta(seconds=window_size)

        previous_watermark = window.get("_last_watermark", "")
        window["_last_watermark"] = event_time

        return {
            "window": window_name,
            "event_time": event_time,
            "waterm_timestamp": waterm_ts.isoformat(),
            "window_end": window_end.isoformat(),
            "window_size_sec": window_size,
            "step_sec": step,
            "delay_sec": delay,
            "previous_watermark": previous_watermark,
            "advanced_at": datetime.now().isoformat(),
        }

    def compute_partition_range(self, window_name: str) -> dict[str, Any]:
        """计算窗口覆盖的分区范围。

        根据窗口大小和当前水位推算需要扫描的数据分区列表，
        用于 partition_scan 和回填任务的分区范围判定。
        """
        window = None
        for row in store.rows(self.MODULE):
            if row.get("窗口名称") == window_name:
                window = row
                break
        if window is None:
            return {"error": f"窗口 {window_name} 不存在"}

        window_field = str(window.get("窗口字段", "时间戳"))
        window_size = int(window.get("窗口大小", "3600"))
        late_strategy = str(window.get("迟到策略", "丢弃"))

        now = datetime.now()
        current_start = now - timedelta(seconds=window_size)
        partitions = []
        for i in range(max(window_size // 3600, 1)):
            p_start = current_start + timedelta(hours=i)
            p_end = p_start + timedelta(hours=1)
            partitions.append({
                "partition_key": p_start.strftime("%Y%m%d%H"),
                "start": p_start.isoformat(),
                "end": p_end.isoformat(),
            })

        return {
            "window": window_name,
            "field": window_field,
            "window_size_sec": window_size,
            "late_strategy": late_strategy,
            "partition_count": len(partitions),
            "partitions": partitions,
            "computed_at": now.isoformat(),
        }

    def detect_late_events(self, window_name: str, max_late_sec: int = 3600) -> dict[str, Any]:
        """检测迟到事件。

        检查 watermark_tracker 中的水位记录，判断事件是否在窗口关闭后到达，
        统计迟到事件数量并产出迟报数据详情。
        """
        waterm_rows = store.rows("watermark_tracker") if "watermark_tracker" in store._tables else []
        late_events: list[dict[str, Any]] = []
        now = datetime.now()
        cutoff = now - timedelta(seconds=max_late_sec)

        for wr in waterm_rows:
            try:
                wt = datetime.fromisoformat(str(wr.get("调度时间", "")).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            if wt < cutoff:
                late_events.append({
                    "pipeline": wr.get("管线标识"),
                    "event_time": str(wr.get("调度时间")),
                    "late_sec": (now - wt).total_seconds(),
                    "current_waterm": wr.get("当前水位"),
                })

        return {
            "window": window_name,
            "cutoff_sec": max_late_sec,
            "late_event_count": len(late_events),
            "late_events": late_events,
            "detected_at": now.isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg