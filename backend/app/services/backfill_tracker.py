"""回填追踪领域服务：管理数据回填进度、写入量统计和完成状态追踪。

上游：rerun_compensation 产生回刷任务 → backfill_tracker 追踪进度
消费：alert_policy 监视回填状态触发完成和失败告警
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BackfillTrackerService(BaseModuleService):
    MODULE = "backfill_tracker"
    ENTITY = "回填进度"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["追踪ID", "回填任务", "当前分区", "完成比例", "预估剩余", "数据写入量", "错误计数"]
    REQUIRED_FIELDS = ["追踪ID", "回填任务", "当前分区"]
    STATUS_ORDER = ["执行中", "已完成", "已暂停", "已失败"]
    ACTION_RULES = {"暂停回填": "执行中", "恢复回填": "已暂停", "标记完成": "执行中"}
    NEGATIVE_ACTIONS = []

    def update_progress(self, tracking_id: str, new_partition: str,
                        rows_written: int, errors: int = 0) -> dict[str, Any]:
        """更新回填进度。

        更新当前分区位置、数据写入量累计和错误计数，
        自动计算完成比例和预估剩余时间。
        """
        tracker = None
        for row in store.rows(self.MODULE):
            if row.get("追踪ID") == tracking_id:
                tracker = row
                break
        if tracker is None:
            return {"error": f"追踪记录 {tracking_id} 不存在"}

        previous_partition = tracker.get("当前分区", "")
        previous_written = int(tracker.get("数据写入量", "0"))
        previous_errors = int(tracker.get("错误计数", "0"))

        tracker["当前分区"] = new_partition
        tracker["数据写入量"] = str(previous_written + rows_written)
        tracker["错误计数"] = str(previous_errors + errors)

        try:
            current = int(new_partition)
            if len(new_partition) == 10:
                total_start_h = 2025010100
                total_end_h = 2025013123
            else:
                total_start_h = 0
                total_end_h = int(tracker.get("预估剩余", "100"))

            progress = min(round(current / max(total_end_h - total_start_h, 1) * 100, 1), 100)
            remaining = max(total_end_h - current, 0)
        except ValueError:
            progress = 0
            remaining = 100

        tracker["完成比例"] = str(progress)
        tracker["预估剩余"] = str(remaining)

        return {
            "tracking_id": tracking_id,
            "previous_partition": previous_partition,
            "current_partition": new_partition,
            "rows_written_this_batch": rows_written,
            "cumulative_rows": previous_written + rows_written,
            "cumulative_errors": previous_errors + errors,
            "progress_pct": progress,
            "remaining_partitions": remaining,
            "updated_at": datetime.now().isoformat(),
        }

    def query_by_task(self, task_name: str) -> list[dict[str, Any]]:
        """按回填任务查询追踪记录。

        从 store 中筛选指定任务的回填追踪数据，
        返回追踪列表供进度展示消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if task_name.casefold() in str(row.get("回填任务", "")).casefold():
                result.append(dict(row))
        return result

    def compute_throughput(self, task_name: str) -> dict[str, Any]:
        """计算回填吞吐量。

        统计指定任务所有追踪记录的写入量总和和错误率，
        产出吞吐量指标供容量规划消费。
        """
        trackers = self.query_by_task(task_name)
        total_rows = 0
        total_errors = 0
        completed = 0
        failed = 0

        for t in trackers:
            total_rows += int(t.get("数据写入量", "0"))
            total_errors += int(t.get("错误计数", "0"))
            status = t.get("status", "")
            if status == "已完成":
                completed += 1
            elif status == "已失败":
                failed += 1

        error_rate = round(total_errors / max(total_rows, 1) * 100, 2)
        return {
            "task": task_name,
            "total_trackers": len(trackers),
            "completed_trackers": completed,
            "failed_trackers": failed,
            "total_rows_written": total_rows,
            "total_errors": total_errors,
            "error_rate_pct": error_rate,
            "avg_rows_per_tracker": round(total_rows / max(len(trackers), 1), 0),
            "snapshot_at": datetime.now().isoformat(),
        }

    def check_completion_blockers(self, tracking_id: str) -> dict[str, Any]:
        """检查回填完成的阻塞因素。

        根据错误计数和当前状态判断回填是否被阻塞，
        产出阻塞分析供运维排障使用。
        """
        tracker = None
        for row in store.rows(self.MODULE):
            if row.get("追踪ID") == tracking_id:
                tracker = row
                break
        if tracker is None:
            return {"error": f"追踪记录 {tracking_id} 不存在"}

        errors = int(tracker.get("错误计数", "0"))
        status = tracker.get("status", "")
        progress = float(tracker.get("完成比例", "0"))
        written = int(tracker.get("数据写入量", "0"))

        blockers: list[str] = []
        if status == "已暂停":
            blockers.append("任务已被手动暂停")
        if errors > 100:
            blockers.append(f"错误次数过多({errors})")
        if progress > 80 and errors > 0 and written > 1000000:
            blockers.append("大数据量下的尾部错误风险")

        return {
            "tracking_id": tracking_id,
            "status": status,
            "progress_pct": progress,
            "errors": errors,
            "rows_written": written,
            "blocked": len(blockers) > 0,
            "blockers": blockers,
            "checked_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg