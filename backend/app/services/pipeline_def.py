"""流水线定义领域服务：管理ETL管线的编排、查询和分支合并。

上游：task_node 产出节点定义 → pipeline_def 进行管线编排
消费：depend_scheduler 消费管线定义触发调度执行
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class PipelineDefService(BaseModuleService):
    MODULE = "pipeline_def"
    ENTITY = "ETL流水线"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["流水线名称", "处理阶段", "输入源表", "输出目标表", "调度类型", "优先级", "超时分钟"]
    REQUIRED_FIELDS = ["流水线名称", "处理阶段", "输入源表"]
    STATUS_ORDER = ["草稿", "已编排", "已上线", "已下线"]
    ACTION_RULES = {"提交编排": "草稿", "上线管线": "已编排", "下线管线": "已上线"}
    NEGATIVE_ACTIONS = ["下线管线"]

    def query_by_stage(self, stage: str) -> list[dict[str, Any]]:
        """按处理阶段查询流水线。

        遍历 store 中已存储的管线记录，按处理阶段字段筛选，
        返回匹配的管线数据列表供排产消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if stage in str(row.get("处理阶段", "")):
                result.append(dict(row))
        return result

    def query_by_source_table(self, table_name: str) -> list[dict[str, Any]]:
        """按输入源表查询流水线。

        在数据血缘分析场景中，需要根据源表反查依赖的管线管道，
        返回所有使用该源表的下游管线记录。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if table_name.casefold() in str(row.get("输入源表", "")).casefold():
                result.append(dict(row))
        return result

    def compute_pipeline_summary(self) -> dict[str, Any]:
        """计算管线汇总统计。

        统计数据存储中所有管线的状态分布和源表/目标表映射，
        产出管线全景快照供容量规划和数据治理消费。
        """
        rows = store.rows(self.MODULE)
        by_status: dict[str, int] = {}
        source_tables: set[str] = set()
        target_tables: set[str] = set()
        total_timeout = 0

        for row in rows:
            status = row.get("status", "未知")
            by_status[status] = by_status.get(status, 0) + 1
            src = str(row.get("输入源表", ""))
            tgt = str(row.get("输出目标表", ""))
            if src:
                source_tables.add(src)
            if tgt:
                target_tables.add(tgt)
            try:
                total_timeout += int(row.get("超时分钟", "0"))
            except ValueError:
                pass

        return {
            "total_pipelines": len(rows),
            "by_status": by_status,
            "source_table_count": len(source_tables),
            "target_table_count": len(target_tables),
            "avg_timeout_min": round(total_timeout / max(len(rows), 1), 1),
            "snapshot_at": datetime.now().isoformat(),
        }

    def detect_pipeline_conflicts(self, pipeline_name: str) -> list[dict[str, Any]]:
        """检测管线冲突。

        检查是否有其他管线在同一源表上定义了重复的处理阶段，
        返回冲突管线列表供编排确认。
        """
        current_row = None
        for row in store.rows(self.MODULE):
            if row.get("流水线名称") == pipeline_name:
                current_row = row
                break
        if not current_row:
            return []

        conflicts: list[dict[str, Any]] = []
        src = str(current_row.get("输入源表", ""))
        stage = str(current_row.get("处理阶段", ""))
        for row in store.rows(self.MODULE):
            if row.get("流水线名称") == pipeline_name:
                continue
            if (src and src.casefold() in str(row.get("输入源表", "")).casefold()
                    and stage and stage.casefold() in str(row.get("处理阶段", "")).casefold()):
                conflicts.append({
                    "conflict_pipeline": row.get("流水线名称"),
                    "shared_source": src,
                    "conflict_stage": stage,
                })
        return conflicts

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg