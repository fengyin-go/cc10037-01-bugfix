"""配置迁移领域服务：管理跨环境管线配置的导出、导入和差异对比。

上游：pipeline_def + task_node 产出配置快照 → config_migration 迁移
消费：backfill_tracker 消费迁移进度用于回填状态追踪
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ConfigMigrationService(BaseModuleService):
    MODULE = "config_migration"
    ENTITY = "迁移任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["任务名称", "源环境", "目标环境", "迁移范围", "依赖检查", "回滚方案", "执行窗口"]
    REQUIRED_FIELDS = ["任务名称", "源环境", "目标环境"]
    STATUS_ORDER = ["待迁移", "迁移中", "已完成", "已回滚", "迁移失败"]
    ACTION_RULES = {"开始迁移": "待迁移", "完成迁移": "迁移中", "回滚迁移": "迁移中", "标记失败": "迁移中"}
    NEGATIVE_ACTIONS = ["回滚迁移"]

    def query_by_environment(self, env: str) -> list[dict[str, Any]]:
        """按环境查询迁移任务。

        筛选指定源环境或目标环境的迁移记录，
        返回任务列表供环境运维面板消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if env.casefold() in str(row.get("源环境", "")).casefold() or \
               env.casefold() in str(row.get("目标环境", "")).casefold():
                result.append(dict(row))
        return result

    def check_dependencies(self, task_name: str) -> dict[str, Any]:
        """检查迁移任务的依赖是否满足。

        根据依赖检查字段中的配置项，在 store 中查找对应的管线配置
        判断依赖是否准备就绪。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return {"error": f"迁移任务 {task_name} 不存在"}

        dep_str = str(task.get("依赖检查", ""))
        deps = [d.strip() for d in dep_str.split(",") if d.strip()]
        dep_status: dict[str, bool] = {}

        for dep in deps:
            found = False
            for pipeline_row in store.rows("pipeline_def"):
                if dep.casefold() in str(pipeline_row.get("流水线名称", "")).casefold():
                    dep_status[dep] = pipeline_row.get("status") == "已上线"
                    found = True
                    break
            if not found:
                dep_status[dep] = False

        all_met = all(dep_status.values()) if dep_status else True
        return {
            "task": task_name,
            "source_env": task.get("源环境"),
            "target_env": task.get("目标环境"),
            "dependencies": dep_status,
            "all_dependencies_met": all_met,
            "can_migrate": all_met and task.get("status") == "待迁移",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_diff_summary(self, task_name: str) -> dict[str, Any]:
        """计算迁移范围的差异摘要。

        对比源和目标环境中的管线配置数量差异，
        产出迁移影响评估数据供审批参考。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return {"error": f"迁移任务 {task_name} 不存在"}

        source_env = str(task.get("源环境", ""))
        target_env = str(task.get("目标环境", ""))

        pipeline_rows = store.rows("pipeline_def")
        task_rows = store.rows("task_node")
        rule_rows = store.rows("quality_rule")

        source_pipelines = len(pipeline_rows)
        source_nodes = len(task_rows)
        source_rules = len(rule_rows)

        return {
            "task": task_name,
            "source_env": source_env,
            "target_env": target_env,
            "items_to_migrate": {
                "pipelines": source_pipelines,
                "task_nodes": source_nodes,
                "quality_rules": source_rules,
            },
            "total_items": source_pipelines + source_nodes + source_rules,
            "rollback_plan": task.get("回滚方案", "未配置"),
            "execution_window": task.get("执行窗口", "未设定"),
            "summary_at": datetime.now().isoformat(),
        }

    def validate_rollback_readiness(self, task_name: str) -> dict[str, Any]:
        """验证回滚准备状态。

        检查回滚方案是否已配置和最近迁移历史，
        确保迁移失败时能安全回滚。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return {"error": f"迁移任务 {task_name} 不存在"}

        rollback = str(task.get("回滚方案", ""))
        has_rollback = len(rollback) > 0 and rollback != "未配置"
        recent_rollbacks = [
            r for r in store.rows(self.MODULE)
            if r.get("status") == "已回滚" and r.get("任务名称") != task_name
        ]

        return {
            "task": task_name,
            "rollback_configured": has_rollback,
            "rollback_plan": rollback,
            "recent_rollback_count": len(recent_rollbacks),
            "safe_to_migrate": has_rollback,
            "validated_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg