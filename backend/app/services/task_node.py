"""处理节点领域服务：管理管线中的节点配置、依赖分析和批次查询。

上游：pipeline_def 产出管线定义 → task_node 配置处理节点
消费：depend_scheduler 根据节点依赖触发调度
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TaskNodeService(BaseModuleService):
    MODULE = "task_node"
    ENTITY = "处理节点"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["节点名称", "所属管线", "节点类型", "处理逻辑", "依赖上游", "下游节点", "重试策略"]
    REQUIRED_FIELDS = ["节点名称", "所属管线", "节点类型"]
    STATUS_ORDER = ["草稿", "已配置", "已启用", "已停用"]
    ACTION_RULES = {"确认配置": "草稿", "启用节点": "已配置", "停用节点": "已启用"}
    NEGATIVE_ACTIONS = ["停用节点"]

    def query_by_pipeline(self, pipeline_name: str) -> list[dict[str, Any]]:
        """按所属管线查询处理节点。

        遍历 store 中存储的节点记录，筛选属于指定管线的节点，
        返回节点批次数据供编排视图渲染。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if pipeline_name.casefold() in str(row.get("所属管线", "")).casefold():
                result.append(dict(row))
        return result

    def analyze_dependencies(self, node_name: str) -> dict[str, Any]:
        """分析节点上下游依赖关系。

        根据节点的依赖上游和下游节点字段构建 DAG 拓扑片段，
        用于依赖调度器判断下游数据管道是否可触发。
        """
        node = store.find(self.MODULE, int(node_name) if node_name.isdigit() else 0)
        if node is None:
            for row in store.rows(self.MODULE):
                if row.get("节点名称") == node_name:
                    node = row
                    break
        if node is None:
            return {"error": f"节点 {node_name} 不存在"}

        upstream_raw = str(node.get("依赖上游", ""))
        downstream_raw = str(node.get("下游节点", ""))
        upstream_list = [u.strip() for u in upstream_raw.split(",") if u.strip()]
        downstream_list = [d.strip() for d in downstream_raw.split(",") if d.strip()]

        upstream_status: dict[str, str] = {}
        for up in upstream_list:
            found = False
            for row in store.rows(self.MODULE):
                if row.get("节点名称") == up:
                    upstream_status[up] = row.get("status", "未知")
                    found = True
                    break
            if not found:
                upstream_status[up] = "未注册"

        return {
            "node": node.get("节点名称"),
            "pipeline": node.get("所属管线"),
            "upstream": upstream_list,
            "upstream_status": upstream_status,
            "downstream": downstream_list,
            "all_upstream_ready": all(
                s in ("已启用", "已停用") for s in upstream_status.values()
            ) if upstream_status else True,
            "analyzed_at": datetime.now().isoformat(),
        }

    def compute_retry_stats(self, node_name: str) -> dict[str, Any]:
        """统计节点的重试执行情况。

        基于 store 中的批次日志记录（通过 batch_log 存储），
        统计该节点的重试次数、成功率和平均延迟。
        """
        batch_rows = store.rows("batch_log") if "batch_log" in store._tables else []
        node_batches = [
            r for r in batch_rows
            if r.get("节点名称") == node_name
        ]

        total = len(node_batches)
        retried = sum(1 for r in node_batches if int(r.get("重试次数", "0")) > 0)
        succeeded = sum(1 for r in node_batches if r.get("结果") == "成功")
        total_retries = sum(int(r.get("重试次数", "0")) for r in node_batches)

        return {
            "node": node_name,
            "total_batches": total,
            "retried_batches": retried,
            "retry_rate_pct": round(retried / max(total, 1) * 100, 1),
            "total_retries": total_retries,
            "success_rate_pct": round(succeeded / max(total, 1) * 100, 1),
            "snapshot_at": datetime.now().isoformat(),
        }

    def find_downstream_impact(self, node_name: str) -> list[dict[str, Any]]:
        """查找节点变更的下游影响范围。

        递归遍历所有以当前节点为上游的节点，生成影响分析列表，
        用于变更窗口的风险评估和回滚计划。
        """
        result: list[dict[str, Any]] = []
        visited: set[str] = {node_name}

        def _traverse(current: str, depth: int):
            if depth > 10:
                return
            for row in store.rows(self.MODULE):
                if current in str(row.get("依赖上游", "")):
                    name = row.get("节点名称", "")
                    if name and name not in visited:
                        visited.add(name)
                        result.append({
                            "node": name,
                            "pipeline": row.get("所属管线"),
                            "depth": depth,
                            "status": row.get("status"),
                        })
                        _traverse(name, depth + 1)

        _traverse(node_name, 1)
        return result

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg