"""计算资源池领域服务：管理资源分配、负载统计和弹性扩缩策略。

上游：task_node 产出节点配置 → resource_pool 分配计算资源
消费：cost_analysis 消费资源使用数据计算费用
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResourcePoolService(BaseModuleService):
    MODULE = "resource_pool"
    ENTITY = "计算资源"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["池名称", "总核数", "总内存", "已分配核数", "已分配内存", "最大并发", "调度策略"]
    REQUIRED_FIELDS = ["池名称", "总核数", "总内存"]
    STATUS_ORDER = ["正常", "高负载", "已满", "已停用"]
    ACTION_RULES = {"标记高负载": "正常", "标记已满": "高负载", "停用池": "正常"}
    NEGATIVE_ACTIONS = ["停用池"]

    def compute_utilization(self, pool_name: str) -> dict[str, Any]:
        """计算资源池利用率。

        根据已分配核数和总核数、已分配内存和总内存，
        计算核数使用率和内存使用率。
        """
        pool = None
        for row in store.rows(self.MODULE):
            if row.get("池名称") == pool_name:
                pool = row
                break
        if pool is None:
            return {"error": f"资源池 {pool_name} 不存在"}

        try:
            total_cores = float(pool.get("总核数", "0"))
            alloc_cores = float(pool.get("已分配核数", "0"))
            total_mem = float(pool.get("总内存", "0"))
            alloc_mem = float(pool.get("已分配内存", "0"))
            max_conc = int(pool.get("最大并发", "0"))
        except ValueError:
            return {"error": "资源数据格式异常"}

        core_util = round(alloc_cores / max(total_cores, 1) * 100, 1)
        mem_util = round(alloc_mem / max(total_mem, 1) * 100, 1)

        return {
            "pool": pool_name,
            "total_cores": total_cores,
            "allocated_cores": alloc_cores,
            "core_utilization_pct": core_util,
            "total_memory_gb": total_mem,
            "allocated_memory_gb": alloc_mem,
            "memory_utilization_pct": mem_util,
            "max_concurrency": max_conc,
            "remaining_concurrency": max_conc - alloc_cores,
            "computed_at": datetime.now().isoformat(),
        }

    def allocate_resources(self, pool_name: str, cores: float, memory_gb: float) -> dict[str, Any]:
        """从资源池分配计算资源。

        尝试从指定池分配核数和内存，检查资源配额是否充足，
        成功时更新已分配字段并返回分配记录。
        """
        pool = None
        for row in store.rows(self.MODULE):
            if row.get("池名称") == pool_name:
                pool = row
                break
        if pool is None:
            return {"error": f"资源池 {pool_name} 不存在"}

        try:
            total_cores = float(pool.get("总核数", "0"))
            alloc_cores = float(pool.get("已分配核数", "0"))
            total_mem = float(pool.get("总内存", "0"))
            alloc_mem = float(pool.get("已分配内存", "0"))
        except ValueError:
            return {"error": "资源数据格式异常"}

        remaining_cores = total_cores - alloc_cores
        remaining_mem = total_mem - alloc_mem

        if cores > remaining_cores:
            return {"error": f"核数不足：需要 {cores}，剩余 {remaining_cores}", "allocated": False}
        if memory_gb > remaining_mem:
            return {"error": f"内存不足：需要 {memory_gb}GB，剩余 {remaining_mem}GB", "allocated": False}

        pool["已分配核数"] = str(alloc_cores + cores)
        pool["已分配内存"] = str(alloc_mem + memory_gb)

        new_util = round((alloc_cores + cores) / max(total_cores, 1) * 100, 1)
        if new_util >= 90:
            pool["status"] = "已满"
        elif new_util >= 70:
            pool["status"] = "高负载"

        return {
            "pool": pool_name,
            "allocated": True,
            "allocated_cores": cores,
            "allocated_memory_gb": memory_gb,
            "new_core_utilization_pct": new_util,
            "remaining_cores": round(remaining_cores - cores, 1),
            "remaining_memory_gb": round(remaining_mem - memory_gb, 1),
            "allocated_at": datetime.now().isoformat(),
        }

    def aggregate_cluster_usage(self) -> dict[str, Any]:
        """汇总集群资源使用全景。

        遍历 store 中所有资源池记录，统计集群总核数、
        总分配和可用资源，产出容量规划数据。
        """
        rows = store.rows(self.MODULE)
        total_cores = 0.0
        total_mem = 0.0
        alloc_cores = 0.0
        alloc_mem = 0.0
        pool_count = len(rows)

        for row in rows:
            try:
                total_cores += float(row.get("总核数", "0"))
                total_mem += float(row.get("总内存", "0"))
                alloc_cores += float(row.get("已分配核数", "0"))
                alloc_mem += float(row.get("已分配内存", "0"))
            except ValueError:
                pass

        return {
            "pool_count": pool_count,
            "total_cores": total_cores,
            "total_memory_gb": total_mem,
            "allocated_cores": alloc_cores,
            "allocated_memory_gb": alloc_mem,
            "remaining_cores": round(total_cores - alloc_cores, 1),
            "remaining_memory_gb": round(total_mem - alloc_mem, 1),
            "core_utilization_pct": round(alloc_cores / max(total_cores, 1) * 100, 1),
            "memory_utilization_pct": round(alloc_mem / max(total_mem, 1) * 100, 1),
            "snapshot_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg