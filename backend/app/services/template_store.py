"""处理模板领域服务：管理数据处理模板的版本控制、参数校验和实例化。

上游：task_node 消费模板定义 → template_store 提供模板版本
消费：pipeline_def 引用模板构建管线处理逻辑
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TemplateStoreService(BaseModuleService):
    MODULE = "template_store"
    ENTITY = "处理模板"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["模板名称", "处理类型", "输入Schema", "输出Schema", "参数列表", "版本号", "使用次数"]
    REQUIRED_FIELDS = ["模板名称", "处理类型", "输入Schema"]
    STATUS_ORDER = ["草稿", "已发布", "已弃用"]
    ACTION_RULES = {"发布模板": "草稿", "弃用模板": "已发布"}
    NEGATIVE_ACTIONS = []

    def query_by_type(self, process_type: str) -> list[dict[str, Any]]:
        """按处理类型查询模板。

        从 store 中筛选指定类型的处理模板，
        返回模板列表供管线构建时选择消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if process_type.casefold() in str(row.get("处理类型", "")).casefold():
                result.append(dict(row))
        return result

    def validate_template(self, template_name: str, params: dict[str, Any]) -> dict[str, Any]:
        """校验模板参数合法性。

        根据模板定义的参数列表，校验传入参数是否满足要求，
        返回校验结果和缺失字段列表。
        """
        template = None
        for row in store.rows(self.MODULE):
            if row.get("模板名称") == template_name:
                template = row
                break
        if template is None:
            return {"error": f"模板 {template_name} 不存在", "valid": False}

        param_list_str = str(template.get("参数列表", ""))
        expected_params = [
            p.strip() for p in param_list_str.split(",") if p.strip()
        ]
        missing: list[str] = []
        extra: list[str] = []

        for ep in expected_params:
            if ep not in params:
                missing.append(ep)
        for pk in params:
            if pk not in expected_params and expected_params:
                extra.append(pk)

        valid = len(missing) == 0
        return {
            "template": template_name,
            "version": template.get("版本号"),
            "expected_params": expected_params,
            "provided_params": list(params.keys()),
            "missing_params": missing,
            "extra_params": extra,
            "valid": valid,
            "validated_at": datetime.now().isoformat(),
        }

    def instantiate_template(self, template_name: str, params: dict[str, Any]) -> dict[str, Any]:
        """实例化处理模板。

        校验参数后产出具体的处理节点配置数据，
        包含输入/输出 Schema 映射和处理类型信息。
        """
        validation = self.validate_template(template_name, params)
        if not validation["valid"]:
            return {"error": "参数校验未通过", "validation": validation}

        template = None
        for row in store.rows(self.MODULE):
            if row.get("模板名称") == template_name:
                template = row
                break

        instance_id = f"{template_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        usage_count = int(template.get("使用次数", "0")) + 1
        template["使用次数"] = str(usage_count)

        return {
            "instance_id": instance_id,
            "template": template_name,
            "version": template.get("版本号"),
            "process_type": template.get("处理类型"),
            "input_schema": template.get("输入Schema"),
            "output_schema": template.get("输出Schema"),
            "params": params,
            "usage_count": usage_count,
            "created_at": datetime.now().isoformat(),
        }

    def get_most_used_templates(self, top_n: int = 10) -> list[dict[str, Any]]:
        """获取使用频率最高的模板。

        按使用次数字段排序，返回 top N 的热门模板，
        供推荐和优化参考。
        """
        rows = store.rows(self.MODULE)
        sorted_rows = sorted(
            rows,
            key=lambda r: int(r.get("使用次数", "0")),
            reverse=True,
        )
        result = []
        for row in sorted_rows[:top_n]:
            result.append({
                "name": row.get("模板名称"),
                "type": row.get("处理类型"),
                "version": row.get("版本号"),
                "usage_count": int(row.get("使用次数", "0")),
                "status": row.get("status"),
            })
        return result

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg