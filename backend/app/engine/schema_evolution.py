"""Schema变更兼容性评估引擎。

评估上游表Schema变更对下游ETL管线的影响：
- 字段增删改的兼容性判断
- 受影响管线的遍历
- 回滚方案可行性评估
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ChangeType(str, Enum):
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    MODIFY_TYPE = "MODIFY_TYPE"
    RENAME_COLUMN = "RENAME_COLUMN"
    ADD_PARTITION = "ADD_PARTITION"
    DROP_PARTITION = "DROP_PARTITION"


class Compatibility(str, Enum):
    COMPATIBLE = "兼容"
    INCOMPATIBLE = "不兼容"
    PARTIALLY = "部分兼容"
    PENDING = "待评估"


@dataclass
class SchemaColumn:
    """表的一个字段定义。"""
    name: str
    data_type: str           # string / int / bigint / float / double / decimal / date / timestamp
    nullable: bool = True
    is_partition: bool = False
    comment: str = ""


@dataclass
class SchemaDefinition:
    """一张表的完整Schema定义。"""
    table_name: str
    columns: list[SchemaColumn]
    partition_columns: list[str] = field(default_factory=list)


@dataclass
class SchemaChange:
    """一次Schema变更记录。"""
    change_id: str
    target_table: str
    change_type: ChangeType
    before: SchemaDefinition | None = None   # 变更前
    after: SchemaDefinition | None = None   # 变更后
    detail: dict[str, Any] = field(default_factory=dict)
    compatibility: Compatibility = Compatibility.PENDING
    affected_pipelines: list[str] = field(default_factory=list)
    rollback_plan: str = ""


class SchemaEvolutionEngine:
    """Schema演进兼容性评估引擎。

    用法:
        eng = SchemaEvolutionEngine()
        old_schema = SchemaDefinition("dw.dwd_order", [...])
        new_schema = SchemaDefinition("dw.dwd_order", [...])
        change = eng.evaluate_change(
            "chg_001", old_schema, new_schema,
            pipeline_deps={"dw.dwd_order": ["p_etl_order", "p_rpt_sales"]}
        )
    """

    # 类型兼容矩阵：from_type -> {to_type: compatible?}
    _TYPE_COMPAT: dict[str, set[str]] = {
        "string": {"string", "text"},
        "text": {"string", "text"},
        "int": {"int", "bigint", "float", "double", "decimal", "string"},
        "bigint": {"bigint", "float", "double", "decimal", "string"},
        "float": {"float", "double", "decimal", "string"},
        "double": {"double", "decimal", "string"},
        "decimal": {"decimal", "string"},
        "date": {"date", "timestamp", "string"},
        "timestamp": {"timestamp", "string"},
        "boolean": {"boolean", "string"},
    }

    def evaluate_change(
        self,
        change_id: str,
        before: SchemaDefinition,
        after: SchemaDefinition,
        pipeline_deps: dict[str, list[str]] | None = None,
    ) -> SchemaChange:
        """评估一次完整Schema变更的兼容性。"""
        pipeline_deps = pipeline_deps or {}
        changes_detail: list[dict[str, Any]] = []
        is_compatible = True
        has_incompatible = False

        before_cols = {c.name: c for c in before.columns}
        after_cols = {c.name: c for c in after.columns}

        # 检测新增列
        for name in after_cols:
            if name not in before_cols:
                changes_detail.append({
                    "type": ChangeType.ADD_COLUMN.value,
                    "column": name,
                    "data_type": after_cols[name].data_type,
                    "compatible": True,
                    "reason": "新增列通常向后兼容（除非SELECT *算子）",
                })

        # 检测删除列
        for name in before_cols:
            if name not in after_cols:
                has_incompatible = True
                is_compatible = False
                changes_detail.append({
                    "type": ChangeType.DROP_COLUMN.value,
                    "column": name,
                    "data_type": before_cols[name].data_type,
                    "compatible": False,
                    "reason": "删除列会导致下游SELECT引用该列的查询失败",
                })

        # 检测类型变更
        for name in before_cols:
            if name in after_cols:
                old_type = before_cols[name].data_type
                new_type = after_cols[name].data_type
                if old_type != new_type:
                    compat = new_type in self._TYPE_COMPAT.get(old_type, set())
                    if not compat:
                        has_incompatible = True
                        is_compatible = False
                    changes_detail.append({
                        "type": ChangeType.MODIFY_TYPE.value,
                        "column": name,
                        "from": old_type,
                        "to": new_type,
                        "compatible": compat,
                        "reason": f"类型不兼容: {old_type} → {new_type}" if not compat
                                  else f"类型可兼容: {old_type} → {new_type}",
                    })

        # 检测可空性变更
        for name in after_cols:
            if name in before_cols:
                if before_cols[name].nullable and not after_cols[name].nullable:
                    has_incompatible = True
                    is_compatible = False
                    changes_detail.append({
                        "type": "NULLABLE_CHANGE",
                        "column": name,
                        "from_nullable": True,
                        "to_nullable": False,
                        "compatible": False,
                        "reason": "NULL改为NOT NULL可能破坏含有NULL值的数据",
                    })

        # 分区变更
        before_partitions = set(before.partition_columns)
        after_partitions = set(after.partition_columns)
        dropped_partitions = before_partitions - after_partitions
        if dropped_partitions:
            has_incompatible = True
            is_compatible = False
            changes_detail.append({
                "type": ChangeType.DROP_PARTITION.value,
                "columns": list(dropped_partitions),
                "compatible": False,
                "reason": "删除分区列将丢失分区裁剪能力",
            })

        new_partitions = after_partitions - before_partitions
        if new_partitions:
            changes_detail.append({
                "type": ChangeType.ADD_PARTITION.value,
                "columns": list(new_partitions),
                "compatible": True,
                "reason": "新增分区列向后兼容",
            })

        # 受影响的管线
        affected = pipeline_deps.get(before.table_name, [])

        # 兼容性级别
        if has_incompatible:
            compat_level = Compatibility.INCOMPATIBLE
        elif changes_detail:
            compat_level = Compatibility.COMPATIBLE
        else:
            compat_level = Compatibility.COMPATIBLE

        return SchemaChange(
            change_id=change_id,
            target_table=before.table_name,
            change_type=ChangeType.MODIFY_TYPE if len(changes_detail) > 1 else
                        ChangeType(changes_detail[0]["type"]) if changes_detail
                        else ChangeType.MODIFY_TYPE,
            before=before,
            after=after,
            detail={"changes": changes_detail, "total_changes": len(changes_detail)},
            compatibility=compat_level,
            affected_pipelines=affected,
            rollback_plan=self._generate_rollback_plan(before, after, changes_detail),
        )

    def walk_affected_pipelines(
        self,
        table_name: str,
        pipeline_graph: dict[str, list[str]],
        max_depth: int = 5,
    ) -> list[dict[str, Any]]:
        """遍历所有受Schema变更影响的ETL管线。

        从上游表出发，沿DAG方向查找所有下游管线。
        """
        from collections import deque

        result: list[dict[str, Any]] = []
        visited: set[str] = {table_name}
        q: deque[tuple[str, int]] = deque([(table_name, 0)])

        while q and len(result) < max_depth * 10:
            node, depth = q.popleft()
            if depth >= max_depth:
                continue
            for dep in pipeline_graph.get(node, []):
                if dep not in visited:
                    visited.add(dep)
                    result.append({
                        "table": dep,
                        "depth": depth + 1,
                        "impact_level": "direct" if depth == 0 else f"level_{depth + 1}",
                    })
                    q.append((dep, depth + 1))

        return result

    @staticmethod
    def compatibility_report(
        changes: list[SchemaChange],
    ) -> dict[str, Any]:
        """多变更汇总报告。"""
        total = len(changes)
        compatible = sum(1 for c in changes if c.compatibility == Compatibility.COMPATIBLE)
        incompatible = sum(1 for c in changes if c.compatibility == Compatibility.INCOMPATIBLE)
        partially = sum(1 for c in changes if c.compatibility == Compatibility.PARTIALLY)
        all_affected = list(set(
            p for c in changes for p in c.affected_pipelines
        ))
        return {
            "total_changes": total,
            "compatible": compatible,
            "incompatible": incompatible,
            "partially_compatible": partially,
            "affected_pipelines": all_affected,
            "affected_pipeline_count": len(all_affected),
            "overall_risk": "high" if incompatible > 0 else
                            "medium" if partially > 0 else "low",
        }

    # ---- 内部 ----

    @staticmethod
    def _generate_rollback_plan(
        before: SchemaDefinition,
        after: SchemaDefinition,
        changes: list[dict[str, Any]],
    ) -> str:
        """生成回滚方案。"""
        incompatible = [c for c in changes if not c.get("compatible", True)]
        if not incompatible:
            return "变更全部兼容，无需回滚方案。Schema可安全部署，下游管线无需改动。"

        steps = []
        # 删除列回滚
        dropped = [c for c in incompatible if c["type"] == ChangeType.DROP_COLUMN.value]
        if dropped:
            cols = ", ".join(c["column"] for c in dropped)
            steps.append(f"1. 恢复被删除列: ALTER TABLE {before.table_name} ADD COLUMNS ({cols})")

        # 类型变更回滚
        type_changes = [c for c in incompatible if c["type"] == ChangeType.MODIFY_TYPE.value]
        if type_changes:
            for tc in type_changes:
                steps.append(
                    f"2. 恢复列类型: ALTER TABLE {before.table_name} "
                    f"CHANGE COLUMN {tc['column']} {tc['column']} {tc['from']}"
                )

        # 分区恢复
        part_drops = [c for c in incompatible if c["type"] == ChangeType.DROP_PARTITION.value]
        if part_drops:
            for pd in part_drops:
                cols = ", ".join(pd.get("columns", []))
                steps.append(f"3. 恢复分区列: 重新添加分区键 {cols}")

        if not steps:
            steps.append("无需回滚操作")

        steps.append(f"4. 验证: 执行 SELECT COUNT(*) FROM {before.table_name} 确认数据完整性")
        return "\n".join(steps)