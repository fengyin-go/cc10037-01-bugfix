"""业务编排API：跨模块的ETL编排、质量门禁、回刷、Schema变更等操作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.engine.alert_matcher import AlertLevel, AlertRule
from app.engine.backfill_plan import BackfillEngine
from app.engine.validation_executor import CheckLevel, CheckType, QualityRule
from app.engine.watermark_progress import WatermarkEngine, WatermarkRecord, WatermarkStatus
from app.orchestrator import ETLOrchestrator

router = APIRouter(prefix="/api/business", tags=["业务编排"])

_orchestrator = ETLOrchestrator()


# ---- 管线编排 ----

@router.post("/pipelines/orchestrate")
def orchestrate_pipeline(body: dict[str, Any]) -> dict[str, Any]:
    """为管线执行完整的DAG拓扑验证和血缘登记。

    body:
      pipeline_id, pipeline_name, source_table, target_table,
      stages: [{"name": "extract", "nodes": [{"id": "n1", "name": "读取"}]}, ...]
    """
    try:
        return _orchestrator.create_pipeline_with_full_orchestration(
            pipeline_id=body["pipeline_id"],
            pipeline_name=body["pipeline_name"],
            source_table=body["source_table"],
            target_table=body["target_table"],
            stages=body.get("stages", []),
        )
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {e}")


# ---- 质量门禁 ----

@router.post("/quality/gate")
def run_quality_gate(body: dict[str, Any]) -> dict[str, Any]:
    """对管线数据执行质量门禁，并产出阻断回执。

    body:
      pipeline_id,
      batch_id (可选，缺省按 pipeline_id 归并),
      target_table / partition (可选，写入回执),
      rules: [{"rule_id", "rule_name", "rule_version", "target_table", "check_field",
               "check_type", "threshold", "level", "extra": {}}],
      sample_data: [dict]
    """
    pipeline_id = body["pipeline_id"]
    rules = [
        QualityRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            target_table=r.get("target_table", ""),
            check_field=r["check_field"],
            check_type=CheckType(r["check_type"]),
            threshold=float(r.get("threshold", 0.01)),
            level=CheckLevel(r.get("level", "WARN")),
            extra=r.get("extra", {}),
            rule_version=str(r.get("rule_version", "v1")),
        )
        for r in body.get("rules", [])
    ]
    result = _orchestrator.run_quality_gate(
        pipeline_id, rules, body.get("sample_data", []),
        batch_id=body.get("batch_id"),
        target_table=body.get("target_table"),
        partition=str(body.get("partition", "")),
    )
    return {
        "pipeline_id": result.pipeline_id,
        "batch_id": result.batch_id,
        "receipt_id": result.receipt_id,
        "overall_pass": result.overall_pass,
        "decision": result.decision,
        "failed_ratio": result.failed_ratio,
        "results": result.results,
        "receipt": result.receipt,
        "alert_events": result.alert_events,
        "gateway": result.gateway,
        "triggered_alerts": [
            {"rule_id": a.rule_id, "rule_name": a.rule_name,
             "matched": a.matched, "level": a.level.value, "message": a.message,
             "actual_values": a.actual_values}
            for a in result.triggered_alerts
        ],
    }


# ---- 质量批次阻断回执 / 告警交接 ----

def _ledger():
    return _orchestrator.ledger


@router.get("/quality/receipts")
def list_receipts() -> dict[str, Any]:
    """列出所有已确认的质量批次阻断回执。"""
    ledger = _ledger()
    receipts = [ledger.receipt_view(r) for r in ledger.list_receipts()]
    return {"total": len(receipts), "items": receipts}


@router.get("/quality/receipts/{receipt_id}")
def get_receipt(receipt_id: str) -> dict[str, Any]:
    """读取单份回执：逐规则阈值判定 + 告警交接状态。"""
    receipt = _ledger().get_receipt(receipt_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"回执 {receipt_id} 不存在")
    return _ledger().receipt_view(receipt)


@router.get("/quality/batches/{batch_id}/disposition")
def batch_disposition(batch_id: str) -> dict[str, Any]:
    """区分原批可重新提交，还是结论已确认、只剩告警待交接。"""
    return _ledger().batch_disposition(batch_id)


@router.post("/quality/receipts/{receipt_id}/alerts")
def register_receipt_alerts(receipt_id: str) -> dict[str, Any]:
    """按回执中的越线规则登记/补投告警给值班组（幂等）。"""
    ledger = _ledger()
    events = ledger.register_alerts_from_receipt(receipt_id)
    if events is None:
        raise HTTPException(status_code=404, detail=f"回执 {receipt_id} 不存在")
    receipt = ledger.get_receipt(receipt_id)
    view = ledger.receipt_view(receipt) if receipt else {}
    return {
        "ok": True,
        "receipt_id": receipt_id,
        "alert_events": [e.as_dict() for e in events],
        "alert_state": view.get("alert_state"),
        "next_action": view.get("next_action"),
        "batch_disposition": view.get("batch_disposition"),
        "message": "告警已按回执越线规则登记",
    }


# ---- 告警接收侧 ----

@router.get("/alerts/inbox")
def alert_inbox(
    receipt_id: str | None = None,
    batch_id: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """告警面板：读取已登记告警事件，可按回执/批次/状态过滤。"""
    ledger = _ledger()
    events = ledger.list_alerts(
        receipt_id=receipt_id, batch_id=batch_id, status=status,
    )
    return {
        "total": len(events),
        "receiver_connected": ledger.receiver_connected,
        "items": [e.as_dict() for e in events],
    }


@router.post("/alerts/receiver")
def control_alert_receiver(body: dict[str, Any] | None = None) -> dict[str, Any]:
    """控制告警接收侧：disconnect 断开，connect/recover 恢复并补投积压。"""
    ledger = _ledger()
    action = str((body or {}).get("action", "connect")).strip().lower()
    if action in {"disconnect", "down", "断连", "断开"}:
        ledger.disconnect_receiver()
        return {
            "ok": True,
            "receiver_connected": False,
            "drained": [],
            "message": "告警接收侧已断开，后续告警保持待交接",
        }
    if action in {"connect", "recover", "up", "恢复", "连接"}:
        drained = ledger.connect_receiver(drain=True)
        return {
            "ok": True,
            "receiver_connected": True,
            "drained": [e.as_dict() for e in drained],
            "message": f"告警接收侧已恢复，补投 {len(drained)} 条积压告警",
        }
    raise HTTPException(status_code=400, detail=f"未知接收侧动作：{action}")


@router.post("/alerts/drain")
def drain_alerts() -> dict[str, Any]:
    """接收侧恢复后手动补投待交接告警。"""
    ledger = _ledger()
    if not ledger.receiver_connected:
        return {
            "ok": False,
            "receiver_connected": False,
            "drained": [],
            "message": "接收侧仍处于断开状态，告警保持待交接",
        }
    drained = ledger.drain_pending()
    return {
        "ok": True,
        "receiver_connected": True,
        "drained": [e.as_dict() for e in drained],
        "message": f"已补投 {len(drained)} 条积压告警",
    }


@router.get("/alerts/receiver/inbox")
def receiver_inbox() -> dict[str, Any]:
    """接收侧（值班组）实际收到的告警投递记录。"""
    items = _ledger().delivered_inbox()
    return {"total": len(items), "items": items}


@router.get("/quality/receipts/{receipt_id}/alert-evaluation")
def evaluate_receipt_alert(receipt_id: str) -> dict[str, Any]:
    """告警面板按回执评估：逐规则阈值判定 + 已登记告警事件。"""
    from app.services.alert_policy import AlertPolicyService

    result = AlertPolicyService().evaluate_receipt_alerts(receipt_id)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# ---- 水位与回刷 ----

@router.post("/watermark/check")
def check_watermark(body: dict[str, Any]) -> dict[str, Any]:
    """检查水位并判断是否需要回刷。

    body:
      pipeline_id, partition_field, current_watermark, new_watermark,
      scheduled_time (ISO), backfill_threshold (可选)
    """
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=body["pipeline_id"],
        partition_field=body["partition_field"],
        current_watermark=body["current_watermark"],
        scheduled_time=datetime.fromisoformat(body["scheduled_time"]),
        status=WatermarkStatus(body.get("status", "正常")),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    return _orchestrator.check_watermark_and_backfill(
        record,
        body["new_watermark"],
        backfill_threshold=body.get("backfill_threshold", 3),
    )


# ---- 回刷计划 ----

@router.post("/backfill/plan")
def generate_backfill_plan(body: dict[str, Any]) -> dict[str, Any]:
    """生成回刷补偿方案。

    body:
      pipeline_id, start_partition, end_partition, parallelism, reason
    """
    engine = BackfillEngine()
    plan = engine.plan(
        pipeline_id=body["pipeline_id"],
        start_partition=body["start_partition"],
        end_partition=body["end_partition"],
        parallelism=int(body.get("parallelism", 4)),
        reason=body.get("reason", ""),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    # 分配资源
    alloc = _orchestrator.allocate_for_pipeline(
        body["pipeline_id"],
        node_count=plan.total_partitions,
        estimated_memory_gb=plan.total_partitions * 0.5,
    )

    return {
        "plan": {
            "pipeline_id": plan.pipeline_id,
            "task_name": plan.task_name,
            "reason": plan.reason,
            "total_partitions": plan.total_partitions,
            "batch_count": len(plan.batches),
            "parallelism": plan.parallelism,
            "estimated_completion": plan.estimated_completion,
            "batches": [
                {
                    "batch_id": b.batch_id,
                    "partitions": b.range.partitions,
                    "count": b.range.count,
                }
                for b in plan.batches
            ],
        },
        "resource_allocation": alloc["allocation"],
    }


# ---- Schema变更评估 ----

@router.post("/schema/evaluate")
def evaluate_schema_change(body: dict[str, Any]) -> dict[str, Any]:
    """评估Schema变更的兼容性和影响。

    body:
      change_id, table_name,
      before_columns: [{"name", "data_type", "nullable", "is_partition"}],
      after_columns: [{"name", "data_type", "nullable", "is_partition"}],
      pipeline_deps: {table_name: [pipeline_ids]}
    """
    from app.engine.schema_evolution import SchemaColumn, SchemaDefinition

    before = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("before_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("before_columns", []) if c.get("is_partition")
        ],
    )
    after = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("after_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("after_columns", []) if c.get("is_partition")
        ],
    )
    result = _orchestrator.evaluate_schema_change(
        change_id=body.get("change_id", "auto"),
        before=before,
        after=after,
        pipeline_deps=body.get("pipeline_deps", {}),
    )
    return {
        "change_id": result.change.change_id,
        "table": result.change.target_table,
        "compatibility": result.change.compatibility.value,
        "changes": result.change.detail.get("changes", []),
        "affected_pipelines": result.change.affected_pipelines,
        "rollback_plan": result.change.rollback_plan,
        "migration_recommendation": result.migration_recommendation,
        "walk_pipelines": result.affected_pipelines,
    }


# ---- 血缘影响分析 ----

@router.get("/lineage/impact/{node_id}")
def analyze_lineage_impact(node_id: str) -> dict[str, Any]:
    """分析数据血缘影响：上游溯源 + 下游影响。"""
    result = _orchestrator.lineage.analyze_impact(node_id)
    return {
        "target_node": result.target_node,
        "upstream_count": len(result.upstream_nodes),
        "downstream_count": len(result.downstream_nodes),
        "affected_pipelines": result.affected_pipelines,
        "lineage_depth": result.lineage_depth,
        "has_cycle": result.has_cycle,
        "cycle_path": result.cycle_path,
    }


# ---- 资源健康 ----

@router.get("/resources/health")
def cluster_health() -> dict[str, Any]:
    """计算资源集群健康状态。"""
    return _orchestrator.balancer.cluster_health()


# ---- 告警批量评估 ----

@router.post("/alerts/evaluate")
def evaluate_alerts(body: dict[str, Any]) -> dict[str, Any]:
    """批量评估告警规则。

    body:
      rules: [{"rule_id", "rule_name", "monitor_target", "condition", "level"}],
      metrics: dict
    """
    rules = [
        AlertRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            monitor_target=r["monitor_target"],
            condition=r["condition"],
            level=AlertLevel(r.get("level", "WARN")),
            channels=r.get("channels", ["email"]),
        )
        for r in body.get("rules", [])
    ]
    matcher = _orchestrator.alert
    results = matcher.match_batch(rules, body.get("metrics", {}))

    return {
        "total_rules": len(rules),
        "triggered": sum(1 for r in results if r.matched),
        "results": [
            {
                "rule_id": r.rule_id,
                "rule_name": r.rule_name,
                "matched": r.matched,
                "level": r.level.value,
                "message": r.message,
            }
            for r in results
        ],
    }


# ---- DAG拓扑分析 ----

@router.post("/dag/analyze")
def analyze_dag(body: dict[str, Any]) -> dict[str, Any]:
    """分析DAG拓扑结构。

    body:
      nodes: [{"id", "name", "weight"}],
      edges: [{"from", "to"}]
    """
    from app.engine.dag_topology import DagNode, DagTopology

    topo = DagTopology()
    for n in body.get("nodes", []):
        topo.add_node(DagNode(
            node_id=n["id"], name=n.get("name", n["id"]),
            weight=float(n.get("weight", 1.0)),
        ))
    for e in body.get("edges", []):
        topo.add_edge(e["from"], e["to"])

    result = topo.solve()
    return {
        "node_count": topo.node_count,
        "has_cycle": result.has_cycle,
        "cycle_nodes": result.cycle_nodes,
        "topology_order": result.order,
        "critical_path": result.critical_path,
        "critical_length": result.critical_length,
        "node_depth": result.node_depth,
    }


# ---- 综合管线运行报告 ----

@router.get("/pipelines/{pipeline_id}/run-report")
def pipeline_run_report(pipeline_id: str) -> dict[str, Any]:
    """获取管线的综合运行报告。"""
    # 模拟生成运行报告
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=pipeline_id,
        partition_field="dt",
        current_watermark=datetime.now().strftime("%Y-%m-%d"),
        scheduled_time=datetime.now(),
        delay_minutes=0,
        status=WatermarkStatus.NORMAL,
    )

    # 模拟质量结果
    quality = _orchestrator.run_quality_gate(
        pipeline_id,
        [
            QualityRule(
                rule_id="r_default",
                rule_name="默认非空检查",
                target_table=f"dw.{pipeline_id}",
                check_field="id",
                check_type=CheckType.NOT_NULL,
                threshold=0.01,
            ),
        ],
        [{"id": "001"}, {"id": None}],
        batch_id=f"RUN-REPORT-{pipeline_id}",
        target_table=f"dw.{pipeline_id}",
    )

    report = _orchestrator.pipeline_run_report(pipeline_id, record, quality)
    report["receipt_id"] = quality.receipt_id
    report["receipt"] = quality.receipt
    return report