"""数据血缘有向图构建与影响分析引擎。

血缘关系建模为属性图：节点=表/视图，边=字段级转换关系。
支持：
- 有向图构建（邻接表）
- 上游溯源（反向BFS）
- 下游影响分析（正向BFS）
- 血缘深度计算
- 循环依赖检测
- 受影响的管线范围计算
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LineageNode:
    """血缘图中的节点（表或视图）。"""
    node_id: str
    name: str
    node_type: str = "table"  # table / view / temp
    schema: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    """血缘图中的边（数据转换关系）。"""
    edge_id: str
    source: str            # 源节点ID
    target: str            # 目标节点ID
    transfer_fields: list[str] = field(default_factory=list)
    transform_type: str = "identity"  # identity / filter / aggregate / join / custom
    data_volume: str = "medium"       # small / medium / large
    depth: int = 1
    active: bool = True


@dataclass
class ImpactAnalysis:
    """影响分析结果。"""
    target_node: str
    upstream_nodes: list[str]       # 被影响的源节点
    downstream_nodes: list[str]     # 受到影响的目标节点
    affected_pipelines: list[str]   # 受影响的管线
    lineage_depth: dict[str, int]   # 各节点血缘深度
    has_cycle: bool
    cycle_path: list[str] = field(default_factory=list)


class LineageGraphEngine:
    """血缘图谱引擎。

    用法:
        g = LineageGraphEngine()
        g.add_node(LineageNode("ods_order", "ods.ods_order"))
        g.add_node(LineageNode("dwd_order", "dw.dwd_order"))
        g.add_edge(LineageEdge("e1", "ods_order", "dwd_order",
                                transfer_fields=["order_id", "amount"]))
        impact = g.analyze_impact("ods_order")
    """

    def __init__(self) -> None:
        self._nodes: dict[str, LineageNode] = {}
        self._edges: dict[str, LineageEdge] = {}
        self._out: dict[str, set[str]] = {}   # source -> {target_ids}
        self._in: dict[str, set[str]] = {}    # target -> {source_ids}

    # ---- 构建 ----

    def add_node(self, node: LineageNode) -> None:
        self._nodes[node.node_id] = node
        if node.node_id not in self._out:
            self._out[node.node_id] = set()
        if node.node_id not in self._in:
            self._in[node.node_id] = set()

    def add_edge(self, edge: LineageEdge) -> None:
        self._edges[edge.edge_id] = edge
        # 确保节点存在
        for nid in (edge.source, edge.target):
            if nid not in self._nodes:
                self._nodes[nid] = LineageNode(node_id=nid, name=nid)
        self._out.setdefault(edge.source, set()).add(edge.target)
        self._in.setdefault(edge.target, set()).add(edge.source)

    def remove_edge(self, edge_id: str) -> bool:
        edge = self._edges.pop(edge_id, None)
        if edge is None:
            return False
        self._out.get(edge.source, set()).discard(edge.target)
        self._in.get(edge.target, set()).discard(edge.source)
        return True

    # ---- 查询 ----

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        return len(self._edges)

    def get_node(self, node_id: str) -> LineageNode | None:
        return self._nodes.get(node_id)

    def get_edge(self, edge_id: str) -> LineageEdge | None:
        return self._edges.get(edge_id)

    # ---- 溯源/影响分析 ----

    def upstream_trace(
        self, node_id: str, max_depth: int = 10
    ) -> list[dict[str, Any]]:
        """上游溯源：BFS反向搜索，返回按层次的节点及路径。"""
        if node_id not in self._nodes:
            return []

        result: list[dict[str, Any]] = []
        visited: set[str] = {node_id}
        frontier = {node_id}
        level = 0

        while frontier and level < max_depth:
            next_frontier: set[str] = set()
            for nid in frontier:
                for up_id in self._in.get(nid, set()):
                    if up_id not in visited:
                        visited.add(up_id)
                        next_frontier.add(up_id)
                        node = self._nodes.get(up_id)
                        if node:
                            result.append({
                                "node_id": node.node_id,
                                "name": node.name,
                                "node_type": node.node_type,
                                "depth": level + 1,
                                "target": nid,
                            })
            frontier = next_frontier
            level += 1

        return result

    def downstream_impact(
        self, node_id: str, max_depth: int = 10
    ) -> list[dict[str, Any]]:
        """下游影响分析：BFS正向搜索。"""
        if node_id not in self._nodes:
            return []

        result: list[dict[str, Any]] = []
        visited: set[str] = {node_id}
        frontier = {node_id}
        level = 0

        while frontier and level < max_depth:
            next_frontier: set[str] = set()
            for nid in frontier:
                for down_id in self._out.get(nid, set()):
                    if down_id not in visited:
                        visited.add(down_id)
                        next_frontier.add(down_id)
                        node = self._nodes.get(down_id)
                        if node:
                            result.append({
                                "node_id": node.node_id,
                                "name": node.name,
                                "node_type": node.node_type,
                                "depth": level + 1,
                                "source": nid,
                            })
            frontier = next_frontier
            level += 1

        return result

    def analyze_impact(
        self,
        node_id: str,
        pipeline_map: dict[str, list[str]] | None = None,
    ) -> ImpactAnalysis:
        """完整影响分析：上游+下游+管线影响。"""
        upstream = self.upstream_trace(node_id)
        downstream = self.downstream_impact(node_id)

        # 判断环
        cycle_path = self._detect_cycle()
        has_cycle = len(cycle_path) > 0

        # 计算深度
        depths = self._compute_depths()

        # 管线影响
        affected_pipelines: list[str] = []
        if pipeline_map:
            affected_nodes = set(item["node_id"] for item in downstream)
            affected_nodes.update(item["node_id"] for item in upstream)
            affected_nodes.add(node_id)
            for table_id in affected_nodes:
                if table_id in pipeline_map:
                    affected_pipelines.extend(pipeline_map[table_id])

        return ImpactAnalysis(
            target_node=node_id,
            upstream_nodes=[item["node_id"] for item in upstream],
            downstream_nodes=[item["node_id"] for item in downstream],
            affected_pipelines=list(set(affected_pipelines)),
            lineage_depth=depths,
            has_cycle=has_cycle,
            cycle_path=cycle_path,
        )

    def field_level_lineage(
        self, source_node: str, field_name: str
    ) -> list[dict[str, Any]]:
        """字段级血缘：追踪某个字段在DAG中的传播路径。"""
        result: list[dict[str, Any]] = []
        visited: set[str] = set()
        stack = [(source_node, field_name)]

        while stack:
            cur_node, cur_field = stack.pop()
            key = f"{cur_node}.{cur_field}"
            if key in visited:
                continue
            visited.add(key)

            for down_id in self._out.get(cur_node, set()):
                # 查找包含该字段的边
                for edge in self._edges.values():
                    if edge.source == cur_node and edge.target == down_id:
                        if cur_field in edge.transfer_fields:
                            target_field = cur_field  # 简化：字段名不变
                            result.append({
                                "source_node": cur_node,
                                "source_field": cur_field,
                                "target_node": down_id,
                                "target_field": target_field,
                                "transform": edge.transform_type,
                                "edge_id": edge.edge_id,
                            })
                            stack.append((down_id, target_field))

        return result

    # ---- 内部 ----

    def _compute_depths(self) -> dict[str, int]:
        """BFS计算各节点的血缘深度。"""
        # 找所有入度为0的节点
        in_deg = {nid: len(self._in.get(nid, set())) for nid in self._nodes}
        from collections import deque
        q = deque(nid for nid, d in in_deg.items() if d == 0)
        depths: dict[str, int] = {}

        while q:
            cur = q.popleft()
            cur_depth = max(
                (depths.get(up, 0) for up in self._in.get(cur, set())), default=-1
            ) + 1
            depths[cur] = cur_depth
            for down in self._out.get(cur, set()):
                in_deg[down] -= 1
                if in_deg[down] == 0:
                    q.append(down)

        # 剩余节点（可能循环）默认深度1
        for nid in self._nodes:
            if nid not in depths:
                depths[nid] = 1

        return depths

    def _detect_cycle(self) -> list[str]:
        """三色标记DFS检测环。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {nid: WHITE for nid in self._nodes}
        parent: dict[str, str | None] = {}

        def dfs(u: str) -> list[str] | None:
            color[u] = GRAY
            for v in self._out.get(u, set()):
                if color.get(v) == GRAY:
                    path = [v, u]
                    cur = u
                    while cur != v:
                        cur = parent.get(cur, "")
                        if not cur:
                            break
                        path.append(cur)
                    return list(reversed(path))
                if color.get(v) == WHITE:
                    parent[v] = u
                    result = dfs(v)
                    if result:
                        return result
            color[u] = BLACK
            return None

        for nid in self._nodes:
            if color.get(nid) == WHITE:
                cycle = dfs(nid)
                if cycle:
                    return cycle
        return []