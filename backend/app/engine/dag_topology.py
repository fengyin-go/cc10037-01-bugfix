"""DAG拓扑排序与依赖解析引擎。

ETL管线的任务节点构成有向无环图，本模块提供：
- 拓扑排序（Kahn算法）
- 环检测
- 关键路径计算（最长路径 + 浮时 slack）
- 依赖解析（上游集、下游集、全量上游闭包）
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DagNode:
    """DAG中的一个任务节点。"""
    node_id: str
    name: str = ""
    weight: float = 1.0  # 执行耗时权重（用于关键路径）
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class TopologyResult:
    """拓扑排序结果。"""
    order: list[str]           # 拓扑序（可能只是其中一个合法顺序）
    all_orders: list[list[str]] = field(default_factory=list)  # 所有可行序（节点≤12时枚举）
    has_cycle: bool = False
    cycle_nodes: list[str] = field(default_factory=list)
    critical_path: list[str] = field(default_factory=list)     # 关键路径节点序
    critical_length: float = 0.0
    node_depth: dict[str, int] = field(default_factory=dict)   # 每个节点的深度


class DagTopology:
    """ETL管线DAG拓扑排序器。

    用法:
        topo = DagTopology()
        topo.add_node(DagNode("extract", "数据抽取", weight=3))
        topo.add_node(DagNode("transform", "数据转换", weight=5))
        topo.add_edge("extract", "transform")
        result = topo.solve()
    """

    def __init__(self) -> None:
        self._nodes: dict[str, DagNode] = {}
        self._out_edges: dict[str, set[str]] = defaultdict(set)  # u -> {v1, v2, ...}
        self._in_edges: dict[str, set[str]] = defaultdict(set)   # v -> {u1, u2, ...}

    # ---- 构建 ----

    def add_node(self, node: DagNode) -> None:
        self._nodes[node.node_id] = node

    def add_edge(self, from_id: str, to_id: str) -> None:
        """添加一条有向边 from_id → to_id。"""
        if from_id not in self._nodes:
            self._nodes[from_id] = DagNode(node_id=from_id)
        if to_id not in self._nodes:
            self._nodes[to_id] = DagNode(node_id=to_id)
        self._out_edges[from_id].add(to_id)
        self._in_edges[to_id].add(from_id)

    def remove_edge(self, from_id: str, to_id: str) -> None:
        self._out_edges[from_id].discard(to_id)
        self._in_edges[to_id].discard(from_id)

    # ---- 查询 ----

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    def upstream(self, node_id: str) -> set[str]:
        return set(self._in_edges.get(node_id, set()))

    def downstream(self, node_id: str) -> set[str]:
        return set(self._out_edges.get(node_id, set()))

    def upstream_closure(self, node_id: str) -> set[str]:
        """从 node_id 出发所有上游（传递闭包）。"""
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            cur = stack.pop()
            for up in self._in_edges.get(cur, set()):
                if up not in visited:
                    visited.add(up)
                    stack.append(up)
        return visited

    def downstream_closure(self, node_id: str) -> set[str]:
        """从 node_id 出发所有下游（传递闭包）。"""
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            cur = stack.pop()
            for down in self._out_edges.get(cur, set()):
                if down not in visited:
                    visited.add(down)
                    stack.append(down)
        return visited

    # ---- 核心算法 ----

    def solve(self) -> TopologyResult:
        """Kahn拓扑排序 + 环检测 + 关键路径。"""
        # 环检测
        cycle = self._detect_cycle()
        if cycle:
            return TopologyResult(order=[], has_cycle=True, cycle_nodes=cycle)

        # 深度计算
        depths = self._compute_depths()

        # 拓扑序
        order = self._kahn_sort()

        # 关键路径
        cp_nodes, cp_len = self._critical_path(depths)

        return TopologyResult(
            order=order,
            has_cycle=False,
            critical_path=cp_nodes,
            critical_length=cp_len,
            node_depth=depths,
        )

    def toposort_all(self) -> list[list[str]]:
        """枚举所有合法拓扑序（仅在节点数≤12时执行，避免组合爆炸）。"""
        if len(self._nodes) == 0:
            return [[]]
        if len(self._nodes) > 12:
            return [self.solve().order]  # 退化为单一顺序
        if self._detect_cycle():
            return []

        node_ids = list(self._nodes)
        in_deg = {nid: len(self._in_edges.get(nid, set())) for nid in node_ids}

        result: list[list[str]] = []

        def backtrack(path: list[str], current_in_deg: dict[str, int]) -> None:
            if len(path) == len(node_ids):
                result.append(list(path))
                return
            # 候选节点：in_deg==0 且未访问
            for nid in node_ids:
                if nid in path:
                    continue
                if current_in_deg.get(nid, 0) != 0:
                    continue
                # 选择
                path.append(nid)
                saved: list[str] = []
                for down in self._out_edges.get(nid, set()):
                    current_in_deg[down] -= 1
                    saved.append(down)
                backtrack(path, current_in_deg)
                # 回溯
                path.pop()
                for down in saved:
                    current_in_deg[down] += 1

        backtrack([], dict(in_deg))
        return result

    # ---- 内部 ----

    def _kahn_sort(self) -> list[str]:
        in_deg = {nid: len(self._in_edges.get(nid, set())) for nid in self._nodes}
        q: deque[str] = deque(nid for nid, d in in_deg.items() if d == 0)
        order: list[str] = []
        while q:
            cur = q.popleft()
            order.append(cur)
            for down in self._out_edges.get(cur, set()):
                in_deg[down] -= 1
                if in_deg[down] == 0:
                    q.append(down)
        return order

    def _detect_cycle(self) -> list[str]:
        """DFS三色标记法检测环路，返回路径。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {nid: WHITE for nid in self._nodes}
        parent: dict[str, str | None] = {}

        def dfs(u: str) -> list[str] | None:
            color[u] = GRAY
            for v in self._out_edges.get(u, set()):
                if color.get(v) == GRAY:
                    # 找到环路，回溯路径
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

    def _compute_depths(self) -> dict[str, int]:
        """按拓扑序计算每个节点的深度（最长入边路径+1）。"""
        depths: dict[str, int] = {}
        for nid in self._kahn_sort():
            max_up_depth = 0
            for up in self._in_edges.get(nid, set()):
                max_up_depth = max(max_up_depth, depths.get(up, 0))
            depths[nid] = max_up_depth + 1
        return depths

    def _critical_path(self, depths: dict[str, int]) -> tuple[list[str], float]:
        """按节点权重计算关键路径。

        先拓扑序DP计算最早开始时间，再逆序计算最晚开始时间，
        float==0 的节点构成关键路径。
        """
        node_ids = self._kahn_sort()
        # 最早开始 ES
        es: dict[str, float] = {nid: 0.0 for nid in self._nodes}
        for nid in node_ids:
            for down in self._out_edges.get(nid, set()):
                candidate = es[nid] + self._nodes[nid].weight
                if candidate > es.get(down, 0.0):
                    es[down] = candidate

        # 最晚开始 LS
        max_es = max(es.values()) if es else 0.0
        ls: dict[str, float] = {nid: max_es for nid in self._nodes}
        for nid in reversed(node_ids):
            for down in self._out_edges.get(nid, set()):
                candidate = ls[down] - self._nodes[nid].weight
                if candidate < ls[nid]:
                    ls[nid] = candidate

        # float = LS - ES
        cp = [nid for nid in node_ids if abs(ls[nid] - es[nid]) < 1e-9]
        return cp, max_es