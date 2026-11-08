"""管線引擎：讀 Workflow 定義，按序實例化算子並呼叫 .execute(ctx)。

引擎本身不寫任何具體算子的邏輯。步驟與參數全由 YAML 宣告：換偵測演算法、關掉 LLM、換收斂方式都只改 YAML。

執行順序：
  載入批次輸入（Hooks.load_batch）
  對每個 target：載入該序列的輸入 -> 依序跑 scope=target 的節點 -> 持久化該序列的結果
  把所有 target 的異常合併 -> 依序跑 scope=batch 的節點 -> 持久化最終結果（事件群、通知、審計）
節點之間只透過 ctx 傳資料；Hooks（由 storage 實作）只在最前面載入、最後面寫入，節點中間不碰 DB。
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from aiops.engine.context import LotEdge, PeerGroup, PeerRef, PipelineContext, Target, TargetError
from aiops.engine.errors import InfrastructureError, PlatformError, WorkflowError
from aiops.engine.registry import OperatorRegistry


class PeerSpec(BaseModel):
    source_type: str = "mock"
    query: str


class PeerGroupSpec(BaseModel):
    """動態同儕：query 撈回整群序列、依 by 標籤分成員。query 可用 ${屬性名} 引用 target 的 attrs。"""

    query: str
    by: str = Field(min_length=1)
    source_type: str = ""  # 空 = 跟 target 同一個資料源


class TargetSpec(BaseModel):
    source_type: str = "mock"
    query: str
    entity: str | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)
    peers: list[PeerSpec] = Field(default_factory=list)
    peer_group: PeerGroupSpec | None = None

    def to_target(self) -> Target:
        group = (PeerGroup(self.peer_group.query, self.peer_group.by, self.peer_group.source_type)
                 if self.peer_group else None)
        return Target(self.source_type, self.query, self.entity or self.query, dict(self.attrs),
                      [PeerRef(p.source_type, p.query) for p in self.peers], group)


class NodeSpec(BaseModel):
    name: str
    operator: str
    params: dict[str, Any] = Field(default_factory=dict)
    # 旁路裝飾節點（LLM 摘要等）：失敗記進 errors 並跳過，不影響後面的節點與最終寫入。
    # 基礎設施錯誤（DB、資料源連不上）不受此限，一律讓整輪失敗。
    optional: bool = False


class Workflow(BaseModel):
    name: str
    description: str = ""
    nodes: list[NodeSpec] = Field(min_length=1)
    targets: list[TargetSpec] = Field(default_factory=list)
    # 明確指定拓撲／Lot 流向時優先於從 trace、Lot 履歷自動推導的結果；給空清單代表「不要用」
    topology: list[tuple[str, str]] | None = None
    lot_edges: list[dict[str, Any]] | None = None

    @model_validator(mode="after")
    def _unique_names(self):
        names = [n.name for n in self.nodes]
        if len(names) != len(set(names)):
            raise ValueError(f"節點名稱不可重複：{names}")
        return self


def load_workflow(path: str | Path) -> Workflow:
    try:
        return Workflow.model_validate(yaml.safe_load(Path(path).read_text(encoding="utf-8")))
    except (ValidationError, yaml.YAMLError, OSError) as e:
        raise WorkflowError(f"Workflow {path} 定義有誤：{e}") from e


class Hooks(Protocol):
    """引擎的載入與寫入點，由 storage 實作。算子拿不到它。"""

    def load_batch(self, ctx: PipelineContext) -> None: ...

    def load_target(self, ctx: PipelineContext, detector_nodes: list[tuple[str, str]]) -> None: ...

    def persist_target(self, ctx: PipelineContext) -> None: ...

    def persist_batch(self, ctx: PipelineContext) -> None: ...


class NullHooks:
    def load_batch(self, ctx: PipelineContext) -> None: ...

    def load_target(self, ctx: PipelineContext, detector_nodes: list[tuple[str, str]]) -> None: ...

    def persist_target(self, ctx: PipelineContext) -> None: ...

    def persist_batch(self, ctx: PipelineContext) -> None: ...


def _run_node(node: NodeSpec, op: Any, ctx: PipelineContext) -> PipelineContext:
    """跑單一節點。optional 節點出錯只記錄並跳過；InfrastructureError 與非 optional 節點的錯誤一律往上丟。"""
    ctx.log(f"{node.name}:{node.operator}")
    if not node.optional:
        return op.execute(ctx)
    try:
        return op.execute(ctx)
    except InfrastructureError:
        raise
    except Exception as e:  # noqa: BLE001  旁路節點無論什麼錯都不該擋住主流程
        entity = ctx.target.entity if ctx.target else f"node:{node.name}"
        ctx.errors.append(TargetError(entity, f"[optional {node.name}] {e}"))
        ctx.log(f"{node.name}:skipped")
        return ctx


class Pipeline:
    def __init__(self, workflow: Workflow, hooks: Hooks | None = None):
        self.workflow = workflow
        self.hooks: Hooks = hooks or NullHooks()
        # 實例化時就驗證：未註冊的算子、拼錯的參數、節點順序，在啟動時擋下，不是跑到一半才發現
        self.nodes = [(n, OperatorRegistry.create(n.operator, n.params)) for n in workflow.nodes]
        for n, op in self.nodes:
            op.node_name = n.name
        scopes = [getattr(op, "scope", "target") for _, op in self.nodes]
        if "batch" in scopes and "target" in scopes[scopes.index("batch"):]:
            raise WorkflowError(f"Workflow {workflow.name}：scope=target 的節點必須全部排在 scope=batch 之前")
        self.target_nodes = [(n, op) for (n, op), s in zip(self.nodes, scopes) if s == "target"]
        self.batch_nodes = [(n, op) for (n, op), s in zip(self.nodes, scopes) if s == "batch"]
        # (節點名稱, 算子名稱)：基準線與調參都以節點區分，同一個演算法掛兩次也不會互相覆蓋
        # 只有會存基準線的節點才需要載入狀態與調參（算子自己宣告 has_baseline；new_only、ensemble 這類不需要）
        self.detector_nodes = [(n.name, n.operator) for n, op in self.target_nodes if getattr(op, "has_baseline", False)]

    def run(self, *, now: datetime | None = None, targets: list[TargetSpec] | None = None,
            incident_id: int | None = None, request: BaseModel | None = None,
            strict: bool = False) -> PipelineContext:
        """strict=False（排程）：某條序列失敗記在 errors，其他序列照跑；strict=True（手動動作）：失敗直接丟出。"""
        now = now or datetime.now(timezone.utc)
        batch = PipelineContext(pipeline=self.workflow.name, now=now, incident_id=incident_id, request=request)
        if self.workflow.topology is not None:
            batch.topology, batch.topology_explicit = list(self.workflow.topology), True
        if self.workflow.lot_edges is not None:
            batch.lot_edges = [LotEdge(e["from"], e["to"], int(e.get("lots", 1))) for e in self.workflow.lot_edges]
            batch.lot_edges_explicit = True
        self.hooks.load_batch(batch)

        if self.target_nodes:
            for spec in (targets if targets is not None else self.workflow.targets):
                ctx = PipelineContext(pipeline=self.workflow.name, now=now, target=spec.to_target(), request=request)
                try:
                    self.hooks.load_target(ctx, self.detector_nodes)
                    for node, op in self.target_nodes:
                        ctx = _run_node(node, op, ctx)
                    self.hooks.persist_target(ctx)
                except InfrastructureError:
                    raise  # 資料庫、資料源連不上：整輪失敗，不要假裝只是這條序列的問題
                except PlatformError as e:
                    if strict:
                        raise
                    batch.errors.append(TargetError(ctx.target.entity, str(e)))
                    continue
                batch.anomalies.extend(ctx.anomalies)
                batch.audit.extend(ctx.audit)
                batch.errors.extend(ctx.errors)  # optional 節點在這條序列留下的錯誤
                batch.result.update(ctx.result)

        for node, op in self.batch_nodes:
            batch = _run_node(node, op, batch)
        self.hooks.persist_batch(batch)
        return batch


def execute_operator(name: str, ctx: PipelineContext, **params: Any) -> PipelineContext:
    """單一算子直接執行（手動動作用，例如核准欄位對照）。"""
    return OperatorRegistry.create(name, params).execute(ctx)
