"""管線引擎：讀 Workflow 定義，按序實例化算子並呼叫 .execute(ctx)。

引擎本身不寫任何具體算子的邏輯。步驟與參數全由 YAML 宣告：換偵測演算法、關掉 LLM、換收斂方式都只改 YAML。

執行順序：
  載入批次輸入（Hooks.load_batch）
  對每個 target：載入該序列的輸入 -> 依序跑 scope=target 的節點 -> 持久化該序列的結果
  把所有 target 的異常合併 -> 依序跑 scope=batch 的節點 -> 持久化最終結果（事件群、通知、審計）
節點之間只透過 ctx 傳資料；Hooks（由 storage 實作）只在最前面載入、最後面寫入，節點中間不碰 DB。
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError, model_validator

from aiops.engine.context import LotEdge, PeerGroup, PeerRef, PipelineContext, Target, TargetError
from aiops.engine.graph import ORIGIN_WORKFLOW, Edge, EdgeKind, Provenance, contradictions
from aiops.engine.errors import InfrastructureError, NoDataError, PlatformError, WorkflowError
from aiops.engine.health import StreamCheck, run_status, stream_status
from aiops.engine.operator import NodeInfo
from aiops.engine.registry import OperatorRegistry

logger = logging.getLogger("aiops.engine")


# 資料源一律明寫、沒有預設值：漏寫時以前會默默退回 mock（假資料），監控平台寧可直接報錯（API 回 400、YAML 載入失敗）
SourceType = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


# 環境（正式／測試…）的名稱：管線層屬性，也是關係圖資料表的範圍鍵。空字串 = 不分環境（舊行為）。
EnvironmentName = Annotated[str, StringConstraints(strip_whitespace=True, max_length=64, pattern=r"^[A-Za-z0-9_.-]*$")]


def environment_in_entity(entity: str, environment: str) -> bool:
    """實體名稱裡有沒有夾帶環境名（cart-prod、prod.cart、cart@prod 這類）。規則：實體維持 service.name，環境只放在
    管線的 environment，不塞進名稱——塞進去的話改環境名就斷基準線，跨環境比較也對不起來。
    以非英數字元切開後逐段比對（不分大小寫），所以 product-api 不會被 prod 誤判。"""
    if not environment:
        return False
    return environment.lower() in {t for t in re.split(r"[^0-9a-z]+", entity.lower()) if t}


class StrictModel(BaseModel):
    """workflow 定義與 API 請求的共同基底：不認得的欄位直接報錯（YAML 載入失敗、API 回 422）。
    Pydantic 預設會把多出來的欄位默默丟掉——`param:`（少了 s）、`windw_days:` 這種筆誤就變成「設定沒生效但也沒人知道」。"""

    model_config = ConfigDict(extra="forbid")


class PeerSpec(StrictModel):
    source_type: SourceType
    query: str


class PeerGroupSpec(StrictModel):
    """動態同儕：query 撈回整群序列、依 by 標籤分成員。query 可用 ${屬性名} 引用 target 的 attrs。"""

    query: str
    by: str = Field(min_length=1)
    source_type: str = ""  # 空 = 跟 target 同一個資料源


EntityName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=256)]


class GraphEdgeSpec(StrictModel):
    """一條宣告的關係邊。kind 必填、沒有預設值：方向由型別決定（engine/graph.KINDS），寫的人只照資料來源的說法選型別。
      call    from 呼叫 to（from 依賴 to）                 -> to 出事拖垮 from
      supply  from 供應 to（電力、氣體、冷卻水、共用主機）  -> from 出事拖垮 to
      flow    from 是 to 的上游（物料、Lot 流向）          -> from 出事流到 to；weight = 走過的量（算子的 min_lots 會篩）
      async   from 發訊息給 to（訊息佇列）                 -> 只給訊息收斂用，不進傳播圖；weight = 延遲 P95 毫秒"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    source: EntityName = Field(alias="from")
    target: EntityName = Field(alias="to")
    kind: Literal["call", "supply", "flow", "async"]
    weight: float = Field(1.0, gt=0, allow_inf_nan=False)

    def to_edge(self) -> Edge:
        return Edge(self.source, self.target, EdgeKind(self.kind), float(self.weight), Provenance(ORIGIN_WORKFLOW))


def check_declared(edges: list[Edge]) -> None:
    """宣告的邊在載入期檢查：自己指自己、不同型別把同一對實體宣告成相反的傳播方向，都直接報錯（ValueError）。"""
    loops = sorted({e.source for e in edges if e.is_loop})
    if loops:
        raise ValueError(f"宣告的邊不能自己指自己：{loops}")
    bad = contradictions(edges)
    if bad:
        a, b = bad[0]
        raise ValueError(f"宣告的邊方向互相矛盾：{a.kind.value} {a.source} -> {a.target} 的意思是「{a.propagation[0]} 出事拖垮 "
                         f"{a.propagation[1]}」，{b.kind.value} {b.source} -> {b.target} 卻是「{b.propagation[0]} 出事拖垮 "
                         f"{b.propagation[1]}」（共 {len(bad)} 組）。同一個依賴關係只要宣告一次")


class TargetSpec(StrictModel):
    # 拼錯欄位（例如 detector: 少了 s）直接報錯；不然白名單會被默默忽略、變成全部偵測器都跑
    source_type: SourceType
    query: str
    entity: str | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)
    peers: list[PeerSpec] = Field(default_factory=list)
    peer_group: PeerGroupSpec | None = None
    # 這條序列要跑哪些偵測節點：節點名稱（nodes[].name）或算子名稱（代表這個算子的所有節點）。不寫 = 全部都跑
    detectors: list[str] | None = Field(default=None, min_length=1)
    # 節點名稱 -> 參數，蓋過節點 YAML 的 params（優先序最高）
    overrides: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @property
    def label(self) -> str:
        return self.entity or self.query

    def to_target(self) -> Target:
        group = (PeerGroup(self.peer_group.query, self.peer_group.by, self.peer_group.source_type)
                 if self.peer_group else None)
        return Target(self.source_type, self.query, self.label, dict(self.attrs),
                      [PeerRef(p.source_type, p.query) for p in self.peers], group,
                      list(self.detectors) if self.detectors is not None else None,
                      {k: dict(v) for k, v in self.overrides.items()})


class NodeSpec(StrictModel):
    name: str
    operator: str
    params: dict[str, Any] = Field(default_factory=dict)
    # 旁路裝飾節點（LLM 摘要等）：失敗記進 errors 並跳過，不影響後面的節點與最終寫入。
    # 基礎設施錯誤（DB、資料源連不上）不受此限，一律讓整輪失敗。
    optional: bool = False


class Workflow(StrictModel):
    name: str
    description: str = ""
    nodes: list[NodeSpec] = Field(min_length=1)
    targets: list[TargetSpec] = Field(default_factory=list)
    # 明確指定拓撲／Lot 流向時優先於從 trace、Lot 履歷自動推導的結果；給空清單代表「不要用」
    topology: list[tuple[str, str]] | None = None
    lot_edges: list[dict[str, Any]] | None = None
    # 宣告的關係邊（每條必寫 kind）：沒有 trace 的環境靠它提供依賴圖。跟資料庫裡觀測到的邊（trace 沉澱、Lot 履歷）
    # 與 admin.graph_import 匯入的邊取聯集——是「補上」，不是「取代」（要整張取代用上面的 topology／lot_edges）。
    graph: list[GraphEdgeSpec] = Field(default_factory=list)
    # 這條偵測管線產生的事件群，預設用哪一條 RCA 管線出報告（POST /api/v1/rca 沒帶 workflow 時用它）。
    # 依場景有沒有 trace 決定：有 trace 的 IT 管線寫 rca_report_llm；沒有 trace 的（工廠、純指標）寫 rca_report
    # （沒有 span 時退回拓撲／Lot 流向推測）。不寫 = rca_report。名稱在載入期檢查（interfaces/runner.build_pipeline）。
    rca_workflow: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]+$", max_length=128)
    # 資料斷流：這一輪「抓不到資料」（資料源回沒資料、或抓回來是空的）的序列比例達到這個值，整輪視為基礎設施錯誤，
    # 心跳不續命、備援通報會響。預設 1.0 = 全部序列都抓不到才算（PVC 滿、APM 寫不進去就是這個樣子；
    # 個別序列本來就稀疏的只記錯誤）。查詢寫錯（BadRequest）是設定問題，不算斷流
    max_failed_target_ratio: float = Field(1.0, gt=0, le=1)
    # 這條管線屬於哪個環境（prod、staging…）。關係圖（service_edges／service_async_edges／lot_paths）只載入同一個
    # 環境的邊，事件群快照記下它（RCA 照它載圖）。空字串 = 不分環境，跟加這個欄位之前完全一樣。
    # 異常點、基準線、事件群、調參本來就以管線名稱分開，一個環境開一條管線即可；實體名稱不要再夾帶環境。
    environment: EnvironmentName = ""
    # 資料健康度紀錄（資料流的狀態區間、每一輪的執行結果）留幾天：每輪寫入時順手清掉這條管線更舊的紀錄。
    # 每條資料流目前的狀態與「第一次收到資料」的紀錄不受影響
    health_retention_days: int = Field(180, gt=0, le=3650)

    @model_validator(mode="after")
    def _unique_names(self):
        names = [n.name for n in self.nodes]
        if len(names) != len(set(names)):
            raise ValueError(f"節點名稱不可重複：{names}")
        check_declared([e.to_edge() for e in self.graph])
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

    def finalize_batch(self, ctx: PipelineContext) -> None:
        """after_persist 節點（通知）都成功之後才做的紀錄：已通知、結案。通知送失敗就不會呼叫到；
        寫失敗則 after_finalize 節點（心跳）不會跑。"""
        ...

    def record_health(self, ctx: PipelineContext) -> None:
        """資料健康度（ctx.stream_checks、ctx.run_check）：整輪結束時呼叫，成功或失敗都會。寫不進去不影響這一輪的結果。"""
        ...


class NullHooks:
    def load_batch(self, ctx: PipelineContext) -> None: ...

    def load_target(self, ctx: PipelineContext, detector_nodes: list[tuple[str, str]]) -> None: ...

    def persist_target(self, ctx: PipelineContext) -> None: ...

    def persist_batch(self, ctx: PipelineContext) -> None: ...

    def finalize_batch(self, ctx: PipelineContext) -> None: ...

    def record_health(self, ctx: PipelineContext) -> None: ...


def _stream_check(ctx: PipelineContext, error: BaseException | None) -> StreamCheck | None:
    """這條序列這一輪的資料流狀態（engine/health.stream_status）。以前有沒有過資料：有「第一次收到資料」的紀錄、
    或已經有基準線、或存過異常點（後兩者讓加這個功能之前就在跑的資料流第一次斷流時不會被記成「從未有過資料」）。"""
    target = ctx.target
    assert target is not None
    seen = ctx.stream_seen_before or bool(ctx.detector_states) or ctx.last_anomaly_ts is not None
    verdict = stream_status(fetched=ctx.raw_df is not None, empty=ctx.raw_df is not None and ctx.raw_df.empty,
                            error=error, seen_before=seen)
    return None if verdict is None else StreamCheck(target.entity, target.query, target.source_type, *verdict,
                                                    known=ctx.stream_seen_before)


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
        order = {"target": 0, "batch": 1, "after_persist": 2, "after_finalize": 3}
        unknown = sorted({s for s in scopes if s not in order})
        if unknown:
            raise WorkflowError(f"Workflow {workflow.name}：未知的節點 scope {unknown}")
        if [order[s] for s in scopes] != sorted(order[s] for s in scopes):
            raise WorkflowError(f"Workflow {workflow.name}：節點順序必須是 scope=target → batch → after_persist → after_finalize")
        # 跨節點的設定（裁決的 bypass、致命地板要認哪些節點…）由算子自己檢查，引擎只負責把整條 workflow 交給它
        infos = [NodeInfo(n.name, n.operator, dict(n.params), op) for n, op in self.nodes]
        for n, op in self.nodes:
            check = getattr(op, "check_workflow", None)
            if check is None:
                continue
            try:
                check(infos)
            except (TypeError, ValueError) as e:
                raise WorkflowError(f"Workflow {workflow.name} 節點 {n.name}（{n.operator}）：{e}") from e
        self.target_nodes = [(n, op) for (n, op), s in zip(self.nodes, scopes) if s == "target"]
        self.batch_nodes = [(n, op) for (n, op), s in zip(self.nodes, scopes) if s == "batch"]
        # 寫入成功之後才跑（通知）：資料庫寫不進去時整輪失敗，這些節點不會執行
        self.after_persist_nodes = [(n, op) for (n, op), s in zip(self.nodes, scopes) if s == "after_persist"]
        # 「已通知」與結案也記完之後才跑（自我健康心跳）：這一輪每一步都成功了才算活著
        self.after_finalize_nodes = [(n, op) for (n, op), s in zip(self.nodes, scopes) if s == "after_finalize"]
        # (節點名稱, 算子名稱)：基準線與調參都以節點區分，同一個演算法掛兩次也不會互相覆蓋
        # 只有會存基準線的節點才需要載入狀態與調參（算子自己宣告 has_baseline；new_only、ensemble 這類不需要）
        self.detector_nodes = [(n.name, n.operator) for n, op in self.target_nodes if getattr(op, "has_baseline", False)]
        self._detectors = {n.name: (n.operator, op) for n, op in self.target_nodes if getattr(op, "has_baseline", False)}
        self._check_targets_are_used(workflow.targets, f"Workflow {workflow.name} 的 targets")
        for spec in workflow.targets:
            self.check_target(spec)
        self._check_entity_routing(workflow.targets)

    def _check_targets_are_used(self, targets: list[TargetSpec], label: str) -> None:
        """沒有逐序列節點的管線（只吃外部告警）給了 targets：不會抓、不會偵測，卻回成功——寫錯就報錯，不默默忽略。"""
        if targets and not self.target_nodes:
            raise WorkflowError(f"{label}：這條管線沒有逐序列的節點（不抓資料、不跑偵測器，只處理推進來的外部告警），"
                                f"targets 不會被用到；告警請從 /api/v1/ingestion/grafana-alerts 或 /anomalies 推進來")

    # ── 每條 target 跑哪些偵測節點 ──

    def _available(self) -> str:
        return "、".join(f"{name}（{operator}）" for name, (operator, _) in self._detectors.items()) or "無"

    def _selected(self, detectors: list[str] | None) -> set[str]:
        """target 的 detectors 選到的節點：寫節點名稱就是那一個，寫算子名稱就是那個算子的所有節點。"""
        if detectors is None:
            return set(self._detectors)
        wanted = set(detectors)
        return {name for name, (operator, _) in self._detectors.items() if name in wanted or operator in wanted}

    def check_target(self, spec: TargetSpec) -> None:
        """載入期檢查 target 的 detectors／overrides。YAML 的 targets 在建立管線時檢查，API 傳進來的在 run() 一開始檢查。"""
        label = f"Workflow {self.workflow.name} 的 target {spec.label}"
        entity = spec.entity or ""  # 沒寫 entity 時名稱是查詢字串（裡面本來就可能有 env="prod" 篩選），不檢查
        if environment_in_entity(entity, self.workflow.environment):
            raise WorkflowError(f"{label}：實體名稱 {entity!r} 夾帶了環境名 {self.workflow.environment!r}；實體請維持 "
                                f"service.name，環境只寫在 workflow 的 environment（改環境名才不會斷基準線）")
        if spec.detectors is not None:
            known = set(self._detectors) | {operator for operator, _ in self._detectors.values()}
            unknown = [d for d in spec.detectors if d not in known]
            if unknown:
                raise WorkflowError(f"{label}：detectors 裡的 {unknown} 對不到任何偵測節點名稱或算子名稱；"
                                    f"可用：{self._available()}")
        selected = self._selected(spec.detectors)
        for name, override in spec.overrides.items():
            if name not in self._detectors:
                raise WorkflowError(f"{label}：overrides 的 {name!r} 不是偵測節點名稱；可用：{self._available()}")
            if name not in selected:
                raise WorkflowError(f"{label}：overrides 指定了 {name}，但 detectors 沒有選它（這組參數永遠不會用到）")
            try:
                self._detectors[name][1].check_override(override)
            except (TypeError, ValueError) as e:
                raise WorkflowError(f"{label}：overrides.{name} 不合法：{e}") from e

    def _check_entity_routing(self, targets: list[TargetSpec]) -> None:
        """節點的 include／exclude_entities 寫了 workflow 裡沒有的實體 -> 報錯。

        止血名單打錯字的後果是「以為停掉了其實沒停」，比啟動失敗更難發現，所以直接擋。
        workflow 沒宣告 targets（全部由 API 傳入）時無從比對，不檢查。
        """
        if not targets:
            return
        entities = {spec.label for spec in targets}
        for name, (_, op) in self._detectors.items():
            for key in ("include_entities", "exclude_entities"):
                unknown = sorted(set(getattr(op, key, None) or ()) - entities)
                if unknown:
                    raise WorkflowError(f"Workflow {self.workflow.name} 節點 {name} 的 {key} 有不存在的實體 {unknown}；"
                                        f"這條 workflow 的實體：{sorted(entities)}")

    def _skip_reason(self, name: str, target: Target) -> str | None:
        """None = 這個偵測節點要對這條序列跑。順序：exclude 優先 -> include -> target 的 detectors。"""
        _, op = self._detectors[name]
        if target.entity in (getattr(op, "exclude_entities", None) or ()):
            return "excluded"
        include = getattr(op, "include_entities", None)
        if include is not None and target.entity not in include:
            return "not-included"
        if name not in self._selected(target.detectors):
            return "not-selected"
        return None

    def run(self, *, now: datetime | None = None, targets: list[TargetSpec] | None = None,
            incident_id: int | None = None, request: BaseModel | None = None,
            strict: bool = False) -> PipelineContext:
        """strict=False（排程）：某條序列失敗記在 errors，其他序列照跑；strict=True（手動動作）：失敗直接丟出。"""
        now = now or datetime.now(timezone.utc)
        self._check_targets_are_used(targets or [], f"Workflow {self.workflow.name}")
        for spec in targets or []:
            self.check_target(spec)  # API 傳進來的 targets 跟 YAML 的一樣要檢查，在做任何事之前擋下
        batch = PipelineContext(pipeline=self.workflow.name, now=now, incident_id=incident_id, request=request,
                                environment=self.workflow.environment)
        if self.workflow.topology is not None:
            batch.topology, batch.topology_explicit = list(self.workflow.topology), True
            batch.edge_provenance.update({(EdgeKind.CALL.value, str(a), str(b)): Provenance(ORIGIN_WORKFLOW)
                                          for a, b in batch.topology})
        if self.workflow.lot_edges is not None:
            batch.lot_edges = [LotEdge(e["from"], e["to"], int(e.get("lots", 1))) for e in self.workflow.lot_edges]
            batch.lot_edges_explicit = True
            batch.edge_provenance.update({(EdgeKind.FLOW.value, e.upstream, e.downstream): Provenance(ORIGIN_WORKFLOW)
                                          for e in batch.lot_edges})
        batch.declared_edges = [e.to_edge() for e in self.workflow.graph]
        # 資料健康度只記排程的輪次（strict=False、不是針對某個事件群、也不是帶請求的手動動作）：手動動作與 RCA 的失敗直接回給呼叫的人。
        # 沒有逐序列節點的管線（只吃外部告警）也記：每一輪一列，證明平台那時候有在跑——告警沒來時分得出是對方沒推還是平台沒跑
        record = not strict and incident_id is None and request is None
        specs = list(targets if targets is not None else self.workflow.targets) if self.target_nodes else []
        try:
            batch = self._run(batch, specs, now, request, strict)
        except Exception as e:
            self._record_health(batch, e, len(specs), record)
            raise
        self._record_health(batch, None, len(specs), record)
        return batch

    def _record_health(self, batch: PipelineContext, error: BaseException | None, targets: int, record: bool) -> None:
        """整輪結束（成功或失敗）時把資料健康度交給 hooks 寫。寫不進去（資料庫連不上）只記 log：
        不讓一輪成功的結果因為旁路紀錄而變成失敗，也不蓋掉原本要丟出去的例外。"""
        if not record:
            return
        batch.run_check = run_status(error, targets=targets, failed_entities=[e.entity for e in batch.errors])
        batch.health_retention_days = self.workflow.health_retention_days
        try:
            self.hooks.record_health(batch)
        except Exception as e:  # noqa: BLE001
            logger.warning("資料健康度寫入失敗（%s 這一輪 %s，沒有留下紀錄）：%s", batch.pipeline, batch.run_check.status, e)

    def _run(self, batch: PipelineContext, specs: list[TargetSpec], now: datetime, request: BaseModel | None,
             strict: bool) -> PipelineContext:
        self.hooks.load_batch(batch)

        if self.target_nodes:
            starved: list[str] = []  # 抓不到資料的序列：資料源說沒資料（NoDataError）、或抓回來是空的
            for spec in specs:
                ctx = PipelineContext(pipeline=self.workflow.name, now=now, target=spec.to_target(), request=request,
                                      environment=self.workflow.environment)
                skipped = {name: reason for name in self._detectors
                           if (reason := self._skip_reason(name, ctx.target)) is not None}
                active = [d for d in self.detector_nodes if d[0] not in skipped]
                # fetch 看這個決定要不要抓訓練窗。不需要歷史資料的節點（requires_training = False：人訂的上下限）不算：
                # 它棄權的那幾輪不會存基準線，算進來的話 fetch 會每一輪都抓訓練窗、其他節點每一輪都重訓，
                # 把正在發生的事故學成常態（11/3 真實資料回放：75 個異常點掉到 51 個）
                ctx.baseline_nodes = [name for name, _ in active
                                      if getattr(self._detectors[name][1], "requires_training", True)]
                loaded = False  # 這條序列的輸入載入完了：之後的錯才算在「抓這條資料流」頭上（載入失敗是資料庫的事）
                try:
                    # 被跳過的節點不載基準線與調參；也不會進 detector_runs，裁決時不算參與者
                    self.hooks.load_target(ctx, active)
                    loaded = True
                    for node, op in self.target_nodes:
                        if node.name in skipped:
                            ctx.log(f"{node.name}:skipped:{skipped[node.name]}")
                            continue
                        ctx = _run_node(node, op, ctx)
                    self.hooks.persist_target(ctx)
                except InfrastructureError as e:
                    if loaded:  # 抓這條資料流時資料源連不上：留下紀錄再讓整輪失敗
                        batch.stream_checks.extend(c for c in [_stream_check(ctx, e)] if c)
                    raise  # 資料庫、資料源連不上：整輪失敗，不要假裝只是這條序列的問題
                except PlatformError as e:
                    if strict:
                        raise
                    batch.stream_checks.extend(c for c in [_stream_check(ctx, e)] if c)
                    batch.errors.append(TargetError(ctx.target.entity, str(e)))
                    if isinstance(e, NoDataError):  # 只有「資料源沒資料」算斷流；偵測器自己查不到東西（NotFound）不算
                        starved.append(ctx.target.entity)
                    continue
                except Exception as e:  # noqa: BLE001  一條序列的演算法 bug（ValueError、KeyError…）不該讓其他健康序列也沒結果
                    if strict:
                        raise
                    logger.exception("target %s 失敗：%s", ctx.target.entity, e)
                    batch.stream_checks.extend(c for c in [_stream_check(ctx, e)] if c)
                    batch.errors.append(TargetError(ctx.target.entity, f"{type(e).__name__}: {e}"))
                    continue
                batch.stream_checks.extend(c for c in [_stream_check(ctx, None)] if c)
                if ctx.raw_df is not None and ctx.raw_df.empty:
                    if strict:  # 手動動作：跟資料源說「沒資料」一樣是 404，不要同一件事一邊 404、一邊 503 斷流
                        raise NoDataError(f"{ctx.target.entity}：這段時間抓回來是空的（資料源回 200 但零筆）")
                    starved.append(ctx.target.entity)
                batch.anomalies.extend(ctx.anomalies)
                batch.detected_anomalies.extend(ctx.anomalies)
                batch.audit.extend(ctx.audit)
                batch.errors.extend(ctx.errors)  # optional 節點在這條序列留下的錯誤
                batch.result.update(ctx.result)
            if specs and starved and len(starved) / len(specs) >= self.workflow.max_failed_target_ratio:
                # 全部（或太多）序列都抓不到資料：不是「今天剛好沒異常」，是資料進不來（PVC 滿、APM 寫不進去、索引不見）。
                # 當成基礎設施錯誤：這一輪不算成功，watchdog 不續命，排程的備援通報會響
                shown = "、".join(starved[:10]) + ("…" if len(starved) > 10 else "")
                raise InfrastructureError(f"資料斷流：{len(starved)}/{len(specs)} 條序列抓不到資料（{shown}），"
                                          f"達到 max_failed_target_ratio={self.workflow.max_failed_target_ratio:g}，這一輪不算成功")

        for node, op in self.batch_nodes:
            batch = _run_node(node, op, batch)
        self.hooks.persist_batch(batch)
        for node, op in self.after_persist_nodes:  # 通知在寫入成功之後才送：送失敗時事件群已是進行中，下一輪心跳補送
            batch = _run_node(node, op, batch)
        self.hooks.finalize_batch(batch)  # 通知都送出去了才記「已通知」與結案
        for node, op in self.after_finalize_nodes:  # 心跳最後送：結案寫不進去也算這一輪失敗，不續命
            batch = _run_node(node, op, batch)
        return batch


def execute_operator(name: str, ctx: PipelineContext, **params: Any) -> PipelineContext:
    """單一算子直接執行（手動動作用，例如核准欄位對照）。"""
    return OperatorRegistry.create(name, params).execute(ctx)
