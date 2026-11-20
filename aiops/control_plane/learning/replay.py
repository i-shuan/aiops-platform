"""學習閉環共用的「現抓現算」：對任意一條序列（不一定是這次請求的 target）抓一段歷史，訓練只用時窗之前的資料，
重跑偵測。黃金事故防護罩（admin.proposal_backtest）、序列體檢
（admin.series_profile）都走這裡。

- 抓資料一律走 stages/ingestion/fetch.fetch_target_frame（跟正式偵測同一條路徑：欄位對照、同儕、lag）。
  長時窗切成多段抓再接起來，避免碰到資料源單次查詢的點數上限（Prometheus 約 11000 點、ES 的 bucket 上限）。
- 訓練排除窗跟正式偵測同一套：StorageHooks 算的這個實體的歷史事故與維護窗，再加上經驗庫裡這個實體的黃金事故；
  套用由偵測器基底的 _training_frame 做（同一套「剩太少就退回全部資料」規則）。
- 回測不存基準線：每次都現抓現算（線上偵測的基準線持久化不受影響）。
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from aiops.engine.context import PipelineContext, Target, TargetError, TraceRow, TrainingExclusion
from aiops.engine.errors import BadRequest, NoDataError, NotFound
from aiops.engine.params import ANY
from aiops.engine.pipeline import load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.interfaces.runner import workflow_path
from aiops.sources.query.trace import parse_trace_query
from aiops.stages.detectors.base import DetectorOperator
from aiops.stages.ingestion.fetch import TRACE_SOURCE, fetch_target_frame
from aiops.storage.hooks import EXCLUSION_MARGIN, StorageHooks
from aiops.storage.repository import Knowledge, Mappings, Traces

DEFAULT_TRAIN_MINUTES = 1440
DEFAULT_STEP_SECONDS = 60
CHUNK_POINTS = 10000  # 單次查詢最多抓這麼多點（Prometheus 上限約 11000）


@dataclass(frozen=True)
class Series:
    """一條要重抓的序列：哪個實體、哪個資料源、哪個查詢（就是 AnomalyEvent 的 entity／source_type／metric）。"""

    entity: str
    source_type: str
    query: str

    def to_dict(self) -> dict[str, str]:
        return {"entity": self.entity, "source_type": self.source_type, "query": self.query}


def incident_series(incident: dict[str, Any], entity: str | None = None) -> list[Series]:
    """事件群快照裡出現過的序列（去重、保持順序）；entity 給了就只留那個實體的。外部匯入的異常沒有資料源，略過。"""
    seen: dict[tuple, Series] = {}
    for e in incident.get("events", []):
        if not e.get("source_type") or not e.get("metric") or (entity is not None and e.get("entity") != entity):
            continue
        s = Series(e["entity"], e["source_type"], e["metric"])
        seen.setdefault((s.entity, s.source_type, s.query), s)
    return list(seen.values())


def fetch_settings(ctx: PipelineContext, pipeline: str) -> tuple[int, int]:
    """這條管線 ingestion.fetch 的 (訓練窗分鐘, step 秒)：重跑時用跟正式偵測一樣長的訓練窗與同樣的取樣間隔，
    分數才跟線上同尺度。讀不到 workflow 或沒有 fetch 節點就用預設（1 天、60 秒）並記錄。"""
    try:
        wf = load_workflow(workflow_path(pipeline))
    except (NotFound, BadRequest) as e:
        ctx.log(f"replay:{pipeline}:no-workflow:default-train-window:{e}")
        return DEFAULT_TRAIN_MINUTES, DEFAULT_STEP_SECONDS
    node = next((n for n in wf.nodes if n.operator == "ingestion.fetch"), None)
    if node is None:
        return DEFAULT_TRAIN_MINUTES, DEFAULT_STEP_SECONDS
    p = node.params
    return int(p.get("train_lookback_minutes") or p.get("lookback_minutes") or DEFAULT_TRAIN_MINUTES), \
        int(p.get("step_seconds") or DEFAULT_STEP_SECONDS)


def series_context(ctx: PipelineContext, series: Series, pipeline: str) -> PipelineContext:
    """給 fetch_target_frame 用的 ctx：target 換成這條序列，欄位對照（與 trace 時序）自己載入。
    跟請求的 target 是同一條序列時直接沿用 ctx（保留同儕設定）。errors／audit 跟原 ctx 共用，抓不到的序列看得到。"""
    t = ctx.target
    if t is not None and (t.entity, t.source_type, t.query) == (series.entity, series.source_type, series.query):
        return replace(ctx, pipeline=pipeline)
    field_maps = dict(ctx.field_maps)
    fields = Mappings.approved_fields(series.source_type)
    if fields:
        field_maps[series.source_type] = fields
    return replace(ctx, pipeline=pipeline, target=Target(series.source_type, series.query, series.entity),
                   field_maps=field_maps, training_exclusions=[], raw_df=None, train_df=None)


def _load_traces(sctx: PipelineContext, start: datetime, end: datetime) -> None:
    service, _field = parse_trace_query(sctx.target.query)
    rows = Traces.window([service], start.replace(tzinfo=None), end.replace(tzinfo=None))
    sctx.trace_rows = {service: [TraceRow(r["service"], as_utc(r["ts"]), r["count"], r["error_rate"], r["p95_ms"])
                                 for r in rows]}


def fetch_span(sctx: PipelineContext, start: datetime, end: datetime, step: int,
               lags: list[int] | tuple[int, ...] = ()) -> pd.DataFrame:
    """抓 [start, end]（**含 end 那一點**），超過 CHUNK_POINTS 點就切段抓再接起來（重疊的邊界點去重）。
    某一段沒資料（資料源丟 NoDataError）只略過那段；整段都沒資料才丟 NoDataError。

    資料源的區間查詢是含頭不含尾（Elasticsearch range lt、Prometheus 對齊後也不含 end 那個桶），所以多抓一個 step：
    呼叫端（replay_window 的 ts <= end、黃金事故時窗、誤報時窗）都把 end 當成最後一個點。以前少抓這一點，
    只有一分鐘的誤報事件群（start == end）會變成「時窗內沒有資料」而退回固定 ×1.2（破壞性驗收場景 7）。"""
    if sctx.target is not None and sctx.target.source_type == TRACE_SOURCE:
        _load_traces(sctx, start, end)
    chunk = timedelta(seconds=step * CHUNK_POINTS)
    last = pd.Timestamp(as_utc(end))
    end = end + timedelta(seconds=step)  # 多抓一個 step 讓不含尾的資料源也給出 end 那一點；含尾的資料源（mock）多出來的點在下面切掉
    frames, cursor = [], start
    while cursor < end:
        stop = min(cursor + chunk, end)
        try:
            frames.append(fetch_target_frame(sctx, cursor, stop, step, lags))
        except NoDataError as e:
            sctx.log(f"replay:no-data:{cursor.isoformat()}~{stop.isoformat()}:{e}")  # 中間斷一段不影響其他段
        cursor = stop
    frames = [f for f in frames if not f.empty]
    if not frames:
        raise NoDataError(f"{sctx.target.entity if sctx.target else '?'} 在 {start.isoformat()} ~ {end.isoformat()} 沒有資料")
    df = pd.concat(frames, ignore_index=True).drop_duplicates("ts", keep="last").sort_values("ts")
    df = df[pd.to_datetime(df["ts"], utc=True) <= last]  # 一律 [start, end] 含兩端，不管資料源含不含尾
    return df.reset_index(drop=True)


def exclusion_windows(ctx: PipelineContext, pipeline: str, entity: str, now: datetime,
                      dept: str | None = None) -> list[TrainingExclusion]:
    """跟正式偵測同一套訓練排除窗（StorageHooks：這個實體近 35 天的事故、維護窗；被標成誤報的事故不排除），
    再加上經驗庫裡這個實體所有的黃金事故（可能早於 35 天、或發生在別條管線）。"""
    probe = replace(ctx, pipeline=pipeline, now=as_utc(now))
    out = StorageHooks().exclusions_for(probe, entity, dept)
    for k in Knowledge.golden("*", entity):
        out.append(TrainingExclusion(as_utc(k["window_start"]) - EXCLUSION_MARGIN,
                                     as_utc(k["window_end"]) + EXCLUSION_MARGIN, f"golden {k['incident_uid']}"))
    return out


@dataclass
class Replay:
    """重跑一段時窗的結果。scores／flags 只含時窗內的點（前面 warmup 的點不計）。"""

    train_points: int
    ts: list[pd.Timestamp]
    scores: list[float]
    flags: list[bool]

    @property
    def anomalies(self) -> int:
        return int(sum(self.flags))

    @property
    def max_score(self) -> float | None:
        finite = [s for s in self.scores if math.isfinite(s)]
        return max(finite) if finite else None


def replay_window(ctx: PipelineContext, cls: type[DetectorOperator], params: dict[str, Any], df: pd.DataFrame,
                  start: datetime, end: datetime, exclusions: list[TrainingExclusion]) -> Replay | str:
    """用 [start 之前] 的資料（扣掉排除窗）訓練、對 [start, end] 偵測。前面多帶 warmup 個點讓滾動窗湊滿，但不計分。
    回傳 Replay；判不了（訓練點不足、演算法棄權、時窗內沒資料）回傳原因字串。"""
    algo = cls(**params)
    ts = pd.to_datetime(df["ts"], utc=True)
    begin, stop = int((ts < pd.Timestamp(start)).sum()), int((ts <= pd.Timestamp(end)).sum())
    if stop <= begin:
        return "時窗內沒有資料"
    train = algo._training_frame(replace(ctx, training_exclusions=exclusions), df.iloc[:begin].reset_index(drop=True))
    points = algo.training_points(train) if not train.empty else 0
    if points < 1:  # 沒有最少點數的門檻（同正式偵測）；一個點都沒有才判不了
        return "時窗前沒有訓練資料"
    offset = min(algo.warmup_points(), begin)
    window = df.iloc[begin - offset:stop].reset_index(drop=True)
    if not algo.can_evaluate(window):
        return "演算法在這段資料上棄權（can_evaluate=False）"
    result = algo.fit(train).detect(window)
    return Replay(points, list(pd.to_datetime(window["ts"], utc=True).iloc[offset:]),
                  [float(s) for s in result.scores[offset:]], [bool(f) for f in result.is_anomaly[offset:]])


def workflow_layers(ctx: PipelineContext, detector: str, pipeline: str, entity: str) -> tuple[dict, dict, dict]:
    """正式偵測疊在核准調參之上的兩層：提案所屬管線裡這個偵測器節點的 YAML params（鎖定鍵）與該實體 target 的 overrides。
    回測不疊這兩層，顯示的效果在正式偵測裡就不會生效。提案不限定管線（*）就沒有這兩層。
    回 (節點 params, overrides, 給回測結果看的說明)。"""
    info: dict = {"pipeline": pipeline, "node": None, "locked": [], "overrides": {}, "note": ""}
    if pipeline == ANY:
        info["note"] = "提案不限定管線：沒有節點 YAML 與 target overrides 可疊"
        return {}, {}, info
    try:
        wf = load_workflow(workflow_path(pipeline))
    except (NotFound, BadRequest) as e:
        info["note"] = f"讀不到 workflow {pipeline}（{e}）：回測沒有疊節點 YAML 與 overrides"
        ctx.errors.append(TargetError(f"proposal:{detector}", info["note"]))  # 回測照跑，但要讓人看到少了一層
        return {}, {}, info
    nodes = [n for n in wf.nodes if n.operator == detector]
    if not nodes:
        info["note"] = f"workflow {pipeline} 沒有 {detector} 的節點：這個提案核准後不會被任何節點用到"
        return {}, {}, info
    node = nodes[0]
    if len(nodes) > 1:
        info["note"] = f"同一個演算法掛了 {len(nodes)} 個節點，這裡用 {node.name} 的 YAML params；其他：{[n.name for n in nodes[1:]]}"
    yaml_params = OperatorRegistry.create(node.operator, node.params).params  # 跟引擎同一條路：扣掉路由與節點行為參數
    target = next((t for t in wf.targets if entity != ANY and t.label == entity), None)
    overrides = dict(target.overrides.get(node.name, {})) if target else {}
    info.update(node=node.name, overrides=overrides)
    return yaml_params, overrides, info
