"""序列體檢（admin.series_profile）：這條序列有沒有日／週週期？沒有就建議別用時段基準線。

detector.hourly_baseline 把一週切成 168 個時段、每個時段學自己的平均與標準差；序列沒有日週期也沒有週週期時，
168 個時段只是把同一個分佈切成 168 份，每份的樣本少、標準差被低估，反而比全段統計吵（誤報變多）。

算法：撈 days 天（預設 14）、每 step_seconds 一點的序列，排到等間隔的格子上（缺的點留空），
- r(24h)、r(168h)：lag = 86400／604800 秒 ÷ step 個點的自相關（Pearson，只用兩邊都有值的配對；step 感知，
  換 step 不用改 lag）。配對數不到一天的點數就不算（None），不硬給一個數字。
- 偏態、峰度（超額峰度，常態 = 0）：給人判斷要不要改用 mad／分位數類的偵測器，不參與決策。
- 兩個都 < min_acf（預設 0.3）→ 建議把 detector.hourly_baseline 從這個 target 排除。任一個算不出來就不建議。

每條 target 跑哪些偵測節點是 workflow YAML 決定的（target.detectors、節點 exclude_entities），資料庫沒有
「核准後自動生效」的機制，所以這個建議寫成 advisory 提案（source=profile、status=advisory、params 空），
admin.proposal_approve 對它回 409 並說明要改 YAML。不自動改 YAML。
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from aiops.control_plane.learning.replay import fetch_span
from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest, NotFound
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import SeriesProfile
from aiops.engine.types import as_utc
from aiops.interfaces.runner import workflow_path
from aiops.storage.repository import ActiveConfigs, Proposals

HOURLY = "detector.hourly_baseline"
DAY, WEEK = 86400, 7 * 86400


def _round(x):
    return None if x is None or pd.isna(x) else round(float(x), 4)


def profile(df: pd.DataFrame, step: int, docs: pd.Series | None = None) -> dict:
    """等間隔化後算 r(24h)、r(168h)、偏態、峰度。docs（每桶文件數）給了就把文件數為 0 的桶當成沒資料（NaN），
    它們不參與自相關的配對，也不算進 points。目前沒有資料源提供 docs（原本的 Elasticsearch 文件數查詢隨直連 ES 一起移除，
    2026-11-20）；會把空桶補成 0 的資料源接進來時，由它提供。"""
    s = df.set_index(pd.to_datetime(df["ts"], utc=True))["value"].astype(float).resample(f"{step}s").mean()
    empty = 0
    if docs is not None:
        c = docs.resample(f"{step}s").sum().reindex(s.index).fillna(0.0)
        empty = int(((c <= 0) & s.notna()).sum())
        s = s.mask(c <= 0)
    out: dict = {"points": int(s.notna().sum()), "grid_points": len(s), "step_seconds": step,
                 "empty_buckets": empty, "coverage": _round(s.notna().mean()) if len(s) else None,
                 "skewness": _round(s.skew()), "kurtosis": _round(s.kurt())}
    for name, seconds in (("r_24h", DAY), ("r_168h", WEEK)):
        lag = seconds // step
        pairs = int((s.notna() & s.shift(lag).notna()).sum()) if lag < len(s) else 0
        out[f"{name}_lag_points"], out[f"{name}_pairs"] = lag, pairs
        out[name] = _round(s.autocorr(lag)) if pairs >= DAY // step else None
    return out


def _routing(pipeline: str, entity: str) -> tuple[list[str], list[str]]:
    """(這條管線的 hourly_baseline 節點, 其中已經不會對這個實體跑的節點)。"""
    try:
        wf = load_workflow(workflow_path(pipeline))
    except (NotFound, BadRequest) as e:
        raise BadRequest(f"讀不到 workflow {pipeline}：{e}") from e
    nodes = [n for n in wf.nodes if n.operator == HOURLY]
    target = next((t for t in wf.targets if t.label == entity), None)
    skipped = []
    for n in nodes:
        op = OperatorRegistry.create(n.operator, n.params)
        excluded = entity in op.exclude_entities or (op.include_entities is not None and entity not in op.include_entities)
        not_listed = target is not None and target.detectors is not None and not ({n.name, n.operator} & set(target.detectors))
        if excluded or not_listed:
            skipped.append(n.name)
    return [n.name for n in nodes], skipped


@OperatorRegistry.register("admin.series_profile")
class SeriesProfileOp(BaseOperator):
    def __init__(self, min_acf: float = 0.3, min_coverage: float = 0.5):
        if not 0 < min_acf < 1:
            raise TypeError("min_acf 必須在 0 到 1 之間")
        if not 0 < min_coverage <= 1:
            raise TypeError("min_coverage 必須在 0 到 1 之間")
        self.min_acf, self.min_coverage = float(min_acf), float(min_coverage)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request
        if not isinstance(req, SeriesProfile):
            raise BadRequest("需要 SeriesProfile 請求")
        step = req.step_seconds
        epoch = int(as_utc(ctx.now).timestamp())
        end = datetime.fromtimestamp(epoch - epoch % step, tz=as_utc(ctx.now).tzinfo)
        start = end - timedelta(days=req.days)
        df = fetch_span(ctx, start, end, step)
        stats = profile(df, step)
        stats.update(start=df["ts"].iloc[0].isoformat(), end=df["ts"].iloc[-1].isoformat(), days=req.days,
                     min_acf=self.min_acf, min_coverage=self.min_coverage)
        r24, r168 = stats["r_24h"], stats["r_168h"]
        nodes, skipped = _routing(req.pipeline, req.entity)
        result: dict = {"profile": stats, "proposal": None, "hourly_nodes": nodes, "already_skipped": skipped}
        if stats["coverage"] is not None and stats["coverage"] < self.min_coverage:
            result["advice"] = (f"資料不足：{req.days} 天裡只有 {stats['coverage']:.0%} 的時段有資料（空桶 {stats['empty_buckets']} 個，"
                                f"門檻 {self.min_coverage:.0%}），不算週期、不給建議，維持既有算子")
        elif r24 is None or r168 is None:
            result["advice"] = f"資料不足以算 {'r(24h)' if r24 is None else 'r(168h)'}（配對不到一天），不給建議"
        elif r24 >= self.min_acf or r168 >= self.min_acf:
            result["advice"] = f"有週期（r24h={r24}、r168h={r168}，至少一個 ≥ {self.min_acf}），時段基準線適用，不建議排除"
        elif not nodes:
            result["advice"] = f"沒有週期，但 workflow {req.pipeline} 沒有 {HOURLY} 節點，不需要排除"
        elif set(nodes) == set(skipped):
            result["advice"] = f"沒有週期；{HOURLY} 節點 {nodes} 本來就不對 {req.entity} 跑，不需要再改"
        else:
            result.update(self._advise(req, stats, [n for n in nodes if n not in skipped]))
        ctx.result = result
        return ctx

    def _advise(self, req: SeriesProfile, stats: dict, nodes: list[str]) -> dict:
        existing = Proposals.with_status(HOURLY, req.pipeline, req.entity, ["advisory"])
        if existing:
            return {"proposal": existing[-1], "existing": True, "advice": existing[-1]["reason"]}
        reason = (f"序列體檢：{req.entity} 近 {req.days} 天 r(24h)={stats['r_24h']}、r(168h)={stats['r_168h']}，"
                  f"都 < {self.min_acf}，沒有日／週週期；偏態 {stats['skewness']}、峰度 {stats['kurtosis']}。"
                  f"建議把 {HOURLY} 從這個 target 排除：在 workflow {req.pipeline} 的節點 {nodes} 的 params 加 "
                  f"exclude_entities: [{req.entity}]，或在該 target 寫 detectors 列出要跑的節點（不含 {nodes}）。"
                  "這是 advisory，資料庫核准不會生效，要改 YAML。")
        proposal = Proposals.create({
            "source": "profile", "detector_name": HOURLY, "params": {}, "reason": reason, "proposer": req.proposer,
            "pipeline": req.pipeline, "entity": req.entity, "status": "advisory",
            "base_version": ActiveConfigs.version(HOURLY, req.pipeline, req.entity), "backtest": None,
            "approved_by": None})
        return {"proposal": proposal, "existing": False, "advice": reason}
