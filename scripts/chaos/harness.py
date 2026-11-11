"""場景共用的接線：真實 PostgreSQL（建表 + migration 0005）、把替身 ES 裝進平台的資料源註冊表、偵測回放、RCA。

平台程式一行不改：只是把 aiops.sources._SPECS["elasticsearch"] 換成「讀記憶體世界」的子類。
"""
from __future__ import annotations

import io
import json
import os
import sys
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore")

PG_URL = os.environ.get("CHAOS_PG_URL", "postgresql+psycopg://postgres:pgchaos@127.0.0.1:55433/chaos")
MIGRATION = ROOT / "migrations" / "postgres" / "0005_knowledge_golden_incidents.sql"

from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow, load_workflow  # noqa: E402
from aiops.interfaces.runner import load_all, workflow_path  # noqa: E402
from aiops.sources.elasticsearch import ElasticsearchSpec  # noqa: E402
from aiops.storage import db, models  # noqa: E402
from aiops.storage.hooks import StorageHooks  # noqa: E402
from aiops.storage.repository import Incidents  # noqa: E402

from scripts.chaos.es import ChaosES  # noqa: E402
from scripts.chaos.world import World  # noqa: E402

load_all()


def fresh_db(apply_migration: bool = True) -> str:
    """真實 PostgreSQL：程式自動建表（跟全新資料庫一樣）再套 migration 0005（context 轉 JSONB、GIN 索引）。"""
    import psycopg

    db.configure(PG_URL)
    models.reset_all()
    if apply_migration:
        with psycopg.connect(PG_URL.replace("postgresql+psycopg", "postgresql"), autocommit=True) as c:
            c.execute(MIGRATION.read_text(encoding="utf-8"))
    return PG_URL


@contextmanager
def es_for(world: World, trace_lo: datetime | None = None, trace_hi: datetime | None = None):
    """把平台的 elasticsearch 資料源指到這個世界。"""
    import aiops.sources as sources

    es = ChaosES(world, trace_lo, trace_hi)
    client = httpx.Client(transport=httpx.MockTransport(es.handler))

    class _Spec(ElasticsearchSpec):
        @classmethod
        def from_env(cls):
            return cls("http://chaos-es", client=client)

        @classmethod
        def configured(cls):
            return True

    original = sources._SPECS["elasticsearch"]
    sources._SPECS["elasticsearch"] = _Spec
    try:
        yield es
    finally:
        sources._SPECS["elasticsearch"] = original


def metric_query(svc: str, kind: str = "latency") -> str:
    """kind=latency：每分鐘 summary.sum 的平均（跟真實 ES target 一樣）；errors：每分鐘失敗請求數；
    latency_mean：Σsummary.sum ÷ Σvalue_count = 每筆請求的平均延遲（µs）。summary.sum 是當分鐘總耗時，數值跟請求數成正比，
    每分鐘 0～3 筆的低流量服務某分鐘多來幾筆就被當成延遲暴增（場景 2 注入前的誤報），低流量 target 要用 latency_mean。"""
    if kind == "errors":
        return json.dumps({"index": "metrics-apm.transaction.1m-*", "agg": "sum", "value_field": "transaction.duration.summary.value_count",
                           "filter": [{"term": {"service.name": svc}}, {"term": {"event.outcome": "failure"}}]})
    if kind == "latency_mean":
        return json.dumps({"index": "metrics-apm.transaction.1m-*", "agg": "sum", "value_field": "transaction.duration.summary.sum",
                           "divide_by": "transaction.duration.summary.value_count", "filter": {"term": {"service.name": svc}}})
    return json.dumps({"index": "metrics-apm.transaction.1m-*", "agg": "avg", "value_field": "transaction.duration.summary.sum",
                       "filter": {"term": {"service.name": svc}}})


def targets(names: list[str], signal: str = "latency", kind: str = "latency") -> list[TargetSpec]:
    return [TargetSpec(source_type="elasticsearch", query=metric_query(n, kind), entity=n, attrs={"dept": "payments", "signal": signal})
            for n in names]


def detection_workflow(workflow: str, tgts: list[TargetSpec]) -> Workflow:
    base = load_workflow(workflow_path(workflow))
    return Workflow.model_validate(base.model_dump() | {"targets": tgts})


@dataclass
class Replay:
    rounds: int = 0
    anomalies: int = 0
    errors: list[str] = field(default_factory=list)
    first_alarm: dict[str, datetime] = field(default_factory=dict)


def replay(wf: Workflow, lo: datetime, hi: datetime, step_min: int = 5) -> Replay:
    """每 step_min 分鐘一輪跑偵測管線（now 是回放時間），事件群寫進資料庫。"""
    pipe = Pipeline(wf, StorageHooks())
    out = Replay()
    for now in pd.date_range(lo, hi, freq=f"{step_min}min", tz="UTC"):
        ctx = pipe.run(now=now.to_pydatetime())
        out.rounds += 1
        out.anomalies += len(ctx.anomalies)
        out.errors += [f"{now:%H:%M} {e.entity}: {e.message[:100]}" for e in ctx.errors]
        for a in ctx.anomalies:
            out.first_alarm.setdefault(a.entity, a.ts)
    return out


def incidents() -> list[dict]:
    return sorted(Incidents.created_since(datetime(2020, 1, 1, tzinfo=timezone.utc)), key=lambda r: r["id"])


def pick(incs: list[dict], entity: str, not_before: datetime) -> dict | None:
    """涉及 entity、且開始時間不早於注入時間的事件群（注入之前冒出來的算誤報，不能拿來當被測對象）。"""
    for i in incs:
        if entity in i["incident"]["entities"] and datetime.fromisoformat(i["incident"]["start"]) >= not_before:
            return i
    return None


def before(incs: list[dict], entity: str, not_before: datetime) -> list[tuple[int, str]]:
    """注入之前就出現的事件群（沒有事故卻告警 = 誤報）。"""
    return [(i["id"], i["incident"]["start"][11:16]) for i in incs
            if entity in i["incident"]["entities"] and datetime.fromisoformat(i["incident"]["start"]) < not_before]


# ── RCA 引擎選擇（新舊並排）──
# legacy = rca_report（舊判斷層 rca.root_cause.evidence，預設，確定性、不需要 LLM）
# llm = rca_report_llm（呼叫圖＋事實＋證據交給 LLM 判斷）。原本的第三個引擎 cognitive（rca.root_cause.bayes）已移除：
# 它前面必須靠 LLM 抽證據，當不了離線備援（程式在 git 歷史與 tag 1.1）
RCA_WORKFLOWS = {"legacy": "rca_report", "llm": "rca_report_llm"}
_RCA = {"mode": "legacy", "llm": "faithful"}   # run.py 用 use_rca() 設；llm="yaml" = 不換供應商，照 YAML 原樣跑
RCA_LOG: list[dict] = []                        # 每次 rca() 的關鍵數字（--compare 並排用）；呼叫端自己清


def use_rca(mode: str, llm: str = "faithful") -> None:
    if mode not in RCA_WORKFLOWS:
        raise ValueError(f"--rca 只能是 {'／'.join(RCA_WORKFLOWS)}，收到 {mode!r}")
    _RCA.update(mode=mode, llm=llm)


def _rca_summary(name: str, incident_id: int, ctx, out: dict) -> dict:
    cands = out.get("root_cause", {}).get("candidates", [])
    return {"workflow": name, "incident": incident_id, "status": getattr(ctx, "rca_status", ""),
            "reason": getattr(ctx, "rca_undetermined_reason", ""), "algorithm": out.get("root_cause", {}).get("algorithm", ""),
            "top": [(c["entity"], c["score"], (c.get("attribution") or {}).get("resource")) for c in cands[:3]],
            "runbook": (out.get("runbook") or {}).get("id", "(無)"),
            "evidence": [(e.kind, e.entity) for e in getattr(ctx, "rca_evidence", [])], "facts": len(getattr(ctx, "rca_facts", [])),
            "judge_log": [a for a in getattr(ctx, "audit", []) if a.startswith("root_cause.llm")]}


def rca(incident_id: int, mutate=None, *, mode: str | None = None, llm: str | None = None) -> dict:
    """完整 RCA 報告（真實 ingestion.incident_traces／incident_logs、根因、段落、Runbook、三行摘要）。
    mode 不給就用 use_rca() 設的（預設 legacy = rca_report）。llm 時 LLM 節點換成腳本化假 LLM（scripted_llm.py，
    模式 llm，預設 faithful；判斷節點接對應的 judge_* 模式），llm="yaml" 時照 YAML 的供應商。"""
    mode, llm = mode or _RCA["mode"], llm or _RCA["llm"]
    name = RCA_WORKFLOWS[mode]
    wf = load_workflow(workflow_path(name))
    if mode != "legacy" and llm != "yaml":
        from scripts.chaos import scripted_llm

        scripted_llm.wire(wf, llm)
    if mutate:
        mutate(wf)
    ctx = Pipeline(wf, StorageHooks()).run(incident_id=incident_id)
    out = ctx.to_result()
    out["_sections"] = {s["key"]: s["body"] for s in out.get("sections", [])}
    out["_errors"] = [f"{e.entity}: {e.message[:120]}" for e in ctx.errors]
    out["_ctx"] = ctx
    RCA_LOG.append(_rca_summary(name, incident_id, ctx, out))
    return out


# ── 場景結果 ──
@dataclass
class Check:
    what: str
    expected: str
    actual: str
    ok: bool
    dead: bool = False   # 已知死區：預期這條不會過（等同 pytest 的 xfail(strict=True)），過了反而要報


@dataclass
class Result:
    n: int | str
    title: str
    injected: str
    checks: list[Check] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    crash: str = ""
    blocked: str = ""     # 前提不在：不是通過也不是平台失敗，明講沒跑

    def check(self, what: str, expected: str, actual, ok: bool, dead: bool = False) -> bool:
        self.checks.append(Check(what, expected, str(actual), bool(ok), dead))
        return bool(ok)

    @property
    def verdict(self) -> str:
        if self.crash:
            return "崩潰"
        if self.blocked:
            return f"未執行（{self.blocked}）"
        if not all(c.ok for c in self.checks if not c.dead):
            return "未通過"
        dead = [c for c in self.checks if c.dead]
        if not dead:
            return "通過"
        # 死區斷言照樣列在表裡、照樣算沒過；哪天它過了，代表死區不見了，標記要拿掉（不准默默變綠）
        return "死區消失（請移除標記）" if any(c.ok for c in dead) else "已知死區"
