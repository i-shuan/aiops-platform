"""平台看門狗（aiops/pipelines/watchdog/platform_watchdog.yaml）在模擬的平台紀錄上回放：誤報幾次、故障多久抓到、哪些抓不到。

平台還沒上線，沒有真實的 pipeline_runs 與 webhook 推入紀錄，所以紀錄是這支腳本依下面的假設產生的（不是量出來的）：
  - 被監控的管線 metrics_only 每 5 分鐘一輪，啟動延遲 0～90 秒；每輪 0.2% 機率整輪失敗、0.3% 機率那一輪沒跑（排程偶發漏跑）
  - 三個團隊的心跳告警：team-a 每 5 分鐘、team-c 每 10 分鐘、team-b 每 15 分鐘（文件要求的上限），送達延遲 0～60 秒，
    每一筆 1% 機率掉了（網路、Grafana 重啟）
  - 前 14 天只有正常紀錄（給稀疏計數學發生率），後 14 天是評估期，評估期中間注入 6 個故障（FAULTS）
看門狗用出貨的 YAML 原樣跑（只多加三個團隊的心跳 target，寫法跟 YAML 註解的範例一樣），參數一個都沒改。
每 15 分鐘跑一輪（偵測看最近 180 分鐘，所以每一格都會被看到），延遲的解析度是 15 分鐘。

用法：python scripts/backtest/watchdog_replay.py --workdir <資料夾> [--seeds 42 7 2026] [--eval-days 14]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

STEP = timedelta(minutes=5)
ROUND = timedelta(minutes=15)
TRAIN_DAYS = 14
P_FAIL, P_SKIP, P_DROP = 0.002, 0.003, 0.01
TEAMS = {"team-a": 5, "team-c": 10, "team-b": 15}  # 心跳間隔（分鐘）
GRACE = timedelta(minutes=60)  # 故障結束後這段時間內的告警還算這個故障的


@dataclass(frozen=True)
class Fault:
    name: str
    entity: str
    node: str
    day: float
    hours: float
    kind: str
    expect: str  # 預期：caught／miss（事先寫下的預期，不是看了結果才填）


FAULTS = [
    Fault("F1 排程停 2 小時", "metrics_only", "stopped_rounds", 2.4, 2, "stop_rounds", "caught"),
    Fault("F2 連續 6 輪整輪失敗（30 分鐘）", "metrics_only", "failures", 4.6, 0.5, "fail_burst", "caught"),
    Fault("F3 每 30 分鐘失敗 1 輪、持續 4 小時", "metrics_only", "failures", 6.3, 4, "fail_sporadic", "caught"),
    Fault("F4 team-a 心跳停 3 小時（5 分鐘一筆）", "team-a", "stopped_heartbeats", 8.5, 3, "hb_stop", "caught"),
    Fault("F5 team-b 心跳停 2 小時（15 分鐘一筆）", "team-b", "stopped_heartbeats", 10.2, 2, "hb_stop", "caught"),
    Fault("F6 team-c 心跳只送到一半、持續 6 小時", "team-c", "stopped_heartbeats", 11.7, 6, "hb_half", "miss"),
]


def build_records(start: datetime, end: datetime, rng: random.Random, eval_start: datetime) -> tuple[list[dict], list[dict]]:
    windows = [(f, eval_start + timedelta(days=f.day), eval_start + timedelta(days=f.day, hours=f.hours)) for f in FAULTS]

    def active(kind: str, entity: str, t: datetime) -> Fault | None:
        return next((f for f, a, b in windows if f.kind == kind and f.entity == entity and a <= t < b), None)

    runs, t, k = [], start, 0
    while t < end:
        k += 1
        fault_start = next((a for f, a, b in windows if f.kind in ("fail_burst", "fail_sporadic") and a <= t < b), None)
        stopped = active("stop_rounds", "metrics_only", t) is not None
        failed = rng.random() < P_FAIL
        if active("fail_burst", "metrics_only", t):
            failed = True
        if active("fail_sporadic", "metrics_only", t) and fault_start is not None and int((t - fault_start) / STEP) % 6 == 0:
            failed = True
        if not stopped and rng.random() >= P_SKIP:
            runs.append({"pipeline": "metrics_only", "run_at": (t + timedelta(seconds=rng.uniform(0, 90))).replace(tzinfo=None),
                         "status": "failed" if failed else "ok", "failure_type": "infrastructure_error" if failed else "",
                         "message": "", "targets": 1, "failed_targets": 0})
        t += STEP
    beats, i = [], 0
    for team, minutes in TEAMS.items():
        t = start + timedelta(seconds=rng.uniform(0, 60 * minutes))
        while t < end:
            if active("hb_stop", team, t) is None and rng.random() >= P_DROP and not (active("hb_half", team, t) and rng.random() < 0.5):
                at = (t + timedelta(seconds=rng.uniform(0, 60))).replace(tzinfo=None)
                beats.append({"pipeline": "it_otel", "source": team, "external_id": f"hb-{i}", "entity": "heartbeat",
                              "metric": "AIOpsHeartbeat", "ts": at, "score": 0.0, "value": None, "attrs": {},
                              "received_at": at, "status": "heartbeat"})
                i += 1
            t += timedelta(minutes=minutes)
    return runs, beats


def replay(seed: int, workdir: Path, eval_days: int) -> dict:
    db_path = workdir / f"watchdog-{seed}.db"
    db_path.unlink(missing_ok=True)
    os.environ["AIOPS_DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow, load_workflow
    from aiops.interfaces.runner import load_all
    from aiops.storage import crud, db, models
    from aiops.storage.hooks import StorageHooks
    from aiops.storage.models import ExternalAnomalyRow, PipelineRunRow

    load_all()
    db.configure(os.environ["AIOPS_DATABASE_URL"])
    models.reset_all()
    rng = random.Random(seed)
    eval_start = datetime(2026, 12, 1, tzinfo=timezone.utc)
    start, end = eval_start - timedelta(days=TRAIN_DAYS), eval_start + timedelta(days=eval_days)
    runs, beats = build_records(start, end, rng, eval_start)
    crud.create_many(PipelineRunRow, runs)
    crud.create_many(ExternalAnomalyRow, beats)

    wf = load_workflow(ROOT / "aiops" / "pipelines" / "watchdog" / "platform_watchdog.yaml")
    extra = [TargetSpec(source_type="platform", query=f"alerts/{team}/heartbeats", entity=team, detectors=["stopped_heartbeats"])
             for team in TEAMS]
    wf = Workflow.model_validate(wf.model_dump() | {"targets": [*wf.model_dump()["targets"], *[t.model_dump() for t in extra]]})
    pipeline = Pipeline(wf, StorageHooks())

    alarms: list[tuple[datetime, str, str, float]] = []  # (哪一輪發現的, 實體, 節點, 分數)
    severities: dict[str, int] = {}
    errors = 0
    t, rounds, began = eval_start, 0, time.time()
    while t < end:
        ctx = pipeline.run(now=t)
        rounds += 1
        errors += len(ctx.errors)
        alarms += [(t, e.entity, e.detector_node, e.score) for e in ctx.detected_anomalies]
        for inc in ctx.incidents:
            if not inc.suppressed_by:
                severities[inc.severity] = severities.get(inc.severity, 0) + 1
        t += ROUND

    windows = [(f, eval_start + timedelta(days=f.day), eval_start + timedelta(days=f.day, hours=f.hours)) for f in FAULTS]
    results = []
    for f, a, b in windows:
        hits = sorted(found for found, ent, node, _ in alarms if ent == f.entity and node == f.node and a <= found <= b + GRACE)
        results.append({"fault": f.name, "expect": f.expect, "caught": bool(hits),
                        "delay_minutes": round((hits[0] - a).total_seconds() / 60) if hits else None})
    false = [(found, ent, node) for found, ent, node, _ in alarms
             if not any(ent == f.entity and node == f.node and a <= found <= b + GRACE for f, a, b in windows)]
    episodes: dict[str, int] = {}
    last: dict[tuple[str, str], datetime] = {}
    for found, ent, node in sorted(false):
        key = (ent, node)
        if key not in last or found - last[key] > GRACE:
            episodes[node] = episodes.get(node, 0) + 1
        last[key] = found
    return {"seed": seed, "eval_days": eval_days, "rounds": rounds, "seconds": round(time.time() - began),
            "records": {"runs": len(runs), "heartbeats": len(beats),
                        "background_failures": sum(1 for r in runs if r["status"] == "failed")},
            "faults": results, "false_alarm_episodes": episodes, "false_alarm_points": len(false),
            "notified_incidents_by_severity": severities, "errors": errors}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", type=Path, required=True)
    ap.add_argument("--seeds", type=int, nargs="+", default=[42, 7, 2026])
    ap.add_argument("--eval-days", type=int, default=14)
    args = ap.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("AIOPS_NOTIFIER", "log")
    logging.disable(logging.WARNING)
    out = [replay(seed, args.workdir, args.eval_days) for seed in args.seeds]
    text = json.dumps(out, ensure_ascii=False, indent=2)
    (args.workdir / "watchdog_replay.json").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
