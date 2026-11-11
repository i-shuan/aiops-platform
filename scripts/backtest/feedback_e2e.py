"""R+SNR 誤報回饋閉環的端到端驗收（ES 真實序列 + 平台本身的偵測／儲存／管理動作）。

流程（全部走平台程式：it_otel 的 Pipeline + StorageHooks、admin_feedback_submit／propose、admin_proposal_backtest／approve）：
  1. 線上偵測「前」：測試期每 5 分鐘一輪跑 it_otel（資料源 elasticsearch，查每分鐘請求數），寫進資料庫（SQLite 檔）。
  2. 人工標記（腳本代替人）：跟注入時窗重疊的事件群 -> true_positive（進黃金事故庫）；其他 -> false_positive。
  3. 回饋閉環：對還沒轉成提案的誤報回饋 admin_feedback_propose(scope=entity) -> 回測 -> 通過防護罩才核准；最多 --rounds 輪，
     提案不再減少誤報、或被防護罩擋下就停。
  4. 示範防護罩：人工提一個會讓黃金事故消失的倍率（residual_chronic ×10）-> 回測被 rejected -> 核准被拒。
  5. 線上偵測「後」：沿用同一份平台狀態（標記、黃金事故、核准的調參），只清掉異常點、基準線、通知，
     同一段測試期重跑一次，比較誤報與三種注入（carry_over 說明為什麼不能用空的資料庫）。

注入：ES 是唯讀的，三種注入（step ×0.3 20 分、ramp 15 分鐘滑到 ×0.3 再 20 分、mild ×0.8 60 分）在讀取端做——
包住 ElasticsearchSpec.fetch_data，對這條序列落在注入時窗的點乘上係數。平台看到的就是注入後的資料（偵測、回測、
黃金事故重抓都一樣）。三種注入放在測試期不同的平日時段，不互相重疊。

只用唯讀 API key（從 secrets.local.yaml 讀進行程環境變數，不印出、不寫檔）、只打 _search；資料不送外部服務。

  python scripts/backtest/feedback_e2e.py --secrets <secrets.local.yaml> --workdir <repo 外的暫存目錄> --service pay-api
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore")

TZ = "Asia/Taipei"
INJECT = {"step": np.full(20, 0.3), "ramp": np.r_[np.linspace(1.0, 0.3, 15), np.full(20, 0.3)], "mild": np.full(60, 0.8)}
EVENT_GAP = 10
TAIL_MINUTES = 60  # 注入結束後這麼久的告警算「拖尾」，不算誤報也不算檢出


def es_query(service: str) -> str:
    return json.dumps({"index": "metrics-apm.transaction.1m-*", "agg": "sum",
                       "value_field": "transaction.duration.summary.value_count",
                       "filter": {"term": {"service.name": service}}}, separators=(",", ":"))


def configure_env(secrets: Path, db_path: Path) -> None:
    cfg = yaml.safe_load(secrets.read_text(encoding="utf-8"))["es"]
    os.environ["AIOPS_ES_URL"] = cfg["url"]
    os.environ["AIOPS_ES_API_KEY"] = cfg["AIOPS_ES_API_KEY"]  # 只放進這個行程的環境變數，不印出、不寫檔
    os.environ["AIOPS_DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    os.environ["AIOPS_NOTIFIER"] = "log"
    for var in ("AIOPS_ALERTMANAGER_URL", "AIOPS_LLM_PROVIDER"):
        os.environ.pop(var, None)


def plan_injections(test_start: pd.Timestamp, test_end: pd.Timestamp) -> dict[str, tuple[pd.Timestamp, int]]:
    """三種注入各放在測試期內不同的平日時段（台北 11:00、15:00、隔天 11:00…），彼此間隔至少 3 小時。"""
    slots = []
    day = test_start.tz_convert(TZ).normalize()
    while day < test_end and len(slots) < 3:
        if day.day_of_week < 5:
            for hour in (11, 15):
                t = (day + pd.Timedelta(hours=hour)).tz_convert("UTC")
                if test_start + pd.Timedelta(hours=8) <= t <= test_end - pd.Timedelta(hours=3):
                    slots.append(t)
        day += pd.Timedelta(days=1)
    if len(slots) < 3:
        raise SystemExit(f"測試期 {test_start} ~ {test_end} 找不到 3 個平日時段放注入")
    return {kind: (slots[i], len(INJECT[kind])) for i, kind in enumerate(INJECT)}


def install_injection(query: str, plan: dict[str, tuple[pd.Timestamp, int]]) -> None:
    from aiops.sources.elasticsearch import ElasticsearchSpec

    real = ElasticsearchSpec.fetch_data
    factors = {}
    for kind, (start, length) in plan.items():
        for k, f in enumerate(INJECT[kind]):
            factors[start + pd.Timedelta(minutes=k)] = f

    def fetch(self, q, start, end, step, field_map=None):
        df = real(self, q, start, end, step, field_map)
        if q != query or df.empty:
            return df
        ts = pd.to_datetime(df["ts"], utc=True)
        mult = np.array([factors.get(t, 1.0) for t in ts])
        return df.assign(value=np.round(df["value"].astype(float).to_numpy() * mult))

    ElasticsearchSpec.fetch_data = fetch


def fresh_db() -> None:
    from aiops.storage import db, models

    db.configure(os.environ["AIOPS_DATABASE_URL"])
    models.reset_all()


def carry_over(src: Path, dst: Path) -> None:
    """「後」的資料庫 = 「前」的整份平台狀態（事件群、誤報／真異常標記、黃金事故庫、核准的調參、提案），
    只清掉這一段測試期線上偵測自己產生的東西：異常點、基準線、通知；事件群全部標成已結案（不讓重跑的異常被舊卡吸收）。
    不能用空的資料庫：訓練排除窗會排除「還沒標記的事件群」、但不排除「已標成誤報的」（StorageHooks 的既有規則），
    空資料庫等於標記全部遺失，重跑時偵測器會比正式環境更敏感（上一版就是這樣量偏的）。"""
    import shutil
    import sqlite3

    shutil.copyfile(src, dst)
    con = sqlite3.connect(dst)
    try:
        with con:
            for table in ("anomalies", "detector_models", "notifications"):
                con.execute(f"DELETE FROM {table}")
            con.execute("UPDATE incidents SET status = 'resolved', resolved_at = COALESCE(resolved_at, CURRENT_TIMESTAMP)")
    finally:
        con.close()


def online(service: str, query: str, test_start, test_end) -> list[pd.Timestamp]:
    """測試期每 5 分鐘一輪跑 it_otel（StorageHooks 寫資料庫），回傳寫進資料庫的告警分鐘。"""
    from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
    from aiops.interfaces.runner import workflow_path
    from aiops.storage.hooks import StorageHooks

    pipe = Pipeline(load_workflow(workflow_path("it_otel")), StorageHooks())
    target = TargetSpec(source_type="elasticsearch", query=query, entity=service)
    minutes: set[pd.Timestamp] = set()
    now = test_start + pd.Timedelta(minutes=60)
    errors = 0
    while now <= test_end:
        # 每輪在整點分鐘前 1 秒跑：最新的一桶是已經寫完的上一分鐘。剛好在整點跑時，ES 的 date_histogram 會因為
        # extended_bounds 補出「end 那一分鐘」的空桶，sum 回 0（不是沒資料）——每一輪最後一點都是假的掉到 0。
        # 正式環境的 CronJob 一樣會碰到（最新一分鐘的 APM 指標還沒寫進來），見 docs/RESIDUAL_SNR_ENSEMBLE.md「沒驗證」
        batch = pipe.run(now=(now - pd.Timedelta(seconds=1)).to_pydatetime(), targets=[target])
        errors += len(batch.errors)
        minutes |= {pd.Timestamp(e.ts) for e in batch.detected_anomalies if e.level == "anomaly"}
        now += pd.Timedelta(minutes=5)
    if errors:
        print(f"  （線上偵測期間 errors {errors} 筆）", flush=True)
    return sorted(minutes)


def score(minutes, plan, test_start, test_end) -> dict:
    """誤報（注入時窗與其後 60 分拖尾以外）分/週・件/週，三種注入的檢出。"""
    days = (test_end - test_start).total_seconds() / 86400
    inj, tail = set(), set()
    out = {}
    for kind, (start, length) in plan.items():
        win = [start + pd.Timedelta(minutes=k) for k in range(length)]
        inj |= set(win)
        tail |= {start + pd.Timedelta(minutes=length + k) for k in range(TAIL_MINUTES)}
        hit = [m for m in minutes if m in set(win)]
        out[kind] = {"caught": f"{len(hit)}/{length}",
                     "first_alert_minute": int((min(hit) - start).total_seconds() // 60) if hit else None,
                     "start_local": f"{start.tz_convert(TZ):%m-%d %a %H:%M}"}
    fp = [m for m in minutes if m not in inj and m not in tail]
    events = 0 if not fp else 1 + sum(1 for a, b in zip(fp, fp[1:]) if (b - a) > pd.Timedelta(minutes=EVENT_GAP))
    out["false_alarm_minutes_per_week"] = round(len(fp) / days * 7, 1)
    out["false_alarm_events_per_week"] = round(events / days * 7, 1)
    out["tail_minutes"] = len([m for m in minutes if m in tail])
    out["test_days"] = round(days, 2)
    return out


def label(plan) -> dict:
    """腳本代替人：跟注入時窗重疊的事件群標 true_positive，其他標 false_positive。"""
    from aiops.engine.requests import FeedbackSubmit
    from aiops.engine.types import Incident
    from aiops.interfaces.runner import run
    from aiops.storage.repository import Incidents

    windows = [(s, s + pd.Timedelta(minutes=n - 1)) for s, n in plan.values()]
    tp, fp = [], []
    for row in Incidents.created_since(datetime(2000, 1, 1)):
        inc = Incident.from_dict(row["incident"], uid=row["uid"])
        a, b = pd.Timestamp(inc.start), pd.Timestamp(inc.end)
        real = any(a <= we and b >= ws for ws, we in windows)
        verdict = "true_positive" if real else "false_positive"
        fb = run("admin_feedback_submit", request=FeedbackSubmit(
            incident_uid=row["uid"], verdict=verdict, author="e2e",
            text="注入的真異常" if real else "測試期正常時段的告警（不是事故）"))["feedback"]
        (tp if real else fp).append({"uid": row["uid"], "feedback_id": fb["id"], "start": a.isoformat(),
                                     "minutes": len(inc.events)})
    return {"true_positive": tp, "false_positive": fp}


def loop(service, query, labels, data_start, test_end, train_ratio, rounds) -> list[dict]:
    from aiops.engine.errors import BadRequest, Conflict
    from aiops.engine.requests import FeedbackPropose, ProposalApprove, ProposalBacktest
    from aiops.interfaces.runner import run
    from aiops.storage.repository import Feedbacks

    history = []
    for r in range(1, rounds + 1):
        open_fb = [f for f in labels["false_positive"] if Feedbacks.get(f["feedback_id"])["proposal_id"] is None]
        if not open_fb:
            history.append({"round": r, "stop": "沒有還沒轉成提案的誤報回饋"})
            break
        fb = max(open_fb, key=lambda f: f["minutes"])  # 先處理告警分鐘最多的那張卡
        try:
            res = run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fb["feedback_id"], proposer="e2e",
                                                                        scope="entity"))
        except BadRequest as e:
            history.append({"round": r, "stop": f"提案失敗：{e}"[:400]})
            break
        p = res["proposal"]
        candidates = (res.get("snr") or {}).get("candidates", [])
        req = ProposalBacktest(proposal_id=p["id"], metric={"source_type": "elasticsearch", "query": query},
                               entity=service, time_range={"start": data_start.to_pydatetime(), "end": (test_end - pd.Timedelta(seconds=1)).to_pydatetime()},
                               train_ratio=train_ratio)
        bt = run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=test_end.to_pydatetime())["proposal"]
        b = bt["backtest"]
        item = {"round": r, "feedback_id": fb["feedback_id"], "proposal_id": p["id"], "detector": p["detector_name"],
                "params": p["params"], "reason": p["reason"], "status": bt["status"],
                "backtest_minutes": [b["current_anomalies"], b["proposed_anomalies"]],
                "backtest_events": [b["current_events"], b["proposed_events"]],
                "golden": {k: len(b["safety"][k]) for k in ("caught", "missed_golden", "baseline_missed", "unchecked")},
                "candidates": [{k: c.get(k) for k in ("detector", "scale", "golden_safe_max", "fp_minutes_before",
                                                      "fp_minutes_after", "silenced")} for c in candidates]}
        if bt["status"] == "backtested":
            try:
                run("admin_proposal_approve", request=ProposalApprove(proposal_id=p["id"], approver="e2e-lead"))
                item["approved"] = True
            except Conflict as e:
                item["approved"] = f"核准失敗：{e}"
        history.append(item)
        print(f"  round {r}: {p['detector_name']} {p['params']} -> {bt['status']} bt {b['current_anomalies']}->{b['proposed_anomalies']} 分",
              flush=True)
        if bt["status"] != "backtested" or b["proposed_anomalies"] >= b["current_anomalies"]:
            break
    return history


def reject_demo(service, query, data_start, test_end, train_ratio, rounds) -> dict:
    """人工提一個「超過提案器算出的黃金事故安全上限」的倍率：防護罩應該擋下。挑安全上限最低的偵測器，倍率 = 上限 + 0.5。
    防護罩沒擋（任何偵測器到 10 倍都不會讓黃金事故消失）就照實記下、不核准。"""
    from aiops.engine.errors import Conflict
    from aiops.engine.requests import ProposalApprove, ProposalBacktest, ProposalCreate
    from aiops.interfaces.runner import run
    from aiops.storage.repository import ActiveConfigs

    cands = [c for r in rounds for c in r.get("candidates", []) if c.get("golden_safe_max") is not None]
    if not cands:
        return {"skipped": "回饋閉環沒有算出任何候選的黃金事故安全上限"}
    pick = min(cands, key=lambda c: c["golden_safe_max"])
    if pick["golden_safe_max"] >= 10.0:
        return {"skipped": f"每個偵測器的倍率到 10 都不會讓黃金事故消失（最低上限 {pick['golden_safe_max']}），沒有能示範的過度提案"}
    scale = min(10.0, round(pick["golden_safe_max"] + 0.5, 2))
    current = (ActiveConfigs.get(pick["detector"], "it_otel", service) or {}).get("params", {})
    pid = run("admin_proposal_create", request=ProposalCreate(
        source="manual", detector=pick["detector"], params={**current, "threshold_scale": scale}, proposer="e2e",
        reason=f"示範：超過黃金事故安全上限 {pick['golden_safe_max']}", pipeline="it_otel", entity=service))["proposal"]["id"]
    req = ProposalBacktest(proposal_id=pid, metric={"source_type": "elasticsearch", "query": query}, entity=service,
                           time_range={"start": data_start.to_pydatetime(), "end": (test_end - pd.Timedelta(seconds=1)).to_pydatetime()},
                           train_ratio=train_ratio)
    bt = run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=test_end.to_pydatetime())["proposal"]
    out = {"proposal_id": pid, "detector": pick["detector"], "scale": scale, "golden_safe_max": pick["golden_safe_max"],
           "status": bt["status"],
           "missed_golden": [m["incident_uid"] for m in bt["backtest"]["safety"]["missed_golden"]],
           "backtest_minutes": [bt["backtest"]["current_anomalies"], bt["backtest"]["proposed_anomalies"]]}
    if bt["status"] != "rejected":
        out["approve"] = "防護罩沒擋下：不核准（示範用提案，照實記錄）"
        return out
    try:
        run("admin_proposal_approve", request=ProposalApprove(proposal_id=pid, approver="e2e-lead"))
        out["approve"] = "核准成功（不應該發生）"
    except Conflict as e:
        out["approve"] = f"拒絕：{e}"
    return out


def approved_configs() -> list[dict]:
    from aiops.storage.repository import Proposals

    rows = Proposals.approved_between(datetime(2000, 1, 1, tzinfo=timezone.utc), datetime(9999, 1, 1, tzinfo=timezone.utc))
    return [{"proposal_id": r["id"], "detector": r["detector_name"], "params": r["params"], "pipeline": r["pipeline"],
             "entity": r["entity"]} for r in rows]


def main() -> None:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--secrets", type=Path, required=True)
    ap.add_argument("--workdir", type=Path, required=True, help="SQLite 檔與結果放這裡（repo 外）")
    ap.add_argument("--service", default="pay-api")
    ap.add_argument("--rounds", type=int, default=6)
    args = ap.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    configure_env(args.secrets, args.workdir / "before.db")

    from aiops.interfaces.runner import load_all
    from scripts.backtest.es_series import load_series

    load_all()
    query = es_query(args.service)
    df = load_series(args.service, args.workdir / "cache", args.secrets)
    train_days = 14 if len(df) / 1440 >= 17 else 7
    data_start = df["ts"].iloc[0]
    test_start = data_start + pd.Timedelta(days=train_days)
    test_end = df["ts"].iloc[-1]
    train_ratio = round(train_days * 1440 / len(df), 3)
    plan = plan_injections(test_start, test_end)
    install_injection(query, plan)
    result = {"service": args.service, "train_days": train_days, "test": [test_start.isoformat(), test_end.isoformat()],
              "injections": {k: (s.isoformat(), n) for k, (s, n) in plan.items()}}

    print("1. 線上偵測（前）", flush=True)
    fresh_db()
    before = online(args.service, query, test_start, test_end)
    result["before"] = score(before, plan, test_start, test_end)
    print("  ", json.dumps(result["before"], ensure_ascii=False), flush=True)
    print("2. 標記", flush=True)
    labels = label(plan)
    result["labels"] = {k: len(v) for k, v in labels.items()}
    print("  ", result["labels"], flush=True)
    print("3. 回饋閉環", flush=True)
    result["rounds"] = loop(args.service, query, labels, data_start, test_end, train_ratio, args.rounds)
    print("4. 防護罩示範", flush=True)
    result["reject_demo"] = reject_demo(args.service, query, data_start, test_end, train_ratio, result["rounds"])
    print("  ", json.dumps(result["reject_demo"], ensure_ascii=False)[:600], flush=True)
    approved = {r["proposal_id"] for r in result["rounds"] if r.get("approved") is True}
    configs = [c for c in approved_configs() if c["proposal_id"] in approved]  # 只帶回饋閉環核准的
    result["approved_configs"] = configs

    print("5. 線上偵測（後：同一份平台狀態，帶著標記、黃金事故與核准的調參）", flush=True)
    after_db = args.workdir / "after.db"
    carry_over(args.workdir / "before.db", after_db)
    os.environ["AIOPS_DATABASE_URL"] = f"sqlite:///{after_db.as_posix()}"
    from aiops.storage import db

    db.configure(os.environ["AIOPS_DATABASE_URL"])
    after = online(args.service, query, test_start, test_end)
    result["after"] = score(after, plan, test_start, test_end)
    print("  ", json.dumps(result["after"], ensure_ascii=False), flush=True)
    (args.workdir / f"feedback_e2e_{args.service}.json").write_text(json.dumps(result, ensure_ascii=False, indent=1,
                                                                               default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
