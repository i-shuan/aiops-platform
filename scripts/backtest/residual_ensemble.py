"""偵測回測，用每分鐘請求數序列的 CSV 快取（當初從 ES 撈的）。偵測節點取自 sample_it_otel（scripts/backtest/variants.py）。
2026-11-20 起沒有表決節點：任何一個偵測節點越線就算告警分鐘。這支腳本改完之後還沒有對 ES 重跑過，
docs/RESIDUAL_SNR_ENSEMBLE.md 的數字是移除前量的。

偵測走真的 Pipeline：從 sample_it_otel.yaml 取出偵測節點，
fetch 換成回放用的 replay.window（每輪給最近 60 分鐘，同 it_otel 的 lookback_minutes），基準線由記憶體版 Hooks 保存——
第一輪沒有基準線時給訓練段 train_df、引擎訓練並交出 new_detector_states，之後每輪載入同一份（等同正式環境基準線還沒到期）。
每 5 分鐘一輪（同 CronJob 週期）；一分鐘只要在任何一輪被任何一個偵測節點判成異常，就算告警分鐘。
「各節點」列出每個節點自己的分鐘數。

訓練／測試切分：資料 >= 17 天用 14 天訓練，其餘 7 天。三種注入（測試期中段某個平日 11–13 點）：
  step  瞬間 ×0.3 持續 20 分    ramp  15 分鐘線性滑到 ×0.3 再維持 20 分    mild  ×0.8 持續 60 分

  python scripts/backtest/residual_ensemble.py --cache <快取目錄> --out <結果.json>

2026-11-20 起平台不再直連 Elasticsearch：直接從 ES 撈序列的 es_series.py 已刪除，這支腳本只讀既有的 CSV 快取
（<快取目錄>/<服務>.csv，欄位 ts、value）。沒有快取就不能跑；指標改經 Grafana 取得之後再補撈取。
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import warnings
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from aiops.engine.operator import BaseOperator  # noqa: E402
from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow  # noqa: E402
from aiops.engine.registry import OperatorRegistry  # noqa: E402
from aiops.engine.types import ANOMALY  # noqa: E402
from aiops.interfaces.runner import load_all  # noqa: E402
from scripts.backtest.variants import detection_nodes  # noqa: E402

warnings.filterwarnings("ignore")
TZ = "Asia/Taipei"
LOOKBACK, STEP = 60, 5
SERVICES = ["mes-api", "wip-worker", "report-svc", "order-svc", "cart-api", "pay-api"]
VARIANTS = {"sample_it_otel": "sample_it_otel"}  # 結果的鍵 -> 偵測節點取自哪條樣本管線
INJECTIONS = {"step": np.full(20, 0.3), "ramp": np.r_[np.linspace(1.0, 0.3, 15), np.full(20, 0.3)],
              "mild": np.full(60, 0.8)}

load_all()


class Replay:
    """這一輪要給引擎的資料（回放腳本自己的狀態，不在 aiops/ 裡）。"""
    raw: pd.DataFrame | None = None
    train: pd.DataFrame | None = None


@OperatorRegistry.register("replay.window")
class ReplayWindow(BaseOperator):
    def execute(self, ctx):
        ctx.raw_df = Replay.raw.reset_index(drop=True)
        missing = [n for n in ctx.baseline_nodes if n not in ctx.detector_states]
        ctx.train_df = Replay.train.reset_index(drop=True) if missing else None
        return ctx


class MemoryHooks:
    """記憶體版基準線存放：load_target 給現有基準線，persist_target 收新訓練的（同 StorageHooks 的生命週期）。"""

    def __init__(self):
        self.states = {}

    def load_batch(self, ctx): ...

    def load_target(self, ctx, nodes):
        ctx.detector_states = {name: self.states[name] for name, _ in nodes if name in self.states}

    def persist_target(self, ctx):
        self.states.update(ctx.new_detector_states)

    def persist_batch(self, ctx): ...

    def finalize_batch(self, ctx): ...


class Runner:
    def __init__(self, workflow: str, service: str, train: pd.DataFrame):
        self.pipe = Pipeline(Workflow(name=f"replay_{workflow}", nodes=detection_nodes(
            "replay.window", workflow=workflow),
                                      targets=[TargetSpec(source_type="mock", query=f"rpm:{service}", entity=service)]),
                             MemoryHooks())
        self.train = train
        self.voters = [n.name for n, op in self.pipe.target_nodes if getattr(op, "has_baseline", False)]

    def flags(self, test: pd.DataFrame, rounds) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        n = len(test)
        pos = {t: k for k, t in enumerate(test["ts"])}
        verdict = np.zeros(n, bool)
        member = {v: np.zeros(n, bool) for v in self.voters}
        Replay.train = self.train
        for i in rounds:
            Replay.raw = test.iloc[i - LOOKBACK + 1:i + 1]
            now = test["ts"].iloc[i].to_pydatetime() + timedelta(seconds=30)
            batch = self.pipe.run(now=now, strict=True)
            for e in batch.anomalies:
                k = pos.get(pd.Timestamp(e.ts))
                if e.level != ANOMALY or k is None:
                    continue
                if e.detector_node in member:  # 任何一個偵測節點越線就是告警
                    verdict[k] = True
                    member[e.detector_node][k] = True
        return verdict, member


def clusters(flag: np.ndarray, gap: int = 10) -> int:
    """事件數：告警分鐘之間隔超過 gap 分鐘就算另一件。"""
    idx = np.flatnonzero(flag)
    return int(1 + (np.diff(idx) > gap).sum()) if len(idx) else 0


def inject(test: pd.DataFrame, kind: str) -> tuple[pd.DataFrame, int, int]:
    ts = test["ts"].dt.tz_convert(TZ)
    idx = np.arange(len(test))
    cand = np.flatnonzero(((ts.dt.hour >= 11) & (ts.dt.hour <= 13) & (ts.dt.day_of_week < 5)).to_numpy()
                          & (idx > 200) & (idx < len(test) - 200))
    s = int(cand[len(cand) // 2])
    f = INJECTIONS[kind]
    v = test["value"].to_numpy(float).copy()
    v[s:s + len(f)] *= f
    out = test.copy()
    out["value"] = v
    return out, s, len(f)


def switch_minutes(test: pd.DataFrame, flag: np.ndarray, hours=(9, 19), pad: int = 5) -> int:
    """當地 09:00／19:00 前後 pad 分鐘內的告警分鐘數（每天固定的階梯切換點）。"""
    loc = test["ts"].dt.tz_convert(TZ)
    minute_of_day = (loc.dt.hour * 60 + loc.dt.minute).to_numpy()
    near = np.zeros(len(test), bool)
    for h in hours:
        near |= np.abs(minute_of_day - h * 60) <= pad
    return int((flag & near).sum())


def run_service(service: str, df: pd.DataFrame) -> dict:
    train_days = 14 if len(df) / 1440 >= 17 else 7
    split = df["ts"].iloc[0] + pd.Timedelta(days=train_days)
    train = df[df["ts"] < split].reset_index(drop=True)
    test = df[df["ts"] >= split].reset_index(drop=True)
    days = len(test) / 1440
    per_week = 7 / days
    loc = test["ts"].dt.tz_convert(TZ)
    out = {"資料": f"{df['ts'].iloc[0]:%m-%d %H:%M} → {df['ts'].iloc[-1]:%m-%d %H:%M} UTC",
           "訓練天": train_days, "測試天": round(days, 2)}
    all_rounds = range(LOOKBACK - 1, len(test), STEP)
    for key, workflow in VARIANTS.items():
        runner = Runner(workflow, service, train)
        verdict, member = runner.flags(test, all_rounds)
        r = {"workflow": workflow,
             "各節點_分/週": {k: round(int(v.sum()) * per_week) for k, v in member.items()},
             "誤報_分/週": round(int(verdict.sum()) * per_week, 1),
             "誤報_事件/週": round(clusters(verdict) * per_week, 1),
             "切換點±5分_分/週": round(switch_minutes(test, verdict) * per_week, 1)}
        if verdict.any():
            hrs = loc[verdict].dt.hour.value_counts().head(4)
            r["誤報集中的小時(當地)"] = {int(h): int(c) for h, c in hrs.items()}
        r["基準線摘要"] = {k: {"noise": v.state["noise"], "lowo_share": v.state["profile"]["lowo_share"],
                           "slots_ready": v.state["profile"]["slots_ready"]}
                       for k, v in runner.pipe.hooks.states.items() if "noise" in v.state}
        r["SNR"] = {k: {x: v.state["_snr"][x] for x in ("snr_db", "status", "noise_sigma", "signal_var", "threshold",
                                                        "threshold_source")}
                    for k, v in runner.pipe.hooks.states.items() if "_snr" in v.state}
        for kind in INJECTIONS:
            inj, s, length = inject(test, kind)
            # 只重跑看得到注入窗（含其後 60 分拖尾）的輪次；基準線沿用同一份（同一個訓練段）
            rounds = [i for i in all_rounds if s <= i <= s + length + LOOKBACK + 30]
            v2, _ = runner.flags(inj, rounds)
            win = np.zeros(len(test), bool)
            win[s:s + length] = True
            after = np.zeros(len(test), bool)
            after[s + length:s + length + 60] = True
            hit = v2 & win
            r[f"注入_{kind}"] = {"抓到": f"{int(hit.sum())}/{length}",
                               "首次告警_分": int(np.argmax(hit[win])) if hit.any() else None,
                               "窗後60分拖尾_分": int((v2 & after & ~verdict).sum()),
                               "注入起點(當地)": f"{loc.iloc[s]:%m-%d %a %H:%M}"}
        out[key] = r
    return out


def load_series(service: str, cache: Path) -> pd.DataFrame:
    path = cache / f"{service}.csv"
    if not path.is_file():
        raise SystemExit(f"{path} 沒有快取。直接從 ES 撈序列的功能已移除（平台不再直連 Elasticsearch），目前只能用既有的快取")
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def main() -> None:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cache", type=Path, required=True, help="序列 CSV 快取目錄（不要放在 repo 裡）")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--services", default=",".join(SERVICES))
    args = ap.parse_args()
    result = {}
    for service in args.services.split(","):
        result[service] = run_service(service, load_series(service, args.cache))
        print(service, json.dumps(result[service], ensure_ascii=False), flush=True)
        args.out.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
