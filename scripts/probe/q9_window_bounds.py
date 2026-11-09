"""Q9：回溯窗含頭含尾的統一性。三條路徑 × 三個資料源：
  (a) 偵測 ingestion.fetch（now=10:00:30、lookback 60 分）
  (b) 回放 replay.fetch_span(09:00:00, 10:00:00)（黃金事故、誤報分位數反推用）
  (c) 提案回測 admin.proposal_backtest 用的 fetch_target_frame(09:00:00, 10:00:00)
假 ES 照 ES 的語意回（range gte／lt：含頭不含尾），假 Prometheus 照 query_range 的語意回（start、end 都含）；
mock 是真的 MockSpec。印出送出去的邊界、拿回來的點數、第一點與最後一點。
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402


def main() -> int:
    use_root(sys.argv)
    os.environ["AIOPS_ES_URL"] = "http://127.0.0.1:9"
    os.environ["AIOPS_PROMETHEUS_URL"] = "http://127.0.0.1:9"
    import httpx

    from aiops.control_plane.learning import replay
    from aiops.engine.context import PipelineContext, Target
    from aiops.interfaces import runner
    from aiops.stages.ingestion.fetch import FetchOperator, fetch_target_frame
    runner.load_all()
    sent = {}

    def iso(x):
        return datetime.fromisoformat(x).astimezone(timezone.utc)

    def fake(self, method, url, **kw):
        req = httpx.Request(method, url)
        if "_search" in url:
            rng = kw["json"]["query"]["bool"]["filter"][0]["range"]["@timestamp"]
            lo, hi = iso(rng["gte"]), iso(rng.get("lt") or rng.get("lte"))
            sent.setdefault("es", []).append(f"gte {lo:%H:%M:%S} {'lt' if 'lt' in rng else 'lte'} {hi:%H:%M:%S}")
            pts, t = [], lo
            while t < hi or ("lte" in rng and t == hi):
                pts.append({"key": int(t.timestamp() * 1000), "v": {"value": 1.0}, "doc_count": 1})
                t += timedelta(minutes=1)
            return httpx.Response(200, request=req, json={"aggregations": {"series": {"buckets": pts}}})
        p = kw["params"]
        lo, hi, step = float(p["start"]), float(p["end"]), int(p["step"])
        sent.setdefault("prom", []).append(f"start {datetime.fromtimestamp(lo, timezone.utc):%H:%M:%S} "
                                           f"end {datetime.fromtimestamp(hi, timezone.utc):%H:%M:%S}（兩端都含）")
        vals, t = [], lo
        while t <= hi:
            vals.append([t, "1"])
            t += step
        return httpx.Response(200, request=req, json={"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {}, "values": vals}]}})

    httpx.Client.request = fake
    es_q = json.dumps({"index": "m", "value_field": "v", "agg": "avg"})
    sources = {"mock": "cpu", "elasticsearch": es_q, "prometheus": "up"}
    t9, t10 = datetime(2026, 11, 3, 9, 0, tzinfo=timezone.utc), datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)

    def show(df):
        return f"{len(df):3} 點 {df['ts'].iloc[0]:%H:%M:%S} … {df['ts'].iloc[-1]:%H:%M:%S}"

    for st, q in sources.items():
        sent.clear()
        tgt = Target(st, q, "svc")
        a = FetchOperator(lookback_minutes=60).execute(PipelineContext("p", t10 + timedelta(seconds=30), target=tgt)).raw_df
        b = replay.fetch_span(PipelineContext("p", t10, target=tgt), t9, t10, 60)
        c = fetch_target_frame(PipelineContext("p", t10, target=tgt), t9, t10, 60, [])
        print(f"── {st}")
        print(f"   (a) 偵測 fetch(now=10:00:30, 60 分)  : {show(a)}")
        print(f"   (b) replay.fetch_span(09:00, 10:00) : {show(b)}")
        print(f"   (c) backtest fetch_target_frame     : {show(c)}")
        for k, v in sent.items():
            print(f"   送出 {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
