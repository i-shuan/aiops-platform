"""Q6：ES 回 504（或連線池逾時）時偵測管線的錯誤隔離邊界。不連真 ES：把 httpx.Client.request 換成假的回應，
走的仍是 sources/_http.request 的重試邏輯。

管線：ingestion.fetch → detector.rules（不需要訓練，任何資料都會判），NullHooks、strict=False（排程模式）。
targets：mock-a（mock 資料源，健康）、es-bad（elasticsearch，假 ES 一律 504）、mock-c（健康）。

情境
  S1 一條序列 504×3：整輪會不會丟 InfrastructureError、健康序列的結果還在不在、花多久、建了幾個 httpx.Client
  S2 同上但 fetch 節點標 optional: true（CLAUDE.md 說不准這樣標）：會不會因此被吞掉
  S3 假 ES 丟 httpx.PoolTimeout（連線池爆滿）
  S4 假 ES 回 200 但 aggregations 格式不對：只記那條序列的錯誤
  S5 假 ES 回 200 但 0 筆，3 條裡 1 條：max_failed_target_ratio=1.0 不觸發；設 0.3 觸發斷流
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402


def main() -> int:
    use_root(sys.argv)
    os.environ["AIOPS_ES_URL"] = "http://127.0.0.1:9"  # 不會真的連：request 被換掉
    os.environ.pop("AIOPS_ES_API_KEY", None)
    import httpx

    from aiops.engine.pipeline import Pipeline, Workflow
    from aiops.interfaces import runner
    from aiops.sources import _http
    runner.load_all()
    _http.RETRY_DELAYS = (0.05, 0.05)  # 探測縮短等待；重試次數不變

    mode = {"v": "504"}
    stats = {"requests": 0, "clients": 0}
    real_init = httpx.Client.__init__

    def counting_init(self, *a, **k):
        stats["clients"] += 1
        real_init(self, *a, **k)

    def fake_request(self, method, url, **kwargs):
        stats["requests"] += 1
        req = httpx.Request(method, url)
        if mode["v"] == "504":
            return httpx.Response(504, request=req, text="gateway timeout")
        if mode["v"] == "pool":
            raise httpx.PoolTimeout("pool exhausted", request=req)
        if mode["v"] == "bad-shape":
            return httpx.Response(200, request=req, json={"aggregations": {"series": {"buckets": [{"no_key": 1}]}}})
        if mode["v"] == "empty":
            return httpx.Response(200, request=req, json={"aggregations": {"series": {"buckets": []}}})
        raise AssertionError(mode)

    httpx.Client.__init__ = counting_init
    httpx.Client.request = fake_request

    es_q = json.dumps({"index": "metrics-x", "value_field": "v", "agg": "avg"})
    now = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)

    def run(label, fetch_optional=False, ratio=None):
        d = {"name": "probe", "nodes": [
            {"name": "fetch", "operator": "ingestion.fetch", "params": {"lookback_minutes": 30},
             "optional": fetch_optional},
            {"name": "rule", "operator": "detector.rules", "params": {"upper": -1e9}}],  # 每點都越線：看得出誰有結果
            "targets": [{"source_type": "mock", "query": "cpu", "entity": "mock-a"},
                        {"source_type": "elasticsearch", "query": es_q, "entity": "es-bad"},
                        {"source_type": "mock", "query": "cpu", "entity": "mock-c"}]}
        if ratio is not None:
            d["max_failed_target_ratio"] = ratio
        stats.update(requests=0, clients=0)
        t0 = time.perf_counter()
        try:
            ctx = Pipeline(Workflow.model_validate(d)).run(now=now, strict=False)
            ents = sorted({a.entity for a in ctx.anomalies})
            print(f"{label:44} 完成：有結果的序列={ents} errors={[(e.entity, e.message[:70]) for e in ctx.errors]}"
                  f" | ES requests={stats['requests']} httpx.Client 建立={stats['clients']} 耗時={time.perf_counter() - t0:.2f}s")
        except Exception as e:  # noqa: BLE001
            print(f"{label:44} 整輪 RAISES {type(e).__name__}: {str(e)[:90]} | ES requests={stats['requests']} "
                  f"httpx.Client 建立={stats['clients']} 耗時={time.perf_counter() - t0:.2f}s")

    mode["v"] = "504"
    run("S1 一條 504×3")
    run("S2 一條 504×3、fetch 標 optional", fetch_optional=True)
    mode["v"] = "pool"
    run("S3 一條 PoolTimeout")
    mode["v"] = "bad-shape"
    run("S4 一條 200 但格式不對")
    mode["v"] = "empty"
    run("S5a 一條 200 但 0 筆（ratio=1.0）")
    run("S5b 一條 200 但 0 筆（ratio=0.3）", ratio=0.3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
