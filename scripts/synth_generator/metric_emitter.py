"""Metrics：每個服務每分鐘的 APM transaction metrics（metrics-apm.transaction.1m），鋪滿整個 window。

每分鐘：請求數 n ~ Poisson(rate)、失敗數 k ~ Binomial(n, e2e_err)、每筆耗時 ~ 對數常態（平均 = e2e_ms）。
跟 Elastic APM 一樣依 event.outcome 拆成兩份文件（success 一定有；有失敗才有 failure）：
- transaction.duration.histogram：ES histogram 型別（values = 桶中心 µs、遞增；counts = 個數），Grafana 的 p95 面板吃它
- transaction.duration.summary：sum（總 µs）、value_count（請求數）
- event.success_count：sum（成功數）、value_count（請求數）
"""
from __future__ import annotations

import heapq
from collections.abc import Iterator
from datetime import timedelta

import numpy as np

from .documents import common
from .faults import iso
from .waveforms import METRIC_SIGMA, SystemState, _rng, lognormal_mu

LOG_STEP = np.log(1.08)   # 直方圖每個桶約 8% 寬


def _minute_docs(state: SystemState, name: str, k_service: int) -> Iterator[tuple[int, dict]]:
    svc = state.topo.services[name]
    r = _rng(state.scenario.seed, 2, k_service)
    n = r.poisson(state.rate[name])
    k = r.binomial(n, state.e2e_err[name])
    total = int(n.sum())
    minute_of = np.repeat(np.arange(len(n)), n)
    mu = lognormal_mu(state.e2e_ms[name], METRIC_SIGMA)
    dur_us = np.exp(mu[minute_of] + METRIC_SIGMA * r.standard_normal(total)) * 1000.0
    first = np.concatenate([[0], np.cumsum(n)[:-1]])
    pos = np.arange(total) - np.repeat(first, n)
    failed = (pos < k[minute_of]).astype(np.int64)
    doc_key = minute_of * 2 + failed                     # 一分鐘兩份：success（0）／failure（1）
    bucket = np.round(np.log(dur_us) / LOG_STEP).astype(np.int64)
    bmin = int(bucket.min()) if total else 0
    width = int(bucket.max()) - bmin + 1 if total else 1
    combo, counts = np.unique(doc_key * width + (bucket - bmin), return_counts=True)
    keys = combo // width
    sums = np.bincount(doc_key, weights=dur_us, minlength=len(n) * 2)
    cnts = np.bincount(doc_key, minlength=len(n) * 2)
    bounds = np.flatnonzero(np.diff(keys)) + 1
    starts = np.concatenate([[0], bounds])
    ends = np.concatenate([bounds, [len(keys)]])
    base = common(svc, "metrics", state.scenario.namespace)
    t0 = state.scenario.window.start
    for s, e in zip(starts, ends):
        key = int(keys[s])
        minute, is_fail = divmod(key, 2)
        b = combo[s:e] % width + bmin
        values = [round(float(np.exp(v * LOG_STEP)), 1) for v in b]
        count = int(cnts[key])
        ts = t0 + timedelta(minutes=minute)
        ok = 0 if is_fail else count
        doc = {
            "@timestamp": iso(ts),
            **base,
            "event": {"outcome": "failure" if is_fail else "success", "ingested": iso(ts + timedelta(seconds=61)),
                      "success_count": {"sum": ok, "value_count": count}},
            "processor": {"event": "metric"},
            "metricset": {"name": "transaction", "interval": "1m"},
            "transaction": {
                "name": svc.transaction, "type": "request", "root": name == state.topo.entry,
                "result": "HTTP 5xx" if is_fail else "HTTP 2xx",
                "duration": {"histogram": {"values": values, "counts": [int(c) for c in counts[s:e]]},
                             "summary": {"sum": round(float(sums[key]), 1), "value_count": count}},
            },
        }
        yield minute, doc


def metric_docs(state: SystemState) -> Iterator[dict]:
    """依時間排序（同一分鐘內依服務名稱、success 在 failure 前），確定性。邊產生邊輸出，不把整段放進記憶體。"""
    names = sorted(state.topo.services)
    streams = [_minute_docs(state, name, k) for k, name in enumerate(names)]
    for _minute, doc in heapq.merge(*streams, key=lambda item: item[0]):
        yield doc
