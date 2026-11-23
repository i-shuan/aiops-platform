"""Q3：子 span 時鐘偏移時 stages/rca/spans.self_times 的行為（main：Σ子耗時再 max(0)；PR A：區間聯集＋clamp＋clock_skew_spans）。

用法：python scripts/probe/q3_clock_skew.py [--root <aiops 原始碼根>]
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402


def main() -> int:
    use_root(sys.argv)
    from aiops.engine.context import SpanRecord
    from aiops.stages.rca import spans as sp

    t0 = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)

    def at(ms: float) -> datetime:
        return t0 + timedelta(microseconds=int(ms * 1000))

    def parent(d=200.0):
        return SpanRecord("t", "P", None, "ord-svc", at(0), d)

    def kid(i, start, dur, kind="external", svc="ord-svc"):
        return SpanRecord("t", f"C{i}", "P", svc, at(start), dur, kind=kind, resource=f"r{i}:443")

    cases = {
        "1 子早 100ms 開始（-100..+50），父 200": [parent(), kid(1, -100, 150)],
        "2 子超出父結尾（100..400），父 200": [parent(), kid(1, 100, 300)],
        "3 子完全在父之後（300..350），父 200": [parent(), kid(1, 300, 50)],
        "4 子比父長且同起點（0..250），父 200": [parent(), kid(1, 0, 250)],
        "5 兩個並行 I/O 子（0..60 ×2），父 100": [parent(100), kid(1, 0, 60), kid(2, 0, 60)],
        "6 依序兩子（0..60, 60..100），父 100": [parent(100), kid(1, 0, 60), kid(2, 60, 40)],
        "7 次毫秒偏移（-0.5..+49.5），父 200": [parent(), kid(1, -0.5, 50)],
        "8 早 100ms＋並行另一子（-100..+50, 0..120），父 200": [parent(), kid(1, -100, 150), kid(2, 0, 120)],
        "9 app 子與 I/O 子重疊（app 0..80, ext 50..150），父 200": [parent(), kid(1, 0, 80, kind="app"),
                                                                 kid(2, 50, 100)],
        "10 負耗時子（-20ms），父 200": [parent(), kid(1, 10, -20)],
    }
    has_detail = hasattr(sp, "self_times_detail")
    print(f"version: {'PR A（self_times_detail 存在）' if has_detail else 'main（只有 self_times）'}")
    for name, spans in cases.items():
        st = sp.self_times(spans)
        skew = sorted(sp.self_times_detail(spans)[1]) if has_detail else "n/a"
        ws = sp.window_stats(spans, at(-1000), at(10_000))
        svc = ws.get("ord-svc")
        cs = getattr(svc, "clock_skew_spans", "n/a") if svc else "-"
        print(f"{name:52} parent_self={st['P']:8.1f} ms  skewed={skew}  clock_skew_spans={cs}  "
              f"svc self/req={svc.self_ms_per_request:7.1f} wait={ {k: round(v, 1) for k, v in svc.wait_ms_per_request.items()} }")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
