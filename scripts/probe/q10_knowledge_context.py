"""Q10：incident_knowledge.context 沒有版本欄位：讀取端對缺鍵／多鍵／型別不對的容忍度。

讀 context 的只有 control_plane/learning/golden._series_of（context.series）；similar_cases 讀的是欄位（entities、metrics、
signals、resolution），narrative.build_context 完全不讀經驗庫。這裡直接餵各種 row 給 _series_of。
回退路徑會查 Incidents.get_by_uid：用 sqlite 記憶體庫（這題只看 Python 端的解析，不是資料庫行為）。
PROBE_PG_URL 有設時，另外驗證 PostgreSQL 的 jsonb「陣列 || 物件」會把 merge_json 的結果變成陣列。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402


def main() -> int:
    use_root(sys.argv)
    for k in list(os.environ):
        if k.startswith("AIOPS_"):
            del os.environ[k]
    from aiops.storage import db
    db.configure("sqlite://")
    db.create_all()
    from aiops.control_plane.learning import golden

    ok = {"entity": "cart", "source_type": "mock", "query": "cpu"}
    rows = {
        "沒有 context 鍵": {"incident_uid": "u1"},
        "context = None": {"incident_uid": "u1", "context": None},
        "context = {}": {"incident_uid": "u1", "context": {}},
        "series 正常": {"incident_uid": "u1", "context": {"series": [ok]}},
        "series 項目多鍵（v2 加欄位）": {"incident_uid": "u1", "context": {"series": [{**ok, "step": 60, "unit": "ms"}]}},
        "series 項目缺 query": {"incident_uid": "u1", "context": {"series": [{"entity": "cart", "source_type": "mock"}]}},
        "series 項目缺 entity": {"incident_uid": "u1", "context": {"series": [{"source_type": "mock", "query": "cpu"}]}},
        "series 是 dict（不是 list）": {"incident_uid": "u1", "context": {"series": {"cart": ok}}},
        "series 是 None": {"incident_uid": "u1", "context": {"series": None}},
        "context 是 list（jsonb 陣列 || 物件 的結果）": {"incident_uid": "u1", "context": [{"series": [ok]}]},
        "context 多了未知頂層鍵": {"incident_uid": "u1", "context": {"series": [ok], "schema": 2, "foo": {"bar": 1}}},
    }
    for name, row in rows.items():
        try:
            out = golden._series_of(row, "cart")
            print(f"{name:42} -> {[(s.entity, s.source_type, s.query) for s in out]}")
        except Exception as e:  # noqa: BLE001
            print(f"{name:42} -> RAISES {type(e).__name__}: {e}")

    url = os.environ.get("PROBE_PG_URL")
    if url:
        from sqlalchemy import create_engine, text
        eng = create_engine(url)
        with eng.connect() as c:
            print("PG: '[1]'::jsonb || '{\"series\": []}'::jsonb =",
                  c.execute(text("""select ('[1]'::jsonb || '{"series": []}'::jsonb)::text""")).scalar())
            print("PG: 'null'::jsonb || '{\"series\": []}'::jsonb =",
                  c.execute(text("""select ('null'::jsonb || '{"series": []}'::jsonb)::text""")).scalar())
            print("PG: SQL NULL || '{\"series\": []}'::jsonb =",
                  c.execute(text("""select (NULL::jsonb || '{"series": []}'::jsonb)::text""")).scalar())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
