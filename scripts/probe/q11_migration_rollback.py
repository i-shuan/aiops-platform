"""Q11：0005（main）與 0006（PR C）註解裡的回滾 SQL 在真 PostgreSQL 上實際跑一次（PROBE_PG_URL，拋棄式容器）。

0005：建表 → 跑 0005 → 寫一筆 false_positive（被降級的黃金事故，context 有 tags 與 series）與一筆 true_positive
      → 跑回滾 → 看資料剩什麼 → 再跑一次 0005（升回新版）→ 看 false_positive 那筆變成什麼
0006：建 service_edges（last_seen NOT NULL 的舊版形狀）→ 跑 0006 → 插一條 last_seen NULL 的邊 → 跑回滾 → 看那條邊
用法：PROBE_PG_URL=... python scripts/probe/q11_migration_rollback.py --prc <PR C 原始碼根（含 migrations/）>
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from _common import REPO, use_root  # noqa: E402


def rollback_sql(path: Path) -> str:
    """把檔頭註解裡「回滾」那段（-- 開頭、BEGIN; 到 COMMIT;）取出來。"""
    text = path.read_text(encoding="utf-8")
    block = text.split("回滾", 1)[1]
    lines = [ln[2:].strip() for ln in block.splitlines() if ln.startswith("--")]
    body = "\n".join(lines)
    return body[body.index("BEGIN;"): body.index("COMMIT;") + len("COMMIT;")]


def main() -> int:
    use_root(sys.argv)
    prc = Path(sys.argv[sys.argv.index("--prc") + 1]) if "--prc" in sys.argv else None
    from sqlalchemy import create_engine, text
    eng = create_engine(os.environ["PROBE_PG_URL"], isolation_level="AUTOCOMMIT")
    m5 = REPO / "migrations/postgres/0005_knowledge_golden_incidents.sql"

    def run(sql: str):
        with eng.connect() as c:  # 走驅動層、不帶參數：檔案裡 RAISE NOTICE 的 % 不會被當成佔位符
            c.connection.driver_connection.execute(sql)

    run("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    # 0005 之前的舊形狀（只保留這題需要的欄位）
    run("""CREATE TABLE incidents (id serial primary key, uid varchar(40) unique, incident json, severity varchar(8));
           CREATE TABLE incident_knowledge (id serial primary key, incident_uid varchar(40) unique, pipeline varchar(128),
             entities json, metrics json, signals json, resolution varchar(4000) NOT NULL, root_cause varchar(1000),
             author varchar(128), created_at timestamp);""")
    run(m5.read_text(encoding="utf-8"))
    run("""INSERT INTO incident_knowledge (incident_uid, pipeline, entities, metrics, signals, resolution, root_cause, author,
             created_at, verdict, severity, window_start, window_end, context) VALUES
           ('inc-fp', 'it', '["cart"]', '[]', '[]', '', '', 'ops', now(), 'false_positive', 'P2', now(), now(),
            '{"tags": ["deploy"], "series": [{"entity": "cart", "source_type": "mock", "query": "cpu"}]}'),
           ('inc-tp', 'it', '["cart"]', '[]', '[]', '擴 pool', 'db', 'ops', now(), 'true_positive', 'P1', now(), now(),
            '{"trace_id": "abc"}')""")
    with eng.connect() as c:
        print("[0005] 回滾前:", c.execute(text("select incident_uid, verdict, context::text from incident_knowledge order by id")).all())
    rb = rollback_sql(m5)
    print("[0005] 回滾 SQL 有 CASCADE？", bool(re.search(r"cascade", rb, re.I)), "；有 DELETE？", bool(re.search(r"\bdelete\b", rb, re.I)))
    run(rb)
    with eng.connect() as c:
        cols = [r[0] for r in c.execute(text("select column_name from information_schema.columns where table_name='incident_knowledge' order by ordinal_position"))]
        print("[0005] 回滾後欄位:", cols)
        print("[0005] 回滾後列:", c.execute(text("select incident_uid, resolution from incident_knowledge order by id")).all())
    run(m5.read_text(encoding="utf-8"))
    with eng.connect() as c:
        print("[0005] 再升級後:", c.execute(text("select incident_uid, verdict, context::text from incident_knowledge order by id")).all())

    if prc is not None:
        m6 = prc / "migrations/postgres/0006_service_edges_last_seen.sql"
        run("CREATE TABLE service_edges (id serial primary key, caller varchar(128), callee varchar(128), calls int, "
            "last_seen timestamp NOT NULL, UNIQUE (caller, callee));")
        run("INSERT INTO service_edges (caller, callee, calls, last_seen) VALUES ('a', 'b', 3, now() - interval '30 days')")
        run(m6.read_text(encoding="utf-8"))
        run("INSERT INTO service_edges (caller, callee, calls, last_seen) VALUES ('c', 'd', 1, NULL)")
        rb6 = rollback_sql(m6)
        print("[0006] 回滾 SQL 有 CASCADE？", bool(re.search(r"cascade", rb6, re.I)), "；有 DELETE？", bool(re.search(r"\bdelete\b", rb6, re.I)))
        run(rb6)
        with eng.connect() as c:
            print("[0006] 回滾後:", [(r[0], r[1], str(r[2])[:19]) for r in c.execute(text("select caller, callee, last_seen from service_edges order by id"))],
                  "（c→d 原本是 NULL = 不知道）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
