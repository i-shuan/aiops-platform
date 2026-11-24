"""平台自己的狀態：收到的告警（待處理）、事故、每一輪的執行紀錄。SQLAlchemy Core，測試用 SQLite、正式用 PostgreSQL。

啟動時自動建表（CREATE TABLE IF NOT EXISTS），沒有 migration 工具：這三張表只有這個程式在用。
之後要改欄位：加欄位就在這裡寫一段可重複執行的 ALTER；不要手動進資料庫改。

  inbox      Webhook 收到、還沒處理的告警。先落地再回 200：等 debounce 的期間服務重啟也不會掉
  incidents  每個事故一列：狀態、嚴重度、成員告警、註記 id、第 2 行摘要
  rounds     每一輪一列：處理了幾筆告警、幾個錯誤
  settings_overrides  管理介面改過的設定（一列，JSON；結構跟 config.yaml 一樣，只有可調的那些鍵）。沒有這一列 = 全部照 config.yaml
  settings_history    每次儲存留一列（改之前、改之後），給管理介面看誰在什麼時候改了什麼；不會自動清
  feedback            管理介面上對 RCA 摘要的回饋（正確／錯誤 + 備註），一張事故可以有多筆，以最新一筆為準
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa

from src.grouping import Alert, Incident

metadata = sa.MetaData()
TS = sa.DateTime(timezone=True)

inbox = sa.Table(
    "inbox", metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("received_at", TS, nullable=False),
    sa.Column("alert", sa.Text, nullable=False),  # JSON：grouping.Alert 的欄位
)

incidents = sa.Table(
    "incidents", metadata,
    sa.Column("uid", sa.String(32), primary_key=True),
    sa.Column("status", sa.String(16), nullable=False, index=True),
    sa.Column("severity", sa.String(4), nullable=False),
    sa.Column("services", sa.Text, nullable=False),   # JSON 陣列
    sa.Column("signals", sa.Text, nullable=False),    # JSON：fingerprint -> 成員告警
    sa.Column("started_at", TS, nullable=False, index=True),
    sa.Column("last_seen_at", TS, nullable=False),
    sa.Column("ended_at", TS),
    sa.Column("resolved_at", TS),
    sa.Column("alert_count", sa.Integer, nullable=False, default=0),
    sa.Column("summary", sa.Text, nullable=False, default=""),
    sa.Column("analyzed", sa.Boolean, nullable=False, default=False),
    sa.Column("annotation_id", sa.BigInteger),
    sa.Column("annotated", sa.Boolean, nullable=False, default=False),
    sa.Column("llm_failures", sa.Integer, nullable=False, default=0, server_default="0"),
    sa.Column("llm_next_try", TS),
    sa.Column("updated_at", TS, nullable=False),
)

rounds = sa.Table(
    "rounds", metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("started_at", TS, nullable=False, index=True),
    sa.Column("finished_at", TS, nullable=False),
    sa.Column("status", sa.String(16), nullable=False),  # ok／degraded（有錯誤但跑完）／failed
    sa.Column("alerts", sa.Integer, nullable=False, default=0),
    sa.Column("open_incidents", sa.Integer, nullable=False, default=0),
    sa.Column("errors", sa.Text, nullable=False, default="[]"),  # JSON 陣列（字串）
)

settings_overrides = sa.Table(
    "settings_overrides", metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),   # 永遠只有 id = 1 這一列
    sa.Column("doc", sa.Text, nullable=False),                            # JSON
    sa.Column("updated_at", TS, nullable=False),
)

settings_history = sa.Table(
    "settings_history", metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("changed_at", TS, nullable=False, index=True),
    sa.Column("before", sa.Text, nullable=False),   # JSON：改之前的 overrides
    sa.Column("after", sa.Text, nullable=False),    # JSON：改之後的 overrides
)

feedback = sa.Table(
    "feedback", metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("incident_uid", sa.String(32), nullable=False, index=True),
    sa.Column("verdict", sa.String(8), nullable=False),    # ok／wrong
    sa.Column("note", sa.Text, nullable=False, default=""),
    sa.Column("summary", sa.Text, nullable=False, default=""),   # 回饋當下的摘要原文：之後摘要重新分析了，也知道當時評的是哪一版
    sa.Column("created_at", TS, nullable=False),
)

VERDICTS = ("ok", "wrong")
MEMBER_TIMES = ("starts_at", "ends_at")


def _utc(t: datetime | None) -> datetime | None:
    """SQLite 讀回來的時間沒有時區：一律當 UTC。"""
    if t is None:
        return None
    return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)


def _iso(t: datetime | None) -> str | None:
    return _utc(t).isoformat() if t else None


def _time(raw: str | None) -> datetime | None:
    return datetime.fromisoformat(raw) if raw else None


class Database:
    def __init__(self, url: str):
        self.engine = sa.create_engine(url, future=True, pool_pre_ping=True)
        metadata.create_all(self.engine)
        self._add_missing_columns()

    def _add_missing_columns(self) -> None:
        """舊版建好的表沒有後來新增的欄位（create_all 不會補）：啟動時補上，可以重複執行。
        只處理「加欄位」；改欄位型別、刪欄位要手動處理。"""
        inspector = sa.inspect(self.engine)
        for table in metadata.sorted_tables:
            have = {c["name"] for c in inspector.get_columns(table.name)}
            for column in table.columns:
                if column.name in have:
                    continue
                ddl = column.type.compile(self.engine.dialect)
                default = f" DEFAULT {column.server_default.arg}" if column.server_default is not None else ""
                with self.engine.begin() as conn:
                    conn.execute(sa.text(f'ALTER TABLE {table.name} ADD COLUMN {column.name} {ddl}'
                                         f'{default}{"" if column.nullable else " NOT NULL"}'))

    def close(self) -> None:
        self.engine.dispose()

    # ── 待處理的告警 ──

    def enqueue(self, alerts: list[Alert], now: datetime) -> None:
        if not alerts:
            return
        rows = [{"received_at": now, "alert": json.dumps({
            **{k: getattr(a, k) for k in ("fingerprint", "status", "rule_uid", "title", "service", "severity", "values")},
            "starts_at": _iso(a.starts_at), "ends_at": _iso(a.ends_at)}, ensure_ascii=False)} for a in alerts]
        with self.engine.begin() as conn:
            conn.execute(sa.insert(inbox), rows)

    def pending(self) -> tuple[list[int], list[Alert]]:
        """依收到的順序回傳 (inbox 的 id, 告警)。"""
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(inbox.c.id, inbox.c.alert).order_by(inbox.c.id)).all()
        alerts = []
        for _id, raw in rows:
            d = json.loads(raw)
            alerts.append(Alert(**{**d, "starts_at": _time(d["starts_at"]), "ends_at": _time(d["ends_at"])}))
        return [r[0] for r in rows], alerts

    # ── 事故 ──

    @staticmethod
    def _incident(row: Any) -> Incident:
        signals = json.loads(row["signals"])
        for member in signals.values():
            for key in MEMBER_TIMES:
                member[key] = _time(member[key])
        return Incident(
            uid=row["uid"], status=row["status"], severity=row["severity"], services=json.loads(row["services"]),
            signals=signals, started_at=_utc(row["started_at"]), last_seen_at=_utc(row["last_seen_at"]),
            ended_at=_utc(row["ended_at"]), resolved_at=_utc(row["resolved_at"]), alert_count=row["alert_count"],
            summary=row["summary"], analyzed=bool(row["analyzed"]), annotation_id=row["annotation_id"],
            annotated=bool(row["annotated"]), llm_failures=row["llm_failures"], llm_next_try=_utc(row["llm_next_try"]))

    def _select(self, where: Any) -> list[Incident]:
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(incidents).where(where).order_by(incidents.c.started_at)).mappings().all()
        return [self._incident(r) for r in rows]

    def open_incidents(self) -> list[Incident]:
        return self._select(incidents.c.status == "open")

    def unannotated_incidents(self) -> list[Incident]:
        """已結案但註記還沒寫成功的（上一輪 Grafana 寫入失敗）：這一輪補寫。"""
        return self._select(sa.and_(incidents.c.status == "closed", incidents.c.annotated.is_(False)))

    def get_incident(self, uid: str) -> Incident | None:
        found = self._select(incidents.c.uid == uid)
        return found[0] if found else None

    def save_incidents(self, items: list[Incident], now: datetime, *, processed: list[int] = ()) -> None:
        """事故與「這些告警處理完了」在同一個交易寫入：要嘛都進去，要嘛都沒有（不會告警消掉了、事故卻沒存到）。"""
        with self.engine.begin() as conn:
            for inc in items:
                signals = {fp: {**m, **{k: _iso(m[k]) for k in MEMBER_TIMES}} for fp, m in inc.signals.items()}
                values = {
                    "status": inc.status, "severity": inc.severity,
                    "services": json.dumps(inc.services, ensure_ascii=False),
                    "signals": json.dumps(signals, ensure_ascii=False),
                    "started_at": inc.started_at, "last_seen_at": inc.last_seen_at, "ended_at": inc.ended_at,
                    "resolved_at": inc.resolved_at, "alert_count": inc.alert_count, "summary": inc.summary,
                    "analyzed": inc.analyzed, "annotation_id": inc.annotation_id, "annotated": inc.annotated,
                    "llm_failures": inc.llm_failures, "llm_next_try": inc.llm_next_try, "updated_at": now}
                done = conn.execute(sa.update(incidents).where(incidents.c.uid == inc.uid).values(**values))
                if done.rowcount == 0:
                    conn.execute(sa.insert(incidents).values(uid=inc.uid, **values))
            if processed:
                conn.execute(sa.delete(inbox).where(inbox.c.id.in_(list(processed))))

    def list_incidents(self, status: str | None = None, limit: int = 50, offset: int = 0) -> tuple[list[Incident], int]:
        """給管理介面：新的在前。回傳 (這一頁, 符合條件的總數)。"""
        where = sa.true() if status is None else incidents.c.status == status
        with self.engine.connect() as conn:
            total = conn.execute(sa.select(sa.func.count()).select_from(incidents).where(where)).scalar_one()
            rows = conn.execute(sa.select(incidents).where(where).order_by(incidents.c.started_at.desc(), incidents.c.uid)
                                .limit(limit).offset(offset)).mappings().all()
        return [self._incident(r) for r in rows], total

    def inbox_size(self) -> int:
        with self.engine.connect() as conn:
            return conn.execute(sa.select(sa.func.count()).select_from(inbox)).scalar_one()

    def ping(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(sa.select(1))

    # ── 管理介面改過的設定 ──

    def get_overrides(self) -> dict[str, Any]:
        with self.engine.connect() as conn:
            raw = conn.execute(sa.select(settings_overrides.c.doc).where(settings_overrides.c.id == 1)).scalar_one_or_none()
        return json.loads(raw) if raw else {}

    def save_overrides(self, doc: dict[str, Any], now: datetime) -> None:
        """整份取代，並在同一個交易留一筆歷史（改之前、改之後）。"""
        after = json.dumps(doc, ensure_ascii=False, sort_keys=True)
        with self.engine.begin() as conn:
            before = conn.execute(sa.select(settings_overrides.c.doc).where(settings_overrides.c.id == 1)).scalar_one_or_none()
            if before is None:
                conn.execute(sa.insert(settings_overrides).values(id=1, doc=after, updated_at=now))
            else:
                conn.execute(sa.update(settings_overrides).where(settings_overrides.c.id == 1).values(doc=after, updated_at=now))
            conn.execute(sa.insert(settings_history).values(changed_at=now, before=before or "{}", after=after))

    def overrides_history(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(settings_history).order_by(settings_history.c.id.desc()).limit(limit)).mappings().all()
        return [{"changed_at": _iso(r["changed_at"]), "before": json.loads(r["before"]), "after": json.loads(r["after"])}
                for r in rows]

    # ── RCA 回饋 ──

    def add_feedback(self, incident_uid: str, verdict: str, note: str, summary: str, now: datetime) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.insert(feedback).values(incident_uid=incident_uid, verdict=verdict, note=note,
                                                    summary=summary, created_at=now))

    def feedback_for(self, uids: list[str]) -> dict[str, list[dict[str, Any]]]:
        """每張事故的回饋，新的在前。"""
        if not uids:
            return {}
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(feedback).where(feedback.c.incident_uid.in_(uids))
                                .order_by(feedback.c.id.desc())).mappings().all()
        out: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            out.setdefault(r["incident_uid"], []).append({
                "verdict": r["verdict"], "note": r["note"], "summary": r["summary"], "created_at": _iso(r["created_at"])})
        return out

    def feedback_counts(self) -> dict[str, int]:
        """以每張事故最新的一筆回饋為準，各有幾張。"""
        latest = sa.select(sa.func.max(feedback.c.id)).group_by(feedback.c.incident_uid)
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(feedback.c.verdict, sa.func.count()).where(feedback.c.id.in_(latest))
                                .group_by(feedback.c.verdict)).all()
        return {**{v: 0 for v in VERDICTS}, **{v: n for v, n in rows}}

    # ── 執行紀錄 ──

    def list_rounds(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(rounds).order_by(rounds.c.id.desc()).limit(limit)).mappings().all()
        return [{"started_at": _iso(r["started_at"]), "finished_at": _iso(r["finished_at"]), "status": r["status"],
                 "alerts": r["alerts"], "open_incidents": r["open_incidents"], "errors": json.loads(r["errors"])} for r in rows]

    def record_round(self, *, started_at: datetime, finished_at: datetime, status: str, alerts: int,
                     open_incidents: int, errors: list[str], retention_days: int = 90) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.insert(rounds).values(
                started_at=started_at, finished_at=finished_at, status=status, alerts=alerts,
                open_incidents=open_incidents, errors=json.dumps(errors, ensure_ascii=False)))
            conn.execute(sa.delete(rounds).where(rounds.c.started_at < started_at - timedelta(days=retention_days)))
