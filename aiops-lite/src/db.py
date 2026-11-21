"""平台自己的狀態：收到的告警（待處理）、事故、每一輪的執行紀錄。SQLAlchemy Core，測試用 SQLite、正式用 PostgreSQL。

啟動時自動建表（CREATE TABLE IF NOT EXISTS），沒有 migration 工具：這三張表只有這個程式在用。
之後要改欄位：加欄位就在這裡寫一段可重複執行的 ALTER；不要手動進資料庫改。

  inbox      Webhook 收到、還沒處理的告警。先落地再回 200：等 debounce 的期間服務重啟也不會掉
  incidents  每個事故一列：狀態、嚴重度、成員告警、註記 id、第 2 行摘要
  rounds     每一輪一列：處理了幾筆告警、幾個錯誤
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

ROUND_RETENTION = timedelta(days=90)
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
            annotated=bool(row["annotated"]))

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
                    "updated_at": now}
                done = conn.execute(sa.update(incidents).where(incidents.c.uid == inc.uid).values(**values))
                if done.rowcount == 0:
                    conn.execute(sa.insert(incidents).values(uid=inc.uid, **values))
            if processed:
                conn.execute(sa.delete(inbox).where(inbox.c.id.in_(list(processed))))

    # ── 執行紀錄 ──

    def record_round(self, *, started_at: datetime, finished_at: datetime, status: str, alerts: int,
                     open_incidents: int, errors: list[str]) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.insert(rounds).values(
                started_at=started_at, finished_at=finished_at, status=status, alerts=alerts,
                open_incidents=open_incidents, errors=json.dumps(errors, ensure_ascii=False)))
            conn.execute(sa.delete(rounds).where(rounds.c.started_at < started_at - ROUND_RETENTION))
