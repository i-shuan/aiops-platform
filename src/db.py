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
    sa.Column("llm_failures", sa.Integer, nullable=False, default=0, server_default="0"),
    sa.Column("llm_next_try", TS),
    sa.Column("candidates", sa.Text),      # 可以是 NULL（舊資料、沒有經過 LLM）
    sa.Column("evidence_note", sa.Text),   # 可以是 NULL（舊資料、還沒收集過證據）
    sa.Column("actions_note", sa.Text),    # 註記上「自動修復｜…」那一行；可以是 NULL（沒有修復流程）
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

deliveries = sa.Table(
    "deliveries", metadata,   # 註記以外的出口（事故 API、自動修復）處理過什麼：見 src/outputs.py
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("channel", sa.String(32), nullable=False),          # incident_api／remediation
    sa.Column("dedupe_key", sa.String(64), nullable=False),       # 同一個出口、同樣的內容只處理一次
    sa.Column("incident_uid", sa.String(32), nullable=False, index=True),
    sa.Column("status", sa.String(16), nullable=False),           # placeholder（沒有送出）／sent／failed
    sa.Column("payload", sa.Text, nullable=False),                # JSON：送出去的（或本來會送的）內容
    sa.Column("detail", sa.Text, nullable=False, default=""),     # 失敗的原因
    sa.Column("attempts", sa.Integer, nullable=False, default=0),
    sa.Column("updated_at", TS, nullable=False),
    sa.UniqueConstraint("channel", "dedupe_key"),
)

calls = sa.Table(
    "observed_calls", metadata,   # 從事故的呼叫鏈裡看到的「誰呼叫誰」，一次一次累積（見 rca.calls_in）。只給 LLM 當線索，不拿來合併事故
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("caller", sa.String(255), nullable=False),
    sa.Column("callee", sa.String(255), nullable=False),
    sa.Column("first_seen", TS, nullable=False),
    sa.Column("last_seen", TS, nullable=False, index=True),
    sa.Column("times", sa.Integer, nullable=False, default=1),   # 有幾次收集證據時看到這條呼叫（不是請求數）
    sa.UniqueConstraint("caller", "callee"),
)

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
            **{k: getattr(a, k) for k in ("fingerprint", "status", "rule_uid", "title", "service", "severity", "values", "remediation")},
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
            annotated=bool(row["annotated"]), llm_failures=row["llm_failures"], llm_next_try=_utc(row["llm_next_try"]),
            evidence_note=row["evidence_note"] or "", candidates=row["candidates"] or "", actions_note=row["actions_note"] or "")

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
                    "llm_failures": inc.llm_failures, "llm_next_try": inc.llm_next_try,
                    "evidence_note": inc.evidence_note, "candidates": inc.candidates, "actions_note": inc.actions_note,
                    "updated_at": now}
                done = conn.execute(sa.update(incidents).where(incidents.c.uid == inc.uid).values(**values))
                if done.rowcount == 0:
                    conn.execute(sa.insert(incidents).values(uid=inc.uid, **values))
            if processed:
                conn.execute(sa.delete(inbox).where(inbox.c.id.in_(list(processed))))

    # ── 出口的處理紀錄 ──

    def delivery(self, channel: str, key: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(sa.select(deliveries).where(deliveries.c.channel == channel, deliveries.c.dedupe_key == key)).mappings().first()
        return dict(row) if row else None

    def has_delivery(self, channel: str, incident_uid: str) -> bool:
        """這張事故在這個出口處理過了沒（佔位也算）：用來分辨第一次（opened）與之後的更新（updated）。"""
        with self.engine.connect() as conn:
            return conn.execute(sa.select(deliveries.c.id).where(deliveries.c.channel == channel, deliveries.c.incident_uid == incident_uid,
                                                                  deliveries.c.status.in_(("sent", "placeholder"))).limit(1)).first() is not None

    def deliveries_of(self, channel: str, incident_uid: str) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(sa.select(deliveries).where(
                deliveries.c.channel == channel, deliveries.c.incident_uid == incident_uid).order_by(deliveries.c.id)).mappings()]

    def record_delivery(self, channel: str, key: str, incident_uid: str, status: str, payload: dict[str, Any], detail: str,
                        attempts: int, now: datetime) -> None:
        values = {"incident_uid": incident_uid, "status": status, "payload": json.dumps(payload, ensure_ascii=False),
                  "detail": detail, "attempts": attempts, "updated_at": now}
        with self.engine.begin() as conn:
            done = conn.execute(sa.update(deliveries).where(deliveries.c.channel == channel, deliveries.c.dedupe_key == key).values(**values))
            if done.rowcount == 0:
                conn.execute(sa.insert(deliveries).values(channel=channel, dedupe_key=key, **values))

    def incidents_to_redeliver(self, max_attempts: int) -> list[Incident]:
        """有出口沒送成功、還可以再試的事故（包含已經結案的：結案那一筆沒送到也要補）。"""
        with self.engine.connect() as conn:
            uids = [r[0] for r in conn.execute(sa.select(deliveries.c.incident_uid).where(
                deliveries.c.status == "failed", deliveries.c.attempts < max_attempts).distinct())]
        return self._select(incidents.c.uid.in_(uids)) if uids else []

    # ── 觀察到的呼叫關係 ──

    def record_calls(self, pairs: list[tuple[str, str]], now: datetime, retention_days: int) -> None:
        """記下這次看到的呼叫（已經有的更新最後看到的時間與次數），順便清掉太久沒再看到的。"""
        with self.engine.begin() as conn:
            for caller, callee in sorted(set(pairs)):
                done = conn.execute(sa.update(calls).where(calls.c.caller == caller, calls.c.callee == callee)
                                    .values(last_seen=now, times=calls.c.times + 1))
                if done.rowcount == 0:
                    conn.execute(sa.insert(calls).values(caller=caller, callee=callee, first_seen=now, last_seen=now, times=1))
            conn.execute(sa.delete(calls).where(calls.c.last_seen < now - timedelta(days=retention_days)))

    def calls_touching(self, services: list[str], now: datetime, retention_days: int, limit: int) -> list[dict[str, Any]]:
        """跟這些系統有關（它呼叫別人、或別人呼叫它）、而且還沒過期的呼叫關係，最近看到的在前。"""
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(calls).where(
                sa.or_(calls.c.caller.in_(services), calls.c.callee.in_(services)),
                calls.c.last_seen >= now - timedelta(days=retention_days)).order_by(calls.c.last_seen.desc(), calls.c.id).limit(limit)).mappings().all()
        return [{"caller": r["caller"], "callee": r["callee"], "last_seen": _iso(r["last_seen"]), "times": r["times"]} for r in rows]

    # ── 執行紀錄 ──

    def record_round(self, *, started_at: datetime, finished_at: datetime, status: str, alerts: int,
                     open_incidents: int, errors: list[str], retention_days: int = 90) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.insert(rounds).values(
                started_at=started_at, finished_at=finished_at, status=status, alerts=alerts,
                open_incidents=open_incidents, errors=json.dumps(errors, ensure_ascii=False)))
            conn.execute(sa.delete(rounds).where(rounds.c.started_at < started_at - timedelta(days=retention_days)))
