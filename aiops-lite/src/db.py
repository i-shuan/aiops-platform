"""平台自己的狀態：規則的基準線、事故、每一輪的執行紀錄。SQLAlchemy Core，測試用 SQLite、正式用 PostgreSQL。

啟動時自動建表（CREATE TABLE IF NOT EXISTS），沒有 migration 工具：這三張表只有這個程式在用。
之後要改欄位：加欄位就在這裡寫一段可重複執行的 ALTER；不要手動進資料庫改。

  rules      每條 Grafana 規則一列：基準線（JSON）、訓練時間、查詢指紋、最後一筆已報異常的時間（new_only 用）
  incidents  每個事故一列：狀態、嚴重度、成員、註記 id、第 2 行摘要
  rounds     每一輪一列：有沒有跑、幾條規則、幾個錯誤。「那個時間沒有列」= 那一輪沒有跑
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa

from src.grouping import Incident

metadata = sa.MetaData()
TS = sa.DateTime(timezone=True)

rules = sa.Table(
    "rules", metadata,
    sa.Column("rule_uid", sa.String(64), primary_key=True),
    sa.Column("title", sa.Text, nullable=False, default=""),
    sa.Column("service", sa.Text, nullable=False, default=""),
    sa.Column("query_hash", sa.String(32), nullable=False, default=""),
    sa.Column("state", sa.Text),             # detectors.fit_all 的結果（JSON）；NULL = 還沒有基準線
    sa.Column("trained_at", TS),
    sa.Column("train_points", sa.Integer, nullable=False, default=0),
    sa.Column("last_anomaly_ts", TS),
    sa.Column("updated_at", TS, nullable=False),
)

incidents = sa.Table(
    "incidents", metadata,
    sa.Column("uid", sa.String(32), primary_key=True),
    sa.Column("status", sa.String(16), nullable=False, index=True),
    sa.Column("severity", sa.String(4), nullable=False),
    sa.Column("services", sa.Text, nullable=False),   # JSON 陣列
    sa.Column("signals", sa.Text, nullable=False),    # JSON：rule_uid -> 成員規則的摘要
    sa.Column("started_at", TS, nullable=False, index=True),
    sa.Column("last_seen_at", TS, nullable=False),
    sa.Column("ended_at", TS),
    sa.Column("quiet_rounds", sa.Integer, nullable=False, default=0),
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
    sa.Column("status", sa.String(16), nullable=False),  # ok／degraded（部分規則失敗）／failed
    sa.Column("rules", sa.Integer, nullable=False, default=0),
    sa.Column("anomalies", sa.Integer, nullable=False, default=0),
    sa.Column("open_incidents", sa.Integer, nullable=False, default=0),
    sa.Column("errors", sa.Text, nullable=False, default="[]"),  # JSON 陣列（字串）
)

ROUND_RETENTION = timedelta(days=90)


def _utc(t: datetime | None) -> datetime | None:
    """SQLite 讀回來的時間沒有時區：一律當 UTC。"""
    if t is None:
        return None
    return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)


def _iso(t: datetime) -> str:
    return _utc(t).isoformat()


class Database:
    def __init__(self, url: str):
        self.engine = sa.create_engine(url, future=True, pool_pre_ping=True)
        metadata.create_all(self.engine)

    def close(self) -> None:
        self.engine.dispose()

    # ── 規則與基準線 ──

    def load_rule(self, rule_uid: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(sa.select(rules).where(rules.c.rule_uid == rule_uid)).mappings().first()
        if row is None:
            return None
        return {"query_hash": row["query_hash"], "state": json.loads(row["state"]) if row["state"] else None,
                "trained_at": _utc(row["trained_at"]), "train_points": row["train_points"],
                "last_anomaly_ts": _utc(row["last_anomaly_ts"])}

    def _upsert_rule(self, conn: sa.Connection, rule_uid: str, values: dict[str, Any], now: datetime) -> None:
        values = {**values, "updated_at": now}
        done = conn.execute(sa.update(rules).where(rules.c.rule_uid == rule_uid).values(**values))
        if done.rowcount == 0:
            conn.execute(sa.insert(rules).values(rule_uid=rule_uid, **values))

    def save_baseline(self, rule_uid: str, *, title: str, service: str, query_hash: str,
                      state: dict[str, Any] | None, train_points: int, now: datetime) -> None:
        """存這次訓練的結果。state=None 表示訓練點數不足：照樣記下訓練時間，隔一個重訓週期再試，不是每 5 分鐘重抓兩週。"""
        with self.engine.begin() as conn:
            self._upsert_rule(conn, rule_uid, {
                "title": title, "service": service, "query_hash": query_hash, "trained_at": now,
                "state": json.dumps(state, ensure_ascii=False) if state is not None else None,
                "train_points": train_points}, now)

    def mark_anomaly(self, rule_uid: str, last_anomaly_ts: datetime, now: datetime) -> None:
        with self.engine.begin() as conn:
            self._upsert_rule(conn, rule_uid, {"last_anomaly_ts": last_anomaly_ts}, now)

    # ── 事故 ──

    @staticmethod
    def _incident(row: Any) -> Incident:
        signals = json.loads(row["signals"])
        for s in signals.values():
            s["first_ts"], s["last_ts"] = datetime.fromisoformat(s["first_ts"]), datetime.fromisoformat(s["last_ts"])
        return Incident(
            uid=row["uid"], status=row["status"], severity=row["severity"], services=json.loads(row["services"]),
            signals=signals, started_at=_utc(row["started_at"]), last_seen_at=_utc(row["last_seen_at"]),
            ended_at=_utc(row["ended_at"]), quiet_rounds=row["quiet_rounds"], alert_count=row["alert_count"],
            summary=row["summary"], analyzed=bool(row["analyzed"]), annotation_id=row["annotation_id"],
            annotated=bool(row["annotated"]))

    def open_incidents(self) -> list[Incident]:
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(incidents).where(incidents.c.status == "open")
                                .order_by(incidents.c.started_at)).mappings().all()
        return [self._incident(r) for r in rows]

    def unannotated_incidents(self) -> list[Incident]:
        """已結案但註記還沒寫成功的（上一輪 Grafana 寫入失敗）：這一輪補寫。"""
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(incidents).where(
                incidents.c.status == "closed", incidents.c.annotated.is_(False))).mappings().all()
        return [self._incident(r) for r in rows]

    def get_incident(self, uid: str) -> Incident | None:
        with self.engine.connect() as conn:
            row = conn.execute(sa.select(incidents).where(incidents.c.uid == uid)).mappings().first()
        return self._incident(row) if row else None

    def save_incidents(self, items: list[Incident], marks: dict[str, datetime], now: datetime) -> None:
        """事故與「各規則已報到哪個時間」在同一個交易寫入：要嘛都進去，要嘛都沒有（不會異常算過了、事故卻沒存到）。"""
        with self.engine.begin() as conn:
            for inc in items:
                signals = {uid: {**s, "first_ts": _iso(s["first_ts"]), "last_ts": _iso(s["last_ts"])}
                           for uid, s in inc.signals.items()}
                values = {
                    "status": inc.status, "severity": inc.severity,
                    "services": json.dumps(inc.services, ensure_ascii=False),
                    "signals": json.dumps(signals, ensure_ascii=False),
                    "started_at": inc.started_at, "last_seen_at": inc.last_seen_at, "ended_at": inc.ended_at,
                    "quiet_rounds": inc.quiet_rounds, "alert_count": inc.alert_count, "summary": inc.summary,
                    "analyzed": inc.analyzed, "annotation_id": inc.annotation_id, "annotated": inc.annotated,
                    "updated_at": now}
                done = conn.execute(sa.update(incidents).where(incidents.c.uid == inc.uid).values(**values))
                if done.rowcount == 0:
                    conn.execute(sa.insert(incidents).values(uid=inc.uid, **values))
            for rule_uid, ts in marks.items():
                self._upsert_rule(conn, rule_uid, {"last_anomaly_ts": ts}, now)

    def incident_windows(self, service: str, since: datetime) -> list[tuple[datetime, datetime]]:
        """這個系統過去的事故時間窗：訓練基準線時要扣掉，不然事故會被學成常態。"""
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(incidents.c.services, incidents.c.started_at, incidents.c.last_seen_at)
                                .where(incidents.c.last_seen_at >= since)).all()
        return [(_utc(start), _utc(end)) for services, start, end in rows if service in json.loads(services)]

    # ── 執行紀錄 ──

    def record_round(self, *, started_at: datetime, finished_at: datetime, status: str, rules_count: int,
                     anomalies: int, open_incidents: int, errors: list[str]) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.insert(rounds).values(
                started_at=started_at, finished_at=finished_at, status=status, rules=rules_count,
                anomalies=anomalies, open_incidents=open_incidents,
                errors=json.dumps(errors, ensure_ascii=False)))
            conn.execute(sa.delete(rounds).where(rounds.c.started_at < started_at - ROUND_RETENTION))
