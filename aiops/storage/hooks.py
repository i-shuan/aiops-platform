"""引擎的載入與寫入點（實作 core.pipeline.Hooks）。

- 載入：Pipeline 跑之前，把「要先存起來、之後被讀進流程」的輸入放進 ctx（靜音、冷卻紀錄、欄位對照、基準線、拓撲…）
- 寫入：Pipeline 跑完，把最終結果寫進資料庫（異常、事件群、通知紀錄、審計、新訓練的基準線）
流程中間的節點完全不碰資料庫。這裡只搬資料，不做任何業務判斷。
"""
from __future__ import annotations

from datetime import datetime, timedelta

from aiops.core.context import (
    DetectorState,
    LotEdge,
    OpenIncident,
    PipelineContext,
    RecentNotification,
    Silence,
    TraceRow,
)
from aiops.core.topology import lot_edges_from_paths
from aiops.core.types import AnomalyEvent, Incident, as_utc
from aiops.storage.repository import (
    ActiveConfigs,
    Anomalies,
    DetectorModels,
    ExternalAnomalies,
    Incidents,
    LotPaths,
    Mappings,
    Notifications,
    ServiceEdges,
    Silences,
    Traces,
)

# 冷卻（dedup）最多往回看多久的通知紀錄；算子自己的 cooldown_minutes 不能超過它
RECENT_NOTIFICATION_DAYS = 7
# 載入 trace 時序的範圍（算子再依自己的偵測窗取用）
TRACE_LOOKBACK_DAYS = 7
_RCA_TRACE_MARGIN = timedelta(minutes=10)


def _trace_service(query: str) -> str:
    return query.strip().rpartition("/")[0]


class StorageHooks:
    # ── 載入 ──

    def load_batch(self, ctx: PipelineContext) -> None:
        ctx.silences = [Silence(r["id"], r["kind"], r["start"], r["end"], r["entity"], r["dept"])
                        for r in Silences.list_all()]
        cutoff = ctx.now - timedelta(days=RECENT_NOTIFICATION_DAYS)
        ctx.recent_notifications = [RecentNotification(r["fingerprint"], r["last_event_ts"])
                                    for r in Notifications.since(cutoff.replace(tzinfo=None))]
        if not ctx.topology_explicit:
            ctx.topology = [(e["caller"], e["callee"]) for e in ServiceEdges.all()]
        if not ctx.lot_edges_explicit:
            ctx.lot_edges = lot_edges_from_paths(LotPaths.paths())

        for row in Incidents.list_open(ctx.pipeline):
            ctx.open_incidents.append(OpenIncident(row["id"], row["uid"], row["preset"], row["severity"],
                                                   Incident.from_dict(row["incident"], uid=row["uid"])))
        entities = sorted({e for o in ctx.open_incidents for e in o.incident.entities})
        ctx.entity_last_seen = {k: as_utc(v) for k, v in Anomalies.entity_last_seen(ctx.pipeline, entities).items()}

        for row in ExternalAnomalies.pending(ctx.pipeline):
            ctx.anomalies.append(AnomalyEvent(row["entity"], row["metric"], as_utc(row["ts"]), row["score"],
                                              row["attrs"] or {}, row["value"]))
            ctx.external_anomaly_ids.append(row["id"])

        if ctx.incident_id is not None:  # RCA：載入指定的事件群與佐證資料
            row = Incidents.get(ctx.incident_id)
            if row is not None:
                incident = Incident.from_dict(row["incident"], uid=row["uid"])
                ctx.incidents = [incident]
                ctx.incident_ids[incident.fingerprint] = row["id"]
                snapshot = row.get("context") or {}  # 建立當下用的拓撲優先（可重現）；沒有才用上面現推導的
                if not ctx.topology_explicit and snapshot.get("topology"):
                    ctx.topology = [(a, b) for a, b in snapshot["topology"]]
                if not ctx.lot_edges_explicit and snapshot.get("lot_edges"):
                    ctx.lot_edges = [LotEdge(e["from"], e["to"], e["lots"]) for e in snapshot["lot_edges"]]
                rows = Traces.window(incident.entities, incident.start - _RCA_TRACE_MARGIN,
                                     incident.end + _RCA_TRACE_MARGIN)
                for r in rows:
                    ctx.trace_rows.setdefault(r["service"], []).append(
                        TraceRow(r["service"], as_utc(r["ts"]), r["count"], r["error_rate"], r["p95_ms"]))

    def load_target(self, ctx: PipelineContext, detector_ops: list[str]) -> None:
        target = ctx.target
        assert target is not None
        for source_type in {target.source_type, *(p.source_type for p in target.peers)}:
            fields = Mappings.approved_fields(source_type)
            if fields:
                ctx.field_maps[source_type] = fields

        refs = [(target.source_type, target.query), *((p.source_type, p.query) for p in target.peers)]
        services = sorted({_trace_service(q) for s, q in refs if s == "trace"})
        if services:
            since = ctx.now - timedelta(days=TRACE_LOOKBACK_DAYS)
            for r in Traces.window(services, since.replace(tzinfo=None), ctx.now.replace(tzinfo=None)):
                ctx.trace_rows.setdefault(r["service"], []).append(
                    TraceRow(r["service"], as_utc(r["ts"]), r["count"], r["error_rate"], r["p95_ms"]))

        for op in detector_ops:
            active = ActiveConfigs.get(op)
            if active:
                ctx.detector_params[op] = active["params"]
            model = DetectorModels.get(ctx.pipeline, target.entity, target.query, op)
            if model:
                ctx.detector_states[op] = DetectorState(model["state"], as_utc(model["trained_at"]),
                                                        model["train_start"], model["train_end"], model["points"])
        last = Anomalies.latest_ts(ctx.pipeline, target.entity, target.query)
        ctx.last_anomaly_ts = as_utc(last) if last else None

    # ── 寫入 ──

    def persist_target(self, ctx: PipelineContext) -> None:
        target = ctx.target
        assert target is not None
        for op, st in ctx.new_detector_states.items():
            DetectorModels.save(ctx.pipeline, target.entity, target.query, op, st.state, st.train_start or ctx.now,
                                st.train_end or ctx.now, st.points, trained_at=st.trained_at)
        algorithm = ctx.anomaly_result.algorithm if ctx.anomaly_result else ""
        Anomalies.create_many([
            {"pipeline": ctx.pipeline, "entity": e.entity, "metric": e.metric, "ts": e.ts,
             "value": float(e.value) if e.value is not None else 0.0, "score": float(e.score),
             "detector_name": algorithm, "attrs": e.attrs}
            for e in ctx.anomalies
        ])

    def persist_batch(self, ctx: PipelineContext) -> None:
        sent: dict[str, int] = {}
        for n in ctx.notifications:
            uid = n.labels.get("incident_uid", "")
            sent[uid] = sent.get(uid, 0) + 1
        loaded = set(ctx.incident_ids)  # 載入進來的既有事件群（例如 RCA）已經存在，只寫這一輪新產生的
        for inc in ctx.incidents:
            if inc.fingerprint in loaded:
                continue
            entities = set(inc.entities)
            snapshot = {
                "topology": [[a, b] for a, b in ctx.topology if a in entities or b in entities],
                "lot_edges": [{"from": e.upstream, "to": e.downstream, "lots": e.lots} for e in ctx.lot_edges
                              if e.upstream in entities or e.downstream in entities],
            }
            row = Incidents.create(inc.uid, ctx.pipeline, inc.to_dict(), inc.severity, inc.suppressed_by,
                                   ctx.pipeline, snapshot)
            ctx.incident_ids[inc.fingerprint] = row["id"]
            if not inc.suppressed_by and sent.get(inc.uid):
                Notifications.record(row["id"], inc.fingerprint, inc.severity, inc.end, sent[inc.uid])
        for incident_id in ctx.resolved_incident_ids:
            Incidents.mark_resolved(incident_id, ctx.now.replace(tzinfo=None))
        ExternalAnomalies.mark_processed(ctx.external_anomaly_ids)
