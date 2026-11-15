"""引擎的載入與寫入點（實作 engine.pipeline.Hooks）。

- 載入：Pipeline 跑之前，把「要先存起來、之後被讀進流程」的輸入放進 ctx（靜音、冷卻紀錄、欄位對照、基準線、拓撲…）
- 寫入：Pipeline 跑完，把最終結果寫進資料庫（異常、事件群、通知紀錄、審計、新訓練的基準線）
流程中間的節點完全不碰資料庫。這裡只搬資料，不做任何業務判斷。
"""
from __future__ import annotations

from datetime import datetime, timedelta

from aiops.engine.context import (
    DetectorState,
    KnowledgeCase,
    LotEdge,
    OpenIncident,
    PipelineContext,
    RecentNotification,
    Silence,
    TargetError,
    TraceRow,
    TrainingExclusion,
)
from aiops.engine.errors import NotFound
from aiops.engine.params import pick_most_specific
from aiops.engine.topology import lot_edges_from_paths
from aiops.engine.types import AnomalyEvent, Incident, as_utc
from aiops.storage.db import run_in_transaction
from aiops.storage.repository import (
    ActiveConfigs,
    Anomalies,
    AsyncEdges,
    DetectorModels,
    ExternalAnomalies,
    Feedbacks,
    Incidents,
    Knowledge,
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
# RCA 找相似事故時，最多拿最近幾筆經驗出來比（避免經驗庫很大時整包載入）
KNOWLEDGE_POOL_SIZE = 500
# 訓練基準線時往回看多久的事故（要涵蓋最長的訓練窗；168 時段基準線建議 3～4 週）與前後多排除幾分鐘
EXCLUSION_LOOKBACK_DAYS = 35
EXCLUSION_MARGIN = timedelta(minutes=10)


def _trace_service(query: str) -> str:
    return query.strip().rpartition("/")[0]


class StorageHooks:
    # ── 載入 ──

    def load_batch(self, ctx: PipelineContext) -> None:
        # 每輪開頭清掉排除窗快取：同一個 hooks 被重複用、兩輪 now 又相同時，不能拿到上一輪的事故清單
        self._pool_key = self._pool = None
        ctx.silences = [Silence(r["id"], r["kind"], r["start"], r["end"], r["entity"], r["dept"])
                        for r in Silences.list_all()]
        cutoff = ctx.now - timedelta(days=RECENT_NOTIFICATION_DAYS)
        ctx.recent_notifications = [RecentNotification(r["fingerprint"], r["last_event_ts"], severity=r["severity"])
                                    for r in Notifications.since(cutoff.replace(tzinfo=None))]
        self._load_graph(ctx, ctx.environment)

        for row in Incidents.list_open(ctx.pipeline):
            ctx.open_incidents.append(OpenIncident(row["id"], row["uid"], row["preset"], row["severity"],
                                                   Incident.from_dict(row["incident"], uid=row["uid"])))
        entities = sorted({e for o in ctx.open_incidents for e in o.incident.entities})
        ctx.entity_last_seen = {k: as_utc(v) for k, v in Anomalies.entity_last_seen(ctx.pipeline, entities).items()}

        for row in ExternalAnomalies.pending(ctx.pipeline):  # 沒有偵測節點，但要記下是哪個外部來源判的
            ctx.external_anomaly_ids.append(row["id"])  # 壞掉的那筆也標成已處理：留著的話每一輪都會再撞一次
            try:
                ctx.anomalies.append(AnomalyEvent(row["entity"], row["metric"], as_utc(row["ts"]), row["score"], row["attrs"] or {},
                                                  row["value"], detector=f"external:{row['source']}"))
            except ValueError as exc:  # 舊資料或繞過 API 寫進來的 NaN／Inf：丟掉這一筆並留紀錄，不讓它拖垮整輪
                ctx.errors.append(TargetError(row["entity"], f"external:{row['source']}:{exc}"))

        if ctx.incident_id is not None:  # RCA：載入指定的事件群與佐證資料
            row = Incidents.get(ctx.incident_id)
            if row is None:
                raise NotFound(f"事件群 {ctx.incident_id} 不存在")  # 載入時就說清楚，不要等到撈 trace 才喊「沒有事件群」
            incident = Incident.from_dict(row["incident"], uid=row["uid"])
            ctx.incidents = [incident]
            ctx.incident_ids[incident.fingerprint] = row["id"]
            ctx.incident_pipeline = row["pipeline"] or ""
            snapshot = row.get("context") or {}  # 建立當下用的拓撲優先（可重現）；沒有才用那個事件群所屬環境現推導的
            # RCA 管線自己的 environment 不算數：圖要照事件群的環境（舊快照沒記就沿用這條管線的，跟以前一樣）
            ctx.incident_environment = str(snapshot.get("environment", ctx.environment))
            if ctx.incident_environment != ctx.environment:
                self._load_graph(ctx, ctx.incident_environment)
            if not ctx.topology_explicit and snapshot.get("topology"):
                ctx.topology = [(a, b) for a, b in snapshot["topology"]]
            if not ctx.lot_edges_explicit and snapshot.get("lot_edges"):
                ctx.lot_edges = [LotEdge(e["from"], e["to"], e["lots"]) for e in snapshot["lot_edges"]]
            rows = Traces.window(incident.entities, incident.start - _RCA_TRACE_MARGIN,
                                 incident.end + _RCA_TRACE_MARGIN)
            for r in rows:
                ctx.trace_rows.setdefault(r["service"], []).append(
                    TraceRow(r["service"], as_utc(r["ts"]), r["count"], r["error_rate"], r["p95_ms"]))
            # RCA 重抓時序（ingestion.incident_series）要用的欄位對照
            for source_type in sorted({e.source_type for e in incident.events if e.source_type}):
                fields = Mappings.approved_fields(source_type)
                if fields:
                    ctx.field_maps[source_type] = fields
            # 歷史相似事故的比對池（排除自己）
            ctx.knowledge_cases = [
                KnowledgeCase(k["incident_uid"], k["pipeline"], list(k["entities"]), list(k["metrics"]),
                              list(k["signals"]), k["resolution"], k["root_cause"], as_utc(k["created_at"]))
                for k in Knowledge.recent(KNOWLEDGE_POOL_SIZE) if k["incident_uid"] != incident.uid]

    @staticmethod
    def _load_graph(ctx: PipelineContext, environment: str) -> None:
        """關係圖只載入這個環境的邊（'' = 不分環境的那一份，跟加 environment 之前一樣）。workflow 明確給的拓撲／Lot 流向不覆蓋。"""
        if not ctx.topology_explicit:
            ctx.topology = [(e["caller"], e["callee"]) for e in ServiceEdges.all(environment)]
            # 非同步邊跟著同一條規則：workflow 自己指定拓撲（含空清單 = 不要用）時不載
            ctx.async_edges = {(e["producer"], e["consumer"]): float(e["lag_ms_p95"])
                               for e in AsyncEdges.all(environment)}
        if not ctx.lot_edges_explicit:
            ctx.lot_edges = lot_edges_from_paths(LotPaths.paths(environment))

    def load_target(self, ctx: PipelineContext, detector_nodes: list[tuple[str, str]]) -> None:
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

        candidates: dict[str, list] = {}  # 同一個演算法掛兩個節點時只查一次
        for node, op in detector_nodes:
            if op not in candidates:
                candidates[op] = ActiveConfigs.candidates(op, ctx.pipeline, target.entity)
            active = pick_most_specific(candidates[op], ctx.pipeline, target.entity)
            if active:
                ctx.detector_params[node] = active["params"]
                ctx.log(f"params:{node}:{op}@{active['pipeline']}/{active['entity']}#v{active['version']}")
            model = DetectorModels.get(ctx.pipeline, target.entity, target.query, op, node)
            if model:
                ctx.detector_states[node] = DetectorState(model["state"], as_utc(model["trained_at"]),
                                                          model["train_start"], model["train_end"], model["points"],
                                                          model["version"], op, model["artifact"],
                                                          model["artifact_format"])
        last = Anomalies.latest_ts(ctx.pipeline, target.entity, target.query)
        ctx.last_anomaly_ts = as_utc(last) if last else None
        if detector_nodes:
            ctx.training_exclusions = self._exclusions_for(ctx, target.entity, (target.attrs or {}).get("dept"))

    def _exclusion_pool(self, ctx: PipelineContext):
        """這一輪所有序列共用：這條管線近期的事故（扣掉被標成誤報的）與維護窗。一輪只查一次。"""
        key = (ctx.pipeline, ctx.now)
        if getattr(self, "_pool_key", None) != key or self._pool is None:
            since = ctx.now - timedelta(days=EXCLUSION_LOOKBACK_DAYS)
            false_alarms = Feedbacks.incident_uids_with_verdict("false_positive")
            incidents = []
            # 先用建立時間粗篩（重放過去的時間時，建立時間是真實時鐘，一定比 since 晚，不會被誤篩），再用事件時間細篩
            for row in Incidents.created_since(since.replace(tzinfo=None)):
                if row["pipeline"] != ctx.pipeline or row["uid"] in false_alarms:
                    continue
                inc = Incident.from_dict(row["incident"], uid=row["uid"])
                if as_utc(inc.end) >= since and as_utc(inc.start) <= ctx.now:
                    incidents.append((as_utc(inc.start), as_utc(inc.end), set(inc.entities), row["uid"]))
            maintenance = [Silence(r["id"], r["kind"], r["start"], r["end"], r["entity"], r["dept"])
                           for r in Silences.list_all() if r["kind"] == "maintenance"]
            self._pool_key, self._pool = key, (incidents, maintenance)
        return self._pool

    def exclusions_for(self, ctx: PipelineContext, entity: str, dept: str | None) -> list[TrainingExclusion]:
        """這個實體在 ctx.pipeline、ctx.now 之前的訓練排除窗。學習閉環的現抓現算（control_plane/learning/replay）
        也用這一份，回測／反推門檻的訓練資料才跟正式偵測排除同樣的時段。"""
        return self._exclusions_for(ctx, entity, dept)

    def _exclusions_for(self, ctx: PipelineContext, entity: str, dept: str | None) -> list[TrainingExclusion]:
        incidents, maintenance = self._exclusion_pool(ctx)
        out = [TrainingExclusion(start - EXCLUSION_MARGIN, end + EXCLUSION_MARGIN, f"incident {uid}")
               for start, end, entities, uid in incidents if entity in entities]
        for s in maintenance:
            if (s.entity is None or s.entity == entity) and (s.dept is None or s.dept == dept):
                out.append(TrainingExclusion(as_utc(s.start), as_utc(s.end), f"maintenance #{s.id}"))
        return out

    # ── 寫入 ──

    def persist_target(self, ctx: PipelineContext) -> None:
        target = ctx.target
        assert target is not None
        for node, st in ctx.new_detector_states.items():
            saved = DetectorModels.save(ctx.pipeline, target.entity, target.query, st.detector, st.state,
                                        st.train_start or ctx.now, st.train_end or ctx.now, st.points,
                                        expected_version=st.version, node=node, trained_at=st.trained_at,
                                        artifact=st.artifact, artifact_format=st.artifact_format)
            if saved is None:  # 別的 worker 先寫了：保留它較新的基準線，這次的丟掉（不是錯誤）
                ctx.log(f"baseline:{node}:{target.entity}:conflict-kept-newer")
        # 異常點不在這裡寫：跟事件群一起在 persist_batch 的單一交易落地。以前每條序列先寫，送通知失敗時
        # 異常已經在庫裡、下一輪 detector.new_only 會把同一批當成「處理過」丟掉，那次告警就永遠發不出去

    def persist_batch(self, ctx: PipelineContext) -> None:
        """一筆交易寫完這一輪的最終結果：異常點、新事件群、進行中事件群的更新、外部匯入異常標成已處理。
        資料庫短暫斷線會整筆回復後重試（db.run_in_transaction）；失敗就什麼都沒寫，下一輪會重新偵測到同一批。
        「已通知」紀錄與結案不在這裡：通知是在寫入之後才送（notifier 在 after_persist），要等送成功（finalize_batch）。"""
        loaded = set(ctx.incident_ids)  # 載入進來的既有事件群（例如 RCA）已經存在，只寫新產生的。先算好：重試時不能被第一次的結果污染
        new_ids: dict[str, int] = {}

        def write() -> None:
            new_ids.clear()  # 每次嘗試都是全新的交易，上一次寫的已經回復
            # 每筆異常帶著自己的偵測器與節點（不是用「最後一個跑的偵測器」一律蓋上）
            Anomalies.create_many([
                {"pipeline": ctx.pipeline, "entity": e.entity, "metric": e.metric, "ts": e.ts,
                 "value": float(e.value) if e.value is not None else 0.0, "score": float(e.score),
                 "detector_name": e.detector, "detector_node": e.detector_node, "level": e.level, "attrs": e.attrs}
                for e in ctx.detected_anomalies
            ])
            for inc in ctx.incidents:
                if inc.fingerprint in loaded:
                    continue
                entities = set(inc.entities)
                snapshot = {
                    "environment": ctx.environment,
                    "topology": [[a, b] for a, b in ctx.topology if a in entities or b in entities],
                    "lot_edges": [{"from": e.upstream, "to": e.downstream, "lots": e.lots} for e in ctx.lot_edges
                                  if e.upstream in entities or e.downstream in entities],
                }
                row = Incidents.create(inc.uid, ctx.pipeline, inc.to_dict(), inc.severity, inc.suppressed_by,
                                       ctx.pipeline, snapshot)
                new_ids[inc.fingerprint] = row["id"]
            for o in ctx.open_incidents:  # 吸收了新異常或升級的進行中事件群：快照寫回
                if o.changed:
                    Incidents.update_snapshot(o.id, o.incident.to_dict(), o.severity)
            ExternalAnomalies.mark_processed(ctx.external_anomaly_ids)
            if ctx.incident_id is not None and ctx.rca_root_algorithm:  # RCA 跑過根因節點：結論等級記回事件群
                Incidents.record_rca(ctx.incident_id, ctx.rca_conclusion_type, ctx.now)

        run_in_transaction(write)
        ctx.incident_ids.update(new_ids)

    def finalize_batch(self, ctx: PipelineContext) -> None:
        """通知都送出去之後才記的東西：已通知（冷卻用，從真正的最後一筆起算）、結案。
        送失敗不會跑到這裡：事件群留在進行中，下一輪心跳用同一組標籤補送（含 endsAt 結案）。"""
        sent: dict[str, int] = {}
        for n in ctx.notifications:
            uid = n.labels.get("incident_uid", "")
            sent[uid] = sent.get(uid, 0) + 1

        def write() -> None:
            for inc in ctx.incidents:
                incident_id = ctx.incident_ids.get(inc.fingerprint)
                if incident_id is not None and not inc.suppressed_by and sent.get(inc.uid):
                    Notifications.record(incident_id, inc.fingerprint, inc.severity, inc.end, sent[inc.uid])
            for o in ctx.open_incidents:
                # 有變動（吸收、升級）就補一筆；沒變動的心跳重送不每輪記，但第一次推送失敗、這一輪才真的送到的要記
                if sent.get(o.uid) and (o.changed or not Notifications.exists_for(o.id)):
                    Notifications.record(o.id, o.incident.fingerprint, o.severity, o.incident.end, sent[o.uid])
            for incident_id in ctx.resolved_incident_ids:
                Incidents.mark_resolved(incident_id, ctx.now.replace(tzinfo=None))

        if sent or ctx.resolved_incident_ids:
            run_in_transaction(write)
