"""Repository：只做讀寫，沒有任何業務判斷（狀態機、是否該抑制、該不該核准都在算子裡）。回傳一律是 dict。

兩種人會用它：
- storage/hooks.py：引擎在 Pipeline 前面載入輸入、後面寫入最終結果
- control_plane/*：手動動作（核准欄位對照、核准調參…）管理自己的狀態
偵測、收斂、抑制、RCA 這類流程算子不得 import 這裡（tests/test_architecture.py 檢查）。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from aiops.engine.params import ANY
from aiops.storage import crud
from aiops.storage.models import (
    ActiveDetectorConfigRow,
    AnomalyRow,
    DetectorModelRow,
    ExternalAnomalyRow,
    FeedbackRow,
    FieldMappingRow,
    IncidentKnowledgeRow,
    IncidentRow,
    LotPathRow,
    NotificationRow,
    ServiceEdgeRow,
    SilenceRow,
    TraceSeriesRow,
    TuningProposalRow,
    utc_now,
)

Row = dict[str, Any]


class Silences:
    @staticmethod
    def create(fields: Row) -> Row:
        return crud.create(SilenceRow, **fields)

    @staticmethod
    def list_all() -> list[Row]:
        return crud.find(SilenceRow)


class Incidents:
    @staticmethod
    def create(uid: str, preset: str, incident: Row, severity: str, suppressed_by: str | None,
               pipeline: str | None, context: Row | None = None) -> Row:
        """uid 由事件群自己帶（通知在存檔前就要用它），不由資料庫產生。"""
        return crud.create(IncidentRow, uid=uid, preset=preset, incident=incident, severity=severity,
                           suppressed_by=suppressed_by, pipeline=pipeline, context=context or {},
                           status="suppressed" if suppressed_by else "open")

    @staticmethod
    def get(incident_id: int) -> Row | None:
        return crud.get(IncidentRow, incident_id)

    @staticmethod
    def get_by_uid(uid: str) -> Row | None:
        return crud.find_one(IncidentRow, uid=uid)

    @staticmethod
    def list_open(pipeline: str) -> list[Row]:
        return crud.find(IncidentRow, pipeline=pipeline, status="open")

    @staticmethod
    def created_between(start: datetime, end: datetime) -> list[Row]:
        return crud.find_between(IncidentRow, "created_at", start, end)

    @staticmethod
    def created_since(start: datetime) -> list[Row]:
        return crud.find_between(IncidentRow, "created_at", start, datetime(9999, 1, 1))

    @staticmethod
    def update_snapshot(incident_id: int, incident: Row, severity: str) -> Row:
        """進行中事件群吸收了新異常或升級：整份快照與嚴重度寫回（uid、建立時間不變）。"""
        return crud.update(IncidentRow, incident_id, incident=incident, severity=severity)

    @staticmethod
    def mark_resolved(incident_id: int, resolved_at: datetime) -> Row:
        return crud.update(IncidentRow, incident_id, status="resolved", resolved_at=resolved_at)


class Notifications:
    @staticmethod
    def record(incident_id: int, fingerprint: str, severity: str, last_event_ts: datetime, count: int) -> Row:
        return crud.create(NotificationRow, incident_id=incident_id, fingerprint=fingerprint, severity=severity,
                           last_event_ts=last_event_ts, count=count)

    @staticmethod
    def since(cutoff: datetime) -> list[Row]:
        """cutoff 之後有通知過的紀錄（冷卻判斷用）。"""
        return crud.find_between(NotificationRow, "last_event_ts", cutoff, datetime(9999, 1, 1))

    @staticmethod
    def exists_for(incident_id: int) -> bool:
        """這個事件群有沒有任何一筆已通知紀錄（第一次推送失敗、靠心跳補送成功的事件群，要在補送那一輪補記）。"""
        return crud.find_one(NotificationRow, incident_id=incident_id) is not None


class Anomalies:
    @staticmethod
    def latest_ts(pipeline: str, entity: str, metric: str) -> datetime | None:
        return crud.max_of(AnomalyRow, "ts", pipeline=pipeline, entity=entity, metric=metric)

    @staticmethod
    def entity_last_seen(pipeline: str, entities: list[str]) -> dict[str, datetime]:
        """實體最近一次異常（偵測到的與外部匯入的都算），給事件群結案判斷用。"""
        seen: dict[str, datetime] = {}
        for model in (AnomalyRow, ExternalAnomalyRow):
            for entity in entities:
                ts = crud.max_of(model, "ts", pipeline=pipeline, entity=entity)
                if ts is not None and (entity not in seen or ts > seen[entity]):
                    seen[entity] = ts
        return seen

    @staticmethod
    def create_many(rows: list[Row]) -> int:
        return crud.create_many(AnomalyRow, rows)


class ExternalAnomalies:
    @staticmethod
    def existing_ids(source: str, external_ids: list[str]) -> set[str]:
        rows = crud.find_in(ExternalAnomalyRow, "external_id", external_ids)
        return {r["external_id"] for r in rows if r["source"] == source}

    @staticmethod
    def create_many(rows: list[Row]) -> int:
        return crud.create_many(ExternalAnomalyRow, rows)

    @staticmethod
    def pending(pipeline: str) -> list[Row]:
        return crud.find(ExternalAnomalyRow, pipeline=pipeline, status="pending")

    @staticmethod
    def mark_processed(ids: list[int]) -> None:
        for row_id in ids:
            crud.update(ExternalAnomalyRow, row_id, status="processed")


class DetectorModels:
    """基準線以 (管線, 實體, 指標, 偵測器, 節點) 區分。node 是 Workflow 節點名稱。"""

    @staticmethod
    def get(pipeline: str, entity: str, metric: str, detector: str, node: str = "") -> Row | None:
        return crud.find_one(DetectorModelRow, pipeline=pipeline, entity=entity, metric=metric, detector_name=detector,
                             node=node)

    @staticmethod
    def for_scope(pipeline: str, entity: str, detector: str) -> list[Row]:
        """某條管線某個實體、某個偵測器的所有基準線（不同指標、不同節點各一筆）。"""
        return crud.find(DetectorModelRow, pipeline=pipeline, entity=entity, detector_name=detector)

    @staticmethod
    def save(pipeline: str, entity: str, metric: str, detector: str, state: Row, train_start: datetime,
             train_end: datetime, points: int, *, expected_version: int, node: str = "",
             trained_at: datetime | None = None, artifact: bytes | None = None,
             artifact_format: str = "") -> Row | None:
        """樂觀鎖寫入。expected_version 是讀取當時的版本（0 = 當時還沒有這一筆）。
        別人已經先寫了就回傳 None，不覆蓋對方較新的基準線。"""
        key = {"pipeline": pipeline, "entity": entity, "metric": metric, "detector_name": detector, "node": node}
        fields = {"state": state, "train_start": train_start, "train_end": train_end, "points": points,
                  "trained_at": trained_at or utc_now(), "artifact": artifact, "artifact_format": artifact_format}
        if expected_version == 0:
            return crud.insert_if_absent(DetectorModelRow, **key, **fields, version=1)
        return crud.update_if_version(DetectorModelRow, key, expected_version, **fields)


class ActiveConfigs:
    """偵測器現行參數，分層存放：(偵測器, 管線, 實體)，"*" = 不限定。整個系統只有 admin.tuning.approve 會呼叫 set()。
    要「套用哪一筆」的優先序規則不在這裡（engine/params.pick_most_specific），這裡只負責撈資料。"""

    @staticmethod
    def get(detector: str, pipeline: str = ANY, entity: str = ANY) -> Row | None:
        """恰好這一層的設定（不往上找）。"""
        return crud.find_one(ActiveDetectorConfigRow, detector_name=detector, pipeline=pipeline, entity=entity)

    @staticmethod
    def candidates(detector: str, pipeline: str, entity: str) -> list[Row]:
        """所有可能適用的層級，一次查詢撈出：pipeline ∈ {pipeline, *}、entity ∈ {entity, *}。"""
        return crud.find_matching(ActiveDetectorConfigRow, {"detector_name": detector},
                                  {"pipeline": sorted({pipeline, ANY}), "entity": sorted({entity, ANY})})

    @staticmethod
    def version(detector: str, pipeline: str = ANY, entity: str = ANY) -> int:
        row = ActiveConfigs.get(detector, pipeline, entity)
        return row["version"] if row else 0

    @staticmethod
    def set(detector: str, params: Row, source: str, proposal_id: int, *, expected_version: int,
            pipeline: str = ANY, entity: str = ANY) -> Row | None:
        """樂觀鎖寫入恰好這一層。expected_version 是核准當下提案的基準版本（0 = 這一層還沒有設定）。
        這一層已被別人推進就回傳 None，不覆蓋。"""
        key = {"detector_name": detector, "pipeline": pipeline, "entity": entity}
        fields = {"params": params, "source": source, "proposal_id": proposal_id}
        if expected_version == 0:
            return crud.insert_if_absent(ActiveDetectorConfigRow, **key, version=1, **fields)
        return crud.update_if_version(ActiveDetectorConfigRow, key, expected_version, **fields)


class Mappings:
    @staticmethod
    def create(row: Row) -> Row:
        return crud.create(FieldMappingRow, **row)

    @staticmethod
    def get(mapping_id: int) -> Row | None:
        return crud.get(FieldMappingRow, mapping_id)

    @staticmethod
    def update(mapping_id: int, **fields: Any) -> Row:
        return crud.update(FieldMappingRow, mapping_id, **fields)

    @staticmethod
    def list_by(source_type: str, status: str) -> list[Row]:
        return crud.find(FieldMappingRow, source_type=source_type, status=status)

    @staticmethod
    def approved_fields(source_type: str) -> dict[str, str] | None:
        row = crud.find_one(FieldMappingRow, source_type=source_type, status="approved")
        return row["fields"] if row else None


class Traces:
    @staticmethod
    def upsert_series(rows: list[Row]) -> int:
        """同一服務同一時間格只留一筆，再合成一次就用最新一批覆蓋。"""
        for r in rows:
            crud.upsert(TraceSeriesRow, {"service": r["service"], "ts": r["ts"]},
                        count=r["count"], error_rate=r["error_rate"], p95_ms=r["p95_ms"])
        return len(rows)

    @staticmethod
    def window(services: list[str], start: datetime, end: datetime) -> list[Row]:
        return crud.find_between(TraceSeriesRow, "ts", start, end, in_column="service", in_values=services)


class ServiceEdges:
    @staticmethod
    def add_calls(edges: dict[tuple[str, str], int]) -> int:
        for (caller, callee), calls in edges.items():
            existing = crud.find_one(ServiceEdgeRow, caller=caller, callee=callee)
            crud.upsert(ServiceEdgeRow, {"caller": caller, "callee": callee},
                        calls=(existing["calls"] if existing else 0) + calls, last_seen=utc_now())
        return len(edges)

    @staticmethod
    def all() -> list[Row]:
        return crud.find(ServiceEdgeRow)


class LotPaths:
    @staticmethod
    def upsert_many(lots: list[Row]) -> int:
        """以 lot_id 為準：同一個 Lot 再合成一次就換成最新的路徑（不會重複計算流向）。"""
        for lot in lots:
            crud.upsert(LotPathRow, {"lot_id": lot["lot_id"]}, path=lot["path"], start=lot["start"], end=lot["end"])
        return len(lots)

    @staticmethod
    def paths() -> list[list[str]]:
        return [r["path"] for r in crud.find(LotPathRow)]


class Proposals:
    @staticmethod
    def create(row: Row) -> Row:
        return crud.create(TuningProposalRow, **row)

    @staticmethod
    def get(proposal_id: int) -> Row | None:
        return crud.get(TuningProposalRow, proposal_id)

    @staticmethod
    def update(proposal_id: int, **fields: Any) -> Row:
        return crud.update(TuningProposalRow, proposal_id, **fields)

    @staticmethod
    def approved_between(start: datetime, end: datetime) -> list[Row]:
        return crud.find_between(TuningProposalRow, "approved_at", start, end)

    @staticmethod
    def open_for(detector: str, pipeline: str, entity: str) -> list[Row]:
        """這一層還沒結案（pending／backtested）的提案。"""
        return crud.find_matching(TuningProposalRow, {"detector_name": detector, "pipeline": pipeline, "entity": entity},
                                  {"status": ["pending", "backtested"]})


class Feedbacks:
    @staticmethod
    def create(row: Row) -> Row:
        return crud.create(FeedbackRow, **row)

    @staticmethod
    def get(feedback_id: int) -> Row | None:
        return crud.get(FeedbackRow, feedback_id)

    @staticmethod
    def link_proposal(feedback_id: int, proposal_id: int) -> Row:
        return crud.update(FeedbackRow, feedback_id, proposal_id=proposal_id)

    @staticmethod
    def for_proposal(proposal_id: int) -> list[Row]:
        return crud.find(FeedbackRow, proposal_id=proposal_id)

    @staticmethod
    def created_between(start: datetime, end: datetime) -> list[Row]:
        return crud.find_between(FeedbackRow, "created_at", start, end)

    @staticmethod
    def incident_uids_with_verdict(verdict: str) -> set[str]:
        return {r["incident_uid"] for r in crud.find(FeedbackRow, verdict=verdict) if r["incident_uid"]}


class Knowledge:
    """歷史事故經驗庫。同一個事件群只留一筆，再寫一次就更新處置內容。"""

    @staticmethod
    def upsert(incident_uid: str, **fields: Any) -> Row:
        return crud.upsert(IncidentKnowledgeRow, {"incident_uid": incident_uid}, **fields)

    @staticmethod
    def recent(limit: int) -> list[Row]:
        """最新的 limit 筆（由新到舊）。"""
        return crud.find_latest(IncidentKnowledgeRow, limit)
