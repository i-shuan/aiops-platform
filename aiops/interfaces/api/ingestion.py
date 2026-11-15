from typing import Any

from fastapi import APIRouter, Body, Query

from aiops.engine.requests import (
    PIPELINE_NAME,
    SOURCE_NAME,
    GrafanaAlertIngest,
    GraphImport,
    IngestAnomalies,
    LotPathsMerge,
    MappingApprove,
    MappingSubmit,
    SourceRef,
    TopologyRefresh,
    TraceMerge,
)
from aiops.interfaces.runner import ingest_grafana_alerts as ingest_grafana, run

router = APIRouter(prefix="/api/v1/ingestion", tags=["ingestion"])


@router.post("/mappings")
def submit_mapping(req: MappingSubmit):
    return {"result": run("admin_mapping_submit", request=req)}


@router.post("/mappings/approve")
def approve_mapping(req: MappingApprove):
    return {"result": run("admin_mapping_approve", request=req)}


@router.get("/mappings/approved/{source_type}")
def get_approved_mapping(source_type: str):
    return {"result": run("admin_mapping_get_approved", request=SourceRef(source_type=source_type))}


@router.post("/traces/merge")
def merge_traces(req: TraceMerge):
    return {"result": run("admin_merge_traces", request=req)}


@router.post("/topology/refresh")
def refresh_topology(req: TopologyRefresh):
    return {"result": run("admin_topology_refresh", request=req)}


@router.post("/graph/import")
def import_graph(req: GraphImport):
    return {"result": run("admin_graph_import", request=req)}


@router.post("/lot-paths/merge")
def merge_lot_paths(req: LotPathsMerge):
    return {"result": run("admin_merge_lot_paths", request=req)}


@router.post("/anomalies")
def ingest_anomalies(req: IngestAnomalies):
    return {"result": run("admin_ingest_anomalies", request=req)}


@router.post("/grafana-alerts")
def ingest_grafana_alerts(payload: dict[str, Any] = Body(...),
                          pipeline: str = Query(..., pattern=PIPELINE_NAME, max_length=128),
                          source: str = Query("grafana", pattern=SOURCE_NAME, max_length=64)):
    """各團隊自己的 Grafana 告警（Webhook 通知管道）推到這裡：網址帶 ?pipeline=<要進哪條管線>&source=<團隊代號>，
    Authorization 用平台的 Bearer token。pipeline 不是存在的偵測管線回 404／400（對方 Grafana 會顯示送達失敗）；
    部分告警格式不對時仍回 200，拒收的列在 rejected（Grafana 收到非 2xx 會重送整批）。"""
    return {"result": ingest_grafana(GrafanaAlertIngest(pipeline=pipeline, source=source, payload=payload))}


@router.get("/sources/check")
def check_sources():
    return {"result": run("admin_sources_check")}
