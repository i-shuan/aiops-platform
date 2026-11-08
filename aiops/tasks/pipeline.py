"""管線調度：依序呼叫各領域的 Command，自己不寫任何業務邏輯。

  TrainDetectorCommand（模型過期才重訓） -> RunDetectionCommand（新異常寫進 anomalies）
  -> SyncOpenIncidentsCommand（進行中的事件群心跳重送；安靜夠久的結案、送恢復通知）
  -> ProcessPendingAnomaliesCommand（alerts 讀未處理的異常，收斂、抑制、推送）

detection 與 alerts 互不 import，只在這一層被串起來。
同步放在處理新異常之前：剛建立的事件群不會在同一輪被送兩次；偵測已先跑完，新異常會讓相關事件群保持進行中。
"""
from datetime import datetime, timedelta, timezone

from aiops.domains.alerts.commands.process_pending import ProcessPendingAnomaliesCommand
from aiops.domains.alerts.commands.sync import SyncOpenIncidentsCommand
from aiops.domains.alerts.schemas import ProcessPendingRequest, SyncIncidentsRequest
from aiops.domains.detection.commands.run import RunDetectionCommand
from aiops.domains.detection.commands.train import TrainDetectorCommand
from aiops.domains.detection.schemas import DetectionRunRequest, DetectionTrainRequest
from aiops.schemas import MetricTarget, TimeRange
from aiops.tasks.config import Job


def _window_end(now: datetime, step_seconds: int) -> datetime:
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    epoch = int(now.timestamp())
    return datetime.fromtimestamp(epoch - epoch % step_seconds, tz=timezone.utc)


def run_job(job: Job, now: datetime | None = None) -> dict:
    end = _window_end(now or datetime.now(timezone.utc), job.step_seconds)
    detect_range = TimeRange(start=end - timedelta(minutes=job.lookback_minutes), end=end, step_seconds=job.step_seconds)
    train_range = TimeRange(start=end - timedelta(minutes=job.train_lookback_minutes), end=end,
                            step_seconds=job.step_seconds)

    trained = persisted = 0
    for target in job.targets:
        metric = MetricTarget(source_type=target.source_type, query=target.query)
        peers = [MetricTarget(source_type=p.source_type, query=p.query) for p in target.peers]
        trained += TrainDetectorCommand(
            DetectionTrainRequest(metric=metric, train_time_range=train_range, detector_name=job.detector_name,
                                  pipeline=job.name, entity=target.entity, peers=peers,
                                  if_older_than_minutes=job.retrain_minutes),
            now=end,
        ).run()["trained"]
        persisted += RunDetectionCommand(
            DetectionRunRequest(metric=metric, time_range=detect_range, detector_name=job.detector_name,
                                params=job.params, preset=job.preset, entity=target.entity, attrs=target.attrs,
                                pipeline=job.name, peers=peers)
        ).run()["persisted"]

    synced = SyncOpenIncidentsCommand(
        SyncIncidentsRequest(pipeline=job.name, resolve_after_minutes=job.resolve_after_minutes), now=end
    ).run()
    alerts = ProcessPendingAnomaliesCommand(
        ProcessPendingRequest(pipeline=job.name, preset=job.preset, context=job.context,
                              plugin_params=job.plugin_params)
    ).run()
    return {
        "job": job.name,
        "window": [detect_range.start.isoformat(), detect_range.end.isoformat()],
        "trained": trained,
        "persisted_anomalies": persisted,
        "processed_anomalies": alerts["processed_anomalies"],
        "incidents": len(alerts["incidents"]),
        "notified_incidents": sum(1 for i in alerts["incidents"] if i["notifications"]),
        "suppressed_incidents": sum(1 for i in alerts["incidents"] if i["suppressed_by"]),
        "refreshed_incidents": synced["refreshed"],
        "resolved_incidents": synced["resolved"],
    }
