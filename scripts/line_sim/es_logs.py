"""產線模擬的錯誤日誌（MQ／CIM／AMHS），OTel 日誌格式，寫進 Elasticsearch 的 logs-linesim.otel-fab12。

  # 先看會寫什麼（不連線）
  python scripts/line_sim/es_logs.py --hours 24 --fault 2026-11-20T13:42:01Z/2026-11-20T13:50:56Z --dry-run
  # 寫入（由人執行；帳密從環境變數讀，不印出來）
  ES_URL=http://127.0.0.1:19200 ES_USER=elastic ES_PASSWORD=... python scripts/line_sim/es_logs.py --hours 24 --fault <開始>/<結束>

這是模擬資料，不是任何真實產線的日誌。平台對 Elasticsearch 只讀（經 Grafana），這支腳本是另外的工具：
要有寫入權限的 ES 帳號，由人在演練環境執行。

日誌跟指標對得起來：每一分鐘寫幾筆，由 model.py 同一組函式決定（CIM 逾時幾次就幾筆逾時日誌、AMHS 中止幾次就幾筆
中止日誌），所以平常是零星幾筆，故障時段照骨牌的順序暴增：
  mq-broker  消費者斷線（故障開始時每個消費者一筆）、佇列積壓超過水位（故障中每分鐘一筆）
  cim-eap    SECS/GEM T3 逾時（平常零星；故障後第 3 分鐘起每分鐘約 4 筆）、等不到派工訊息
  amhs-mcs   搬運任務中止（平常零星；故障後第 6 分鐘起每分鐘約 2 筆）
每筆帶產線的識別欄位（attributes）：eqp_id、lot_id、carrier_id、queue_name——哪幾個有值依訊息種類而定。

為什麼寫得進 logs-*.otel-* 而不會被改寫到 k8s-*：ES 上的 k8s-logs-otel pipeline 只處理帶 resource.attributes.group 與
k8s.cluster.name 的文件（自家 Collector 送的），這裡不帶那兩個欄位，文件留在原地。
同一份參數重跑不會重複：文件 _id 由（服務、時間、序號、訊息種類）算出來，已經有的回 409、略過。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import model  # noqa: E402

DATA_STREAM = "logs-linesim.otel-fab12"
QUEUE = "eap.lot.event"
ERROR = 17  # OTel severity_number：ERROR


def _pick(name: str, minute: int, n: int, prefix: str, width: int, span: int) -> str:
    """固定種子挑一個編號（例如 EQP-0412）：同一分鐘同一筆永遠挑到同一個。"""
    rng = model._rng(f"{name}:{n}", minute)
    return f"{prefix}{rng.randrange(1, span + 1):0{width}d}"


def _doc(ts: datetime, service: str, kind: str, text: str, attrs: dict[str, str], seq: int) -> tuple[str, dict]:
    doc_id = hashlib.blake2b(f"{service}:{ts.isoformat()}:{seq}:{kind}".encode(), digest_size=12).hexdigest()
    return doc_id, {
        "@timestamp": ts.isoformat().replace("+00:00", "Z"),
        "data_stream": {"type": "logs", "dataset": "linesim.otel", "namespace": "fab12"},
        "severity_number": ERROR, "severity_text": "ERROR",
        "body": {"text": text},
        "resource": {"attributes": {"service.name": service, "line": model.LINE}},
        "attributes": {"exception.type": kind, **attrs},
    }


def minute_logs(ts: datetime, elapsed: float | None, backlog: float, started: bool = False) -> list[tuple[str, dict]]:
    """這一分鐘的錯誤日誌。elapsed 是故障已經進行幾分鐘（None = 沒有故障）；started = 故障就是在這一分鐘開始的。"""
    minute = int(ts.timestamp() // 60)
    value = (lambda name: model.faulted(name, ts, elapsed, backlog)) if elapsed is not None else (lambda name: model.normal(name, ts))
    out: list[tuple[str, dict]] = []

    def at(n: int, total: int) -> datetime:  # 這一分鐘裡平均分散
        return ts + timedelta(seconds=round(60 * (n + 0.5) / max(total, 1), 3))

    timeouts = int(value("cim_timeout_count"))
    for n in range(timeouts):
        eqp, lot = _pick("eqp", minute, n, "EQP-", 4, 120), _pick("lot", minute, n, "LOT-2611", 4, 900)
        out.append(_doc(at(n, timeouts), "cim-eap", "SecsT3Timeout",
                        f"SECS/GEM T3 reply timeout: eqp_id={eqp} S6F11 no reply within 45s lot_id={lot}",
                        {"eqp_id": eqp, "lot_id": lot}, n))
    aborts = int(value("amhs_abort_count"))
    for n in range(aborts):
        carrier, eqp, lot = _pick("carrier", minute, n, "FOUP-", 5, 4000), _pick("dest", minute, n, "EQP-", 4, 120), _pick("alot", minute, n, "LOT-2611", 4, 900)
        reason = "NO_DISPATCH_ACK" if elapsed is not None else "LOADPORT_OCCUPIED"
        out.append(_doc(at(n, aborts), "amhs-mcs", "TransferAbort",
                        f"Transfer command aborted: carrier_id={carrier} to {eqp} reason={reason} lot_id={lot}",
                        {"carrier_id": carrier, "eqp_id": eqp, "lot_id": lot}, n))
    if elapsed is not None:
        if started:  # 故障開始那一分鐘：四個消費者各斷一次
            for n in range(4):
                out.append(_doc(ts + timedelta(seconds=2 + n), "mq-broker", "MqConsumerLost",
                                f"Consumer connection lost on queue {QUEUE}: channel closed by peer consumer_tag=eap-worker-{n + 1}",
                                {"queue_name": QUEUE}, n))
        depth = int(value("mq_queue_depth"))
        out.append(_doc(ts + timedelta(seconds=30), "mq-broker", "MqBacklogHigh",
                        f"Queue {QUEUE} backlog exceeds high watermark: depth={depth} consumers=0",
                        {"queue_name": QUEUE}, 0))
        if elapsed >= 3:  # CIM 等不到派工訊息
            lot = _pick("plot", minute, 0, "LOT-2611", 4, 900)
            out.append(_doc(ts + timedelta(seconds=45), "cim-eap", "DispatchEventMissing",
                            f"MES TrackIn pending: no dispatch event received from queue {QUEUE} lot_id={lot}",
                            {"queue_name": QUEUE, "lot_id": lot}, 0))
    return out


def generate(start: datetime, end: datetime, faults: list[tuple[datetime, datetime]]) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    backlog, t = 0.0, start.replace(second=0, microsecond=0)
    while t < end:
        active = next((f for f in faults if f[0] <= t + timedelta(minutes=1) and t < f[1]), None)
        if active:
            backlog = min(500000.0, backlog + model.BACKLOG_PER_MINUTE)
            elapsed = max(0.0, (t + timedelta(minutes=1) - active[0]).total_seconds() / 60 - 1)
            out += minute_logs(t, elapsed, backlog, started=t <= active[0] < t + timedelta(minutes=1))
        else:
            backlog = max(0.0, backlog - model.DRAIN_PER_MINUTE)
            out += minute_logs(t, None, backlog)
        t += timedelta(minutes=1)
    return out


def _time(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24, help="往回產生幾小時（到 --end 為止）")
    ap.add_argument("--end", type=_time, default=None, help="結束時間（ISO 8601，預設現在）")
    ap.add_argument("--fault", action="append", default=[], help="故障時段 <開始>/<結束>（ISO 8601，可重複）")
    ap.add_argument("--dry-run", action="store_true", help="不連線：印出筆數、各種訊息幾筆、前幾筆")
    args = ap.parse_args()
    end = args.end or datetime.now(timezone.utc)
    faults = [tuple(_time(x) for x in f.split("/")) for f in args.fault]
    docs = generate(end - timedelta(hours=args.hours), end, faults)
    kinds: dict[str, int] = {}
    for _, d in docs:
        key = f"{d['resource']['attributes']['service.name']}/{d['attributes']['exception.type']}"
        kinds[key] = kinds.get(key, 0) + 1
    print(f"{len(docs)} 筆，{end - timedelta(hours=args.hours):%m-%d %H:%M}～{end:%m-%d %H:%M} UTC：{json.dumps(kinds, ensure_ascii=False)}")
    if args.dry_run:
        for _, d in docs[-3:]:
            print(json.dumps(d, ensure_ascii=False))
        return

    import httpx
    client = httpx.Client(base_url=os.environ["ES_URL"].rstrip("/"), auth=(os.environ["ES_USER"], os.environ["ES_PASSWORD"]), timeout=60)
    created = existed = 0
    for i in range(0, len(docs), 2000):
        lines = []
        for doc_id, d in docs[i:i + 2000]:
            lines += [json.dumps({"create": {"_id": doc_id}}), json.dumps(d, ensure_ascii=False)]
        r = client.post(f"/{DATA_STREAM}/_bulk", content=("\n".join(lines) + "\n").encode("utf-8"),
                        headers={"Content-Type": "application/x-ndjson"})
        r.raise_for_status()
        for item in r.json()["items"]:
            status = item["create"]["status"]
            if status == 201:
                created += 1
            elif status == 409:
                existed += 1
            else:
                raise SystemExit(f"寫入失敗（HTTP {status}）：{json.dumps(item['create'].get('error'), ensure_ascii=False)[:400]}")
    print(f"寫入 {DATA_STREAM}：新增 {created} 筆、已經有的 {existed} 筆")


if __name__ == "__main__":
    main()
