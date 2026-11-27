"""設定，以及「一輪」的流程。常駐服務（src/server.py）在 debounce 時間到的時候呼叫 process()；
也可以手動 `python -m src.main` 立刻處理一輪（不等 debounce，除錯用）。

  1. 取出這個 debounce 視窗內收到的告警（Webhook 收到時已經先存進資料庫）
  2. 收斂     同實體、關係邊、併入進行中事故；成員全部 resolved 並持續一段時間才結案
  3. RCA      收集證據交給 LLM（失敗退回原始摘要）；同時產生註記上「證據」的幾行
  4. 寫回     先寫自己的資料庫，再寫 Grafana 註記（每個事故一筆：標題、摘要、證據的幾行、時間）

Grafana 連不上時註記寫不進去：事故照樣存在資料庫，下一輪補寫。資料庫連不上這一輪才算失敗，告警留在 inbox 下一輪再處理。

**所有設定都在一份 config.yaml**（沒有其他環境變數）。檔案路徑預設 /etc/aiops/config.yaml，要換用環境變數
AIOPS_CONFIG_FILE 指定（唯一的一個）。檔案不存在、鍵名打錯、型別不對、必填沒填，啟動時直接報錯。完整範例見 config.example.yaml。
檔案裡有 Token 與密碼：不要推上 Git（.gitignore 已排除 config.yaml）；Kubernetes 用 Secret 掛載。
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from src import grouping, rca, report
from src.db import Database
from src import graph
from src import logs as logstores
from src import outputs as outlets
from src import traces as tracestores
from src.grafana import Grafana, GrafanaError

from src.config import ConfigError, Settings  # noqa: F401（server.py 與測試從這裡拿）

log = logging.getLogger("aiops_lite")

def log_store(settings: "Settings", grafana: Grafana) -> Any:
    """照 logs.type 建日誌的來源；沒設 datasource_uid 回 None（沒有日誌證據）。"""
    if not settings.logs_datasource_uid:
        return None
    if settings.logs_type == "elasticsearch":
        return logstores.Elasticsearch(grafana, settings.logs_datasource_uid, settings.log_fields, settings.logs_index)
    return logstores.VictoriaLogs(grafana, settings.logs_datasource_uid, settings.log_fields, settings.logs_filter)


def trace_store(settings: "Settings", grafana: Grafana) -> Any:
    if not settings.traces_datasource_uid:
        return None
    kind = tracestores.Tempo if settings.traces_type == "tempo" else tracestores.VictoriaTraces
    return kind(grafana, settings.traces_datasource_uid)


def split_batch(alerts: list[grouping.Alert]) -> tuple[list[grouping.Alert], list[grouping.Alert]]:
    """把一個視窗內依序收到的告警整理成 (響過的, 最新狀態是 resolved 的)；同一個 fingerprint 各留最後一筆。"""
    firing: dict[str, grouping.Alert] = {}
    latest: dict[str, grouping.Alert] = {}
    for alert in alerts:
        latest[alert.fingerprint] = alert
        if alert.status == "firing":
            firing[alert.fingerprint] = alert
    return list(firing.values()), [a for a in latest.values() if a.status == "resolved"]


def backoff_seconds(failures: int, initial: int, maximum: int) -> int:
    """連續失敗第 n 次之後要等多久：initial、2×、4×…，最多 maximum（指數退避）。"""
    return min(initial * 2 ** min(max(failures - 1, 0), 30), maximum)


MAX_LINK_TRACES = 5   # 判斷要不要合併時，最多取回幾條失敗的 Trace 來看誰呼叫誰。版面與查詢量的上限，未驗證泛化


def service_graph_of(settings: Settings, grafana: Grafana, start: datetime, end: datetime) -> list[dict[str, Any]]:
    return graph.read_service_graph(grafana, traces_type=settings.traces_type, traces_uid=settings.traces_datasource_uid,
                                    metrics_uid=settings.service_graph_datasource_uid, start=start, end=end)


def observe(settings: Settings, grafana: Grafana, db: Database, firing: list[grouping.Alert],
            open_incidents: list[grouping.Incident], now: datetime) -> tuple[list[tuple[str, str]], list[dict[str, Any]]]:
    """這一輪開始時看一次系統之間的關係。回傳 (可以拿來合併事故的連結, 這段時間 service graph 上的呼叫)。

    合併的條件（src/graph.py 的 failing_links）：兩個系統都在告警，而且這段時間它們之間有失敗的呼叫、或一方的錯誤日誌指名另一方。
    只在有新告警的輪次才看。先看 service graph（有設的話，兩次查詢）；它已經把在告警的系統連成一塊，就不再查 Trace 與日誌；
    還有連不起來的，才去查失敗的 Trace 與錯誤日誌。查不動就當成沒有連結（各開各的），不擋這一輪。
    看到的關係順手記進資料庫（observed_calls），之後的事故可以拿來當線索。狀態都在資料庫，不留在記憶體。"""
    if not settings.graph_retention_days or not firing:
        return [], []   # 沒有新的告警：成員不會變，不用看（沒有變化的續跑輪次不對外呼叫任何東西）
    alerting = {a.service for a in firing} | {s for inc in open_incidents for s in inc.services}
    started = min([a.starts_at for a in firing] + [inc.started_at for inc in open_incidents])
    start = started - timedelta(minutes=settings.evidence.window_padding_minutes)
    graph_edges = service_graph_of(settings, grafana, start, now)
    trace_calls: list[tuple[str, str]] = []    # 失敗的 Trace 裡看到的全部呼叫：記進圖裡
    failing_calls: list[tuple[str, str]] = []  # 其中自己失敗的那幾條：才能拿來合併
    messaging_calls: list[tuple[str, str]] = []   # 其中經訊息佇列的
    mentioned: list[tuple[str, str]] = []
    linked = graph.failing_links(alerting, graph_edges=graph_edges, trace_calls=[], log_mentions=[])
    if len(grouping.group_alerts([grouping.Alert(n, "firing", "", "", n, "P3", now, None) for n in alerting], linked)) >= 2:
        names = sorted(alerting)
        traces, logs = trace_store(settings, grafana), log_store(settings, grafana)
        try:
            if traces is not None:
                found = sorted(traces.search_error_traces(names, start, now, rca.TRACE_SEARCH_LIMIT), key=lambda t: t["start"], reverse=True)
                chains = []
                for t in found[:MAX_LINK_TRACES]:
                    raw = t["trace"] or traces.get_trace(t["id"]) or {}
                    chains.append(rca.summarize_trace(raw, t["id"], settings.evidence, rca.senders_of(raw, traces)))
                seen = {"call_chains": [c for c in chains if c]}
                trace_calls, failing_calls = rca.calls_in(seen), rca.calls_in(seen, failed_only=True)
                messaging_calls = rca.calls_in(seen, messaging_only=True)
            if logs is not None:
                rows = logs.search_errors(names, start, now, settings.evidence.max_hits, error_levels=settings.evidence.error_levels)
                mentioned = graph.mentions([{"system": r.get(logs.fields.service), "message": r.get(logstores.MESSAGE_FIELD)} for r in rows], alerting)
        except GrafanaError as e:
            log.warning("查不到系統之間這段時間的呼叫，這一輪不跨系統合併：%s", e)
    for pairs, source, messaging in (
            ([(e["caller"], e["callee"]) for e in graph_edges], "service_graph", [(e["caller"], e["callee"]) for e in graph_edges if e["kind"] == "messaging"]),
            (trace_calls, "trace", messaging_calls), (mentioned, "log", [])):
        if pairs:
            db.record_calls(pairs, source, now, settings.graph_retention_days, messaging)
    return graph.failing_links(alerting, graph_edges=graph_edges, trace_calls=failing_calls, log_mentions=mentioned), graph_edges


def relations_for(settings: Settings, db: Database, incident: grouping.Incident, graph_edges: list[dict[str, Any]],
                  now: datetime) -> list[dict[str, Any]]:
    """跟這張事故的系統有關、學到過的關係（給 LLM 當線索）。service graph 的邊帶上這段時間的請求數與失敗數。"""
    if not settings.graph_retention_days:
        return []
    live = {(e["caller"], e["callee"]): e for e in graph_edges}
    out = []
    for row in db.calls_touching(incident.services, now, settings.graph_retention_days, rca.MAX_RELATIONS):
        numbers = live.get((row["caller"], row["callee"])) if row["source"] == "service_graph" else None
        out.append({**row, **({k: numbers[k] for k in ("requests", "failed", "kind") if numbers[k] is not None} if numbers else {})})
    return out


def process(settings: Settings, *, now: datetime | None = None, grafana_transport: httpx.BaseTransport | None = None,
            llm_transport: httpx.BaseTransport | None = None, outputs_transport: httpx.BaseTransport | None = None) -> dict[str, Any]:
    """跑一輪，回傳這一輪的摘要（needs_followup = 還有事要做，沒有新 Webhook 也要再跑）。
    transport 參數只是 HTTP 連線的注入點（預設真的連出去）。"""
    started = datetime.now(timezone.utc)
    now = (now or started).astimezone(timezone.utc)
    db = Database(settings.database_url)
    grafana = Grafana(settings.grafana_url, settings.grafana_token, transport=grafana_transport,
                      timeout=settings.grafana_timeout_seconds, retries=settings.grafana_retries,
                      retry_backoff_seconds=settings.grafana_retry_backoff_seconds)
    outputs = outlets.Outputs(settings.outputs, db, transport=outputs_transport)
    errors: list[str] = []
    undelivered: list[str] = []   # 這一輪事故 API、修復請求沒送成功的（每一輪重新算，不跨輪）
    incidents: list[grouping.Incident] = []
    batch: list[grouping.Alert] = []
    status = "failed"
    try:
        ids, batch = db.pending()
        firing, resolved = split_batch(batch)
        still_open = db.open_incidents()
        links, graph_edges = observe(settings, grafana, db, firing, still_open, now)
        incidents = grouping.converge(still_open, firing, resolved, links, now=now,
                                      close_after_seconds=settings.close_after_seconds)
        fetched: dict[str, list[dict[str, Any]]] = {}   # 這一輪讀到的 service graph，只活在這一輪

        def graph_now(inc: grouping.Incident) -> list[dict[str, Any]]:
            """要分析事故時才需要 service graph 上這段時間的請求數與失敗數。這一輪開頭讀過就用那一份，沒讀過才讀（一輪最多一次）。"""
            if "edges" not in fetched:
                fetched["edges"] = graph_edges if firing or not settings.graph_retention_days else service_graph_of(
                    settings, grafana, inc.started_at - timedelta(minutes=settings.evidence.window_padding_minutes), now)
            return fetched["edges"]
        known = {i.uid for i in incidents}
        incidents += [i for i in db.unannotated_incidents() if i.uid not in known]
        known = {i.uid for i in incidents}
        incidents += [i for i in db.incidents_to_redeliver(outlets.MAX_ATTEMPTS) if i.uid not in known]   # 出口上次沒送成功的
        can_analyze = bool(settings.llm_api_url and settings.llm_model)
        for inc in incidents:
            if inc.status != "open":
                if not inc.summary:
                    inc.summary = f"[未經分析] {rca.raw_summary(inc)}"
                continue
            # 成員或嚴重度變了就重新分析；上一輪 LLM 沒成功的進行中事故，到了退避時間再試
            if not (inc.fresh or not inc.summary or (can_analyze and not inc.analyzed)):
                continue
            if can_analyze and inc.llm_next_try is not None and now < inc.llm_next_try:
                # 退避中：不查日誌、不打 LLM。摘要退回原始版本，但成員變了要跟著更新（誠實：沒有分析過）
                # 註記上直接列的那幾行證據是上次收集的，照舊；只有第一行（哪些告警響了）跟著現在的成員更新
                wait = int((inc.llm_next_try - now).total_seconds())
                raw = rca.failure_line("unreachable", inc.llm_failures, wait) if inc.evidence_note.startswith("・") \
                    else f"[未經分析] {rca.raw_summary(inc)}"
                note = "\n".join(rca.alerts_fact(inc) if line.startswith("・告警：") else line for line in inc.evidence_note.splitlines())
                same_text = lambda text: text.split("（連續失敗")[0]   # 剩幾分鐘每一輪都在變：只有那個數字不同不算內容變了
                if (same_text(inc.summary), inc.analyzed, inc.candidates, inc.evidence_note, inc.suggestions) != (same_text(raw), False, "", note, ""):
                    inc.summary, inc.analyzed, inc.candidates, inc.evidence_note, inc.suggestions = raw, False, "", note, ""
                    inc.changed, inc.annotated = True, False
                log.debug("事故 %s 的 LLM 分析退避中，%s 之前不再試", inc.uid, inc.llm_next_try.isoformat())
                continue
            evidence = rca.collect_evidence(
                inc, relations_for(settings, db, inc, graph_now(inc), now), grafana, logs=log_store(settings, grafana),
                traces=trace_store(settings, grafana), id_fields=list(settings.id_fields), now=now, limits=settings.evidence,
                metrics=settings.metrics)
            if settings.graph_retention_days:
                # 這次的證據裡看到的關係記下來，之後的事故用得到：呼叫鏈裡誰呼叫誰；錯誤日誌裡提到了哪個已知的系統
                for pairs, source, messaging in (
                        (rca.calls_in(evidence), "trace", rca.calls_in(evidence, messaging_only=True)),
                        (graph.mentions(evidence["error_logs"], db.known_systems() | set(inc.services)), "log", [])):
                    if pairs:
                        db.record_calls(pairs, source, now, settings.graph_retention_days, messaging)
            before = (inc.summary, inc.analyzed, inc.evidence_note, inc.candidates, inc.suggestions)
            # 「證據」那幾行由程式從實際撈到的結果產生；同一份也交給 LLM，讓它講得出缺的是哪一種
            groups = rca.overview(evidence, settings.log_fields, settings.evidence.error_levels)
            inc.evidence_note = rca.overview_brief(groups)   # 註記上只放一行；完整的說明交給 LLM
            got = rca.analyze_full(inc, evidence, api_url=settings.llm_api_url, api_key=settings.llm_api_key,
                                   model=settings.llm_model, transport=llm_transport, options=settings.llm,
                                   overview_text=rca.overview_text(groups))
            inc.summary, inc.analyzed, inc.candidates, reason = got.line, got.analyzed, got.candidates, got.reason
            inc.suggestions = got.actions
            if can_analyze and inc.analyzed:
                if inc.llm_failures or inc.llm_next_try:
                    inc.llm_failures, inc.llm_next_try, inc.changed = 0, None, True
            elif can_analyze:
                # LLM 不通或回答沒過檢查：流程照走。註記第 2 行說明原因與什麼時候再試，底下直接列程式收集到的證據（沒有推論）
                inc.llm_failures += 1
                delay = backoff_seconds(inc.llm_failures, settings.llm_retry_initial_seconds, settings.llm_retry_max_seconds)
                inc.llm_next_try, inc.changed = now + timedelta(seconds=delay), True
                inc.summary = rca.failure_line(got.failure, inc.llm_failures, delay)
                inc.evidence_note = "\n".join([*rca.evidence_facts(inc, evidence), inc.evidence_note])
                errors.append(f"事故 {inc.uid} 未經 LLM 分析（連續第 {inc.llm_failures} 次）：{reason}；{delay} 秒後再試")
            # 沒設定 LLM 不是故障：不算失敗、不退避，第 2 行是原始摘要
            if (inc.summary, inc.analyzed, inc.evidence_note, inc.candidates, inc.suggestions) != before:
                inc.changed, inc.annotated = True, False
        # 寫入順序：先落地自己的資料庫（事故 + 這批告警處理完了），再對外：修復請求 → Grafana 註記 → 事故 API。
        # 註記寫成功才記 annotated；出口各自記在 deliveries 表，失敗的下一輪再試，不影響註記
        db.save_incidents([i for i in incidents if i.changed], now, processed=ids)
        for inc in incidents:
            undelivered += outputs.remediate(inc, now)   # 註記上要顯示修復請求的狀態，所以排在寫註記之前
        db.save_incidents([i for i in incidents if i.changed], now)
        written = []
        for inc in incidents:
            if inc.annotated:
                continue
            try:
                inc.annotation_id = grafana.upsert_annotation(
                    inc.annotation_id, report.uid_tag(inc.uid), report.render(inc, settings.grafana_url, settings.tz,
                                                                        settings.annotation_max_rule_links))
                inc.annotated = True
                written.append(inc)
            except GrafanaError as e:
                log.warning("事故 %s 的註記沒寫成功，下一輪補：%s", inc.uid, e)
                errors.append(f"事故 {inc.uid} 的註記寫入失敗：{e}")
        db.save_incidents(written, now)
        for inc in incidents:
            undelivered += outputs.publish(inc, settings.grafana_url, now)
        errors += undelivered
        status = "degraded" if errors else "ok"
    except Exception as e:
        log.exception("這一輪失敗")
        errors.append(f"{type(e).__name__}: {e}")
        raise
    finally:
        open_count = sum(1 for i in incidents if i.status == "open")
        summary = {"status": status, "alerts": len(batch), "open_incidents": open_count, "errors": errors,
                   "incidents": [i.uid for i in incidents if i.changed or i.annotated],
                   "needs_followup": open_count > 0 or any(not i.annotated for i in incidents)
                   or bool(undelivered)}   # 出口沒送成功的：下一輪再試
        try:
            db.record_round(started_at=started, finished_at=datetime.now(timezone.utc), status=status,
                            alerts=len(batch), open_incidents=open_count, errors=errors,
                            retention_days=settings.rounds_retention_days)
        except Exception:  # 連執行紀錄都寫不進去（資料庫斷線）：留一行 log，不蓋掉原本的錯誤
            log.exception("這一輪的執行紀錄寫不進資料庫")
        outputs.close()
        grafana.close()
        db.close()
    return summary


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # 每個 HTTP 請求一行太吵；錯誤我們自己會記


def log_summary(summary: dict[str, Any]) -> None:
    for line in summary["errors"]:
        log.warning("%s", line)
    log.info("這一輪結束：status=%s alerts=%d open_incidents=%d", summary["status"], summary["alerts"],
             summary["open_incidents"])


def main() -> int:
    setup_logging()
    try:
        settings = Settings.load()
        setup_logging(settings.log_level)
        log_summary(process(settings))
    except ConfigError as e:
        log.error("設定錯誤：%s", e)
        return 2
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
