"""跑評估案例（eval/scenarios.py）並計分。開發工具，不是平台的一部分：不會被服務載入，也不進映像檔。

每個案例：把日誌寫進 VictoriaLogs → 用 aiops-lite 的程式（Editor Token 經 Grafana 的 VictoriaLogs 資料源代理）收集證據 →
同一份證據問真的 LLM 幾次 → 計分。算對的條件：回答通過幻覺檢查、排第一的可能原因跟答案一樣（答案是「無法判定」時：不能有 40 分以上的可能原因）、
有提到重點（any 裡任何一個詞）。

  python -m eval.run --label baseline --runs 3 --out /tmp/eval-baseline.json
  python -m eval.run --label ablation --drop error_patterns,warnings         # 拿掉某幾包證據再問，看少了它差多少

需要：本機拋棄式的 VictoriaLogs（寫測試資料用，這是這個工具唯一直連 VictoriaLogs 的地方）、裝了 VictoriaLogs 資料源外掛的
Grafana（Editor Token）、LLM。VictoriaLogs 不能刪單筆資料，所以每次執行、每個案例各寫進自己的 dataset（stream 欄位），
查詢時用 logs.extra_filter 只看那一個；跑完資料留在裡面，整個資料目錄刪掉就清乾淨。
設定用環境變數（這是開發工具，不走 config.yaml）：
  EVAL_VL_URL（--backend elasticsearch 時改用 EVAL_ES_URL）、EVAL_GRAFANA_URL、EVAL_GRAFANA_TOKEN、EVAL_GRAFANA_ADMIN（user:password，用來建 VictoriaLogs 資料源）、
  LLM_API_URL（預設 https://api.openai.com/v1）、LLM_API_KEY、LLM_CHAT_MODEL
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx

from dataclasses import replace

from eval.scenarios import HELD_OUT, SCENARIOS
from src import evidence as gather
from src import report
from src import rca
from src.grafana import Grafana
from src.logs import Elasticsearch, LogFields, VictoriaLogs
from src.grouping import Alert, converge
from src.main import Settings


UNPARSED_FIELDS = LogFields(service="kubernetes.container_name", level="")


def load_logs(vl: httpx.Client, dataset: str, logs: list, now: datetime, unparsed: bool = False) -> int:
    """把案例的日誌寫進 VictoriaLogs（/insert/jsonline），等到查得到才回來。
    unparsed：照沒拆欄位的 K8s 容器日誌寫（等級只在訊息文字裡、識別碼也只在訊息裡）。"""
    lines = []
    for ago, service, level, message, fields in logs:
        stamp = (now - timedelta(seconds=ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        doc = ({"@timestamp": stamp, "stream": "stdout", "kubernetes": {"container_name": service, "namespace_name": "factory"},
                "message": f"{stamp} | {level:<5} | worker-1 | {message}"} if unparsed else
               {"@timestamp": stamp, "service": {"name": service}, "log": {"level": level}, "message": message, **fields})
        lines.append(json.dumps({**doc, "dataset": dataset}, ensure_ascii=False))
    if not lines:
        return 0
    r = vl.post("/insert/jsonline", params={"_stream_fields": "dataset", "_msg_field": "message", "_time_field": "@timestamp"},
                content="\n".join(lines).encode())
    r.raise_for_status()
    for _attempt in range(30):   # 寫入後要等幾秒才查得到
        got = vl.post("/select/logsql/query", data={"query": f'{{dataset="{dataset}"}} | stats count() as n',
                                                     "start": (now - timedelta(days=6)).strftime("%Y-%m-%dT%H:%M:%SZ")})
        if got.status_code == 200 and json.loads(got.text.splitlines()[0])["n"] == str(len(lines)):
            return len(lines)
        time.sleep(1)
    raise RuntimeError(f"{dataset}：寫了 {len(lines)} 筆，但 30 秒後 VictoriaLogs 還查不到全部")


def ensure_datasource(grafana_url: str, admin: str, vl_url: str) -> str:
    user, password = admin.split(":", 1)
    with httpx.Client(base_url=grafana_url, auth=(user, password), timeout=30) as http:
        old = http.get("/api/datasources/name/eval-victorialogs")
        if old.status_code == 200:
            return old.json()["uid"]
        made = http.post("/api/datasources", json={"name": "eval-victorialogs", "type": "victoriametrics-logs-datasource",
                                                   "access": "proxy", "url": vl_url})
        made.raise_for_status()
        return made.json()["datasource"]["uid"]


def load_es(es: httpx.Client, index: str, logs: list, now: datetime, unparsed: bool = False) -> int:
    """unparsed：照沒拆欄位的 K8s 容器日誌寫（動態 mapping、等級只在訊息文字裡、識別碼也只在訊息裡）。"""
    es.delete(f"/{index}")
    mapping = {"@timestamp": {"type": "date"}} if unparsed else {
        "@timestamp": {"type": "date"}, "service": {"properties": {"name": {"type": "keyword"}}},
        "log": {"properties": {"level": {"type": "keyword"}}}, "message": {"type": "text"}}
    es.put(f"/{index}", json={"mappings": {"properties": mapping}}).raise_for_status()
    lines = []
    for ago, service, level, message, fields in logs:
        stamp = (now - timedelta(seconds=ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        doc = ({"@timestamp": stamp, "stream": "stdout", "kubernetes": {"container_name": service, "namespace_name": "factory"},
                "message": f"{stamp} | {level:<5} | worker-1 | {message}"} if unparsed else
               {"@timestamp": stamp, "service": {"name": service}, "log": {"level": level}, "message": message, **fields})
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(doc, ensure_ascii=False))
    r = es.post(f"/{index}/_bulk?refresh=true", content="\n".join(lines) + "\n", headers={"Content-Type": "application/x-ndjson"})
    r.raise_for_status()
    assert not r.json()["errors"], r.text[:300]
    return len(logs)


def ensure_es_datasource(grafana_url: str, admin: str, es_url: str) -> str:
    user, password = admin.split(":", 1)
    with httpx.Client(base_url=grafana_url, auth=(user, password), timeout=30) as http:
        old = http.get("/api/datasources/name/eval-es")
        if old.status_code == 200:
            return old.json()["uid"]
        made = http.post("/api/datasources", json={"name": "eval-es", "type": "elasticsearch", "access": "proxy", "url": es_url,
                                                   "jsonData": {"index": "eval-*", "timeField": "@timestamp"}})
        made.raise_for_status()
        return made.json()["datasource"]["uid"]


def build_evidence(scenario: dict, grafana: Grafana, store, now: datetime, settings: Settings):
    alerts = [Alert(f"{scenario['name']}-{i}", "firing", f"rule-{i}", title, service, severity, now - timedelta(seconds=ago), None, {"A": 1})
              for i, (service, title, severity, ago) in enumerate(scenario["alerts"])]
    edges = list(scenario["edges"])
    incidents = converge([], alerts, [], edges, now=now, close_after_seconds=300)
    assert len(incidents) == 1, f"{scenario['name']}：告警沒有收斂成一張事故（{len(incidents)} 張），案例的關係邊要補"
    [incident] = incidents
    evidence = gather.collect_evidence(incident, edges, grafana, logs=store, id_fields=list(settings.id_fields), now=now, limits=settings.evidence)
    overview = gather.overview_text(gather.overview(evidence, store.fields, settings.evidence.error_levels))
    return evidence, overview


def _same_name(said: str, answer: str) -> bool:
    """排第一的名稱跟答案是不是同一個：大小寫不同（MariaDb／MariaDB）、後面多帶連接埠（host:8481）都算同一個。"""
    strip = lambda name: re.sub(r":\d+$", "", name.strip().lower())
    return strip(said) == strip(answer)


def ask(evidence: dict, overview: str, scenario: dict, llm: dict) -> dict:
    """問一次並計分。回傳 {ok, why, source, grade, summary}。

    答案是某個系統：排第一的可能原因要是它。答案是「無法判定」：不能有依據等級是「直接證據」的可能原因
    （可以列間接證據或推測，那正是要它做的事；等級是程式照證據判定的，排序也照等級）。
    重點字在摘要或任何一個可能原因的理由裡出現都算。"""
    try:
        raw = rca.call_llm(llm["url"], llm["key"], llm["model"], rca.build_prompt(evidence, overview), timeout=120)
        summary, candidates = rca.read_answer(raw, evidence)
    except Exception as e:   # 連線、格式、幻覺檢查沒過：都算這一次沒答對
        return {"ok": False, "why": f"{type(e).__name__}: {str(e).replace(llm['key'], '<key>')[:160]}", "source": None,
                "grade": None, "summary": ""}
    top = candidates[0] if candidates else {"source": "", "grade": ""}
    said = " ".join([summary, *(c["reason"] for c in candidates)])
    listed = "、".join(f"{c['source']} {report.GRADE_TEXT[c['grade']]}" for c in candidates) or "（沒有列）"
    problems = []
    if scenario["source"] == "":
        strong = [c for c in candidates if c["grade"] == "direct"]
        if strong:
            problems.append(f"答案是無法判定，卻把 {strong[0]['source']} 列為直接證據")
    elif scenario["source"] is not None and _same_name(top["source"], scenario["source"]) is False:
        problems.append(f"排第一的是 {top['source'] or '（沒有）'}，答案是 {scenario['source']}")
    if scenario["any"] and not any(word in said for word in scenario["any"]):
        problems.append(f"沒有提到 {'／'.join(scenario['any'])}")
    return {"ok": not problems, "why": "；".join(problems), "source": top["source"], "grade": top["grade"],
            "count": len(candidates), "candidates": listed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="run")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--only", default="", help="只跑名稱含這個字的案例")
    parser.add_argument("--set", default="tuning", choices=["tuning", "heldout"], help="tuning＝開發時用的十個；heldout＝留出的")
    parser.add_argument("--backend", default="victorialogs", choices=["victorialogs", "elasticsearch"], help="日誌寫進哪一種、從哪一種查")
    parser.add_argument("--drop", default="", help="問之前從證據裡拿掉這些鍵（逗號分隔），例如 error_patterns,warnings")
    parser.add_argument("--out", default="")
    parser.add_argument("--show-prompt", action="store_true")
    args = parser.parse_args()
    env = os.environ
    llm = {"url": env.get("LLM_API_URL", "https://api.openai.com/v1"), "key": env["LLM_API_KEY"], "model": env["LLM_CHAT_MODEL"]}
    on_es = args.backend == "elasticsearch"
    writer = httpx.Client(base_url=env["EVAL_ES_URL" if on_es else "EVAL_VL_URL"], timeout=60)
    uid = (ensure_es_datasource if on_es else ensure_datasource)(env["EVAL_GRAFANA_URL"], env["EVAL_GRAFANA_ADMIN"], str(writer.base_url))
    grafana = Grafana(env["EVAL_GRAFANA_URL"], env["EVAL_GRAFANA_TOKEN"])
    settings = Settings(grafana_url=env["EVAL_GRAFANA_URL"], grafana_token=env["EVAL_GRAFANA_TOKEN"], webhook_token="unused")
    drop = [k for k in args.drop.split(",") if k]
    now = datetime.now(timezone.utc)
    results, right, total = [], 0, 0
    base_settings = settings
    for scenario in (SCENARIOS if args.set == "tuning" else HELD_OUT):
        if args.only not in scenario["name"]:
            continue
        unparsed = bool(scenario.get("unparsed"))
        settings = replace(base_settings, log_fields=UNPARSED_FIELDS) if unparsed else base_settings
        dataset = f"eval-{scenario['name'].replace('_', '-')}-{int(now.timestamp())}"
        if on_es:
            index = "eval-" + scenario["name"].replace("_", "-")
            load_es(writer, index, scenario["logs"], now, unparsed)
            store = Elasticsearch(grafana, uid, settings.log_fields, index)
        else:
            load_logs(writer, dataset, scenario["logs"], now, unparsed)
            store = VictoriaLogs(grafana, uid, settings.log_fields, f'{{dataset="{dataset}"}}')
        evidence, overview = build_evidence(scenario, grafana, store, now, settings)
        for key in drop:
            if key in evidence:
                evidence[key] = [] if isinstance(evidence[key], list) else {}
        prompt = rca.build_prompt(evidence, overview)
        if args.show_prompt:
            print(prompt, "\n")
        with ThreadPoolExecutor(max_workers=args.runs) as pool:
            answers = list(pool.map(lambda _i: ask(evidence, overview, scenario, llm), range(args.runs)))
        ok = sum(a["ok"] for a in answers)
        right, total = right + ok, total + len(answers)
        named = [a for a in answers if a["source"]]
        grades = sorted({report.GRADE_TEXT[a["grade"]] for a in named})
        tops = "、".join(sorted({a["source"] for a in named})) or "—"
        spread = "／".join(grades) or "—"
        listed = "／".join(str(a.get("count", 0)) for a in answers)
        print(f"{scenario['name']:<32} {ok}/{len(answers)}  排第一的：{tops}（{spread}）  各列了 {listed} 個  （提示詞 {len(prompt)} 字）")
        for a in answers:
            if not a["ok"]:
                print(f"      ✗ {a['why']}｜{a.get('candidates', '')}｜{a['summary'][:90]}")
        results.append({"name": scenario["name"], "ok": ok, "runs": len(answers), "prompt_chars": len(prompt), "answers": answers})
    print(f"\n[{args.label}] 答對 {right}/{total}" + (f"（拿掉 {'、'.join(drop)}）" if drop else ""))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({"label": args.label, "model": llm["model"], "drop": drop, "right": right, "total": total,
                       "at": now.isoformat(), "results": results}, f, ensure_ascii=False, indent=1)
    grafana.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
