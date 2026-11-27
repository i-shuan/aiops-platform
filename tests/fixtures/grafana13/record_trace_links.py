"""錄製 trace_links.json 的腳本（留著是為了交代來源，測試不會執行它）。

情境：訊息佇列的「連結」接法。eap 發一筆訊息（一條 Trace），service-a／b／c 各自開一條新的 Trace 來處理，
只用連結（OTLP 的 links）指回發送的那一段；service-b 處理失敗。四條 Trace、四個 trace id。
前提：同樣的四條 Trace 用 OTLP 分別送進 VictoriaTraces v0.5.0 與 Tempo 2.9.0（scratch 的 send_links.py），
本機 Grafana 13.0.7 上各有一個資料源（Jaeger 型別、Tempo 型別）。
做的事：**用 Editor 服務帳號的 Token**，用 src/traces.py 的兩個類別把「service-b 那一條」與「eap 那一條」取回來，請求與回應原樣記下來。

用法：python record_trace_links.py <Editor Token 的檔案> <Jaeger 資料源 uid> <Tempo 資料源 uid> <ids.json>
ids.json：{"vt": {"eap": id, "service-b": id, …}, "tempo": {…}}（送的時候記下來的 trace id）
"""
import json
import pathlib
import sys

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src.grafana import Grafana  # noqa: E402
from src.traces import Tempo, VictoriaTraces  # noqa: E402

HERE = pathlib.Path(__file__).parent
token = pathlib.Path(sys.argv[1]).read_text().strip()
uids = {"vt": sys.argv[2], "tempo": sys.argv[3]}
ids = json.loads(pathlib.Path(sys.argv[4]).read_text())
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"})
    path = request.url.path
    for uid in uids.values():
        path = path.replace(uid, "<uid>")
    seen.append({"request": {"method": request.method, "path": path}, "status": r.status_code, "response": r.json()})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": r.headers.get("content-type", "")})


grafana = Grafana("http://127.0.0.1:23000", token, transport=httpx.MockTransport(recording), retries=0)
out = {}
for backend, store in (("vt", VictoriaTraces(grafana, uids["vt"])), ("tempo", Tempo(grafana, uids["tempo"]))):
    for which in ("service-b", "eap"):
        trace = store.get_trace(ids[backend][which])
        out[f"{backend}_{which}"] = seen[:]
        seen.clear()
        print(backend, which, len(trace["spans"]), "spans", [ref for sp in trace["spans"] for ref in sp["references"]][:2])
(HERE / "trace_links.json").write_text(json.dumps({
    "versions": {"grafana": "13.0.7", "victoriatraces": "v0.5.0", "tempo": "2.9.0"}, "token_role": "Editor", "trace_ids": ids,
    "exchanges": out}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
