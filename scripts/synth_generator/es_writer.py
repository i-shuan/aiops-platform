"""第 4 層的後半：把產生器的 NDJSON 載進 Elasticsearch。**給使用者自己跑**（要 ES 管理員密碼；平台程式與 AI 助理都不跑）。

    # 1. 預設 dry-run：只讀本機檔案，印出會建哪些 index template／data stream、各寫幾筆，不連網路
    .venv/Scripts/python -m scripts.synth_generator.es_writer synth-out/nov03_cascade

    # 2.（建議先做一次）用唯讀 key 抓既有 scn 資料串流的 mapping，存成 schema/*.json，新資料串流照抄同樣的 mapping
    .venv/Scripts/python -m scripts.synth_generator.es_writer --fetch-schema

    # 3. 真的寫：建 index template（只對這個劇本的三個 gen* 資料串流）、用 _bulk create 寫入
    .venv/Scripts/python -m scripts.synth_generator.es_writer synth-out/nov03_cascade --apply

金鑰從 repo 根目錄的 secrets.local.yaml 讀（被 .gitignore 擋住）：es.url、寫入用 es.admin_password（帳號 elastic）、
抓 schema 用唯讀的 es.AIOPS_ES_API_KEY。不印任何金鑰。
安全規則（程式裡擋死）：
- 只寫 manifest.json 記的 namespace（必須 gen 開頭，例如 gencascade）的 metrics-apm.transaction.1m-<ns>、traces-apm-<ns>、
  logs-apm.error-<ns> 三個資料串流；檔案裡出現別的索引名稱就拒絕
- 既有的 scn、synth 資料一筆都不碰：不刪任何東西，也不改既有的 index template（只建／更新名稱 aiops-synth-<ns>-* 的）
- 資料串流已經存在就拒絕寫（避免重跑寫兩份）；確定要追加才加 --append
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml

from .documents import KINDS, check_namespace, data_stream
from .generate import FILES

ROOT = Path(__file__).resolve().parents[2]
SECRETS = ROOT / "secrets.local.yaml"
SCHEMA_DIR = Path(__file__).resolve().parent / "schema"
SCHEMA_FILES = {"metrics": "metrics-apm.transaction.1m.json", "traces": "traces-apm.json", "logs": "logs-apm.error.json"}
SOURCE_PREFIX = {"metrics": "metrics-apm.transaction.1m-", "traces": "traces-apm-", "logs": "logs-apm.error-"}
TEMPLATE_PRIORITY = 500   # 比 ES 內建 APM template 高，這三個名稱才會用我們的 mapping
CHUNK_DOCS = 5000


def template_name(kind: str, namespace: str) -> str:
    return f"aiops-synth-{check_namespace(namespace)}-{kind}"


def scan(out: Path) -> tuple[str, dict[str, dict[str, Any]]]:
    """讀本機 NDJSON：每個檔的資料串流、筆數、時間範圍。索引名稱不是這個劇本的 gen* 資料串流就丟錯。"""
    manifest = out / "manifest.json"
    if not manifest.exists():
        raise SystemExit(f"找不到 {manifest}（先跑 python -m scripts.synth_generator 產生）")
    namespace = check_namespace(json.loads(manifest.read_text(encoding="utf-8"))["namespace"])
    plan = {}
    for kind, fname in FILES.items():
        ds = data_stream(kind, namespace)
        path = out / fname
        if not path.exists():
            raise SystemExit(f"找不到 {path}（先跑 python -m scripts.synth_generator 產生）")
        n, t_min, t_max = 0, None, None
        with open(path, encoding="utf-8") as f:
            for k, line in enumerate(f):
                if k % 2 == 0:
                    index = json.loads(line)["create"]["_index"]
                    if index != ds:
                        raise SystemExit(f"{path} 第 {k + 1} 行要寫到 {index}，不是允許的 {ds}，拒絕")
                    n += 1
                else:
                    ts = line[15:39] if line.startswith('{"@timestamp":"') else json.loads(line)["@timestamp"]
                    t_min = ts if t_min is None or ts < t_min else t_min
                    t_max = ts if t_max is None or ts > t_max else t_max
        plan[kind] = {"data_stream": ds, "file": str(path), "docs": n, "from": t_min, "to": t_max}
    return namespace, plan


def load_schema(kind: str) -> dict[str, Any]:
    return json.loads((SCHEMA_DIR / SCHEMA_FILES[kind]).read_text(encoding="utf-8"))


def template_body(kind: str, ds: str) -> dict[str, Any]:
    schema = load_schema(kind)
    return {"index_patterns": [ds], "data_stream": {}, "priority": TEMPLATE_PRIORITY,
            "template": {"mappings": schema["mappings"]},
            "_meta": {"managed_by": "scripts/synth_generator/es_writer.py", "schema_source": schema["source"]}}


def secrets() -> dict[str, Any]:
    if not SECRETS.exists():
        raise SystemExit(f"找不到 {SECRETS}（格式見 secrets.local.example.yaml）")
    es = (yaml.safe_load(SECRETS.read_text(encoding="utf-8")) or {}).get("es") or {}
    if not es.get("url"):
        raise SystemExit("secrets.local.yaml 沒有 es.url")
    return es


def _client(es: dict[str, Any], admin: bool) -> httpx.Client:
    if admin:
        if not es.get("admin_password"):
            raise SystemExit("secrets.local.yaml 沒有 es.admin_password（寫入要管理員）")
        return httpx.Client(base_url=str(es["url"]).rstrip("/"), auth=("elastic", str(es["admin_password"])), timeout=120)
    if not es.get("AIOPS_ES_API_KEY"):
        raise SystemExit("secrets.local.yaml 沒有 es.AIOPS_ES_API_KEY（唯讀 key，建法見 README）")
    return httpx.Client(base_url=str(es["url"]).rstrip("/"),
                        headers={"Authorization": f"ApiKey {es['AIOPS_ES_API_KEY']}"}, timeout=60)


def _call(c: httpx.Client, method: str, path: str, **kw) -> httpx.Response:
    try:
        return c.request(method, path, **kw)
    except httpx.ConnectError as e:
        raise SystemExit(f"連不到 {c.base_url}（{e}）。叢集上的 ES 要先 port-forward，例如："
                         "kubectl --context rancher-desktop -n es-test port-forward svc/elasticsearch 19200:9200") from e


def _clean_mapping(m: Any) -> Any:
    """抄 mapping 時去掉跟來源綁死的部分：constant_keyword 的值（data_stream.namespace 會是 scn）、資料串流的內部欄位。"""
    if isinstance(m, dict):
        out = {k: _clean_mapping(v) for k, v in m.items() if k not in ("_data_stream_timestamp", "_meta")}
        if out.get("type") == "constant_keyword":
            out.pop("value", None)
        return out
    if isinstance(m, list):
        return [_clean_mapping(v) for v in m]
    return m


def fetch_schema(namespace: str) -> int:
    c = _client(secrets(), admin=False)
    for kind in KINDS:
        source = SOURCE_PREFIX[kind] + namespace   # 既有資料串流：只讀它的 mapping
        r = _call(c, "GET", f"/{source}/_mapping")
        if r.status_code != 200:
            print(f"✗ {source}：HTTP {r.status_code} {r.text[:200]}（{SCHEMA_FILES[kind]} 不變）")
            continue
        backing = sorted(r.json())[-1]
        mappings = _clean_mapping(r.json()[backing]["mappings"])
        doc = {"source": f"GET /{source}/_mapping（{backing}），"
                         f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} 抓的", "mappings": mappings}
        (SCHEMA_DIR / SCHEMA_FILES[kind]).write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        hist = (((mappings.get("properties") or {}).get("transaction") or {}).get("properties") or {}) \
            .get("duration", {}).get("properties", {}).get("histogram", {}).get("type")
        print(f"✓ {source} -> schema/{SCHEMA_FILES[kind]}" + (f"（transaction.duration.histogram: {hist}）" if hist else ""))
    return 0


def apply(namespace: str, plan: dict[str, dict[str, Any]], append: bool) -> int:
    c = _client(secrets(), admin=True)
    for info in plan.values():
        r = _call(c, "GET", f"/_data_stream/{info['data_stream']}")
        if r.status_code == 200 and not append:
            raise SystemExit(f"{info['data_stream']} 已經存在。為了不重複寫入，預設拒絕；確定要追加請加 --append"
                             "（本程式不會刪任何資料）")
    for kind, info in plan.items():
        ds, name = info["data_stream"], template_name(kind, namespace)
        r = _call(c, "PUT", f"/_index_template/{name}", json=template_body(kind, ds))
        if r.status_code >= 300:
            raise SystemExit(f"建 index template {name} 失敗：HTTP {r.status_code} {r.text[:300]}")
        sim = _call(c, "POST", f"/_index_template/_simulate_index/{ds}").json()
        hist = (sim.get("template", {}).get("mappings", {}).get("properties", {}).get("transaction", {})
                .get("properties", {}).get("duration", {}).get("properties", {}).get("histogram", {}).get("type"))
        print(f"✓ template {name} -> {ds}" + (f"（histogram 欄位型別：{hist}）" if kind == "metrics" else ""))
        if kind == "metrics" and hist != "histogram":
            raise SystemExit("metrics 的 transaction.duration.histogram 不是 histogram 型別，Grafana 的 p95 面板會壞；停止")
    for info in plan.values():
        ds, sent = info["data_stream"], 0
        with open(info["file"], encoding="utf-8") as f:
            while True:
                lines = [f.readline() for _ in range(CHUNK_DOCS * 2)]
                lines = [ln for ln in lines if ln]
                if not lines:
                    break
                r = _call(c, "POST", "/_bulk", content="".join(lines).encode("utf-8"),
                          headers={"Content-Type": "application/x-ndjson"})
                body = r.json() if r.content else {}
                if r.status_code >= 300 or body.get("errors"):
                    bad = [next(iter(i.values())).get("error") for i in body.get("items", [])
                           if next(iter(i.values())).get("error")][:3]
                    raise SystemExit(f"寫 {ds} 失敗（已寫 {sent} 筆）：HTTP {r.status_code} {bad or r.text[:300]}")
                sent += len(lines) // 2
                print(f"  {ds}: {sent}/{info['docs']}", end="\r")
        _call(c, "POST", f"/{ds}/_refresh")
        count = _call(c, "GET", f"/{ds}/_count").json().get("count")
        print(f"✓ {ds}: 寫入 {sent} 筆，資料串流現在共 {count} 筆")
    return 0


def main(argv: list[str] | None = None) -> int:
    getattr(sys.stdout, "reconfigure", lambda **_: None)(encoding="utf-8")
    ap = argparse.ArgumentParser(prog="python -m scripts.synth_generator.es_writer", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out", nargs="?", help="產生器的輸出資料夾")
    ap.add_argument("--apply", action="store_true", help="真的寫進 ES（預設只 dry-run）")
    ap.add_argument("--append", action="store_true", help="資料串流已存在時仍追加")
    ap.add_argument("--fetch-schema", action="store_true", help="用唯讀 key 抓既有資料串流的 mapping 存成 schema/*.json")
    ap.add_argument("--source-namespace", default="scn", help="--fetch-schema 要抄哪個 namespace 的 mapping（預設 scn）")
    args = ap.parse_args(argv)
    if args.fetch_schema:
        return fetch_schema(args.source_namespace)
    if not args.out:
        ap.error("要給輸出資料夾")
    namespace, plan = scan(Path(args.out))
    print("dry-run（沒有連 ES）" if not args.apply else "要寫入 ES：")
    for kind, info in plan.items():
        print(f"  index template {template_name(kind, namespace)}（priority {TEMPLATE_PRIORITY}，"
              f"只套用到 {info['data_stream']}），mapping 來源：{load_schema(kind)['source']}")
    for info in plan.values():
        print(f"  data stream {info['data_stream']}: {info['docs']} 筆（{info['from']} ~ {info['to']}），分 "
              f"{-(-info['docs'] // CHUNK_DOCS)} 批 _bulk create")
    print("  不刪除、不修改任何既有資料或 template；ground_truth.json 不會寫進 ES")
    if not args.apply:
        print("要真的寫入請加 --apply（需要 secrets.local.yaml 的 es.url 與 es.admin_password）")
        return 0
    return apply(namespace, plan, args.append)


if __name__ == "__main__":
    raise SystemExit(main())
