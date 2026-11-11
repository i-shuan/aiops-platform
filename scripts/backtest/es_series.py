"""從 Elasticsearch 撈「每分鐘請求數」序列（唯讀）：metrics-apm.transaction.1m-* 的
sum(transaction.duration.summary.value_count)，依 service.name 分開。

只用唯讀 API key（secrets.local.yaml 的 es.AIOPS_ES_API_KEY）、只打 _search；金鑰不印、不寫檔。
撈回來的序列存成 CSV 快取（--cache 目錄，不要放在 repo 裡），之後回測不用再連 ES。資料只在本機算，不送到任何外部服務。

先開 port-forward：kubectl --context rancher-desktop -n es-test port-forward svc/elasticsearch 19200:9200
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

INDEX = "metrics-apm.transaction.1m-*"


def _client(secrets: Path):
    import httpx
    import yaml

    cfg = yaml.safe_load(secrets.read_text(encoding="utf-8"))["es"]
    return httpx.Client(base_url=cfg["url"], headers={"Authorization": "ApiKey " + cfg["AIOPS_ES_API_KEY"]}, timeout=120)


def fetch_es(service: str, secrets: Path) -> pd.DataFrame:
    body = {"size": 0, "query": {"term": {"service.name": service}}, "aggs": {"h": {"date_histogram": {
        "field": "@timestamp", "fixed_interval": "1m", "min_doc_count": 0},
        "aggs": {"n": {"sum": {"field": "transaction.duration.summary.value_count"}}}}}}
    with _client(secrets) as client:
        resp = client.post(f"/{INDEX}/_search", json=body)
        resp.raise_for_status()
    buckets = resp.json()["aggregations"]["h"]["buckets"]
    df = pd.DataFrame({"ts": pd.to_datetime([b["key"] for b in buckets], unit="ms", utc=True),
                       "value": [b["n"]["value"] or 0.0 for b in buckets]})
    if not (df["value"] > 0).any():
        raise SystemExit(f"{service}：ES 裡沒有資料")
    # 服務上線前的空分鐘不算（不然訓練期前段全是 0）
    return df[df["ts"] >= df.loc[df["value"] > 0, "ts"].iloc[0]].reset_index(drop=True)


def load_series(service: str, cache: Path, secrets: Path | None) -> pd.DataFrame:
    path = cache / f"{service}.csv"
    if path.is_file():
        df = pd.read_csv(path)
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df
    if secrets is None or not secrets.is_file():
        raise SystemExit(f"{path} 沒有快取，又沒給 --secrets（secrets.local.yaml）可以連 ES")
    df = fetch_es(service, secrets)
    cache.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df
