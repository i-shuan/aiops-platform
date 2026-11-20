# Elasticsearch（es-test）

這是開發叢集上放 APM 資料（metrics、trace、log）的 ES。**只有 Grafana 連它，而且只讀。**
本平台不連它（2026-11-20 起）：平台只持有 Grafana 的網址與 service account token，trace／日誌經 Grafana 的資料源 proxy 讀。

## 現況（誠實版）

- 叢集上有 helm release `elasticsearch`（namespace `es-test`，chart `elasticsearch-test-0.1.0`，ES 8.19.23，3 個 pod、21Gi 持久資料）與 `es-setup`（建角色與 API key 的 Job）。
- **這兩個 chart 的原始碼不在任何本機 repo**（舊 repo 的 `elasticsearch/` 只有一份 Kibana 指令檔）。所以這裡**沒有 chart**，只有：
  - [`es-test.snapshot.yaml`](es-test.snapshot.yaml)：從執行中的 release 匯出的 manifest，**已去掉 Secret 與所有機密**，只供重建時參考，不是可以直接當 chart 用的東西。
- 要讓 ES 真的由本 repo 管理，需要先找回原始 chart，或把快照整理成 chart。**這件事還沒做。**

## 本 repo 對 ES 的規則

1. **只讀，而且平台不直連。** 平台的 `GrafanaSpec`（`aiops/sources/grafana.py`）只對 Grafana 送資料源 proxy 的 `_msearch`（Grafana 對 ES 資料源也只放行這一條 POST）與兩個 GET；Grafana 的 ES 資料源只讀。任何寫入（`_bulk`、`_doc`、`PUT`、`DELETE`）都不能出現在平台程式裡（`tests/test_grafana_source.py`）。
2. **機密不進 repo。** `deploy/` 底下不得有 Secret 的資料、明文密碼或 API key。快照裡引用 Secret 的地方只能是引用。
3. **用唯讀 key。** 只有 Grafana 持有 ES 的 key（`GRAFANA_ES_API_KEY`，**唯讀**，只給需要的索引 pattern 讀取權限）。不要用 `elastic` 超級帳號。以前發給平台的那把 key（`AIOPS_ES_API_KEY`）已經沒有人用，要在 ES 上撤銷。
4. **建角色與 key 的是 `es-setup`，不是平台。** 平台不會建立索引、角色或 key。

## 索引

| 資料 | 索引 pattern | 資料源（Grafana） |
|---|---|---|
| metrics | `metrics-apm.transaction.1m-*` | `apm-metrics` |
| trace | `traces-apm-*` | `apm-traces` |
| log | `logs-apm.*` | `apm-logs` |

## 為 Grafana 建唯讀 key（範例）

在 Kibana Dev Tools 用管理員身分執行（**這一步要人做，平台不會做**）：

```
POST /_security/api_key
{
  "name": "grafana-readonly",
  "role_descriptors": {
    "apm-read": {
      "indices": [{
        "names": ["metrics-apm.transaction.1m-*", "traces-apm-*", "logs-apm.*"],
        "privileges": ["read", "view_index_metadata"]
      }]
    }
  }
}
```

回傳的 `encoded` 就是 `GRAFANA_ES_API_KEY` 的值，放進 K8s Secret。
