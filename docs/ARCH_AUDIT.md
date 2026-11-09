> **關於這份文件**
>
> - 稽核基準：`main@bd16b7a`（2026-11-09），原始分支 `claude/arch-audit-e98c0f`；原本放在 `synth-out/reports/arch_audit.md`（不進 git），搬到這裡，內文未改寫。
> - 缺口的 strict xfail 測試在 `tests/test_arch_audit_gaps.py`（28 個）。修好一項就會變 XPASS → 紅燈，修的人把對應的 xfail 拿掉。
> - **修復進度由 `fix/arch-audit-gaps` 分支處理**（G6–G9、G11、G13、G17–G19），這份文件記錄的是稽核當下的狀態，不隨修復更新。
> - 文末「實驗檔」一節提到的 `tests/_audit_exp_a.py`／`_b.py`／`_c.py` 已提煉成正規測試後刪除（只保留「現在就是乾淨的行為」）：
>   Q2 → `tests/test_architecture.py::test_engine_and_runner_never_special_case_an_operator_name`；
>   Q4 → `tests/test_algorithms.py::test_lineage_is_fixed_at_detection_and_lands_unchanged_in_the_table_and_the_snapshot`；
>   Q5、Q13、Q14 → `tests/test_target_detectors.py`（`test_cold_start_and_skipped_nodes_leave_one_voter_who_decides_alone`、`test_routing_order_is_visible_in_the_skip_reason_and_skipped_nodes_load_nothing`、`test_nested_params_*`）；
>   Q6 → `tests/test_dynamic_topology.py::test_rca_and_trace_import_share_one_edge_parser`；
>   Q7 → `tests/test_security_and_tuning.py`（`test_api_rejects_more_path_shapes_on_both_entry_points`、`test_api_unknown_workflow_name_is_404_not_a_path_lookup`、`test_directory_link_inside_pipelines_pointing_outside_is_refused`）；
>   Q8、Q10、Q11 → `tests/test_stability_guards.py`（空 DataFrame 斷流、寫入失敗 0 通知、交易重試逐欄比對 ctx）；
>   Q15 → `tests/test_fail_fast.py`。
>   文末「突變測試」表裡沒變紅的非等價突變（空 df 不算斷流、Q11 三種交易內污染、淺層合併），以及 Q2 的「引擎寫死算子名」突變，現在都有測試會紅；表裡標成等價的「include 先於 exclude」也因為新測試斷言了跳過原因字串而會紅。

# 架構與資料契約稽核 — main@bd16b7a（2026-11-09）

分支 `claude/arch-audit-e98c0f`。只驗證、不修；缺口都寫成 `xfail(strict=True)` 測試放在 `tests/test_arch_audit_gaps.py`。唯一動到的非測試檔是兩處純文件錯誤（見文末）。

證據等級：
- **【測試】** 自動化測試已涵蓋——列出測試名稱，並用突變實驗（把生產程式改壞）確認它真的會變紅
- **【實驗】** 我自己跑實驗驗證（腳本與輸出放在本文）
- **【推論】** 看程式推論、沒驗證

## 總覽

| # | 題目 | 結論 | 缺口 |
|---|---|---|---|
| 1 | stages 私自碰 storage／連外 | 乾淨 | 架構測試有 3 個盲點（importlib、http.client、enrichment 可 import sources） |
| 2 | engine／runner 寫死算子名稱 | 乾淨 | 沒有任何測試守這條（只能靠人看） |
| 3 | 循環引用、可變狀態 | 乾淨（只有 db↔models 函式內延遲 import） | 可變狀態測試 5 個盲點 |
| 4 | AnomalyEvent 血緣固化 | 大致乾淨 | anomalies 表沒有 voted_by；attrs 是淺層凍結 |
| 5 | ensemble 分母 | 白名單／冷啟動／沒資料都正確棄權 | rules 無規則、窗口比資料長的節點「不判卻投反對票」 |
| 6 | 拓撲解析只有一份 | 邊的解析只有一份 | 「入口 span」定義跟邊不一致（孤兒 span 算成外部入口） |
| 7 | 路徑安全 | 乾淨 | — |
| 8 | 斷流攔截 | 主要行為正確；ES／Prometheus／VM 回 200 零筆都實測走到 NoDataError → 斷流 → 心跳 0 次 | 非 fetch 的 NotFound 被算成斷流；單條序列的 ValueError 讓整輪失敗；ES 不濾 NaN／Inf（還寫進基準線）；格式怪的回應丟原始例外 |
| 9 | 冷啟動門檻 | rules 1 點能判 | 門檻數列數不數有效點；到期重訓資料不足時連舊基準線也不用 |
| 10 | 寫入失敗 0 通知 | 乾淨 | — |
| 11 | 交易重試不污染 ctx | 乾淨 | — |
| 12 | 結案推送與 finalize 失敗 | 補送行為正確 | watchdog 在 finalize 之前續命；叢集 AM 實測被權限擋下 |
| 13 | 路由順序與零開銷 | 順序正確、不載基準線與調參 | 全部節點被跳過的序列每輪仍抓整段訓練窗 |
| 14 | 參數優先序 deep_merge | 偵測時逐鍵合併正確 | 回測不套 YAML／overrides；調參沒有節點層（文件已改） |
| 15 | 載入期報錯 | 乾淨：設定錯誤全在碰資料庫前擋下（0 SQL、0 對外請求） | （題目外）RCA 不存在的 incident_id 入口不擋，訊息不相干 |

**乾淨 5 題（2、7、10、11、15；其中 2 沒有測試防線、11 測試只驗一半、15 附帶一個題目範圍外的小缺口 G19），有缺口 10 題。缺口共 19 項（G1～G19），其中 28 個案例寫成 strict xfail。**

> 追加（2026-11-09，依發起 session 轉達的需求）：Q8 用 httpx.MockTransport 實測「HTTP 200 但零筆／格式怪」；Q7、Q15 寫明每一層擋下的時機，並以「報錯前有沒有資料庫存取或對外請求」實測。新增 G17～G19。

---

## 基線

```
$ .venv/Scripts/python -m pytest tests -q
547 passed, 1 skipped, 3 xfailed, 43 warnings in 68.30s

$ .venv/Scripts/python -m pytest tests/test_architecture.py -v
... 23 passed, 1 warning in 1.44s
```

跟交辦的基線一致。

加上本分支的 `tests/test_arch_audit_gaps.py`（28 個 xfail）之後：

```
$ .venv/Scripts/python -m pytest tests -q
547 passed, 1 skipped, 31 xfailed, 43 warnings in 66.56s
$ .venv/Scripts/python -m pytest tests/test_architecture.py -q
23 passed
$ .venv/Scripts/python -m pytest tests/test_arch_audit_gaps.py -q --runxfail   # 確認每個都是因為預期的原因失敗
28 failed
```

每個 xfail 我都用 `--runxfail` 逐一看過失敗訊息，都是缺口本身造成的（不是測試寫錯）；架構盲點那組另外跑過對照組（既有防線抓得到的寫法，harness 回 True）。

---

## 一、模組邊界與單向依賴

### Q1 `stages/` 有沒有私自碰 storage、開 session、連外？

**結論：沒有。** stages 只往 engine 與（fetch.py 才能用的）sources 依賴；連外只在 `enrichment/providers.py` 與 `notification/transport.py`。

**證據等級：【實驗】AST 全掃 ＋【測試】（已用突變確認會紅），但架構測試有盲點。**

我自己寫的 AST 腳本（解析 `import`／`from … import`，含函式內、相對 import）掃 `aiops/stages/**`：

```
NET     aiops/stages/enrichment/providers.py:5 -> httpx
SOURCES aiops/stages/ingestion/fetch.py:25 -> aiops.sources
SOURCES aiops/stages/ingestion/fetch.py:26 -> aiops.sources.base
SOURCES aiops/stages/ingestion/fetch.py:27 -> aiops.sources.query.trace
SOURCES aiops/stages/ingestion/fetch.py:28 -> aiops.sources.query.window
NET     aiops/stages/notification/transport.py:8 -> httpx
（沒有任何 aiops.storage、sqlalchemy、aiops.control_plane、aiops.interfaces）
```

層級依賴（`aiops.<層>` 之間）：

```
aiops.control_plane    -> ['aiops.engine', 'aiops.sources', 'aiops.stages', 'aiops.storage']
aiops.interfaces       -> ['aiops.engine', 'aiops.storage']
aiops.sources          -> ['aiops.engine']
aiops.stages           -> ['aiops.engine', 'aiops.sources']
aiops.storage          -> ['aiops.engine']
（aiops.engine 不依賴任何其他層）
```

grep `importlib|__import__|subprocess|socket|Session(|sessionmaker|create_engine|open(|os.environ` 於 `aiops/stages`、`aiops/engine`：只有 `enrichment/registry.py:54` 的 importlib（只掃 `aiops.stages.enrichment` 自己的子模組）與 notifiers／providers 讀環境變數，沒有開 session 或其他連線。

**突變實驗（暫時放一個違規檔進 aiops/，跑對應的架構測試，跑完刪掉）：**

| 違規寫法 | 架構測試 | 結果 |
|---|---|---|
| `stages/grouping` 裡 `from aiops.storage.repository import Incidents` | `test_operators_do_not_touch_orm_or_storage_except_admin` | 紅 ✅ |
| `from aiops.storage import db` | 同上 | 紅 ✅ |
| `import sqlalchemy.orm` | 同上 | 紅 ✅ |
| `import httpx` | `test_operators_do_not_use_the_network_except_llm_notifiers_admin` | 紅 ✅ |
| `stages/rca` 裡 `from aiops.sources import get_spec` | 同上 | 紅 ✅ |
| `importlib.import_module('aiops.storage.repository')` | storage 那條 | **綠（沒抓到）** |
| `import http.client` | 網路那條 | **綠（沒抓到）**：禁用前綴只有 httpx／requests／urllib／socket |
| `stages/enrichment/` 裡 `from aiops.sources import base` | 網路那條 | **綠（沒抓到）**：整個 enrichment/ 被豁免，連 sources 也放行；但 CLAUDE.md 說只有 fetch.py 能用 sources |

**缺口**：上面三個盲點。現在的程式沒有這樣寫（實驗已掃過），所以是防線缺口、不是現存違規。
- 測試：`test_import_guards_catch_indirect_imports[importlib-storage|http.client|enrichment-imports-sources]`（xfail）
- 建議修法：`_violations` 另外掃 `importlib.import_module("…")`／`__import__("…")` 的字串常數；網路前綴加 `http.client`、`aiohttp`、`grpc`；網路豁免清單把 `stages/enrichment/` 跟 `aiops.sources` 分開判斷。約 15 行，只改 `tests/test_architecture.py`。

### Q2 engine／runner 有沒有寫死算子名稱特判？

**結論：沒有。** 引擎全部用算子自己宣告的屬性分流（`scope`、`has_baseline`、`include_entities`／`exclude_entities`），算子名稱只透過 `OperatorRegistry.get/create` 查表。

**證據等級：【實驗】grep。沒有自動化測試守這條。**

```
$ grep -rnE "[\"'](detector|alert|notifier|rca|ingestion|enrichment|admin|...)[._]" aiops/engine aiops/interfaces aiops/storage
aiops/engine/requests.py:46:    workflow: str = Field("rca_report", ...)      # workflow 名稱的預設值，不是算子特判
aiops/interfaces/api/*.py: run("admin_…")                                   # workflow 名稱（CLAUDE.md 規定的寫法）
aiops/storage/*: "detector_name"／"detector_models"                          # 欄位名、表名

$ grep -rnE "operator(_name)? *(==|!=|in )|\.startswith\(|getattr\(op" aiops/engine aiops/interfaces/runner.py
aiops/engine/pipeline.py:159  getattr(op, "scope", "target")
aiops/engine/pipeline.py:172-173  getattr(op, "has_baseline", False)
aiops/engine/pipeline.py:188  `name in wanted or operator in wanted`   # target.detectors 白名單：拿使用者寫的字串比，不是寫死
aiops/engine/pipeline.py:221,229,231  getattr(op, "include_entities"/"exclude_entities")
aiops/engine/registry.py:38   names(prefix) 的 startswith（列表用）
```

全專案搜 `"detector.xxx"` 字串常數（排除 register 裝飾器）只有：
- `control_plane/learning/learning.py:95-109`：`admin.rule_promote` 寫死只支援 `detector.zscore`／`detector.mad` 的基準線換算成上下限（控制面、不是引擎；新增演算法時規則升級不會自動支援——屬於設計限制，列為觀察）
- `stages/rca/algorithms/evidence.py:67`：檢查 `fallback` 參數必須是 `rca.root_cause.*`（參數驗證）

**缺口（測試防線）**：`test_core_and_engine_do_not_import_concrete_operators` 只看 import；在 engine 放 `if op.operator_name == 'detector.mad'` 或 `startswith('alert.')` 的突變實驗是**綠**的。沒有寫 xfail（現在的程式是乾淨的，沒有「現在會失敗」的行為可以寫），建議加一條 AST 檢查：`aiops/engine/**` 與 `interfaces/runner.py` 不准出現符合 `^(detector|alert|notifier|rca|ingestion|enrichment|admin)\.` 的字串常數。約 10 行。

### Q3 循環引用與可變狀態

**結論：沒有模組層級的循環引用；只有 `storage.db` ↔ `storage.models` 一個函式內延遲 import 形成的環（`db.create_all()` 裡 `from aiops.storage import models`），是刻意的、不影響載入順序。執行期也沒有被修改的模組／類別層級狀態。**

**證據等級：【實驗】自寫 Tarjan SCC ＋【測試】（部分，有盲點）。**

```
[module-level] modules=107 edges=266 SCC>1: 0
[including function-level] modules=107 edges=269 SCC>1: 1
    ['aiops.storage.db', 'aiops.storage.models']
```

算子實例的跨輪殘留：我另外 AST 掃所有類別方法（`__init__` 以外）對 `self.x` 的寫入：
- 偵測器的 `fit`／`load_state` 寫 `self.mean_` 等——但 `DetectorOperator.execute` 每次都 `algo = type(self)(**params)` 新建實例（`stages/detectors/base.py:121`），寫的是 `algo` 不是節點實例
- `StorageHooks._pool_key/_pool`：`load_batch` 開頭每輪清掉（`storage/hooks.py:63`）
- `OpenIncident.remember`、`PipelineContext.log`：掛在每輪新建的 ctx 上
- CLI loop 每輪 `run_context()` → `build_pipeline()` 重建 Pipeline 與算子（`interfaces/cli/__main__.py:27`、`interfaces/runner.py:70-72`）
- 全專案沒有 `lru_cache`／`functools.cache`

**`test_no_module_or_class_level_state_is_mutated_at_runtime` 的涵蓋範圍**（`tests/test_architecture.py:212`）：模組或類別 body 裡用 `{}`／`[]`／`dict()`／`Counter()`… 宣告的名稱，在任一函式裡被「直接」`append/update/…` 或下標賦值；以及 `global` 敘述。突變實驗：

| 寫法 | 結果 |
|---|---|
| `_SEEN = {}` / `_SEEN[k] = 1` | 紅 ✅ |
| 別名：`s = _SEEN; s.append(k)` | **綠（沒抓到）** |
| 類別層級整數計數器：`type(self).runs += 1` | **綠** |
| `@functools.lru_cache` 函式（跨輪快取、無限長大） | **綠** |
| 模組層級物件的屬性賦值：`_STATE.n = …` | **綠** |
| 跨模組改別人的模組屬性：`topology.CACHE = {}` | **綠** |

**缺口**：上面 5 個盲點（目前程式沒有這樣寫）。另外它完全不看「節點實例的 self 屬性在 execute 裡被改」——目前靠「每輪重建 Pipeline」保證，但 `Pipeline` 物件若被常駐重用（API 未來快取 Pipeline）就會漏。
- 測試：`test_mutable_state_guard_catches_indirect_mutation[alias|class-counter|lru_cache|attr-assign|cross-module]`（xfail）
- 建議修法：(1) 追蹤區域別名（`x = <模組層級名>`）；(2) 類別屬性的 `AugAssign`／`Assign` 透過 `type(self).x`、`cls.x`、`<類別名>.x`；(3) 禁止 `functools.lru_cache/cache`（或列白名單）；(4) 模組層級名稱的 `Attribute` 賦值；(5) `<import 的模組>.<名稱> = …`。另外加一條「`BaseOperator.execute` 裡不准寫 `self.*`」。約 40 行，只改測試。

---

## 二、資料契約與血緣

### Q4 `AnomalyEvent` 的 detector／detector_node 是否在偵測當下固化？

**結論：是。`AnomalyEvent` 是 `@dataclass(frozen=True)`（`engine/types.py:28`），全專案只有 3 個地方建立它，建立後沒有任何地方改寫 detector／detector_node；落地與快照都原樣帶出。**

**證據等級：【實驗】＋【測試】。**

建立點（`grep "AnomalyEvent("`，加上 `AnomalyEvent.from_dict`）：
1. `stages/detectors/base.py:160`：偵測當下帶 `self.operator_name, key`（key = 節點名稱）
2. `stages/ensemble/ensemble.py:94`：裁決事件是**新建**的，`detector=detector.ensemble`、`detector_node=<裁決節點>`、`voted_by=<投票節點>`；成員事件預設拿掉（`keep_members=true` 才留）
3. `storage/hooks.py:81`：外部匯入的異常，`detector=f"external:{row['source']}"`、`detector_node=""`

全專案 grep `dataclasses.replace|__setattr__|\.detector *=|\.detector_node *=|\.attrs\[…\] =`：沒有任何寫入。`alert.absorb_open` 合併事件時以 `(entity, metric, ts, detector_node)` 為 key、`setdefault` 保留舊的（`stages/grouping/absorb.py:23,47`），不改寫。

實驗（zscore＋mad＋ensemble union＋外部匯入一筆 FDC 異常，StorageHooks、記憶體 SQLite）：

```
Q4a frozen：set detector -> FrozenInstanceError: cannot assign to field 'detector'
           但 e.attrs["dept"] = "後來塞的" 成功 -> {'dept': '後來塞的'}
Q4b anomalies 表：[{entity: svc, detector_name: detector.ensemble, detector_node: vote, level: anomaly}]
    事件群快照：
      tool-9: [("external:fdc", "", [])]
      svc:    [("detector.ensemble", "vote", ["m", "z"])]
    anomalies 表欄位：id, pipeline, entity, metric, ts, value, score, detector_name, detector_node, level, attrs   ← 沒有 voted_by
```

外部匯入的異常**不寫** anomalies 表（`persist_batch` 只寫 `ctx.detected_anomalies`，`storage/hooks.py:205`），只存在 external_anomalies 表與事件群快照——符合 `context.py:262` 的設計。

自動化測試：`test_ensemble_in_a_pipeline_records_the_decision_with_lineage`（tests/test_algorithms.py:281）。突變結果見文末「突變測試」。

**缺口**：
1. **anomalies 表沒有 `voted_by`**：裁決事件落地後只知道是 `vote` 節點判的，不知道哪些節點投票；`keep_members=false`（預設）時成員事件也不寫。之後只能從事件群快照反查。影響：調參成效、誤報歸因（是哪個演算法造成）只能看快照。建議：`AnomalyRow` 加 `voted_by JSON`（migrations/postgres 補一支 SQL）、`persist_batch` 寫入。約 10 行＋1 支 migration。測試：`test_anomalies_table_keeps_who_voted`（xfail）
2. **淺層凍結**：`attrs` 是普通 dict，下游可以原地改（目前 grep 確認沒人改）。建議建立時包 `types.MappingProxyType`（`to_dict` 轉回 dict），或至少加架構測試禁止 `.attrs[...] =`。約 5 行。測試：`test_anomaly_event_attrs_cannot_be_mutated_in_place`（xfail）

### Q5 `detector.ensemble` 的分母

**結論：分母 = `ctx.detector_runs` 裡、排在它前面的節點（`stages/ensemble/ensemble.py:65-66`）。被白名單／include／exclude 跳過的、冷啟動棄權的、沒資料的節點都不進 `detector_runs`，所以都不算；只剩 1 個參與者時任何策略都是 pass-through（`_needed` 下限 1、上限 voters，`ensemble.py:56-62`）。但「有跑、卻不可能判出任何東西」的節點會被算成反對票。**

**證據等級：【實驗】＋【測試】。**

實驗（3 個節點：`n_skip` 被 target 白名單跳過、`n_cold` 設 `min_train_points: 10000` 冷啟動棄權、`n_ok` 正常；`strategy: intersection`）：

```
audit:
  n_skip:skipped:not-selected
  n_cold:detector.mad
  train:n_cold:too-few-points:120<10000
  n_ok:detector.zscore
  ensemble:vote:intersection:voters=1:need=1:1->1
anomalies: [("detector.ensemble", "vote", ("n_ok",), "anomaly")]      ← 1 個參與者，intersection 也 pass-through
```

沒資料（raw_df 空）：`detector_runs=[]`、ensemble 直接 return（`ensemble.py:70`）；經過引擎時這種序列會被斷流攔截（Q8）。

自動化測試：`test_ensemble_denominator_counts_only_nodes_that_evaluated`、`test_ensemble_in_pipeline_skipped_and_empty_nodes_abstain`、`test_ran_but_found_nothing_is_a_no_vote`（tests/test_target_detectors.py）、`test_abstaining_detector_is_not_counted_by_the_ensemble`（tests/test_stability_guards.py）。突變結果見文末。

**缺口：「不判」卻進 `detector_runs`，投反對票**（違反 CLAUDE.md 第 2 節「新偵測器若資料不足就不判，要在加入 detector_runs 之前 return」）：

```
Q5c zscore + rules(require_rule=false、沒有 upper/lower) + majority
    ensemble:vote:majority:voters=2:need=2:1->0      ← zscore 抓到的 500 尖峰被否決
Q5d zscore + quantile_drift(window=600，這一輪只有 120 點) + majority
    ensemble:vote:majority:voters=2:need=2:1->0      ← 一個分數都算不出來的節點投了反對票
```

- `detector.rules` 沒規則時 `detect` 回全 0（`stages/detectors/rules.py:46-47`），但 `base.execute` 在 `detect` 之前就 `detector_runs.append`（`base.py:148`）。`rules` 搭 `require_rule: false` 是 `admin.rule_promote` 的用法（「有核准規則的實體才判」），真實 workflow 會碰到：還沒升級出規則的實體，這個節點都在否決其他節點。
- `quantile_drift`／`chronic` 的 `rolling(window, min_periods=window)` 在資料比窗口短時全 NaN → `fillna(0)`。
- 影響：majority／intersection 下漏報，而且 audit 看起來像「兩票、一票反對」，很難發現。
- 建議修法：讓演算法能回報「這一輪沒有可評估的點」（例如基底在 `detect` 前呼叫 `can_evaluate(df) -> bool`，或 `AnomalyResult` 帶「哪些點有分數」），全部不可評估時不進 `detector_runs`。約 20～30 行（base＋rules＋quantile_drift＋chronic）。
- 測試：`test_rules_without_a_rule_abstains_instead_of_vetoing`、`test_window_longer_than_the_data_abstains[detector.quantile_drift|detector.chronic]`（xfail）

### Q6 拓撲邊的解析是不是只有一份？

**結論：邊只有一份。`rca.build_dynamic_topology`（`stages/rca/topology_builder.py:16,36`）與 `admin.merge_traces`（`control_plane/operations/ingest.py:12,63`）都呼叫 `engine/topology.service_edges`，全專案沒有第二份「parent_id → 呼叫邊」的邏輯。但「入口 span」在 `stages/rca/spans.py` 有自己的定義，跟 `service_edges` 對孤兒 span 的處理不一致。**

**證據等級：【實驗】＋【測試】（邊）／【實驗】（入口定義，沒有測試）。**

`grep parent_id` 全專案（engine/topology.py 以外）：只有 `spans.py:38,47,72,95-96,108`（self time、子樹失敗、入口、呼叫者、呼叫樹）、`rca/sections/evidence.py:44`、資料源轉換（elasticsearch.py:276、mock.py）。

三種定義：

| 位置 | 定義 |
|---|---|
| `engine/topology.py:130-132` service_edges | `parent_id` 在樣本裡、且父 span 服務 ≠ 自己 → 邊 |
| `rca/spans.py:72` window_stats、`:96` inbound_callers | `parent_id is None` **或** `service_of.get(parent_id) != service` → 入口（父 span 不在樣本裡也算入口，呼叫者記成 `""` = 外部） |
| `rca/sections/evidence.py:44` 最慢 trace | 只有 `parent_id is None` 才是根 |

實驗：
```
spans: api(a1, 根) → cart(b1, 父 a1)；cart(b2, 父 "zz-missing" 不在樣本)；cart(c3, 父 "")
service_edges:   {"api->cart": 1}
inbound_callers: {"api": {"": 1}, "cart": {"api": 1, "": 2}}     ← 兩個孤兒都被當成「外部入口請求」
window_stats requests: {"api": 1, "cart": 3}
```

**缺口**：孤兒 span（撈取時間窗邊界切掉的父 span、抽樣上限、沒裝 APM 的上游）在邊裡被忽略，在入口統計裡被當成外部流量。`rca.root_cause.evidence` 用 inbound_callers 算流量倍數（`algorithms/evidence.py:80`），孤兒在事件期間變多時該服務的流入倍數被灌高；追放大者時又因為 `if c` 濾掉 `""`（`:92`）而斷鏈。影響：根因「流量來源」可能把「上游被切掉」說成「外部流量暴增」。
- 建議修法：`spans.py` 把「父 span 不在樣本裡」記成獨立的呼叫者（例如 `"?"`），不計入外部入口；三處入口定義抽成一個 `is_entry(span, service_of)` 共用函式，跟 `service_edges` 放在一起。約 15 行。
- 測試：`test_orphan_spans_are_not_counted_as_external_entry_requests`（xfail）

---

## 三、執行階段防護

### Q7 路徑安全

**結論：乾淨。三道都在：Pydantic（`engine/requests.py:33,46` 的 `pattern=^[A-Za-z0-9_-]+$`、`max_length=128`）在進 runner 前就回 422；runner 的 `fullmatch` 白名單（`interfaces/runner.py:25,49`）；找到檔案後再擋軟連結（`:60-62`）與 `resolve()` 跑出 pipelines/ 外（`:65-66`）。**

**證據等級：【實驗】打 API ＋【測試】。**

`TestClient` 打 `POST /api/v1/runs`（`targets: []`）與 `POST /api/v1/rca`（`incident_id: 1`）：

| workflow | /runs | /rca |
|---|---|---|
| `../x`、`..\x`、`a/b`、`C:/Windows/win.ini`、`/etc/passwd`、`metrics_only.yaml`、`metrics_only\n`、`metrics_only\x00`、`""` | 422 `string_pattern_mismatch` | 422 |
| `metrics_only` | 200 | 200 |
| `METRICS_ONLY` | 200（Windows 檔案系統不分大小寫，rglob 找到 metrics_only.yaml） | 200 |
| `nope` | 404 `找不到 workflow：nope` | 404 |

（Pydantic v2 用 Rust regex，`$` 不吃結尾換行，`metrics_only\n` 在 422 就擋掉。）

runner 直接呼叫（模擬 API 擋不到時的第二道）：`../x`、`a/b`、`metrics_only.yaml`、`metrics_only\n`、`C:\x` 全是 `BadRequest：不合法的 workflow 名稱`。

符號連結：這台 Windows 沒有建立 symlink 的權限（`WinError 1314`），改用 **junction**（`mklink /J pipelines\audit_dir → 暫存資料夾`，裡面放 `evil2.yaml`）：
```
POST /runs {"workflow": "evil2"} -> 400 「workflow evil2 不在 …\aiops\pipelines 底下，拒絕載入」
is_symlink=False, is_junction=True   ← junction 不是 symlink，is_symlink 檢查擋不到，由 resolve() 那道擋下
```
檔案層級 symlink 在這台機器無法實測；`test_symlink_pointing_outside_is_refused`／`test_symlink_branch_without_needing_symlink_privileges`（tests/test_security_and_tuning.py:52,65）用 monkeypatch 涵蓋。

自動化測試：`test_workflow_names_outside_the_whitelist_are_rejected`、`test_api_rejects_path_like_workflow_names`、`test_symlink_pointing_outside_is_refused`、`test_symlink_branch_without_needing_symlink_privileges`、`test_resolved_path_outside_the_workflow_dir_is_refused`、`test_cli_may_load_its_own_file_but_the_api_path_may_not`（突變結果見文末）。

**擋在哪一層、第幾步（追加實測，`tests/_audit_exp_c.py::exp_q7_q15_fail_fast`）**：包住 `runner.workflow_path`、`runner.load_workflow`、`Pipeline.__init__`、`Pipeline.check_target`、`Pipeline.run`、`StorageHooks.load_batch/load_target` 記錄有沒有被呼叫，SQLAlchemy `before_cursor_execute` 依動詞計 SQL，`httpx.Client.send` 計非 testserver 的對外請求。`POST /api/v1/runs` 的處理順序：

| 步 | 層 | 擋什麼 | 狀態碼 |
|---|---|---|---|
| 1 | FastAPI／Pydantic `RunWorkflowRequest`（`engine/requests.py:33-41`） | workflow 名稱 pattern、長度；API targets 的 `TargetSpec`（`extra=forbid`、`detectors` 至少 1 個、`source_type` 非空） | 422 |
| 2 | `runner.build_pipeline` → `load_all()` | 只 import 算子模組（無 IO） | — |
| 3 | `runner.workflow_path`（`runner.py:37-67`） | 第二道名稱白名單 → rglob 找檔 → 重名 → 軟連結 → `resolve()` 範圍 | 400／404 |
| 4 | `load_workflow`（`pipeline.py:100-104`） | YAML 語法、`Workflow` 模型（含 YAML targets 的 `extra=forbid`、節點名稱重複、ratio 範圍）。**不檢查 workflow 名稱**——路徑已經由第 3 步決定 | 400 |
| 5 | `Pipeline.__init__`（`pipeline.py:152-176`） | 未註冊算子、算子參數錯、scope 順序、YAML targets 的 detectors／overrides、include／exclude 的實體 | 400 |
| 6 | `Pipeline.run` 第一行（`:243-244`） | API targets 的 detectors／overrides | 400 |
| 7 | `load_batch`（第一次碰資料庫） | — | — |

（第 3 步之後的 CLI 路徑相同，只是沒有第 1 步，`allow_file=True` 時第 3 步只檢查檔案存在。）

結果見 Q15 的表：**第 1～6 步擋下的所有案例，SQL 0 句、對外 HTTP 0 次**；正常跑完的對照組是 SELECT 13、INSERT 3。

觀察（不算缺口）：`/rca` 可以帶偵測 workflow 名稱（實驗中 `/rca {"workflow":"metrics_only"}` 真的跑了一輪偵測、送了心跳），`/runs` 也能指定 `admin_*`／`rca_*`。需要同一把 token，不是越權；若要分類可以在 `RcaRequest` 限制 `rca/` 底下的名稱。

### Q8 斷流攔截

**結論：「資料源說沒資料」（NoDataError ⊂ NotFound）與「抓回來是空的」才算斷流；查詢寫錯（BadRequest）、偵測器的 PlatformError／BadRequest 都只記 errors。但判斷依據是「例外型別是 NotFound」而不是「誰丟的」，所以非資料源的 NotFound 也會被算成斷流。**

**證據等級：【實驗】＋【測試】。**

程式：`engine/pipeline.py:271-279`（`except PlatformError` → `isinstance(e, NotFound)` 算斷流；成功但 `raw_df.empty` 也算），`:285-290` 比例判斷。

實驗（mock 資料源 monkeypatch 出各種失敗，每組 2 條 target，`max_failed_target_ratio=1.0`，strict=False）：

```
全部 NoDataError                         -> InfrastructureError：資料斷流：2/2
全部抓回空 df                             -> InfrastructureError：資料斷流：2/2
全部 BadRequest（查詢寫錯）                 -> 成功；errors=[bad:a 查詢寫錯, bad:b 查詢寫錯]
全部值是 NaN                              -> 成功；errors=[]                        ← 見下方觀察
全部偵測器 PlatformError                   -> 成功；errors=[…]
全部偵測器 BadRequest                      -> 成功；errors=[…]
全部偵測器 NotFound                        -> InfrastructureError：資料斷流：2/2      ← 誤判
全部 peer_group 只撈到自己（fetch.py:82）   -> InfrastructureError：資料斷流：2/2      ← 誤判
1 條 NoData + 1 條 BadRequest             -> 成功；errors=[empty:a 沒資料, bad:b 查詢寫錯]
1 條偵測器 ValueError + 1 條正常            -> ValueError：偵測器程式錯誤（整輪失敗）   ← 見缺口 3
```

strict=True（手動動作）：
```
NoData     -> NoDataError(404)              ← 直接丟出，不走斷流判斷
空 df       -> InfrastructureError(503) 資料斷流：1/1
BadRequest -> InvalidQueryError(400)
```

自動化測試：`test_all_targets_without_data_is_starvation_and_no_heartbeat`、`test_some_targets_without_data_is_only_errors_and_heartbeat_goes_out`、`test_starvation_ratio_is_configurable`、`test_bad_queries_are_configuration_errors_not_starvation`、`test_workflow_without_targets_does_not_divide_by_zero`（tests/test_stability_guards.py）。突變結果見文末。

**缺口**：
1. **非資料源的 NotFound 被算成斷流**。`ingestion/fetch.py:82` 的「peer_group 只撈到自己（沒有同儕）」是拓撲狀態（服務縮到 1 個副本），不是資料進不來；偵測器或其他 target 節點丟 NotFound 也一樣。影響：一個 workflow 的服務全部縮成單副本（或 ratio 設低時一部分）→ 整輪 InfrastructureError、watchdog 停止續命、備援通報誤響。建議：斷流只認資料源層的 `NoDataError`（`sources/base.py:27`），或 fetch 把「抓不到」記在 ctx 上，引擎看那個欄位而不是看例外型別；`fetch.py:82` 改丟 BadRequest 或專用例外。約 10 行。測試：`test_peer_group_with_only_itself_is_not_data_starvation`、`test_not_found_from_a_non_fetch_node_is_not_starvation`（xfail）
2. **strict=True 下同一件事回不同狀態碼**：資料源丟 NoDataError → 404；資料源回空 df → 503 資料斷流。建議統一。約 3 行。（沒寫 xfail：哪個才對要你決定）
3. **單條序列的非 PlatformError 讓整輪失敗**：`Pipeline.run` 的 docstring（`pipeline.py:241`）說 strict=False 時「某條序列失敗記在 errors，其他序列照跑」，但只接 `PlatformError`；演算法 bug（ValueError、KeyError、pandas 例外）讓所有序列都沒結果、這一輪不寫入、心跳停。CLAUDE.md 第 4 節「預設任何節點出錯都讓這一輪（或這條序列）失敗」語意含糊。建議：strict=False 時 target 迴圈多接 `Exception`、記成 errors 並 `logger.exception`（InfrastructureError 照舊往上丟），或在 CLAUDE.md 明寫「程式錯誤讓整輪失敗是刻意的」。約 5 行。測試：`test_a_bug_on_one_target_does_not_take_down_the_whole_round`（xfail）

**追加：HTTP 200 但零筆／格式怪（`tests/_audit_exp_c.py::exp_q8_http200_empty`）**。用 `httpx.MockTransport` 當 ES／Prometheus／VictoriaMetrics（monkeypatch 各 spec 的 `from_env` 換成帶 MockTransport client 的實例，走真的 `sources/*` 解析程式），每組 2 條 target、`max_failed_target_ratio=1.0`，workflow 是 fetch → zscore → group → payload → `notifier.alertmanager`（指向本機假 AM）→ `notifier.watchdog`（`AIOPS_NOTIFIER=alertmanager`，也指向假 AM）。

| 資料源 \| 回應（都是 HTTP 200） | 資料源層 | 引擎 | 心跳 | 送到 AM | DB 寫入 |
|---|---|---|---|---|---|
| ES \| `buckets: []` | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| ES \| 每桶 `v.value: null` | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| ES \| 沒有 `aggregations`（回應形狀不對） | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| ES \| 每桶都是 NaN | **回 60 列，全是 NaN** | **成功，不算斷流** | **1** | AIOpsWatchdog | **INSERT 2（基準線 `{'mean': nan, 'std': nan}`）** |
| ES \| 每桶都是 Infinity | **回 60 列，全是 Inf** | **成功，不算斷流** | **1** | AIOpsWatchdog | （另跑一次：存成 `{'mean': inf, 'std': nan}`） |
| ES \| 桶缺 `key` | **KeyError（不是 PlatformError）** | KeyError，整輪失敗 | 0 | 無 | 無 |
| ES \| `value: "abc"` | **ValueError（不是 PlatformError）** | ValueError，整輪失敗 | 0 | 無 | 無 |
| ES \| 對照：正常 60 桶 | 60 列 | 成功 | 1 | AIOpsWatchdog | — |
| Prometheus \| `data.result: []` | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| Prometheus \| 沒有 `data` | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| Prometheus \| values 全是 `"NaN"` | NoDataError（只有 NaN／Inf） | 資料斷流 2/2 | 0 | 無 | 無 |
| Prometheus \| values 全是 `"+Inf"` | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| Prometheus \| 序列沒有 `values` | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| Prometheus \| value `"abc"` | **ValueError（不是 PlatformError）** | ValueError，整輪失敗 | 0 | 無 | 無 |
| Prometheus \| 200 但 `status: error` | InvalidQueryError | 成功；errors 記 a、b（不是斷流） | 1 | AIOpsWatchdog | 無 |
| Prometheus \| 回兩條序列 | InvalidQueryError | 成功；errors 記 a、b | 1 | AIOpsWatchdog | 無 |
| Prometheus \| 對照：正常 60 點 | 60 列 | 成功 | 1 | AIOpsWatchdog | INSERT 2 |
| VictoriaMetrics \| `data.result: []` | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |
| VictoriaMetrics \| values 全是 NaN | NoDataError | 資料斷流 2/2 | 0 | 無 | 無 |

每一列「資料源請求數」都是 2（每條 target 一次；KeyError／ValueError 那幾列是 1——第一條就讓整輪停了）。

結論：**「HTTP 200 但零筆」三種資料源都正確走到 `NoDataError`（`sources/elasticsearch.py:58-59`、`sources/prometheus.py:53-54,75-76`）→ 引擎算成斷流（`engine/pipeline.py:275-276,285-290`）→ after_persist 不執行，watchdog 心跳 0 次、AM 0 則、DB 0 寫入**。【實驗】。但格式怪的回應分成三種下場，而且兩種不一致：
- Prometheus 的 NaN／Inf 被濾掉 → NoDataError → 斷流（合理）；**ES 的 NaN／Inf 沒有濾**（只濾 `None`，`elasticsearch.py:57`）→ 當成有資料，心跳照送，還把 NaN／Inf 基準線寫進資料庫，到下次重訓（預設 1440 分鐘）前那個節點都判不出任何東西。另外 PostgreSQL 的 JSON 欄位不接受 NaN，正式環境上這一步應該會變成寫入失敗（【推論】，只在 SQLite 上實測）。→ 缺口 G17
- 結構不對（ES 桶缺 key、值是字串、Prometheus 值是字串）丟原始 KeyError／ValueError，不是 PlatformError：不會被記成那條序列的錯誤，而是整輪失敗（心跳也不送——結果上「看起來像斷流」，但原因被歸成程式錯誤，跟 G9 同一個洞）。→ 缺口 G18
- ES 回應缺 `aggregations`（回應形狀不對、例如被 proxy 換成別的 JSON）被歸成「沒資料」→ 斷流。這比較像設定／資料源錯誤，但斷流反而會讓人去查，算可以接受（觀察）。

自動化測試：`test_datasources.py` 涵蓋各資料源的正常解析與部分 NoData 情境；**沒有測試把「資料源 200 零筆 → 斷流 → 心跳沒送」整條串起來**（既有的斷流測試都直接 monkeypatch `MockSpec.fetch_data` 丟 NoDataError）。本節的實驗補上了這條鏈的實測。

觀察：mock 資料源 monkeypatch 成全 NaN 的序列（不經過真的資料源解析）不算斷流、也不會有異常——真實資料源裡 Prometheus 會在資料源層攔下，ES 不會（即 G17）。

### Q9 冷啟動門檻

**結論：`min_train_points`（預設 60，`stages/detectors/base.py:55,136,250`）只在「要訓練」時檢查（基準線不存在或 `train_df` 有值＝到期重訓）。10 個繼承 `DetectorOperator` 的偵測器只有 `detector.rules` 豁免；`new_only`、`ensemble` 不是 DetectorOperator（沒有基準線），不受門檻影響。`detector.rules` 一個點能判。**

**證據等級：【實驗】＋【測試】。**

全部 12 個 `detector.*`（`OperatorRegistry.names("detector.")`，實驗輸出）：

| 算子 | DetectorOperator | has_baseline | requires_training | 學的是什麼 | 60 列門檻合理嗎 |
|---|---|---|---|---|---|
| detector.zscore | ✔ | ✔ | True | 平均、標準差 | 合理 |
| detector.mad | ✔ | ✔ | True | 中位數、MAD | 合理 |
| detector.shift | ✔ | ✔ | True | 一階差分標準差 | 合理 |
| detector.chronic | ✔ | ✔ | True | 平均、標準差（偵測用 window 滾動平均） | 列數合理；但 window 比偵測資料長時永遠算不出分數（Q5 缺口） |
| detector.quantile_drift | ✔ | ✔ | True | 滾動分位數的中位數與 MAD | 訓練比 window 短時退回整段分位數（`quantile_drift.py:43-44`）；同上 |
| detector.hourly_baseline | ✔ | ✔ | True | 168 時段的平均／標準差 | 60 點遠不夠學 168 時段，但它有 min_points／min_weeks 退回全段統計（等於 zscore），可接受 |
| detector.peer | ✔ | ✔ | True | 自己 − 同儕中位數 的偏離分布 | **數的是列數，不是同儕有值的列**（缺口 1） |
| detector.dod | ✔ | ✔ | True | 現在 − 1 天前 的差值分布 | **數的是列數，不是 lag 有值的列**（缺口 1） |
| detector.wow | ✔ | ✔ | True | 現在 − 7 天前 的差值分布 | 同上 |
| detector.rules | ✔ | ✔ | **False** | 不學（人訂上下限） | 豁免合理 |
| detector.new_only | ✘ | ✘ | — | 不學 | 不適用 |
| detector.ensemble | ✘ | ✘ | — | 不學 | 不適用 |

沒有「不需要訓練卻被擋」的；有「需要訓練、實際上被豁免」的（peer／dod／wow 的有效點數不受門檻約束）。

實驗：
```
Q9a rules(upper=80)，1 個點、值 500 -> detector_runs=["detector.rules"], anomalies=1   ✅
Q9b wow：訓練 100 列（過門檻）、只有最後 2 列有 lag_10080
    -> detector_runs=["n"], learned_state={center: 0.5, scale: 2.2239}, points_recorded=100, anomalies=1
       ← 用 2 個點學基準線，還記成 100 點
Q9c 有可用的舊基準線（1440 點、2 天前訓練），到期重訓但訓練窗只抓到 20 點，這一輪有 500 的尖峰
    -> detector_runs=[], anomalies=0, audit=["train:n:too-few-points:20<60"]
       ← 舊基準線也不用，尖峰沒抓到
```

自動化測試：`test_too_few_training_points_means_no_training_no_votes_no_alerts`、`test_min_train_points_is_a_node_param_with_default_60`、`test_static_rules_do_not_need_training_and_still_fire_on_one_point`、`test_abstaining_detector_is_not_counted_by_the_ensemble`（tests/test_stability_guards.py）。突變結果見文末。

**缺口**：
1. **門檻數列數不數有效點**（peer／dod／wow）。影響：上線第一週的 wow、剛擴容的 peer，用幾個點學出的 scale 很小或退回 1.0，誤報或漏報都可能，而且基準線紀錄的 points 是假的。建議：`DetectorOperator` 加 `training_points(train) -> int`（預設 `len(train)`），lag 類回傳 `lag_* notna` 的列數、peer 回傳 `_deviation(train).notna()` 的列數；門檻與 `DetectorState.points` 都用它。約 15 行。測試：`test_cold_start_gate_counts_usable_points_for_lag_detectors`（xfail）
2. **到期重訓資料不足時連舊基準線也不用**（`base.py:133-140`：`train_df is not None` 就走訓練分支，門檻不過就 return）。資料斷一天之後恢復的那幾輪，整個節點不判。CLAUDE.md 寫的理由是「冷啟動」，但這時不是冷啟動。建議：門檻不過時若 `state` 存在就退回 `load_state(state)` 照判、記 `train:…:kept-old-baseline`。約 6 行。測試：`test_retrain_with_too_few_points_keeps_using_the_old_baseline`（xfail）。這條算設計選擇，要你決定。

---

## 四、資料庫交易與副作用隔離

### Q10 `persist_batch` 失敗時保證 0 則外部通知？

**結論：是。所有 `notifier.*` 都是 `scope=after_persist`（`stages/notification/notifiers.py:85,100,127,165`），引擎在 `hooks.persist_batch` 成功之後才跑（`engine/pipeline.py:294-296`）；`persist_batch` 丟任何例外，after_persist 節點都不會執行。batch 層的 `alert.heartbeat`、`alert.notify_payload` 只產生 `ctx.notifications`、不連外。**

**證據等級：【實驗】（假 Alertmanager 計數）＋【測試】。**

實驗：本機起一個會記錄 POST 的 HTTP 伺服器，`notifier.alertmanager` 的 endpoint 跟 `AIOPS_ALERTMANAGER_URL`（watchdog 用）都指向它：

```
Incidents.create OperationalError（每次）     -> InfrastructureError；送到假 AM 的請求數 0；重試等待 [0.5, 1.5]；anomalies/incidents 列數 (0, 0)
Anomalies.create_many OperationalError（每次）-> InfrastructureError；0；[0.5, 1.5]；(0, 0)
Incidents.create IntegrityError（不重試）     -> IntegrityError；0；[]；(0, 0)
Incidents.create RuntimeError（程式錯誤）     -> RuntimeError；0；[]；(0, 0)
對照組：資料庫正常                            -> 2 則請求：aiops_incident、AIOpsWatchdog
```

自動化測試：`test_batch_persist_gives_up_cleanly_when_the_database_stays_down`（含 `sent == []`）、`test_alert_is_delivered_after_the_first_push_fails`（tests/test_stability_guards.py）。突變結果見文末。

缺口：無。（觀察：非 OperationalError 的資料庫錯誤——IntegrityError、PostgreSQL 字串超長的 DataError——原樣往上丟，不是 InfrastructureError，API 回 500 而不是 503；不影響「0 通知」。`detector_node` 是 `String(128)`、`NodeSpec.name` 沒有長度限制，在 PostgreSQL 上超長節點名稱會讓整輪寫入失敗。）

### Q11 交易重試時 ctx 有沒有被第一次嘗試改動？

**結論：沒有。`persist_batch` 的 `write()` 只讀 ctx；`loaded` 在交易外先算好（`storage/hooks.py:195`）、`new_ids` 每次嘗試開頭清空（`:199`）、`ctx.incident_ids` 只在交易成功之後 `update`（`:225`）；`run_in_transaction` 每次重試都是全新 session（`storage/db.py:139-151`）。**

**證據等級：【實驗】（逐欄比對）＋【測試】（部分）。**

實驗：包住 `StorageHooks.persist_batch`，進入前 deepcopy ctx 的相關欄位（detected_anomalies、anomalies、incidents、incident_ids、open_incidents、external_anomaly_ids、notifications、resolved_incident_ids、errors），讓第一次嘗試在「Anomalies 已寫入、Incidents.create 失敗」當下再 deepcopy 一次：

```
Q11a 第 1 輪：Anomalies.create_many 已執行、Incidents.create 第一次 OperationalError
     create 呼叫 2 次；重試等待 [0.5]
     嘗試前 vs 失敗當下 不同的欄位：[]
     嘗試前 vs 完成後 不同的欄位：["incident_ids"]   ← 只有成功後才寫入的 incident_ids
     完成後 incident_ids {"svc": 1}；資料庫 anomalies 1 列、incidents 1 列（沒有寫兩份）
Q11b 第 2 輪：同一實體 6 分鐘後又異常、被吸收進進行中事件群；update_snapshot 第一次失敗
     update 呼叫 2 次；重試等待 [0.5]
     嘗試前 vs 失敗當下 不同的欄位：[]；嘗試前 vs 完成後：[]
     資料庫 anomalies 2 列、incidents 1 列、事件群快照 events 2 筆
```

自動化測試：`test_batch_persist_is_one_transaction_that_retries`（斷言 `len(Incidents.list_open)==1 and len(ctx.incident_ids)==1`）只驗了「重試後沒有寫兩份、incident_ids 正確」；**沒有逐欄比對 ctx、也沒有 update_snapshot 路徑的重試**。突變結果見文末（哪些污染寫法會被抓到、哪些不會）。

缺口：程式無缺口；測試只驗一半。建議把 Q11 實驗收進正式測試（約 40 行）。不算程式缺口，所以沒寫 xfail。

### Q12 結案推送失敗、finalize_batch 寫入失敗

**結論：推送失敗 → 事件群維持 open、下一輪 heartbeat 用同一組標籤補送 endsAt，正確。`finalize_batch` 本身寫入失敗（通知已送出）→ 這一輪丟 InfrastructureError；「已通知」紀錄與結案都沒寫，下一輪重送同一則（firing 或 endsAt），成功時才記。但 watchdog 心跳在 finalize 之前就送出了。**

**證據等級：【實驗】（假 AM）＋【測試】；叢集 Alertmanager 實測【未做】（見下）。**

實驗（假 AM，含 `notifier.watchdog`）：

```
1 初發：finalize 的 Notifications.record 失敗
  -> InfrastructureError：資料庫無法使用（已重試 2 次）
     送出：aiops_incident(inc-3475…, 無 endsAt)、AIOpsWatchdog     ← 心跳續命了，但這一輪是失敗
     事件群 (open, P1)；notifications 0 列
2 正常
  -> 成功；再送一次 aiops_incident(同 uid、同標籤)＋心跳；notifications 1 列（exists_for=False 所以補記）
3 安靜滿 15 分：送 endsAt，但 Incidents.mark_resolved 失敗
  -> InfrastructureError；送出 aiops_incident(endsAt)、AIOpsWatchdog；事件群仍 (open, P1)
4 正常：再送一次 endsAt
  -> 成功；送出 aiops_incident(endsAt)；事件群 (resolved, P1)
5 正常
  -> 只剩心跳
```

自動化測試：`test_resolution_is_resent_when_its_push_fails`、`test_alert_is_delivered_after_the_first_push_fails`。**沒有任何測試涵蓋「finalize_batch 自己寫入失敗」**。

**重複送 endsAt 對 Alertmanager 無害嗎**：【推論】Alertmanager v2 以整組 labels 辨識告警，同 labels 再送一次 endsAt 只更新同一筆告警的結束時間；已 resolved 的告警再送 resolved 不會產生新告警，最多再觸發一次 resolved 通知（看 receiver 的 `send_resolved` 與 group 狀態）。`heartbeat` 送的 endsAt 都帶同一個 `startsAt`（初發時間）。

**叢集實測沒做**：我寫好的腳本（送一則 `alertname=arch_audit_probe, preset=arch_audit, severity=none` 的 firing，再連送 3 次 `endsAt=now`，每次查 `GET /api/v2/alerts?filter=preset="arch_audit"`）執行時被 Claude Code 的自動權限分類器以「修改共享資源」擋下，我沒有繞過。執行前 `GET /api/v2/alerts?filter=preset="arch_audit"` 回 `[]`，所以叢集上沒有留下任何告警。腳本在 `C:\Users\cjy93\AppData\Local\Temp\claude\85c052f7c679c0cf\de234d9e-45a3-4c72-b5fe-2b0a6cabccbc\scratchpad\am_endsat.py`，你同意的話在 worktree 根目錄跑（最後會印 active 的 arch_audit 告警數，應該是 0）：
```
PYTHONPATH=. .venv/Scripts/python C:/Users/cjy93/AppData/Local/Temp/claude/85c052f7c679c0cf/de234d9e-45a3-4c72-b5fe-2b0a6cabccbc/scratchpad/am_endsat.py
```

**缺口**：
1. **watchdog 在 finalize 之前續命**：`notifier.watchdog` 跟其他 notifier 同是 after_persist，`finalize_batch` 在全部 after_persist 之後（`pipeline.py:295-297`）。finalize 失敗的那一輪對外是失敗（InfrastructureError、排程備援通報會響），但 Dead Man's Snitch 已經續命 ttl 分鐘——兩個健康訊號互相矛盾；若 DB 從此寫不進「已通知／結案」，每輪都重送同一則通知、每輪都續命心跳。建議：加一個 `after_finalize` scope 給 watchdog（引擎在 finalize_batch 之後才跑）。約 10 行（引擎 scope 順序＋watchdog 宣告＋CLAUDE.md）。測試：`test_watchdog_does_not_renew_when_finalize_fails`（xfail）
2. 測試只驗一半：finalize_batch 寫入失敗（Notifications.record、mark_resolved）沒有正式測試；建議把上面 5 輪實驗收進 test_stability_guards.py。

---

## 五、設定彈性與參數優先級

### Q13 路由順序與零開銷

**結論：順序是 exclude → include → target detectors（`engine/pipeline.py:226-236`），跟 CLAUDE.md 第 2 節、README:292 一致。被跳過的節點不載基準線、不載調參（`load_target` 只收沒被跳過的節點，`pipeline.py:262`）、不進 `detector_runs`、不存基準線，只在 audit 留 `<節點>:skipped:<原因>`。但「全部節點都被跳過」的序列，fetch 每輪仍抓整段訓練窗。**

**證據等級：【實驗】（呼叫計數）＋【測試】。**

實驗（`both` 同時 `include:[a]`、`exclude:[a]`；`inc_b` 只 include b；`sel` 無路由。target a 選 `[both, sel]`、target b 選 `[both]`；計數 `DetectorModels.get`、`ActiveConfigs.candidates`、資料源 `fetch_data`）：

```
round 1: DetectorModels.get=["sel"]  ActiveConfigs.candidates=["detector.shift"]  fetch_data=4
         skip_audit: both:skipped:excluded（a：exclude 先於 include）、inc_b:skipped:not-included、
                     both:skipped:not-included（b：include 先於白名單）、inc_b:skipped:not-selected、sel:skipped:not-selected
round 2: DetectorModels.get=["sel"]  ActiveConfigs.candidates=["detector.shift"]  fetch_data=3
存下的基準線 (entity, node)：[("a", "sel")]
Q13b 偵測節點全被跳過的序列 c，每輪 fetch_data 次數：[2, 2, 2]
```

round 2 的 3 次 = a 的偵測窗 1 次（a 已有基準線、不重訓）＋ b 的偵測窗與訓練窗 2 次。b 的所有偵測節點都被跳過，卻每輪抓 1440 點的訓練窗。

自動化測試：`test_exclude_beats_include_and_whitelist_and_include_limits_entities`、`test_whitelist_by_node_name_runs_only_that_node_of_the_same_operator`、`test_whitelist_by_operator_name_selects_all_nodes_of_that_operator`、`test_routing_params_never_reach_the_algorithm_or_tuning`、`test_ensemble_in_pipeline_skipped_and_empty_nodes_abstain`（tests/test_target_detectors.py）。突變結果見文末。

**缺口**：`ingestion.fetch._needs_training`（`stages/ingestion/fetch.py:125-131`）在 `ctx.detector_states` 為空時一律回 True；它不知道這條序列有沒有任何「要訓練的」節點。影響：用 exclude 止血、或白名單把某些序列只留給不存基準線的節點時，每輪多一次大查詢（ES／Prometheus 負擔，跟「零開銷」的說法不符）。建議：引擎在 `load_target` 時把「這條序列要跑的有基準線節點」放進 ctx（例如 `ctx.active_detector_nodes`），fetch 只在它非空時考慮訓練窗。約 8 行。測試：`test_target_whose_detectors_are_all_skipped_does_not_fetch_training_data`（xfail）

### Q14 參數優先序是不是逐鍵 deep_merge？

**結論：偵測時是。`DetectorOperator.effective_params`（`stages/detectors/base.py:198-218`）= `deep_merge(deep_merge(核准調參, 節點 YAML), target overrides)`，`engine/params.deep_merge` 遞迴合併、不改兩邊輸入。巢狀字典實測沒有被淺層覆蓋。但回測不是走這條，而且核准調參沒有節點這一層。**

**證據等級：【實驗】（巢狀）＋【測試】（只有平的參數）。**

內建偵測器的參數都是平的（`quantile_drift` 也只有 threshold／window／quantile），所以我註冊了一個帶巢狀參數 `bands` 的測試偵測器：

```
核准調參 ctx.detector_params["n"] = {"bands": {"hi": 1, "lo": 2, "nested": {"z": 9}}, "threshold": 4.0}
節點 YAML params              = {"bands": {"hi": 5, "nested": {"x": 1}}}
target overrides["n"]         = {"bands": {"mid": 3, "nested": {"y": 2}}}
effective = {"bands": {"hi": 5, "lo": 2, "nested": {"z": 9, "x": 1, "y": 2}, "mid": 3}, "threshold": 4.0}
audit: params:n:yaml-locked:bands、params:n:target-override:bands
```

兄弟鍵 `lo`、`z`、`x`、`y` 都保留；`hi` 由 YAML 蓋過調參。（audit 的 `yaml-locked:bands` 以頂層鍵判斷，實際只有 `bands.hi` 被鎖，`bands.lo` 仍吃調參——訊息比實際範圍寬，觀察、不算缺口。）

自動化測試：`test_override_precedence_default_tuned_yaml_target`、`test_override_changes_what_the_node_flags`（tests/test_target_detectors.py:203,214）——**都是平的參數**，突變成淺層合併時會不會紅見文末。

**缺口**：
1. **回測用的參數跟正式偵測不一樣**：`admin.proposal_backtest` 用 `cls(**current_params)`、`cls(**row["params"])`（`control_plane/learning/tuning.py:124-125`），只有核准調參，沒有疊節點 YAML params（鎖定鍵）與 target overrides。YAML 鎖了 threshold 的管線，回測顯示「提案多抓 N 個」、核准後正式偵測完全不變，只在 audit 留一行 `yaml-locked`。內建 YAML 目前都沒有鎖 tunable 參數，所以是潛在問題。建議：回測依提案的 pipeline 載入 workflow、找到該演算法的節點，用跟 `effective_params` 同一套合併，鎖定的鍵在回測結果標出來；提案只改到被鎖的鍵時回 409。約 30 行。測試：`test_backtest_uses_the_same_effective_params_as_the_pipeline`（xfail）
2. **調參沒有節點層**：`ActiveDetectorConfigRow` 的唯一鍵是 `(detector_name, pipeline, entity)`（`storage/models.py:250`），`load_target` 以算子名查（`storage/hooks.py:131-135`），同一條管線兩個 zscore 節點（短窗、長窗）拿到同一份核准調參。CLAUDE.md 原本寫「基準線、調參、異常血緣都以節點名稱為準」——**這是文件錯誤，已直接改 CLAUDE.md**。要不要讓調參真的分節點是設計決定，沒寫 xfail。

### Q15 target 的 detectors／overrides 寫錯是在載入期報錯嗎？

**結論：是。YAML 的 targets 在 `Pipeline.__init__` 逐一 `check_target`（`engine/pipeline.py:174-176`）；API 傳入的 targets 在 `run()` 第一行檢查（`:243-244`），早於 `PipelineContext` 建立、`load_batch`、任何 fetch。**

**證據等級：【實驗】（兩條路徑）＋【測試】。**

```
Q15a YAML targets -> Pipeline.__init__
  detectors 不存在     -> WorkflowError：…detectors 裡的 ['nope'] 對不到任何偵測節點名稱或算子名稱；可用：detect（detector.zscore）
  overrides 不存在     -> WorkflowError：…overrides 的 'nope' 不是偵測節點名稱
  overrides 參數名錯   -> WorkflowError：…overrides.detect 不合法：detector.zscore 沒有參數 ['thresh']
  overrides 範圍錯     -> WorkflowError：…overrides.detect 不合法：detector.zscore.threshold=999 超出範圍
Q15b API：POST /api/v1/runs {"workflow":"metrics_only","targets":[{正常的 ok}, {錯的 e}]}
  detectors 不存在 -> 400 {"message":"Workflow metrics_only 的 target e：detectors 裡的 ['nope'] …"}
  overrides 不存在 -> 400
  StorageHooks.load_batch 被呼叫次數 0、資料源 fetch 次數 0   ← 第一條正常的 target 也沒被跑
```

自動化測試：`test_bad_overrides_fail_at_load_time`、`test_override_for_a_node_the_target_did_not_select_is_rejected`、`test_unknown_detector_name_fails_at_load_and_lists_available_nodes`、`test_api_targets_get_the_same_checks`、`test_routing_entities_must_exist_in_the_workflow_targets`、`test_misspelled_target_field_and_empty_whitelist_fail`（tests/test_target_detectors.py）。突變結果見文末。

**追加：Fail-Fast 實測（判準：報錯之前有沒有任何資料庫存取或對外請求；`tests/_audit_exp_c.py::exp_q7_q15_fail_fast`）**。各層順序見 Q7 的表。

`POST /api/v1/runs`：

| 案例 | HTTP | 擋在 | 走到的層 | SQL | 對外 HTTP |
|---|---|---|---|---|---|
| `workflow='../x'`、`'x.yaml'` | 422 | 步 1 Pydantic | （無） | 0 | 0 |
| `workflow='nope'` | 404 | 步 3 workflow_path | workflow_path | 0 | 0 |
| YAML 語法錯 | 400 | 步 4 load_workflow | workflow_path → load_workflow | 0 | 0 |
| YAML target 欄位拼錯（`detector:`） | 400 | 步 4 load_workflow（`extra=forbid`） | workflow_path → load_workflow | 0 | 0 |
| YAML 算子未註冊 | 400 | 步 5 `__init__` | … → Pipeline.__init__ | 0 | 0 |
| YAML scope 順序錯 | 400 | 步 5 `__init__` | … → Pipeline.__init__ | 0 | 0 |
| **YAML target 的 detectors 拼錯** | 400 | **步 5 `__init__`（載入期）** | … → Pipeline.__init__ → check_target | 0 | 0 |
| API target 欄位拼錯（`detector:`） | 422 | 步 1 Pydantic | （無） | 0 | 0 |
| API target `detectors: []` | 422 | 步 1 Pydantic（`min_length=1`） | （無） | 0 | 0 |
| **API target detectors 拼錯（第 2 條，第 1 條正常）** | 400 | **步 6 `run()` 第一行** | … → Pipeline.__init__ → Pipeline.run | 0 | 0 |
| API target overrides 範圍錯 | 400 | 步 6 `run()` 第一行 | … → Pipeline.run | 0 | 0 |
| 對照：正常 | 200 | — | … → load_batch → load_target | SELECT 13、INSERT 3 | 0 |

（「走到的層」的 `check_target` 在 API 案例裡也出現，是 `__init__` 先檢查 YAML 的 targets。）

CLI `run_context(<yaml 路徑>, allow_file=True)`：`bad_detectors.yaml` → WorkflowError，走到 workflow_path → load_workflow → Pipeline.__init__ → check_target，SQL 0；`bad_operator.yaml` → WorkflowError，停在 Pipeline.__init__，SQL 0。

**結論：設定類錯誤全部在碰資料庫之前擋下，0 句 SQL、0 次對外請求。** 拼錯的 detectors：YAML 的在 `Pipeline.__init__`（載入期），API 的在 `run()` 第一行（因為 API targets 是執行時才傳進來）；兩者都早於 `load_batch`。沒有任何入口會先連資料庫、先載基準線才報這類錯。【實驗】

`POST /api/v1/rca` 帶不存在的 `incident_id: 999999`：**404「沒有可以撈 trace 的事件群」**，走到 load_batch，**7 句 SELECT、0 寫入、0 對外請求**。`load_batch` 查不到事件群時默默略過（`storage/hooks.py:86-87` `if row is not None`），直到 `ingestion.incident_traces` 節點才因為 `ctx.incidents` 是空的丟 NotFound（`stages/ingestion/fetch.py:234`）。以「沒有寫入、沒有對外請求」的判準來看沒問題，但它不是在入口擋的，錯誤訊息也沒提到 incident id。→ 缺口 G19（小）

缺口（設定錯誤的部分）：無。（觀察：`_check_entity_routing` 只比對 YAML 的 targets；API 傳入另一組 targets 時，節點 include／exclude 寫了不在這組裡的實體不會報錯——`pipeline.py:214-217` 註明是刻意的。）

---

## 突變測試：既有測試真的會在被破壞時變紅嗎？

做法：對生產程式做單點突變（一次一個，跑完立刻還原；跑完 `git diff` 確認生產檔沒有殘留），跑 11 個相關測試檔（test_target_detectors、test_stability_guards、test_security_and_tuning、test_algorithms、test_dynamic_topology、test_incident_lifecycle、test_persistence、test_resilience、test_core、test_pipeline_e2e、test_api，共 209 項）。腳本：scratchpad `mutants.py`。

| 題 | 突變 | 變紅的測試 |
|---|---|---|
| Q15 | API targets 不在 `run()` 開頭檢查 | `test_api_targets_get_the_same_checks` |
| Q15 | YAML targets 不在 `__init__` 檢查 | `test_bad_overrides_fail_at_load_time`×4、`test_override_for_a_node_the_target_did_not_select_is_rejected`、`test_unknown_detector_name_fails_at_load_and_lists_available_nodes` |
| Q13 | 被跳過的節點照樣載入基準線與調參 | `test_whitelist_by_node_name_runs_only_that_node_of_the_same_operator` |
| Q13 | include 檢查移到 exclude 前面 | **沒有變紅**——但這是等價突變：三個條件都是「不過就跳過」，順序只影響 audit 的原因字串，不影響哪些節點會跑 |
| Q5／Q9 | 冷啟動棄權也進 detector_runs | `test_abstaining_detector_is_not_counted_by_the_ensemble`、`test_too_few_training_points_means_no_training_no_votes_no_alerts` |
| Q5 | 沒資料也進 detector_runs | `test_ensemble_in_pipeline_skipped_and_empty_nodes_abstain` |
| Q5 | ensemble 分母不看 detector_runs | `test_ensemble_denominator_counts_only_nodes_that_evaluated` |
| Q9 | 拿掉冷啟動門檻 | `test_abstaining_detector_is_not_counted_by_the_ensemble`、`test_too_few_training_points_means_no_training_no_votes_no_alerts` |
| Q9 | rules 改成需要訓練 | `test_static_rules_do_not_need_training_and_still_fire_on_one_point`、`test_abstaining_detector_is_not_counted_by_the_ensemble` |
| Q8 | BadRequest 也算斷流 | `test_bad_queries_are_configuration_errors_not_starvation`、`test_without_a_mapping_the_short_query_is_a_target_error_and_nothing_is_sent` 等 4 項 |
| Q8 | **空 df 不算斷流** | **沒有變紅** ← 「抓回來是空的」這條斷流路徑沒有測試（既有測試都用 NoDataError） |
| Q10 | 先送通知再寫資料庫 | `test_alert_is_delivered_after_the_first_push_fails`、`test_batch_persist_gives_up_cleanly_when_the_database_stays_down`、`test_notify_failure_fails_the_run_keeps_the_incident_open_and_records_no_notification` |
| Q12 | finalize 移到送通知之前 | `test_resolution_is_resent_when_its_push_fails`、`test_alert_is_delivered_after_the_first_push_fails`、`test_notify_failure_fails_the_run_…` |
| Q11 | 交易裡就把 id 寫進 `ctx.incident_ids` | **沒有變紅** |
| Q11 | `loaded` 改成每次嘗試從 `ctx.incident_ids` 重算 | **沒有變紅** |
| Q11 | 拿掉 `new_ids.clear()` | **沒有變紅** |
| Q11 | 交易內把 `detected_anomalies` pop 空 | `test_batch_persist_is_one_transaction_that_retries` |
| Q4 | anomalies 表的 detector_node 寫空字串 | `test_ensemble_in_a_pipeline_records_the_decision_with_lineage`、`test_each_anomaly_is_stored_with_its_own_detector_when_algorithms_differ`、`test_same_algorithm_twice_keeps_separate_baselines_and_lineage` |
| Q4 | 外部異常的 detector 不填 `external:` | `test_external_anomalies_record_which_source_judged_them` |
| Q14 | **effective_params 改成淺層合併** | **沒有變紅** ← 巢狀參數沒有測試 |
| Q14 | YAML 不鎖定（調參蓋過 YAML） | `test_yaml_keys_are_locked_but_other_keys_take_the_tuned_value`、`test_override_precedence_default_tuned_yaml_target` |
| Q7 | 拿掉 symlink 檢查 | `test_symlink_branch_without_needing_symlink_privileges` |
| Q7 | 拿掉 `resolve()` 範圍檢查 | `test_resolved_path_outside_the_workflow_dir_is_refused` |
| Q7 | API 不做 pattern 驗證（只剩 runner） | `test_api_rejects_path_like_workflow_names`×6 |
| Q6 | service_edges 同服務也算邊 | `test_service_edges_follow_parent_ids_across_services_only` 等 5 項 |

沒變紅的 4 個非等價突變說明：
- **Q8 空 df**：測試缺口。建議在 test_stability_guards.py 加「資料源回空 DataFrame」的斷流案例（約 8 行）。
- **Q11 三個交易內污染寫法**：既有測試的失敗點在第一次 `Incidents.create`，那時還沒有任何 id 寫進 ctx，所以單獨的污染寫法不會改變結果；我的實驗 Q11a 也是同一個失敗點，同樣抓不到。【推論】「交易裡就寫 `ctx.incident_ids`」＋「`loaded` 每次重算」兩個一起出現時，失敗點若在 `Incidents.create` **之後**（例如 `ExternalAnomalies.mark_processed`），重試會跳過建立事件群，事件群就丟了。建議補一個失敗點在 `mark_processed` 的重試測試，逐欄比對 ctx（約 30 行）。現在的程式沒有這個 bug，所以不算程式缺口。
- **Q14 淺層合併**：測試缺口。建議把 Q14 實驗（巢狀 `bands`）收進 test_target_detectors.py（約 20 行）。

---

## 缺口清單（給你決定修不修）

| # | 題 | 缺口 | 位置 | 影響 | 建議修法／改動量 | xfail 測試（tests/test_arch_audit_gaps.py） |
|---|---|---|---|---|---|---|
| G1 | Q1 | import 防線不看 importlib／http.client；enrichment/ 可 import sources | tests/test_architecture.py:46-59 | 未來違規不會紅 | 掃 import_module 字串、補前綴、拆豁免；~15 行（只改測試） | `test_import_guards_catch_indirect_imports`×3 |
| G2 | Q2 | 引擎寫死算子名稱沒有測試防線 | tests/test_architecture.py | 未來違規不會紅 | AST 掃 engine／runner 的算子名稱字串；~10 行 | （無：現在程式乾淨，寫不出「現在會失敗」的行為） |
| G3 | Q3 | 可變狀態防線 5 個盲點 | tests/test_architecture.py:212-237 | 常駐進程跨輪殘留不會被抓 | 追別名、類別屬性賦值、lru_cache、屬性賦值、跨模組；~40 行（只改測試） | `test_mutable_state_guard_catches_indirect_mutation`×5 |
| G4 | Q4 | anomalies 表沒有 voted_by | storage/models.py:82-97、storage/hooks.py:201-206 | 裁決的投票者只剩事件群快照 | 加欄位＋migration；~10 行＋1 SQL | `test_anomalies_table_keeps_who_voted` |
| G5 | Q4 | AnomalyEvent.attrs 淺層凍結 | engine/types.py:37 | 下游可改血緣附帶資料 | MappingProxyType；~5 行 | `test_anomaly_event_attrs_cannot_be_mutated_in_place` |
| G6 | Q5 | 不判的節點（rules 無規則、窗口比資料長）投反對票 | stages/detectors/base.py:148、rules.py:46、quantile_drift.py:36、chronic.py:42 | majority／intersection 漏報 | 「可評估」訊號，不可評估就不進 detector_runs；~25 行 | `test_rules_without_a_rule_abstains_instead_of_vetoing`、`test_window_longer_than_the_data_abstains`×2 |
| G7 | Q6 | 孤兒 span 被算成外部入口 | stages/rca/spans.py:72,95-97、rca/sections/evidence.py:44 | 流量倍數灌高、放大者追溯斷鏈 | 共用 is_entry、孤兒另記；~15 行 | `test_orphan_spans_are_not_counted_as_external_entry_requests` |
| G8 | Q8 | 非資料源的 NotFound 算成斷流（peer_group 只有自己、節點丟 NotFound） | engine/pipeline.py:275-276、stages/ingestion/fetch.py:82 | 單副本服務 → 整輪失敗、心跳停、備援誤響 | 只認 NoDataError 或 fetch 狀態旗標；~10 行 | `test_peer_group_with_only_itself_is_not_data_starvation`、`test_not_found_from_a_non_fetch_node_is_not_starvation` |
| G9 | Q8 | 單條序列的 ValueError 讓整輪失敗 | engine/pipeline.py:260-277 | 一個演算法 bug 讓所有序列沒結果 | strict=False 多接 Exception（或改文件）；~5 行 | `test_a_bug_on_one_target_does_not_take_down_the_whole_round` |
| G10 | Q8 | strict=True：NoData 回 404、空 df 回 503 | engine/pipeline.py:272-290 | 同一件事兩種錯誤 | 統一；~3 行（要你決定方向） | （無） |
| G11 | Q9 | 冷啟動門檻數列數不數有效點（peer／dod／wow） | stages/detectors/base.py:136 | 2 個點學基準線、points 紀錄是假的 | `training_points()`；~15 行 | `test_cold_start_gate_counts_usable_points_for_lag_detectors` |
| G12 | Q9 | 到期重訓資料不足時不用舊基準線 | stages/detectors/base.py:133-140 | 資料恢復初期偵測盲區 | 退回舊 state；~6 行（設計選擇） | `test_retrain_with_too_few_points_keeps_using_the_old_baseline` |
| G13 | Q12 | watchdog 在 finalize_batch 之前續命 | engine/pipeline.py:295-297、notifiers.py:127 | 失敗的一輪仍續命心跳 | after_finalize scope；~10 行 | `test_watchdog_does_not_renew_when_finalize_fails` |
| G14 | Q13 | 全部節點被跳過的序列每輪抓訓練窗 | stages/ingestion/fetch.py:125-131 | 多一次 1440 點查詢／輪 | ctx 帶要訓練的節點清單；~8 行 | `test_target_whose_detectors_are_all_skipped_does_not_fetch_training_data` |
| G15 | Q14 | 回測不套 YAML 鎖定鍵與 overrides | control_plane/learning/tuning.py:124-125 | 回測結果與正式偵測不符 | 回測走 effective_params；~30 行 | `test_backtest_uses_the_same_effective_params_as_the_pipeline` |
| G16 | Q14 | 調參沒有節點層 | storage/models.py:250、storage/hooks.py:131 | 同演算法多節點共用調參 | 文件已改；要不要分節點是設計決定 | （無） |
| G17 | Q8 | ES 回 200、全是 NaN／Inf 時當成有資料；NaN／Inf 基準線寫進資料庫 | sources/elasticsearch.py:57 | 不算斷流、心跳照送；節點到重訓前都判不出東西；PostgreSQL JSON 不收 NaN（推論） | 比照 Prometheus 用 `math.isfinite` 濾掉，全部不能用就 NoDataError；~3 行 | `test_elasticsearch_all_nan_or_inf_is_no_data`×2 |
| G18 | Q8 | 資料源回 200 但結構不對時丟原始 KeyError／ValueError | sources/elasticsearch.py:56、sources/query/elasticsearch.py:102、sources/prometheus.py:59 | 不記成那條序列的錯誤，整輪失敗 | 解析包 try，轉成 InvalidQueryError；~8 行 | `test_malformed_source_response_is_a_platform_error`×3 |
| G19 | Q15 | RCA 帶不存在的 incident_id，入口不擋，走到 incident_traces 才報不相干的訊息 | storage/hooks.py:86-87、stages/ingestion/fetch.py:234 | 7 句 SELECT 後 404，訊息沒提 incident id（沒有寫入、沒有對外請求） | load_batch 查不到就丟 NotFound（帶 id）；~2 行 | `test_rca_for_a_missing_incident_says_which_incident` |

測試只驗一半（程式本身沒問題，突變實驗證實）：Q8（「空 df」斷流路徑沒有測試）、Q11（失敗點在建立事件群之後的重試、逐欄比對 ctx）、Q12（finalize_batch 自己寫入失敗）、Q14（巢狀參數：改成淺層合併不會紅）、Q2（沒有防線）。

## 直接修掉的文件錯誤

1. `CLAUDE.md` 第 2 節：「基準線、調參、異常血緣都以 Workflow 節點名稱為準」→ 改成基準線與血緣以節點為準、調參只分 (演算法, 管線, 實體)、同演算法多節點共用（G16）。
2. `aiops/storage/models.py:83` `AnomalyRow` docstring：「由 persist_target 寫入」→ 實際是 `persist_batch` 單一交易寫入；並註明 voted_by 只在快照（G4）。

## 實驗檔

- `tests/_audit_exp_a.py`（Q4、Q5、Q6、Q8、Q9、Q13、Q14、Q15）、`tests/_audit_exp_b.py`（Q7、Q10、Q11、Q12）、`tests/_audit_exp_c.py`（追加：Q8 的 200 零筆、Q7／Q15 的 Fail-Fast）：檔名不以 `test_` 開頭，預設不會被收集；`.venv/Scripts/python -m pytest tests/_audit_exp_a.py -s -q` 重跑會印出本文的輸出。
- AST 腳本（import 圖、循環、self 寫入）、架構測試突變、生產程式突變腳本：scratchpad 的 `imports.py`、`selfmut.py`、`mutate.py`、`mutants.py`。
