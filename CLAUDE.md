# CLAUDE.md - aiops-lite 開發規範

## 0. 這個 repo 是什麼

事件驅動的極簡 AIOps：公司 Grafana 的告警經 Webhook 推進來 → 等一段 debounce → 跨系統收斂成事故 → 收集證據交給 LLM → 寫回 Grafana 註記。

原本的 aiops-platform（管線加可插拔算子、自帶偵測器、自己的 Grafana 與 Alertmanager chart）已經移除，要找請看 Git 歷史。
這裡沒有管線引擎、沒有算子註冊、沒有偵測演算法；不要照舊平台的寫法加回來，除非使用者明確要求。

---

## 1. 不可違背的界線

| 界線 | 內容 |
| :--- | :--- |
| **對外只有一把 Grafana Editor Token** | 日誌、Trace、註記全部經 Grafana（資料源代理、Annotations API）。嚴禁直連 Elasticsearch、Tempo、產線資料庫 |
| **什麼算異常由各系統自己的 Grafana 規則決定** | 平台不判斷數值、不放偵測器。告警沒推進來，平台就什麼都不做。指標走勢那包證據只做算術（基準、最大、倍數），不准加「超過多少算異常」的門檻；要抓趨勢就寫成 Grafana 規則（`INPUTS.md`） |
| **輸出即終點** | 產出只有 Grafana 註記（事故卡與 RCA 摘要）。不派工、不重啟、不改設定、不起子行程 |
| **LLM 的結論不准看起來像定論** | 註記第 2 行固定是 `根因分析｜[LLM 推論] …` 或 `根因分析｜[未經分析] …`；可能原因的信心（高／中／低）不是機率，沒有直接證據指向的系統由程式壓成低，不准改成百分比或拿掉這個把關；LLM 失敗、格式不對、點名了證據裡沒有的東西，一律退回原始摘要，不硬猜 |
| **資料源的缺口不在程式裡補** | 沒日誌、欄位對不上、時間戳錯，照實寫進證據的補充說明與文件的「已知限制」。格式差異只能用 `config.yaml` 的欄位對照吸收，不准寫業務特判 |
| **設定只有一份 `config.yaml`** | 唯一的環境變數是 `AIOPS_CONFIG_FILE`。鍵名打錯、型別不對、必填沒填，啟動時直接報錯 |
| **機密不進 Git** | Token、密碼只放 `config.yaml`／`secrets.values.yaml`（已被 `.gitignore` 排除）；Helm 用 Secret 掛載 |
| **單副本** | 一個 aiops-lite 對一個 Grafana，只跑 1 個副本（單一工作執行緒，debounce 狀態在記憶體） |
| **不動公司的 Grafana 本體** | 只在它的介面或 API 上設 Contact Point、通知路由、資料源、儀表板；不改映像檔、不裝外掛 |

---

## 2. 檔案與依賴方向

```
grouping（不依賴任何模組）
   ▲
report、db、grafana
   ▲
rca（用 grafana）
   ▲
main（設定與一輪的流程）
   ▲
server（HTTP 服務與排程，只呼叫 main 的一輪流程）
```

| 檔案 | 職責 |
| :--- | :--- |
| `src/server.py` | 收 Webhook（Bearer 驗證、先落地再回 200）、debounce、單一工作執行緒、`/healthz` |
| `src/main.py` | 讀 `config.yaml`（嚴格檢查）與「一輪」的流程 |
| `src/grafana.py` | 所有對 Grafana 的呼叫：解析 Webhook、ES／Tempo／Prometheus 相容資料源的代理、註記 |
| `src/grouping.py` | 收斂（純函式）：同實體合併、關係邊、併入進行中事故、結案 |
| `src/rca.py` | 收集證據、組 Prompt、呼叫 LLM、幻覺檢查 |
| `src/report.py` | 註記的固定格式（純函式） |
| `src/db.py` | SQLAlchemy Core（SQLite／PostgreSQL），啟動自動建表與補欄位 |

依賴方向不准反過來（例如 `grouping` import `grafana`）。目前沒有自動檢查，靠 code review。

---

## 3. 新東西放哪裡

 * **要調門檻、時間、欄位名稱、標籤名稱？** 不寫程式：改 `config.yaml`（Helm 是 `values.yaml` 或 `extraConfig`）。可調項目與內部常數的分界見 `CONFIG.md`。
 * **要讓某個系統的告警進來？** 不寫程式：在公司 Grafana 的規則貼標籤、設通知路由（`README.md`、`GO_LIVE_CHECKLIST.md`）。
 * **要讓指標走勢成為 RCA 證據？** 不寫程式：`config.yaml` 的 `metrics.queries` 加一條查詢。全部的輸入與最理想的狀態見 `INPUTS.md`。
 * **要改提示詞、或改證據交給 LLM 之前的處理？** 先照 `EVAL.md` 跑一次量出現況，改完再跑。留出的案例變差就不合併；不准為了讓留出的案例過關去調程式。前處理只能歸類與計數，不准貼「暴增」「雜訊」這類要門檻的標籤。
 * **要讓日誌成為 RCA 證據？** 不寫程式：照 `FACTORY_METRICS.md` 的統一欄位規範送進 ES，範例在 `deploy/fluent-bit/`。
 * **要加新的設定項？** `src/main.py` 的 `Settings` 與 `from_dict`（要有範圍檢查），同步更新 `config.example.yaml`、`CONFIG.md`、Helm `values.yaml`。
 * **要加新的證據來源？** 對 Grafana 的呼叫寫在 `src/grafana.py`，收集與整理寫在 `src/rca.py`；撈不到要寫進 `evidence["notes"]`，不准讓整輪失敗。
 * **要加資料庫欄位？** `src/db.py`（啟動時會自動補欄位，不用另外寫遷移）。
 * **要改儀表板？** `dashboards/*.json`，在公司 Grafana 匯入。

---

## 4. 送出修改前的驗證

```bash
python -m pytest tests -q                                             # SQLite
TEST_DATABASE_URL=postgresql+psycopg://… python -m pytest tests -q    # PostgreSQL 再跑一遍
helm lint deploy/helm/aiops-lite                                      # 有動 chart 的時候
python -m eval.run --set tuning && python -m eval.run --set heldout   # 有動提示詞或證據前處理的時候：改之前、改之後各跑一次
```

測試用的 Grafana、Tempo 回應是實際錄下來的（`tests/fixtures/grafana13/`），不要手寫假的回應格式去取代。

---

## 99. 開發行為與驗收鐵律

1. **不准拿環境或安全問題當不驗證的藉口**：開發與驗證用 `.gitignore` 排除的 `config.yaml`。缺資料或連線時，主動給最精簡的一行指令，不要求使用者走繁複流程。
2. **真實元件優先於單元測試**：「測試全過」不等於完成。動到查詢、Webhook、註記、LLM 的改動，要對真的 Grafana／ES／Tempo／LLM 跑過；回報時貼實際的結果（撈到幾筆、漏掉什麼、誤撈什麼、LLM 的回應），不准只報「通過 N 個測試」。
3. **先拿到結果，周邊清理後置**：使用者要求跑驗證時，不主動轉去重構周邊。

---

## 100. 測試誠實性

1. **不准改既有測試的數值或斷言去迎合程式**。行為真的變了要改斷言時，在 commit 訊息寫明原因與語意變更。
2. **程式不准知道自己在被測試**：不准有 `if is_test`、`if "mock" in …` 這類分支。隔離只能靠注入（`transport`、`run_round`、`db_url`）。
3. **狀態不准跨輪殘留**：這是常駐服務。新增旗標、快取、計數器時，要說明它在跨輪執行時怎麼重置；不准用模組層級或類別層級的可變狀態，也不准依賴重啟來重置。要跨重啟保留的（例如 LLM 退避次數）存資料庫。
4. **死區照實寫**：某種資料撈不到、某種情況判不出來，寫進文件的「已知限制」或測試的 docstring，不准換測試資料粉飾。
5. **不准用客製化補丁讓測試過關**：不准針對特定服務名、資料長度、場景寫分支或字面值比較。新增或調整門檻與常數時，寫明來源、驗證的資料、有沒有留出驗證；沒有就寫「未驗證泛化」。
6. **沒驗證的就說沒驗證**：文件的「驗證狀態」要分開寫實測過的與沒測過的；假資料跑出來的結果不准寫成產線結果。
