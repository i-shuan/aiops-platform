"""以服務拓撲（DAG）與故障劇本因果驅動的合成 APM 資料產生器（metrics、traces、error logs 出自同一份系統狀態）。

四層：topology（呼叫 DAG）→ faults（劇本：根因、機制、lag）→ waveforms（系統狀態）＋ metric_emitter／trace_emitter（發射器）
→ generate（本機 NDJSON；載入 ES 的 es_writer 已刪除）。不在 aiops/ 套件裡，平台程式不 import 它。
"""
