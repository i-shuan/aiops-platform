-- 資料健康度：三張新表，都放在平台現有的同一個 PostgreSQL（不另開資料庫）。
--   data_stream_health      每條資料流（管線 × 實體 × 查詢）的狀態區間：狀態沒變只把 last_checked_at 往後推，變了才開新列
--   pipeline_runs           管線每一輪的執行結果：一輪一列（成功／有序列出錯／失敗，失敗類型，訊息）
--   data_stream_first_seen  每條資料流第一次收到資料的時間：用來分「從未有過資料」與「有過之後斷流」
-- 必須先執行 0001～0010。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表（含索引），跑這個檔什麼都不會變。
--
-- 為什麼：資料斷流、查詢寫錯、整輪失敗以前只留在當輪的輸出（errors）與 log，事後無法證明「那個時間點沒有資料進來」，
--   也分不出「從未有過資料」與「有過之後斷流」。狀態的定義在 aiops/engine/health.py；寫入者只有 StorageHooks.record_health。
--   三張都是新表，不動任何既有資料表；舊版程式不認得它們，也不受影響（可以先跑這個檔再升級程式）。
--
-- 索引（Grafana 每次開儀表板或自動重新整理都會下 SQL，每個查詢都限制時間範圍，靠這些索引不掃全表）：
--   data_stream_health
--     ix_data_stream_health_stream        (pipeline, entity, query, started_at)  某條資料流的歷史；寫入時找它目前的那一列
--     ix_data_stream_health_current       (is_current, status)                   目前有問題的資料流
--     ix_data_stream_health_last_checked  (last_checked_at)                      時間範圍（跟範圍有重疊的區間）、保留期限的清除
--   pipeline_runs
--     uq_pipeline_runs_pipeline_run_at    UNIQUE (pipeline, run_at)              某條管線的時間範圍；同一輪重跑覆蓋
--     ix_pipeline_runs_run_at             (run_at)                               不分管線的時間範圍、保留期限的清除
--   data_stream_first_seen
--     uq_data_stream_first_seen_stream    UNIQUE (pipeline, entity, query)       每輪每條資料流查一次「以前有沒有過資料」
--
-- 保留期限：平台每一輪寫入時順手清掉「這條管線」超過保留期限的舊紀錄（管線 YAML 的 health_retention_days，預設 180 天）：
--   data_stream_health 清 is_current = false 而且 last_checked_at 太舊的列（每條資料流目前的那一列不刪）；
--   pipeline_runs 清 run_at 太舊的列；data_stream_first_seen 不清（一條資料流一列）。
--   已經不再跑的管線沒有人替它清，要手動：
--     DELETE FROM data_stream_health WHERE pipeline = '<管線>';
--     DELETE FROM pipeline_runs WHERE pipeline = '<管線>';
--     DELETE FROM data_stream_first_seen WHERE pipeline = '<管線>';
--
-- Grafana 的「資料健康度」儀表板（deploy/helm/aiops-grafana）用唯讀帳號讀前兩張表。建帳號（由 DBA 執行一次；
--   密碼只放進 Grafana 的 Secret（grafana-env 的 AIOPS_DB_RO_PASSWORD），不進 chart、不進 repo）：
--     CREATE ROLE aiops_grafana_ro LOGIN PASSWORD '<自己產生的密碼>';
--     GRANT CONNECT ON DATABASE aiops TO aiops_grafana_ro;
--     GRANT USAGE ON SCHEMA public TO aiops_grafana_ro;
--     GRANT SELECT ON data_stream_health, pipeline_runs TO aiops_grafana_ro;
--   只給這兩張表的 SELECT：事件群、回饋、調參、data_stream_first_seen 都不開放給 Grafana。
--
-- 回滾（會丟掉全部健康度紀錄；先退回舊版程式）：
--   DROP TABLE IF EXISTS data_stream_health; DROP TABLE IF EXISTS pipeline_runs; DROP TABLE IF EXISTS data_stream_first_seen;
BEGIN;

SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS data_stream_health (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(128) NOT NULL,
    entity VARCHAR(256) NOT NULL,
    query VARCHAR(512) NOT NULL,
    source_type VARCHAR(64) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL,
    message VARCHAR(1000) NOT NULL DEFAULT '',
    started_at TIMESTAMP NOT NULL,
    last_checked_at TIMESTAMP NOT NULL,
    checks INTEGER NOT NULL DEFAULT 1,
    is_current BOOLEAN NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS ix_data_stream_health_stream ON data_stream_health (pipeline, entity, query, started_at);
CREATE INDEX IF NOT EXISTS ix_data_stream_health_current ON data_stream_health (is_current, status);
CREATE INDEX IF NOT EXISTS ix_data_stream_health_last_checked ON data_stream_health (last_checked_at);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(128) NOT NULL,
    run_at TIMESTAMP NOT NULL,
    status VARCHAR(16) NOT NULL,
    failure_type VARCHAR(32) NOT NULL DEFAULT '',
    message VARCHAR(1000) NOT NULL DEFAULT '',
    targets INTEGER NOT NULL DEFAULT 0,
    failed_targets INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT uq_pipeline_runs_pipeline_run_at UNIQUE (pipeline, run_at)
);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_run_at ON pipeline_runs (run_at);

CREATE TABLE IF NOT EXISTS data_stream_first_seen (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(128) NOT NULL,
    entity VARCHAR(256) NOT NULL,
    query VARCHAR(512) NOT NULL,
    first_data_at TIMESTAMP NOT NULL,
    CONSTRAINT uq_data_stream_first_seen_stream UNIQUE (pipeline, entity, query)
);

COMMIT;
