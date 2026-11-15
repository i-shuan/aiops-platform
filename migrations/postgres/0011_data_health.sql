-- 資料健康度：data_stream_health（每條資料流的狀態區間）、pipeline_run_health（每條管線執行結果的狀態區間）。
-- 必須先執行 0001～0010。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表，跑這個檔什麼都不會變。
--
-- 為什麼：資料斷流、查詢寫錯、整輪失敗以前只留在當輪的輸出（errors）與 log，事後無法證明「那個時間點沒有資料進來」，
--   也分不出「從未有過資料」與「有過之後斷流」。這兩張表存狀態區間：狀態沒變只把 last_checked_at 往後推（不是每輪一列），
--   狀態變了才開新列。狀態的定義在 aiops/engine/health.py；寫入者只有 StorageHooks.record_health。
--   兩張都是新表，不動任何既有資料表；舊版程式不認得它們，也不受影響（可以先跑這個檔再升級程式）。
--
-- Grafana 的「資料健康度」儀表板（deploy/helm/aiops-grafana）用唯讀帳號讀這兩張表。建帳號（由 DBA 執行一次，密碼放 Secret）：
--   CREATE ROLE aiops_grafana_ro LOGIN PASSWORD '<放進 grafana-env Secret 的密碼>';
--   GRANT CONNECT ON DATABASE aiops TO aiops_grafana_ro;
--   GRANT USAGE ON SCHEMA public TO aiops_grafana_ro;
--   GRANT SELECT ON data_stream_health, pipeline_run_health TO aiops_grafana_ro;
--   只給這兩張表的 SELECT：事件群、回饋、調參都不開放給 Grafana。
--
-- 保留期：這兩張表只增不刪（狀態區間，量 = 狀態變化次數，不是輪數）。要清舊資料：
--   DELETE FROM data_stream_health WHERE NOT is_current AND last_checked_at < now() - interval '180 days';
--   DELETE FROM pipeline_run_health WHERE NOT is_current AND last_checked_at < now() - interval '180 days';
--
-- 回滾（會丟掉全部健康度紀錄；先退回舊版程式）：
--   DROP TABLE IF EXISTS data_stream_health; DROP TABLE IF EXISTS pipeline_run_health;
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

CREATE TABLE IF NOT EXISTS pipeline_run_health (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    message VARCHAR(1000) NOT NULL DEFAULT '',
    started_at TIMESTAMP NOT NULL,
    last_checked_at TIMESTAMP NOT NULL,
    runs INTEGER NOT NULL DEFAULT 1,
    targets INTEGER NOT NULL DEFAULT 0,
    failed_targets INTEGER NOT NULL DEFAULT 0,
    is_current BOOLEAN NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_health_pipeline ON pipeline_run_health (pipeline, started_at);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_health_current ON pipeline_run_health (is_current, status);

COMMIT;
