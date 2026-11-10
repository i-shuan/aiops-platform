-- 訊息佇列非同步邊（admin.topology_refresh 沉澱、alert.messaging_link 讀）：新增 service_async_edges。
-- 必須先執行 0001～0006。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表，跑這個檔什麼都不會變。
--
-- 為什麼是新的一張表、不是在 service_edges 加欄位：service_edges 的 (caller, callee) 是同步呼叫，異常傳播方向是
--   callee -> caller；非同步邊剛好相反（生產者出事 -> 消費者晚點出事）。放同一張表的話，換版期間或回滾後的舊程式
--   會把非同步邊讀成同步拓撲，方向顛倒。分開放，舊程式完全看不到它。
--
-- 執行順序：先跑這個檔再換版（新版每一輪偵測開頭都會讀這張表；表不存在會讓整輪失敗）。舊程式不認得這張表，不受影響，不用停機。
--   API 行程啟動時也會自動建表（create_all），但排程 CronJob 的帳號若沒有建表權限就一定要先跑這個檔。
--
-- 鎖：只建新表與索引，不碰既有的表。
--
-- 回滾（退回舊版程式之後才做；新版程式還在跑的時候不能刪）：
--   BEGIN; SET LOCAL lock_timeout = '5s';
--   DROP TABLE IF EXISTS service_async_edges;
--   COMMIT;
BEGIN;

SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS service_async_edges (
    id SERIAL PRIMARY KEY,
    producer VARCHAR(256) NOT NULL,
    consumer VARCHAR(256) NOT NULL,
    messages INTEGER NOT NULL DEFAULT 0,
    lag_ms_p95 DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_seen TIMESTAMP NULL,
    UNIQUE (producer, consumer)
);
CREATE INDEX IF NOT EXISTS ix_service_async_edges_last_seen ON service_async_edges (last_seen);

DO $$
DECLARE
    total bigint;
BEGIN
    SELECT count(*) INTO total FROM service_async_edges;
    RAISE NOTICE 'service_async_edges：共 % 條非同步邊', total;
END $$;

COMMIT;
