-- 服務拓撲自動沉澱（admin.topology_refresh）：service_edges.last_seen 改成可空，prune 依它移除太久沒出現的邊。
-- 必須先執行 0001～0005。可重複執行（連跑兩次結果相同）。全新資料庫由程式自動建表也已經是可空，跑這個檔什麼都不會變。
--
-- 為什麼要改：舊版程式建的表 last_seen 是 NOT NULL，但從來沒有人用它；新版 admin.topology_refresh 用
--   ServiceEdges.prune(before) 刪掉 last_seen 早於「現在 − prune_days」的邊。舊列的 last_seen 是寫入當時的時間，
--   照樣可以拿來判斷；欄位不存在（極舊的手建表）就補上、舊列留 NULL，prune 把 NULL 當「不知道」不刪。
--
-- 執行順序：新舊程式都能跑在改完的表上（舊程式寫入一定會帶 last_seen），先跑這個檔再換版即可，不用停機。
--
-- 鎖：ADD COLUMN（可空、無預設值）與 DROP NOT NULL 都只改中繼資料，不重寫整張表。lock_timeout 5 秒拿不到鎖就整筆回復，找空檔重跑。
--
-- 回滾（只在要退回舊版程式、而且堅持要 NOT NULL 時才需要；舊版程式在可空欄位上也能正常跑，通常不必回滾）：
--   BEGIN; SET LOCAL lock_timeout = '5s';
--   UPDATE service_edges SET last_seen = (now() AT TIME ZONE 'utc') WHERE last_seen IS NULL;
--   ALTER TABLE service_edges ALTER COLUMN last_seen SET NOT NULL;
--   DROP INDEX IF EXISTS ix_service_edges_last_seen;
--   COMMIT;
BEGIN;

SET LOCAL lock_timeout = '5s';

-- 1. 欄位不存在就補上（舊列留 NULL = 不知道多久沒出現，prune 不刪）。
ALTER TABLE service_edges ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP NULL;

-- 2. 舊版程式建的表是 NOT NULL：改成可空。已經可空時 DROP NOT NULL 不會報錯。
ALTER TABLE service_edges ALTER COLUMN last_seen DROP NOT NULL;

-- 3. prune 依 last_seen 範圍刪除。
CREATE INDEX IF NOT EXISTS ix_service_edges_last_seen ON service_edges (last_seen);

DO $$
DECLARE
    nulls bigint;
    total bigint;
BEGIN
    SELECT count(*) FILTER (WHERE last_seen IS NULL), count(*) INTO nulls, total FROM service_edges;
    RAISE NOTICE 'service_edges：共 % 條邊，last_seen 為 NULL（prune 不會刪）% 條', total, nulls;
END $$;

COMMIT;
