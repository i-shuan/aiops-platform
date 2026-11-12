-- 關係圖分環境：service_edges、service_async_edges、lot_paths 加 environment 欄位，唯一鍵改成含 environment。
-- 必須先執行 0001～0008。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表（已含欄位與唯一鍵），跑這個檔什麼都不會變。
--
-- 為什麼：這三張表以前沒有任何範圍欄位，全平台共用一張圖。正式與測試環境有同名服務時（cart 在 prod 呼叫 inventory、
--   在 staging 呼叫 inventory-mock），兩邊的邊會混在一起，收斂與 RCA 拿到另一個環境的呼叫關係。
--   environment = Workflow.environment（'' = 不分環境）；既有的列全部回填 ''，所以沒設 environment 的管線看到的圖跟升級前一模一樣。
--
-- 唯一鍵：
--   service_edges        (caller, callee)      -> uq_service_edges_env_pair       (environment, caller, callee)
--   service_async_edges  (producer, consumer)  -> uq_service_async_edges_env_pair (environment, producer, consumer)
--   lot_paths            (lot_id)              -> uq_lot_paths_env_lot            (environment, lot_id)
--   舊的唯一約束是程式或 0007 建的、沒有固定名稱，所以依「欄位組合」找出來刪，不靠名稱。
--   既有列全是 ''，新鍵不可能撞重複（舊鍵唯一 => 加上同一個 '' 之後也唯一），不用先清資料。
--
-- 執行順序：先停掉排程與 API，跑完這個檔再啟動新版程式。新舊程式都不能跨版跑：
--   新版程式寫入會帶 environment 欄位（欄位不存在就失敗）；舊版程式寫入用 ON CONFLICT (caller, callee)，
--   舊唯一鍵刪掉後 PostgreSQL 會報「沒有符合的唯一約束」。
--
-- 鎖：加「常數預設值」的欄位只改中繼資料（PostgreSQL 11 以上）；建唯一約束要掃整張表（這三張表是邊與 Lot 路徑，
--   通常幾百到幾萬列，毫秒到秒級）。lock_timeout 5 秒：拿不到鎖就整筆回復、什麼都沒改，找空檔重跑。
--
-- 回滾（退回舊版程式之前做；如果已經有非 '' 的環境資料，同一對服務在兩個環境各有一列，回滾前要先決定留哪一份，
--   否則建舊唯一鍵會撞重複——下面的 DELETE 只留 environment = '' 的列）：
--   BEGIN; SET LOCAL lock_timeout = '5s';
--   DELETE FROM service_edges WHERE environment <> '';
--   DELETE FROM service_async_edges WHERE environment <> '';
--   DELETE FROM lot_paths WHERE environment <> '';
--   ALTER TABLE service_edges DROP CONSTRAINT IF EXISTS uq_service_edges_env_pair, ADD UNIQUE (caller, callee), DROP COLUMN IF EXISTS environment;
--   ALTER TABLE service_async_edges DROP CONSTRAINT IF EXISTS uq_service_async_edges_env_pair, ADD UNIQUE (producer, consumer), DROP COLUMN IF EXISTS environment;
--   ALTER TABLE lot_paths DROP CONSTRAINT IF EXISTS uq_lot_paths_env_lot, ADD UNIQUE (lot_id), DROP COLUMN IF EXISTS environment;
--   COMMIT;
BEGIN;

SET LOCAL lock_timeout = '5s';

-- 1. 欄位（舊列回填 ''）。
ALTER TABLE service_edges ADD COLUMN IF NOT EXISTS environment VARCHAR(64) NOT NULL DEFAULT '';
ALTER TABLE service_async_edges ADD COLUMN IF NOT EXISTS environment VARCHAR(64) NOT NULL DEFAULT '';
ALTER TABLE lot_paths ADD COLUMN IF NOT EXISTS environment VARCHAR(64) NOT NULL DEFAULT '';

-- 2. 刪掉舊的唯一約束（依欄位組合找，名稱不固定）。新的約束欄位組合不同，不會被誤刪。
DO $$
DECLARE
    spec record;
    con record;
BEGIN
    FOR spec IN SELECT * FROM (VALUES
        ('service_edges', ARRAY['callee', 'caller']),
        ('service_async_edges', ARRAY['consumer', 'producer']),
        ('lot_paths', ARRAY['lot_id'])) AS t(tbl, cols)
    LOOP
        FOR con IN
            SELECT c.conname
            FROM pg_constraint c
            WHERE c.conrelid = spec.tbl::regclass AND c.contype = 'u'
              AND (SELECT array_agg(a.attname::text ORDER BY a.attname::text)
                   FROM unnest(c.conkey) k JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k) = spec.cols
        LOOP
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', spec.tbl, con.conname);
            RAISE NOTICE '%：刪掉舊唯一約束 %（%）', spec.tbl, con.conname, array_to_string(spec.cols, ', ');
        END LOOP;
    END LOOP;
END $$;

-- 3. 新的唯一約束（已存在就跳過）。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_service_edges_env_pair') THEN
        ALTER TABLE service_edges ADD CONSTRAINT uq_service_edges_env_pair UNIQUE (environment, caller, callee);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_service_async_edges_env_pair') THEN
        ALTER TABLE service_async_edges ADD CONSTRAINT uq_service_async_edges_env_pair UNIQUE (environment, producer, consumer);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_lot_paths_env_lot') THEN
        ALTER TABLE lot_paths ADD CONSTRAINT uq_lot_paths_env_lot UNIQUE (environment, lot_id);
    END IF;
END $$;

DO $$
DECLARE
    e bigint; a bigint; l bigint;
BEGIN
    SELECT count(*) INTO e FROM service_edges;
    SELECT count(*) INTO a FROM service_async_edges;
    SELECT count(*) INTO l FROM lot_paths;
    RAISE NOTICE '關係圖分環境：service_edges % 列、service_async_edges % 列、lot_paths % 列（舊列 environment 皆為 ''''）', e, a, l;
END $$;

COMMIT;
