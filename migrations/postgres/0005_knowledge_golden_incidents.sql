-- 歷史事故經驗庫升級成「黃金事故庫」：判定、嚴重度、事件時窗、擴充資訊。必須先執行 0001～0004。可重複執行。
-- 全新資料庫由程式自動建表（context 會是通用 JSON），要 GIN 索引的話全新資料庫也跑一次這個檔即可（會轉成 JSONB）。
--
-- 為什麼要改：admin.proposal_backtest 的黃金事故防護罩要撈「這條管線、這個實體、確認是真異常的事件群」的時窗重跑偵測；
-- 以前這張表只在寫了處置（resolution）時才寫、也沒有時窗，撈不出來。
--
-- 執行順序：先停掉排程與 API（或確保沒有 worker 在寫），跑完這個檔再啟動新版程式。
-- 新版程式不能跑在沒有這些欄位的資料庫上（寫入會失敗），一定要先跑這個檔、再換新版程式。
--
-- 鎖：PostgreSQL 11 以上，加「常數預設值」的欄位只改中繼資料。context 轉 JSONB 會重寫整張表，
--     這張表是人工確認的事故（幾百到幾千筆），重寫是毫秒級。lock_timeout 5 秒拿不到鎖就整筆回復，找空檔重跑。
--
-- 回滾（只在要退回舊版程式時才需要；舊版程式碰到多出來的欄位也能正常跑）：
--   BEGIN; SET LOCAL lock_timeout = '5s';
--   DROP INDEX IF EXISTS ix_incident_knowledge_context_gin;
--   DROP INDEX IF EXISTS ix_incident_knowledge_pipeline_window;
--   DROP INDEX IF EXISTS ix_incident_knowledge_verdict_window;
--   ALTER TABLE incident_knowledge DROP COLUMN IF EXISTS verdict, DROP COLUMN IF EXISTS severity,
--     DROP COLUMN IF EXISTS window_start, DROP COLUMN IF EXISTS window_end, DROP COLUMN IF EXISTS context,
--     DROP COLUMN IF EXISTS updated_at;
--   COMMIT;
BEGIN;

SET LOCAL lock_timeout = '5s';

-- 1. 新欄位。既有的列都是「確認是真異常、寫了處置」才寫進來的，所以 verdict 預設 true_positive 是正確的回填。
ALTER TABLE incident_knowledge ADD COLUMN IF NOT EXISTS verdict VARCHAR(16) NOT NULL DEFAULT 'true_positive';
ALTER TABLE incident_knowledge ADD COLUMN IF NOT EXISTS severity VARCHAR(8) NOT NULL DEFAULT '';
ALTER TABLE incident_knowledge ADD COLUMN IF NOT EXISTS window_start TIMESTAMP NULL;
ALTER TABLE incident_knowledge ADD COLUMN IF NOT EXISTS window_end TIMESTAMP NULL;
ALTER TABLE incident_knowledge ADD COLUMN IF NOT EXISTS context JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE incident_knowledge ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc');

-- 2. true_positive 不一定有處置了：resolution 允許空字串（RCA 相似案例只拿非空的）。
ALTER TABLE incident_knowledge ALTER COLUMN resolution SET DEFAULT '';

-- 3. 程式自動建表的資料庫 context 是 JSON：轉成 JSONB 才能建 GIN。已經是 JSONB 就什麼都不做。
DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_name = 'incident_knowledge' AND column_name = 'context') = 'json' THEN
        ALTER TABLE incident_knowledge ALTER COLUMN context DROP DEFAULT;
        ALTER TABLE incident_knowledge ALTER COLUMN context TYPE JSONB USING context::jsonb;
        ALTER TABLE incident_knowledge ALTER COLUMN context SET DEFAULT '{}'::jsonb;
    END IF;
END $$;

-- 4. 回填既有列的時窗與嚴重度（從事件群快照拿；事件群已被刪掉的就留空，防護罩會略過它們）。
UPDATE incident_knowledge k
SET window_start = ((i.incident ->> 'start')::timestamptz AT TIME ZONE 'utc'),
    window_end = ((i.incident ->> 'end')::timestamptz AT TIME ZONE 'utc'),
    severity = COALESCE(i.severity, '')
FROM incidents i
WHERE i.uid = k.incident_uid AND k.window_start IS NULL;

-- 5. 索引：防護罩依 (pipeline, window_start) 撈、成效報表依 (verdict, window_start) 撈；context 的 tags／trace id 查詢走 GIN。
CREATE INDEX IF NOT EXISTS ix_incident_knowledge_pipeline_window ON incident_knowledge (pipeline, window_start);
CREATE INDEX IF NOT EXISTS ix_incident_knowledge_verdict_window ON incident_knowledge (verdict, window_start);
CREATE INDEX IF NOT EXISTS ix_incident_knowledge_context_gin ON incident_knowledge USING GIN (context);

COMMIT;
