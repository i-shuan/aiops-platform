-- 模型檔欄位、漏報補錄、調參時間、歷史事故經驗庫。必須先執行 0001、0002。可重複執行。
-- 全新資料庫由程式自動建表，不需要這個檔。
--
-- 執行順序：先停掉排程與 API（或確保沒有 worker 在寫），跑完這個檔再啟動新版程式。
BEGIN;

-- 1. 基準線：非 JSON 的模型本體（選用）。格式只允許 npz，空字串 = 沒有。
ALTER TABLE detector_models ADD COLUMN IF NOT EXISTS artifact_format VARCHAR(16) NOT NULL DEFAULT '';
ALTER TABLE detector_models ADD COLUMN IF NOT EXISTS artifact BYTEA NULL;

-- 2. 回饋：真正的漏報（平台沒告警）不指向事件群，改記管線、實體與發生時段。
ALTER TABLE feedback ALTER COLUMN incident_uid DROP NOT NULL;
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS pipeline VARCHAR(128) NULL;
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS entity VARCHAR(256) NULL;
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS time_start TIMESTAMP NULL;
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS time_end TIMESTAMP NULL;

-- 3. 提案：核准時間（調參成效報表用來切前後）與建立時間。
--    既有已核准的提案沒有核准時間，不會出現在成效報表裡（不回填，免得前後切點是錯的）。
ALTER TABLE tuning_proposals ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP NULL;
ALTER TABLE tuning_proposals ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc');

-- 4. 歷史事故經驗庫。
CREATE TABLE IF NOT EXISTS incident_knowledge (
    id SERIAL PRIMARY KEY,
    incident_uid VARCHAR(40) NOT NULL UNIQUE,
    pipeline VARCHAR(128) NOT NULL DEFAULT '',
    entities JSON NOT NULL,
    metrics JSON NOT NULL,
    signals JSON NOT NULL,
    resolution VARCHAR(4000) NOT NULL,
    root_cause VARCHAR(1000) NOT NULL DEFAULT '',
    author VARCHAR(128) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS ix_incident_knowledge_pipeline ON incident_knowledge (pipeline);

COMMIT;
