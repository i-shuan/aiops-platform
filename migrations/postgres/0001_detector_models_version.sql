-- 偵測器基準線的樂觀鎖版本號。
-- 全新資料庫由程式自動建表，不需要這個檔；只有「已經在跑的 PostgreSQL」要執行一次。可重複執行。
BEGIN;

ALTER TABLE detector_models ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE active_detector_configs ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;

COMMIT;
