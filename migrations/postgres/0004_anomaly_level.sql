-- 異常事件的等級：anomaly（正式異常）或 warning（接近門檻的預警）。必須先執行 0001～0003。可重複執行。
-- 全新資料庫由程式自動建表，不需要這個檔。
--
-- 既有資料一律視為 anomaly（舊版只記超過門檻的點）。
-- 執行順序：先停掉排程與 API（或確保沒有 worker 在寫），跑完這個檔再啟動新版程式。
BEGIN;

ALTER TABLE anomalies ADD COLUMN IF NOT EXISTS level VARCHAR(16) NOT NULL DEFAULT 'anomaly';

COMMIT;
