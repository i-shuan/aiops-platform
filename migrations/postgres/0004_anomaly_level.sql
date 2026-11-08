-- 異常事件的等級：anomaly（正式異常）或 warning（接近門檻的預警）。必須先執行 0001～0003。可重複執行。
-- 全新資料庫由程式自動建表，不需要這個檔。
--
-- 既有資料一律視為 anomaly（舊版只記超過門檻的點）。
-- 執行順序：先停掉排程與 API（或確保沒有 worker 在寫），跑完這個檔再啟動新版程式。
--
-- 鎖：PostgreSQL 11 以上，加「常數預設值」的欄位只改中繼資料、不重寫整張表，持鎖只有幾毫秒；
--     PostgreSQL 10 以下會整表重寫，大表請排維護時段。
--     但 ALTER 要先拿 ACCESS EXCLUSIVE 鎖，前面有慢查詢時它會排隊、後面所有讀寫 anomalies 的請求都卡在它後面。
--     所以設 lock_timeout：5 秒拿不到鎖就放棄並報錯（交易整個回復，什麼都沒改），找個空檔重跑即可。
--
-- 回滾（只在要退回舊版程式、而且想把欄位拿掉時才需要；舊版程式碰到多出來的欄位也能正常跑，可以不做）：
--   BEGIN; SET LOCAL lock_timeout = '5s'; ALTER TABLE anomalies DROP COLUMN IF EXISTS level; COMMIT;
-- 注意：新版程式不能跑在沒有這個欄位的資料庫上（寫入會失敗），一定要先跑這個檔、再換新版程式。
BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE anomalies ADD COLUMN IF NOT EXISTS level VARCHAR(16) NOT NULL DEFAULT 'anomaly';

COMMIT;
