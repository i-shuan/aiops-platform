-- 事件群記下最近一次 RCA 的結論等級：incidents 加 rca_conclusion_type、rca_concluded_at。
-- 必須先執行 0001～0009。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表（已含欄位），跑這個檔什麼都不會變。
--
-- 為什麼：RCA 的結論以前只有「有候選／無法判定」，沒有 trace 時依拓撲推測的候選、只看得到症狀位置的候選，
--   看起來跟有直接證據的一樣肯定。結論等級（confirmed／symptom_location／topology_inference／undetermined，
--   aiops/engine/rca_types.py）由根因演算法宣告，跟著 API 回傳，也記在事件群上，事後查得到當時給的是哪一級。
--   既有的列回填 ''（= 還沒跑過 RCA），rca_concluded_at 是 NULL。
--
-- 執行順序：先跑這個檔再啟動新版程式（新版程式讀寫 incidents 會帶這兩個欄位，欄位不存在就失敗）。
--   舊版程式不認得這兩個欄位、也不會寫它們，欄位有預設值，所以跑完這個檔之後舊版程式仍可照常運作。
--
-- 鎖：加「常數預設值」的欄位與可為 NULL 的欄位都只改中繼資料（PostgreSQL 11 以上），不重寫資料表。
--   lock_timeout 5 秒：拿不到鎖就整筆回復、什麼都沒改，找空檔重跑。
--
-- 回滾（退回舊版程式之後才做；會丟掉已記的結論等級）：
--   ALTER TABLE incidents DROP COLUMN IF EXISTS rca_conclusion_type, DROP COLUMN IF EXISTS rca_concluded_at;
BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS rca_conclusion_type VARCHAR(32) NOT NULL DEFAULT '';
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS rca_concluded_at TIMESTAMP;

COMMIT;
