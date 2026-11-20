-- 事故卡的分項覆核：feedback 加 review（JSONB）、incidents 加 rca_root_entity。
-- 必須先執行 0001～0012。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表（已含欄位，review 會是通用 JSON），跑這個檔會把它轉成 JSONB。
--
-- 為什麼：既有的回饋只有「真異常／誤報／漏報」，是給偵測器調門檻用的。收斂（併錯、拆錯）、RCA（指錯根因、該找到卻說無法判定）、
--   結案太早、嚴重度給錯，以前沒有結構化的地方記（aiops/control_plane/learning/review.py）。
--   feedback.review：有填的覆核項目＋送出當下平台對這張卡的說法（嚴重度、狀態、實體、RCA 結論等級與第一候選）。
--     既有的列回填 '{}'（= 沒有分項覆核）。
--   incidents.rca_root_entity：最近一次 RCA 排第一的候選。RCA 報告不落地、重跑會變，覆核「根因指錯」時要對得回平台當時指的是誰。
--     既有的列回填 ''（= 沒有候選或還沒跑過；已經跑過 RCA 的舊列不補，重跑一次 RCA 就會寫入）。
--
-- 執行順序：先跑這個檔再啟動新版程式（新版程式讀寫 incidents、feedback 會帶這兩個欄位，欄位不存在就失敗）。
--   舊版程式不認得這兩個欄位、也不會寫它們，欄位有預設值，所以跑完這個檔之後舊版程式仍可照常運作。
--
-- 鎖：加「常數預設值」的欄位只改中繼資料（PostgreSQL 11 以上），不重寫資料表。
--   程式自動建表的資料庫 review 是 JSON，轉 JSONB 會重寫 feedback：這張表是人工回饋（幾百到幾千筆），重寫是毫秒級。
--   lock_timeout 5 秒：拿不到鎖就整筆回復、什麼都沒改，找空檔重跑。
--
-- 回滾（退回舊版程式之後才做；會丟掉已收的分項覆核與已記的第一候選）：
--   ALTER TABLE feedback DROP COLUMN IF EXISTS review;
--   ALTER TABLE incidents DROP COLUMN IF EXISTS rca_root_entity;
BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS rca_root_entity VARCHAR(256) NOT NULL DEFAULT '';
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS review JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'feedback' AND column_name = 'review') = 'json' THEN
        ALTER TABLE feedback ALTER COLUMN review DROP DEFAULT;
        ALTER TABLE feedback ALTER COLUMN review TYPE JSONB USING review::jsonb;
        ALTER TABLE feedback ALTER COLUMN review SET DEFAULT '{}'::jsonb;
    END IF;
END $$;

COMMIT;
