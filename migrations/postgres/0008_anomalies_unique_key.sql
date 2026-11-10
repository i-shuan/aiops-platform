-- 異常點的唯一鍵：同一條管線 × 實體 × 指標 × 時間點 × 偵測節點只留一筆（uq_anomalies_key）。
-- 必須先執行 0001～0007。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表（含這個唯一鍵），跑這個檔什麼都不會變。
--
-- 為什麼：anomalies 以前沒有任何唯一約束。同一輪被重跑（排程重疊、手動補跑同一個時間點、寫入後行程被殺再重來）
--   就會把同一個異常點寫兩次，下游數異常點的報表（調參成效、誤報率）跟著灌水。新版程式寫入時撞到這個鍵就跳過
--   （INSERT ... ON CONFLICT DO NOTHING），同一批寫幾次結果都一樣。
--
-- 自然鍵為什麼是這五欄：一個偵測節點對一條序列的一個時間點只會產生一筆事件（anomaly 或 warning 擇一）；
--   同一個點被兩個節點抓到是兩筆（detector_node 不同），ensemble 的裁決事件 detector_node 是裁決節點名稱，也不會撞。
--   detector_name、level、score、value 不在鍵裡：同一個鍵重跑時留先寫的那一筆，不覆蓋。
--
-- 這個檔會刪資料：建索引前先刪掉既有的重複列（同一個鍵留 id 最小、也就是最先寫入的那一筆），刪幾筆會印在 NOTICE。
--   沒有任何表用外鍵指到 anomalies.id，刪重複列不影響事件群（事件群快照自己存一份事件）。想先看會刪幾筆：
--   SELECT count(*) FROM (SELECT row_number() OVER (PARTITION BY pipeline, entity, metric, ts, detector_node ORDER BY id) AS rn
--                         FROM anomalies) t WHERE rn > 1;
--
-- 執行順序：先停掉排程（不要有 worker 在寫 anomalies），跑完這個檔再啟動新版程式。
--   新版程式在還沒跑這個檔的資料庫上也能寫（ON CONFLICT DO NOTHING 沒有唯一鍵時就是一般 INSERT），只是沒有去重效果；
--   舊版程式在跑過這個檔的資料庫上也能寫，但重跑同一輪時會撞唯一鍵、那一輪整筆失敗（以前是默默寫兩份）。
--
-- 鎖：DELETE 與 CREATE UNIQUE INDEX 在同一筆交易裡，建索引期間 anomalies 不能寫（可以讀），時間跟資料量成正比
--   （200 萬筆實測 7.7 秒，見 README「升級既有 PostgreSQL」）。lock_timeout 5 秒：拿不到鎖就整個放棄、什麼都沒改，找空檔重跑。
--
-- 回滾（唯一鍵拿掉；被刪掉的重複列不會回來，要留就先備份）：
--   BEGIN; SET LOCAL lock_timeout = '5s';
--   ALTER TABLE anomalies DROP CONSTRAINT IF EXISTS uq_anomalies_key;
--   DROP INDEX IF EXISTS uq_anomalies_key;
--   COMMIT;
--   （兩句都要：這個檔建的是索引，程式自動建表建的是同名的約束。）
BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    removed bigint := 0;
    total bigint;
BEGIN
    IF to_regclass('uq_anomalies_key') IS NULL THEN
        LOCK TABLE anomalies IN SHARE ROW EXCLUSIVE MODE;  -- 刪完到建好索引之間不能有人再寫進重複列
        DELETE FROM anomalies WHERE id IN (
            SELECT id FROM (
                SELECT id, row_number() OVER (PARTITION BY pipeline, entity, metric, ts, detector_node ORDER BY id) AS rn
                FROM anomalies) ranked
            WHERE rn > 1);
        GET DIAGNOSTICS removed = ROW_COUNT;
        CREATE UNIQUE INDEX uq_anomalies_key ON anomalies (pipeline, entity, metric, ts, detector_node);
    END IF;
    SELECT count(*) INTO total FROM anomalies;
    RAISE NOTICE 'anomalies：刪掉 % 筆重複列，現有 % 筆；唯一鍵 uq_anomalies_key 已就緒', removed, total;
END $$;

COMMIT;
