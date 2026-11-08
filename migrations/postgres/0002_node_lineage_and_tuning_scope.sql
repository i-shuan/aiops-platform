-- 異常與基準線綁定 Workflow 節點；調參分層（管線／實體）。必須先執行 0001。可重複執行。
-- 全新資料庫由程式自動建表，不需要這個檔。
--
-- 執行順序：先停掉排程與 API（或確保沒有 worker 在寫），跑完這個檔再啟動新版程式。
BEGIN;

-- 1. 異常：記下是哪個節點抓到的。
--    舊資料的 detector_name 是演算法短名（zscore），新資料是算子名（detector.zscore）；舊資料不回填，查詢時兩種寫法都要考慮。
ALTER TABLE anomalies ADD COLUMN IF NOT EXISTS detector_node VARCHAR(128) NOT NULL DEFAULT '';

-- 2. 基準線：唯一鍵加上節點名稱，同一個演算法掛兩個節點才不會共用一筆。
ALTER TABLE detector_models ADD COLUMN IF NOT EXISTS node VARCHAR(128) NOT NULL DEFAULT '';
-- 舊基準線沒有節點名稱。內建 workflow 的偵測節點都叫 detect，直接掛上去才不用全部重訓一次；
-- 自己改過節點名稱的 workflow，舊基準線讀不到，第一次執行會自動重訓（不會出錯）。
UPDATE detector_models SET node = 'detect' WHERE node = '';
ALTER TABLE detector_models DROP CONSTRAINT IF EXISTS detector_models_pipeline_entity_metric_detector_name_key;
ALTER TABLE detector_models DROP CONSTRAINT IF EXISTS uq_detector_models_key;
ALTER TABLE detector_models
    ADD CONSTRAINT uq_detector_models_key UNIQUE (pipeline, entity, metric, detector_name, node);

-- 3. 現行參數：多一個 (管線, 實體) 維度，'*' = 不限定。既有的設定全部視為全域。
ALTER TABLE active_detector_configs ADD COLUMN IF NOT EXISTS pipeline VARCHAR(128) NOT NULL DEFAULT '*';
ALTER TABLE active_detector_configs ADD COLUMN IF NOT EXISTS entity VARCHAR(256) NOT NULL DEFAULT '*';
ALTER TABLE active_detector_configs DROP CONSTRAINT IF EXISTS active_detector_configs_detector_name_key;
ALTER TABLE active_detector_configs DROP CONSTRAINT IF EXISTS uq_active_detector_configs_scope;
ALTER TABLE active_detector_configs
    ADD CONSTRAINT uq_active_detector_configs_scope UNIQUE (detector_name, pipeline, entity);

-- 4. 提案：記下要改哪一層。既有提案視為全域。
ALTER TABLE tuning_proposals ADD COLUMN IF NOT EXISTS pipeline VARCHAR(128) NOT NULL DEFAULT '*';
ALTER TABLE tuning_proposals ADD COLUMN IF NOT EXISTS entity VARCHAR(256) NOT NULL DEFAULT '*';

COMMIT;
