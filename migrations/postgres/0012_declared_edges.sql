-- 宣告的關係邊：declared_edges（admin.graph_import 寫入，沒有 trace 的環境靠它提供依賴圖）。
-- 必須先執行 0001～0011（本檔原為 0010，合併進主線時改號，內容沒變；只依賴 0009 的環境欄位，跟 0010／0011 無關）。可重複執行（連跑兩次結果相同）。全新資料庫由程式啟動時自動建表，跑這個檔什麼都不會變。
--
-- 為什麼另開一張表、不塞進 service_edges：service_edges 是 trace 沉澱的同步呼叫邊，會依 last_seen 汰除，而且只有一種型別；
--   宣告的邊每條自帶型別（call／supply／flow／async，engine/graph.EdgeKind），不會因為「很久沒被 trace 看到」而消失。
--
-- 執行順序：先跑這個檔再啟動新版程式（新版每一輪都會讀這張表；表不存在會失敗）。舊版程式不讀它，先建表不影響舊版。
-- 鎖：只建新表與唯一約束，不碰既有的表。
-- 回滾（退回舊版程式之後做）：DROP TABLE IF EXISTS declared_edges;

BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS declared_edges (
    id          SERIAL PRIMARY KEY,
    environment VARCHAR(64)  NOT NULL DEFAULT '',
    source      VARCHAR(256) NOT NULL,
    target      VARCHAR(256) NOT NULL,
    kind        VARCHAR(16)  NOT NULL,
    weight      DOUBLE PRECISION NOT NULL DEFAULT 1,
    origin      VARCHAR(128) NOT NULL DEFAULT '',
    updated_at  TIMESTAMP
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_declared_edges_env_edge') THEN
        ALTER TABLE declared_edges
            ADD CONSTRAINT uq_declared_edges_env_edge UNIQUE (environment, source, target, kind);
    END IF;
END $$;

COMMIT;
