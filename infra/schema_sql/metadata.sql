CREATE SCHEMA IF NOT EXISTS "formula_one_pipeline_metadata";

-- Single table to track all sync operations
CREATE TABLE "formula_one_pipeline_metadata"."sync_status" (
  "entity_name" VARCHAR(100) PRIMARY KEY,
  "last_updated" TIMESTAMP NOT NULL DEFAULT NOW(),
  "last_successful_sync" TIMESTAMP,
  "status" VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 'success', 'failed', 'running'
  "error_message" TEXT,
  "total_records" INTEGER DEFAULT 0,

    -- Watermarks for incremental loading
  "last_season_year" INTEGER,                       -- Last season loaded (e.g., 2024)
  "last_round_number" INTEGER,                      -- Last round loaded (e.g., 18)

  CONSTRAINT check_status CHECK (status IN ('pending', 'running', 'success', 'failed'))
);

-- Optional: Simple audit log (recommended for showcase)
CREATE TABLE "formula_one_pipeline_metadata"."sync_log" (
  "id" SERIAL PRIMARY KEY,
  "entity_name" VARCHAR(100) NOT NULL,
  "sync_timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
  "status" VARCHAR(20) NOT NULL,
  "records_affected" INTEGER DEFAULT 0,
  "duration_seconds" INTEGER,
  "error_message" TEXT
);

-- Index for quick lookups
CREATE INDEX idx_sync_log_entity ON "formula_one_pipeline_metadata"."sync_log"(entity_name, sync_timestamp DESC);
CREATE INDEX idx_sync_log_timestamp ON "formula_one_pipeline_metadata"."sync_log"(sync_timestamp DESC);

-- ============================================
-- INITIALIZE TRACKING FOR YOUR TABLES
-- ============================================

INSERT INTO "formula_one_pipeline_metadata"."sync_status" (entity_name, status, last_season_year) VALUES
  ('circuit', 'pending', 2024),
  ('season', 'pending', 2024),
  ('team', 'pending', 2024),
  ('driver', 'pending', 2024),
  ('round', 'pending', 2024),
  ('session', 'pending', 2024),
  ('driver_championship', 'pending', 2024),
  ('team_championship', 'pending', 2024),
  ('team_driver', 'pending', 2024);