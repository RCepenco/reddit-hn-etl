CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.load_runs (
  run_id uuid PRIMARY KEY,
  phase text NOT NULL,
  source_file text,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  status text NOT NULL CHECK (status IN ('running', 'success', 'failed')),
  rows_copied bigint,
  rows_merged_inserted bigint,
  rows_merged_updated bigint,
  error_message text
);

CREATE INDEX IF NOT EXISTS idx_load_runs_phase_started
  ON audit.load_runs (phase, started_at DESC);
