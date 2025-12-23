CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.hn_stories (
  id bigint PRIMARY KEY,
  type text,
  by text,
  time bigint NOT NULL,
  time_utc timestamptz NOT NULL,
  title text NOT NULL,
  url text,
  score bigint,
  descendants bigint,
  kids_count bigint,
  text text,
  extracted_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hn_stories_extracted_at
  ON staging.hn_stories (extracted_at);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.hn_stories_tmp (
  id bigint NOT NULL,
  type text,
  by text,
  time bigint NOT NULL,
  time_utc timestamptz NOT NULL,
  title text NOT NULL,
  url text,
  score bigint,
  descendants bigint,
  kids_count bigint,
  text text,
  extracted_at timestamptz NOT NULL
);

TRUNCATE TABLE staging.hn_stories_tmp;

CREATE INDEX IF NOT EXISTS idx_hn_tmp_id ON staging.hn_stories_tmp (id);
CREATE INDEX IF NOT EXISTS idx_hn_tmp_extracted ON staging.hn_stories_tmp (extracted_at);
