-- ==========================================
-- Phase 5 MART refresh (latest batch only)
-- CHANGE_ME: if your source table name differs
-- Source: staging.hn_items
-- ==========================================

-- ------------------------------------------
-- v1.0 Safety validation (fail fast)
-- Prevents silent success when staging is empty
-- ------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM staging.hn_stories
    WHERE extracted_at IS NOT NULL
    LIMIT 1
  ) THEN
    RAISE EXCEPTION 'Phase 5 blocked: staging.hn_items is empty or has no extracted_at';
  END IF;
END $$;

-- 1) daily story metrics
CREATE TABLE IF NOT EXISTS mart.daily_story_metrics (
  metric_date date PRIMARY KEY,
  stories_count int NOT NULL,
  total_score bigint NOT NULL,
  avg_score numeric(10,2) NOT NULL,
  total_comments bigint NOT NULL,
  avg_comments numeric(10,2) NOT NULL,
  last_batch_extracted_at timestamptz NOT NULL
);

-- 2) top domains daily
CREATE TABLE IF NOT EXISTS mart.top_domains_daily (
  metric_date date NOT NULL,
  domain text NOT NULL,
  stories_count int NOT NULL,
  avg_score numeric(10,2) NOT NULL,
  last_batch_extracted_at timestamptz NOT NULL,
  PRIMARY KEY (metric_date, domain)
);

-- 3) user activity daily
CREATE TABLE IF NOT EXISTS mart.user_activity_daily (
  metric_date date NOT NULL,
  author text NOT NULL,
  stories_count int NOT NULL,
  avg_score numeric(10,2) NOT NULL,
  last_batch_extracted_at timestamptz NOT NULL,
  PRIMARY KEY (metric_date, author)
);

-- =========================
-- A) mart.daily_story_metrics
-- =========================
WITH latest AS (
  SELECT MAX(extracted_at) AS extracted_at
  FROM staging.hn_stories
),
base AS (
  SELECT
    i.id,
    i.type,
    i.by AS author,
    i.time_utc AS created_at,
    COALESCE(i.score, 0) AS score,
    COALESCE(i.descendants, 0) AS comments,
    i.url,
    i.extracted_at,
    CASE
      WHEN i.url IS NULL OR i.url = '' THEN NULL
      ELSE lower(split_part(replace(replace(i.url, 'https://', ''), 'http://', ''), '/', 1))
    END AS domain
  FROM staging.hn_stories i
  JOIN latest l ON i.extracted_at = l.extracted_at
  WHERE i.type = 'story'
    AND i.time_utc IS NOT NULL
)
INSERT INTO mart.daily_story_metrics (
  metric_date, stories_count, total_score, avg_score,
  total_comments, avg_comments, last_batch_extracted_at
)
SELECT
  (created_at AT TIME ZONE 'UTC')::date AS metric_date,
  COUNT(*)::int,
  SUM(score)::bigint,
  AVG(score)::numeric(10,2),
  SUM(comments)::bigint,
  AVG(comments)::numeric(10,2),
  MAX(extracted_at)
FROM base
GROUP BY 1
ON CONFLICT (metric_date) DO UPDATE SET
  stories_count = EXCLUDED.stories_count,
  total_score = EXCLUDED.total_score,
  avg_score = EXCLUDED.avg_score,
  total_comments = EXCLUDED.total_comments,
  avg_comments = EXCLUDED.avg_comments,
  last_batch_extracted_at = EXCLUDED.last_batch_extracted_at;

-- B) mart.top_domains_daily
-- ========================
WITH latest AS (
  SELECT MAX(extracted_at) AS extracted_at
  FROM staging.hn_stories
),
base AS (
  SELECT
    i.id,
    i.type,
    i.by AS author,
    i.time_utc AS created_at,
    COALESCE(i.score, 0) AS score,
    COALESCE(i.descendants, 0) AS comments,
    i.url,
    i.extracted_at,
    CASE
      WHEN i.url IS NULL OR i.url = '' THEN NULL
      ELSE lower(split_part(replace(replace(i.url, 'https://', ''), 'http://', ''), '/', 1))
    END AS domain
  FROM staging.hn_stories i
  JOIN latest l ON i.extracted_at = l.extracted_at
  WHERE i.type = 'story'
    AND i.time_utc IS NOT NULL
)
INSERT INTO mart.top_domains_daily (
  metric_date, domain, stories_count, avg_score, last_batch_extracted_at
)
SELECT
  (created_at AT TIME ZONE 'UTC')::date AS metric_date,
  COALESCE(domain, '(no_domain)') AS domain,
  COUNT(*)::int,
  AVG(score)::numeric(10,2),
  MAX(extracted_at)
FROM base
GROUP BY 1, 2
ON CONFLICT (metric_date, domain) DO UPDATE SET
  stories_count = EXCLUDED.stories_count,
  avg_score = EXCLUDED.avg_score,
  last_batch_extracted_at = EXCLUDED.last_batch_extracted_at;

-- C) mart.user_activity_daily
-- ==========================
WITH latest AS (
  SELECT MAX(extracted_at) AS extracted_at
  FROM staging.hn_stories
),
base AS (
  SELECT
    i.id,
    i.type,
    i.by AS author,
    i.time_utc AS created_at,
    COALESCE(i.score, 0) AS score,
    COALESCE(i.descendants, 0) AS comments,
    i.url,
    i.extracted_at,
    CASE
      WHEN i.url IS NULL OR i.url = '' THEN NULL
      ELSE lower(split_part(replace(replace(i.url, 'https://', ''), 'http://', ''), '/', 1))
    END AS domain
  FROM staging.hn_stories i
  JOIN latest l ON i.extracted_at = l.extracted_at
  WHERE i.type = 'story'
    AND i.time_utc IS NOT NULL
)
INSERT INTO mart.user_activity_daily (
  metric_date, author, stories_count, avg_score, last_batch_extracted_at
)
SELECT
  (created_at AT TIME ZONE 'UTC')::date AS metric_date,
  COALESCE(author, '(unknown)') AS author,
  COUNT(*)::int,
  AVG(score)::numeric(10,2),
  MAX(extracted_at)
FROM base
GROUP BY 1, 2
ON CONFLICT (metric_date, author) DO UPDATE SET
  stories_count = EXCLUDED.stories_count,
  avg_score = EXCLUDED.avg_score,
  last_batch_extracted_at = EXCLUDED.last_batch_extracted_at;
