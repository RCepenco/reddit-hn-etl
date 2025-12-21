-- ============================================================
-- Phase 5 — MART
-- Source: staging.hn_stories (ALL rows, no latest-batch filter)
-- Strategy: full refresh via upsert (idempotent)
-- ============================================================


-- ============================================================
-- A) mart.daily_story_metrics
-- ============================================================
WITH base AS (
    SELECT
        i.time_utc::date                AS metric_date,
        COALESCE(i.score, 0)            AS score,
        COALESCE(i.descendants, 0)      AS comments,
        i.extracted_at
    FROM staging.hn_stories i
    WHERE i.type = 'story'
      AND i.time_utc IS NOT NULL
)
INSERT INTO mart.daily_story_metrics (
    metric_date,
    stories_count,
    total_score,
    avg_score,
    total_comments,
    avg_comments,
    last_batch_extracted_at
)
SELECT
    metric_date,
    COUNT(*)::int                           AS stories_count,
    SUM(score)::bigint                      AS total_score,
    AVG(score)::numeric(10,2)               AS avg_score,
    SUM(comments)::bigint                   AS total_comments,
    AVG(comments)::numeric(10,2)            AS avg_comments,
    MAX(extracted_at)                       AS last_batch_extracted_at
FROM base
GROUP BY metric_date
ON CONFLICT (metric_date) DO UPDATE SET
    stories_count            = EXCLUDED.stories_count,
    total_score              = EXCLUDED.total_score,
    avg_score                = EXCLUDED.avg_score,
    total_comments           = EXCLUDED.total_comments,
    avg_comments             = EXCLUDED.avg_comments,
    last_batch_extracted_at  = EXCLUDED.last_batch_extracted_at;



-- ============================================================
-- B) mart.top_domains_daily
-- ============================================================
WITH base AS (
    SELECT
        i.time_utc::date AS metric_date,
        CASE
            WHEN i.url IS NULL OR i.url = '' THEN '(no_domain)'
            ELSE lower(
                split_part(
                    replace(replace(i.url, 'https://', ''), 'http://', ''),
                    '/',
                    1
                )
            )
        END                         AS domain,
        COALESCE(i.score, 0)        AS score,
        i.extracted_at
    FROM staging.hn_stories i
    WHERE i.type = 'story'
      AND i.time_utc IS NOT NULL
)
INSERT INTO mart.top_domains_daily (
    metric_date,
    domain,
    stories_count,
    avg_score,
    last_batch_extracted_at
)
SELECT
    metric_date,
    domain,
    COUNT(*)::int                   AS stories_count,
    AVG(score)::numeric(10,2)       AS avg_score,
    MAX(extracted_at)               AS last_batch_extracted_at
FROM base
GROUP BY metric_date, domain
ON CONFLICT (metric_date, domain) DO UPDATE SET
    stories_count           = EXCLUDED.stories_count,
    avg_score               = EXCLUDED.avg_score,
    last_batch_extracted_at = EXCLUDED.last_batch_extracted_at;



-- ============================================================
-- C) mart.user_activity_daily
-- ============================================================
WITH base AS (
    SELECT
        i.time_utc::date         AS metric_date,
        COALESCE(i.by, '(unknown)') AS author,
        COALESCE(i.score, 0)     AS score,
        i.extracted_at
    FROM staging.hn_stories i
    WHERE i.type = 'story'
      AND i.time_utc IS NOT NULL
)
INSERT INTO mart.user_activity_daily (
    metric_date,
    author,
    stories_count,
    avg_score,
    last_batch_extracted_at
)
SELECT
    metric_date,
    author,
    COUNT(*)::int               AS stories_count,
    AVG(score)::numeric(10,2)   AS avg_score,
    MAX(extracted_at)           AS last_batch_extracted_at
FROM base
GROUP BY metric_date, author
ON CONFLICT (metric_date, author) DO UPDATE SET
    stories_count           = EXCLUDED.stories_count,
    avg_score               = EXCLUDED.avg_score,
    last_batch_extracted_at = EXCLUDED.last_batch_extracted_at;
