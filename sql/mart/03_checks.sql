-- Row counts
SELECT
  (SELECT COUNT(*) FROM mart.daily_story_metrics) AS daily_metrics_rows,
  (SELECT COUNT(*) FROM mart.top_domains_daily)   AS domains_rows,
  (SELECT COUNT(*) FROM mart.user_activity_daily) AS users_rows;

-- Date ranges
SELECT 'daily_story_metrics' AS mart, MIN(metric_date) min_date, MAX(metric_date) max_date, COUNT(*) rows
FROM mart.daily_story_metrics
UNION ALL
SELECT 'top_domains_daily', MIN(metric_date), MAX(metric_date), COUNT(*)
FROM mart.top_domains_daily
UNION ALL
SELECT 'user_activity_daily', MIN(metric_date), MAX(metric_date), COUNT(*)
FROM mart.user_activity_daily;

-- Last day rowcount
WITH last_day AS (SELECT MAX(metric_date) AS d FROM mart.user_activity_daily)
SELECT COUNT(*) AS rows_last_day
FROM mart.user_activity_daily u
JOIN last_day ld ON u.metric_date = ld.d;

-- Duplicates by PK (should be 0 rows)
SELECT metric_date, author, COUNT(*)
FROM mart.user_activity_daily
GROUP BY metric_date, author
HAVING COUNT(*) > 1;
