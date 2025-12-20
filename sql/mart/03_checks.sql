SELECT MAX(extracted_at) AS latest_extracted_at FROM staging.hn_items;

SELECT * FROM mart.daily_story_metrics ORDER BY metric_date DESC LIMIT 10;

SELECT * FROM mart.top_domains_daily
ORDER BY metric_date DESC, stories_count DESC
LIMIT 20;

SELECT * FROM mart.user_activity_daily
ORDER BY metric_date DESC, stories_count DESC
LIMIT 20;
