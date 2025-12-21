CREATE INDEX IF NOT EXISTS idx_mart_dsm_date ON mart.daily_story_metrics (metric_date);
CREATE INDEX IF NOT EXISTS idx_mart_td_date_domain ON mart.top_domains_daily (metric_date, domain);
CREATE INDEX IF NOT EXISTS idx_mart_uad_date_author ON mart.user_activity_daily (metric_date, author);
