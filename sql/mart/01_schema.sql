CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

COMMENT ON SCHEMA mart IS 'Analytics MART schema (portfolio project)';

-- ============================================================
-- MART tables (DDL)
-- Created once, populated via UPSERT in 02_marts.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS mart.daily_story_metrics (
  metric_date date PRIMARY KEY,
  stories_count int NOT NULL,
  total_score bigint NOT NULL,
  avg_score numeric(10,2) NOT NULL,
  total_comments bigint NOT NULL,
  avg_comments numeric(10,2) NOT NULL,
  last_batch_extracted_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.top_domains_daily (
  metric_date date NOT NULL,
  domain text NOT NULL,
  stories_count int NOT NULL,
  avg_score numeric(10,2) NOT NULL,
  last_batch_extracted_at timestamptz NOT NULL,
  PRIMARY KEY (metric_date, domain)
);

CREATE TABLE IF NOT EXISTS mart.user_activity_daily (
  metric_date date NOT NULL,
  author text NOT NULL,
  stories_count int NOT NULL,
  avg_score numeric(10,2) NOT NULL,
  last_batch_extracted_at timestamptz NOT NULL,
  PRIMARY KEY (metric_date, author)
);

