WITH ins AS (
  INSERT INTO staging.hn_stories (
    id, type, by, time, time_utc, title, url, score, descendants, kids_count, text, extracted_at
  )
  SELECT
    id, type, by, time, time_utc, title, url, score, descendants, kids_count, text, extracted_at
  FROM staging.hn_stories_tmp
  ON CONFLICT (id) DO NOTHING
  RETURNING 1
),
upd AS (
  UPDATE staging.hn_stories t
  SET
    type = s.type,
    by = s.by,
    time = s.time,
    time_utc = s.time_utc,
    title = s.title,
    url = s.url,
    score = s.score,
    descendants = s.descendants,
    kids_count = s.kids_count,
    text = s.text,
    extracted_at = s.extracted_at
  FROM staging.hn_stories_tmp s
  WHERE t.id = s.id
    AND s.extracted_at > t.extracted_at
  RETURNING 1
)
SELECT
  (SELECT COUNT(*) FROM ins) AS inserted,
  (SELECT COUNT(*) FROM upd) AS updated;
