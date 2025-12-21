SELECT COUNT(*) AS tmp_rows FROM staging.hn_stories_tmp;
SELECT COUNT(*) AS target_rows FROM staging.hn_stories;
SELECT MIN(extracted_at), MAX(extracted_at) FROM staging.hn_stories;

SELECT id, COUNT(*)
FROM staging.hn_stories
GROUP BY id
HAVING COUNT(*) > 1;
