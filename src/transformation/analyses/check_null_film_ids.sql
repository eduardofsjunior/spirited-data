-- Identify which film_slugs have NULL film_id (prevents Kaggle runtime mapping)
SELECT
    film_slug,
    language_code,
    COUNT(*) as record_count,
    MAX(minute_offset) as max_minute
FROM {{ source('raw', 'film_emotions') }}
WHERE film_id IS NULL
GROUP BY film_slug, language_code
ORDER BY film_slug, language_code
