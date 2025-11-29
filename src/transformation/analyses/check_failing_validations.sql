SELECT
    film_slug,
    language_code,
    max_minute_offset,
    expected_duration_minutes,
    overrun_minutes,
    validation_status,
    film_title
FROM {{ ref('int_emotion_data_quality_checks') }}
WHERE validation_status = 'FAIL'
ORDER BY overrun_minutes DESC
