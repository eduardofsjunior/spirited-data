-- Detailed analysis of the 9 FAIL validations
SELECT
    film_slug,
    language_code,
    film_title,
    max_minute_offset,
    expected_duration_minutes,
    overrun_minutes,
    buffer_minutes,
    validation_status,
    data_quality_score,
    requires_investigation,
    -- Calculate how far beyond the buffer
    (overrun_minutes - buffer_minutes) AS beyond_buffer_minutes,
    -- Categorize severity
    CASE
        WHEN overrun_minutes > 50 THEN 'CRITICAL (>50 min overrun)'
        WHEN overrun_minutes > 20 THEN 'SEVERE (20-50 min overrun)'
        WHEN overrun_minutes > 10 THEN 'MODERATE (10-20 min overrun)'
        ELSE 'MINOR (<10 min overrun)'
    END AS severity
FROM {{ ref('int_emotion_data_quality_checks') }}
WHERE validation_status = 'FAIL'
ORDER BY overrun_minutes DESC
