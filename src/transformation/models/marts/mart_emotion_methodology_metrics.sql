{{
    config(
        materialized='table',
        description='Emotion analysis methodology metrics across multiple rolling window sizes showing signal processing trade-offs'
    )
}}

-- ============================================================================
-- Methodology Transparency Mart
-- ============================================================================
-- Purpose: Demonstrate rolling window smoothing trade-offs for Epic 5
-- Analysis: Compares 5 window sizes (3, 5, 10, 15, 20 minutes)
-- Metrics: noise_level (smoothness), peak_count, temporal_precision_loss_pct
-- Recommended: 10-minute window balances noise reduction and precision
-- ============================================================================

WITH base_emotions AS (
    -- Extract base emotion data with film metadata
    -- Using emotion_joy as representative dimension for methodology demo
    SELECT
        fe.film_id,
        f.title AS film_title,
        f.running_time AS film_duration_minutes,
        fe.language_code,
        fe.minute_offset,
        fe.emotion_joy AS emotion_score
    FROM {{ source('raw', 'film_emotions') }} fe
    JOIN {{ ref('stg_films') }} f ON fe.film_id = f.id
    WHERE fe.language_code = 'en'  -- Focus on English for methodology demo
      AND fe.emotion_joy IS NOT NULL
),

-- ============================================================================
-- Window Size CTEs: Apply rolling average smoothing with different window sizes
-- Formula: ROWS BETWEEN n PRECEDING AND n FOLLOWING (symmetric windows)
-- ============================================================================

windowed_3min AS (
    SELECT
        film_id,
        film_title,
        film_duration_minutes,
        language_code,
        minute_offset,
        3 AS window_size_minutes,
        AVG(emotion_score) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING  -- 3-point window
        ) AS smoothed_score
    FROM base_emotions
),

windowed_5min AS (
    SELECT
        film_id,
        film_title,
        film_duration_minutes,
        language_code,
        minute_offset,
        5 AS window_size_minutes,
        AVG(emotion_score) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING  -- 5-point window
        ) AS smoothed_score
    FROM base_emotions
),

windowed_10min AS (
    SELECT
        film_id,
        film_title,
        film_duration_minutes,
        language_code,
        minute_offset,
        10 AS window_size_minutes,
        AVG(emotion_score) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 4 PRECEDING AND 5 FOLLOWING  -- 10-point window
        ) AS smoothed_score
    FROM base_emotions
),

windowed_15min AS (
    SELECT
        film_id,
        film_title,
        film_duration_minutes,
        language_code,
        minute_offset,
        15 AS window_size_minutes,
        AVG(emotion_score) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING  -- 15-point window
        ) AS smoothed_score
    FROM base_emotions
),

windowed_20min AS (
    SELECT
        film_id,
        film_title,
        film_duration_minutes,
        language_code,
        minute_offset,
        20 AS window_size_minutes,
        AVG(emotion_score) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 9 PRECEDING AND 10 FOLLOWING  -- 20-point window
        ) AS smoothed_score
    FROM base_emotions
),

-- Combine all window sizes
all_windows AS (
    SELECT * FROM windowed_3min
    UNION ALL
    SELECT * FROM windowed_5min
    UNION ALL
    SELECT * FROM windowed_10min
    UNION ALL
    SELECT * FROM windowed_15min
    UNION ALL
    SELECT * FROM windowed_20min
),

-- ============================================================================
-- Derivative Calculations: Measure rate of change for noise detection
-- First derivative: Change in emotion score between consecutive minutes
-- Second derivative: Change in first derivative (measures "jaggedness")
-- ============================================================================

with_derivatives AS (
    SELECT
        *,
        smoothed_score - LAG(smoothed_score) OVER (
            PARTITION BY film_id, language_code, window_size_minutes
            ORDER BY minute_offset
        ) AS first_derivative
    FROM all_windows
),

with_second_derivatives AS (
    SELECT
        *,
        first_derivative - LAG(first_derivative) OVER (
            PARTITION BY film_id, language_code, window_size_minutes
            ORDER BY minute_offset
        ) AS second_derivative
    FROM with_derivatives
),

-- ============================================================================
-- Peak Detection: Identify local maxima in smoothed curves
-- Peak criteria: value > previous AND value > next
-- ============================================================================

with_peaks AS (
    SELECT
        *,
        CASE
            WHEN smoothed_score > LAG(smoothed_score) OVER w
                 AND smoothed_score > LEAD(smoothed_score) OVER w
            THEN 1
            ELSE 0
        END AS is_peak
    FROM with_second_derivatives
    WINDOW w AS (
        PARTITION BY film_id, language_code, window_size_minutes
        ORDER BY minute_offset
    )
),

-- ============================================================================
-- Aggregate Metrics: Calculate final statistics per film+language+window
-- ============================================================================

final_metrics AS (
    SELECT
        film_id,
        film_title,
        language_code,
        window_size_minutes,
        film_duration_minutes,

        -- Noise level: Standard deviation of second derivative (lower = smoother)
        STDDEV(second_derivative) AS noise_level,

        -- Peak count: Number of detected local maxima
        SUM(is_peak) AS peak_count,

        -- Average intensity: Mean emotion score across film
        AVG(smoothed_score) AS avg_intensity,

        -- Temporal precision loss: Percentage of film duration consumed by window
        ROUND((window_size_minutes::FLOAT / film_duration_minutes) * 100, 2) AS temporal_precision_loss_pct,

        -- Recommendation flag: 10-minute window is optimal
        CASE WHEN window_size_minutes = 10 THEN TRUE ELSE FALSE END AS is_recommended

    FROM with_peaks
    WHERE second_derivative IS NOT NULL  -- Exclude NULL derivatives at boundaries
    GROUP BY film_id, film_title, language_code, window_size_minutes, film_duration_minutes
)

-- ============================================================================
-- Final Output: Format and order results
-- ============================================================================

SELECT
    film_id,
    film_title,
    language_code,
    window_size_minutes,
    ROUND(noise_level, 4) AS noise_level,
    peak_count,
    ROUND(avg_intensity, 4) AS avg_intensity,
    temporal_precision_loss_pct,
    is_recommended
FROM final_metrics
ORDER BY film_title, window_size_minutes
