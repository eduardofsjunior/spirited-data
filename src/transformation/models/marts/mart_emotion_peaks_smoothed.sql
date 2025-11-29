{{
    config(
        materialized='table',
        tags=['marts', 'emotion', 'analytics', 'peaks']
    )
}}

/*
Emotion Peaks Catalog Mart (Smoothed - Narrative Level)
========================================================
Top 5 emotion peaks per film×language×emotion using 10-minute rolling average.

Purpose: Powers Epic 5 Film Explorer smoothed emotion timeline view showing
         interpretable emotional arcs with reduced noise from short dialogue spikes.

Methodology: Applies 10-minute rolling average (±5 minutes window) to emotion
            scores before peak detection. This smooths out transient dialogue-level
            fluctuations to reveal sustained narrative emotional beats.

Trade-offs: Smoothing improves interpretability but may miss short emotional spikes.
           Use mart_emotion_peaks_raw for dialogue-level precision.

Example Query:
    SELECT emotion_type, peak_minute_offset, intensity_score, scene_description
    FROM main_marts.mart_emotion_peaks_smoothed
    WHERE film_title = 'Spirited Away' AND language_code = 'en'
      AND emotion_type = 'joy'
    ORDER BY peak_rank;
*/

WITH film_emotions_raw AS (
    -- Load per-minute emotion data with film metadata
    SELECT
        f.id AS film_id,
        f.title AS film_title,
        fe.language_code,
        fe.minute_offset,
        fe.dialogue_count,
        -- All 28 GoEmotions dimensions
        fe.emotion_admiration,
        fe.emotion_amusement,
        fe.emotion_anger,
        fe.emotion_annoyance,
        fe.emotion_approval,
        fe.emotion_caring,
        fe.emotion_confusion,
        fe.emotion_curiosity,
        fe.emotion_desire,
        fe.emotion_disappointment,
        fe.emotion_disapproval,
        fe.emotion_disgust,
        fe.emotion_embarrassment,
        fe.emotion_excitement,
        fe.emotion_fear,
        fe.emotion_gratitude,
        fe.emotion_grief,
        fe.emotion_joy,
        fe.emotion_love,
        fe.emotion_nervousness,
        fe.emotion_optimism,
        fe.emotion_pride,
        fe.emotion_realization,
        fe.emotion_relief,
        fe.emotion_remorse,
        fe.emotion_sadness,
        fe.emotion_surprise,
        fe.emotion_neutral
    FROM {{ source('raw', 'film_emotions') }} fe
    LEFT JOIN {{ ref('stg_films') }} f
        ON LOWER(REPLACE(f.title, ' ', '_')) = REGEXP_REPLACE(fe.film_slug, '_[a-z]{2}$', '')
    WHERE f.id IS NOT NULL
),

film_emotions_smoothed AS (
    -- Apply 10-minute rolling average to all emotions
    SELECT
        film_id,
        film_title,
        language_code,
        minute_offset,
        -- 10-minute rolling average for all 28 emotions (±5 minutes window)
        AVG(emotion_admiration) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_admiration,
        AVG(emotion_amusement) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_amusement,
        AVG(emotion_anger) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_anger,
        AVG(emotion_annoyance) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_annoyance,
        AVG(emotion_approval) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_approval,
        AVG(emotion_caring) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_caring,
        AVG(emotion_confusion) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_confusion,
        AVG(emotion_curiosity) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_curiosity,
        AVG(emotion_desire) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_desire,
        AVG(emotion_disappointment) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_disappointment,
        AVG(emotion_disapproval) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_disapproval,
        AVG(emotion_disgust) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_disgust,
        AVG(emotion_embarrassment) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_embarrassment,
        AVG(emotion_excitement) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_excitement,
        AVG(emotion_fear) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_fear,
        AVG(emotion_gratitude) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_gratitude,
        AVG(emotion_grief) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_grief,
        AVG(emotion_joy) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_joy,
        AVG(emotion_love) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_love,
        AVG(emotion_nervousness) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_nervousness,
        AVG(emotion_optimism) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_optimism,
        AVG(emotion_pride) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_pride,
        AVG(emotion_realization) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_realization,
        AVG(emotion_relief) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_relief,
        AVG(emotion_remorse) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_remorse,
        AVG(emotion_sadness) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_sadness,
        AVG(emotion_surprise) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_surprise,
        AVG(emotion_neutral) OVER (
            PARTITION BY film_id, language_code
            ORDER BY minute_offset
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS emotion_neutral
    FROM film_emotions_raw
),

emotions_unpivoted AS (
    -- Unpivot all 28 emotions to rows using UNION ALL pattern (DuckDB doesn't have UNPIVOT)
    SELECT film_id, film_title, language_code, minute_offset, 'admiration' AS emotion_type, emotion_admiration AS intensity_score FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'amusement', emotion_amusement FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'anger', emotion_anger FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'annoyance', emotion_annoyance FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'approval', emotion_approval FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'caring', emotion_caring FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'confusion', emotion_confusion FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'curiosity', emotion_curiosity FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'desire', emotion_desire FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'disappointment', emotion_disappointment FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'disapproval', emotion_disapproval FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'disgust', emotion_disgust FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'embarrassment', emotion_embarrassment FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'excitement', emotion_excitement FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'fear', emotion_fear FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'gratitude', emotion_gratitude FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'grief', emotion_grief FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'joy', emotion_joy FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'love', emotion_love FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'nervousness', emotion_nervousness FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'optimism', emotion_optimism FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'pride', emotion_pride FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'realization', emotion_realization FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'relief', emotion_relief FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'remorse', emotion_remorse FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'sadness', emotion_sadness FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'surprise', emotion_surprise FROM film_emotions_smoothed
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, 'neutral', emotion_neutral FROM film_emotions_smoothed
),

ranked_peaks AS (
    -- Rank peaks within each film+language+emotion combination
    SELECT
        film_id || '_' || language_code || '_' || emotion_type || '_' || minute_offset AS peak_id,
        film_id,
        film_title,
        language_code,
        emotion_type,
        minute_offset AS peak_minute_offset,
        ROUND(intensity_score, 4) AS intensity_score,
        ROW_NUMBER() OVER (
            PARTITION BY film_id, language_code, emotion_type
            ORDER BY intensity_score DESC, minute_offset ASC  -- Break ties with earlier minute
        ) AS peak_rank,
        10 AS smoothing_window_minutes
    FROM emotions_unpivoted
)

-- Final output: Top 5 peaks per film+language+emotion
SELECT
    peak_id,
    film_id,
    film_title,
    language_code,
    emotion_type,
    peak_minute_offset,
    intensity_score,
    peak_rank,
    'Minutes ' || CAST((peak_minute_offset - 5) AS VARCHAR) || '-' || CAST((peak_minute_offset + 5) AS VARCHAR) AS scene_description,
    smoothing_window_minutes
FROM ranked_peaks
WHERE peak_rank <= 5  -- Top 5 only
ORDER BY film_title, language_code, emotion_type, peak_rank
