{{
    config(
        materialized='table',
        tags=['marts', 'emotion', 'analytics', 'peaks']
    )
}}

/*
Emotion Peaks Catalog Mart (Raw - Dialogue Level)
==================================================
Top 5 emotion peaks per film×language×emotion using 1-minute buckets (no smoothing).

Purpose: Powers Epic 5 Film Explorer raw dialogue-level view and methodology
         transparency page showing exact emotional peaks at dialogue resolution.

Methodology: Uses raw 1-minute bucketed emotion scores without any smoothing.
            Preserves all short-term emotional fluctuations for precision analysis.

Trade-offs: Raw data may include transient noise from single dialogue lines but
           provides exact timestamps for peak emotional moments. Use
           mart_emotion_peaks_smoothed for interpretable narrative arcs.

Example Query:
    SELECT emotion_type, minute_offset, intensity_score, dialogue_count
    FROM main_marts.mart_emotion_peaks_raw
    WHERE film_title = 'Spirited Away' AND language_code = 'en'
      AND emotion_type = 'fear'
    ORDER BY peak_rank;
*/

WITH film_emotions_raw AS (
    -- Load per-minute emotion data (NO smoothing) with film metadata
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

emotions_unpivoted AS (
    -- Unpivot all 28 emotions to rows using UNION ALL pattern (DuckDB doesn't have UNPIVOT)
    SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'admiration' AS emotion_type, emotion_admiration AS intensity_score FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'amusement', emotion_amusement FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'anger', emotion_anger FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'annoyance', emotion_annoyance FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'approval', emotion_approval FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'caring', emotion_caring FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'confusion', emotion_confusion FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'curiosity', emotion_curiosity FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'desire', emotion_desire FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'disappointment', emotion_disappointment FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'disapproval', emotion_disapproval FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'disgust', emotion_disgust FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'embarrassment', emotion_embarrassment FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'excitement', emotion_excitement FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'fear', emotion_fear FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'gratitude', emotion_gratitude FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'grief', emotion_grief FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'joy', emotion_joy FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'love', emotion_love FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'nervousness', emotion_nervousness FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'optimism', emotion_optimism FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'pride', emotion_pride FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'realization', emotion_realization FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'relief', emotion_relief FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'remorse', emotion_remorse FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'sadness', emotion_sadness FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'surprise', emotion_surprise FROM film_emotions_raw
    UNION ALL SELECT film_id, film_title, language_code, minute_offset, dialogue_count, 'neutral', emotion_neutral FROM film_emotions_raw
),

ranked_peaks AS (
    -- Rank peaks within each film+language+emotion combination
    SELECT
        film_id || '_' || language_code || '_' || emotion_type || '_' || minute_offset AS peak_id,
        film_id,
        film_title,
        language_code,
        emotion_type,
        minute_offset,
        ROUND(intensity_score, 4) AS intensity_score,
        dialogue_count,
        ROW_NUMBER() OVER (
            PARTITION BY film_id, language_code, emotion_type
            ORDER BY intensity_score DESC, minute_offset ASC  -- Break ties with earlier minute
        ) AS peak_rank,
        FALSE AS is_smoothed
    FROM emotions_unpivoted
)

-- Final output: Top 5 peaks per film+language+emotion
SELECT
    peak_id,
    film_id,
    film_title,
    language_code,
    emotion_type,
    minute_offset,
    intensity_score,
    peak_rank,
    dialogue_count,
    NULL AS dialogue_excerpt,  -- Can be populated later via post-processing
    is_smoothed
FROM ranked_peaks
WHERE peak_rank <= 5  -- Top 5 only
ORDER BY film_title, language_code, emotion_type, peak_rank
