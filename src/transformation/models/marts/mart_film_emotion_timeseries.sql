{{
    config(
        materialized='table',
        tags=['marts', 'emotion', 'timeseries'],
        description='Full emotion timeline data with 10-minute rolling average for all films and languages'
    )
}}

/*
Film Emotion Time-Series Mart
==============================
Complete minute-by-minute emotion data for all films across all languages.

Purpose: Powers Epic 5 interactive emotion journey visualization showing full
         emotional timeline curves (not just top 5 peaks).

Methodology: Applies 10-minute rolling average (±5 minutes window) to all 28
            emotion dimensions to smooth dialogue-level noise while preserving
            narrative arc. Preserves ALL minutes (no filtering to peaks).

Example Query:
    SELECT minute_offset, emotion_joy, emotion_fear, emotion_sadness
    FROM main_marts.mart_film_emotion_timeseries
    WHERE film_title = 'Spirited Away' AND language_code = 'en'
    ORDER BY minute_offset;
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
)

-- Apply 10-minute rolling average to all emotions (preserving ALL minutes)
SELECT
    film_id,
    film_title,
    language_code,
    minute_offset,
    dialogue_count,
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
ORDER BY film_title, language_code, minute_offset
