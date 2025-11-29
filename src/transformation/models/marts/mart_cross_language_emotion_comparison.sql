{{
    config(
        materialized='table',
        description='Cross-language emotion comparison matrix for all film pairs and language pairs'
    )
}}

WITH film_emotions AS (
    -- Source: mart_film_emotion_summary (film-level emotions by language)
    SELECT
        film_id,
        film_title,
        language_code,
        emotion_admiration,
        emotion_amusement,
        emotion_anger,
        emotion_annoyance,
        emotion_approval,
        emotion_caring,
        emotion_confusion,
        emotion_curiosity,
        emotion_desire,
        emotion_disappointment,
        emotion_disapproval,
        emotion_disgust,
        emotion_embarrassment,
        emotion_excitement,
        emotion_fear,
        emotion_gratitude,
        emotion_grief,
        emotion_joy,
        emotion_love,
        emotion_nervousness,
        emotion_neutral,
        emotion_optimism,
        emotion_pride,
        emotion_realization,
        emotion_relief,
        emotion_remorse,
        emotion_sadness,
        emotion_surprise
    FROM {{ ref('mart_film_emotion_summary') }}
),

-- Unpivot 28 emotion columns into rows
emotions_unpivoted AS (
    SELECT film_id, film_title, language_code, 'admiration' AS emotion_type, emotion_admiration AS avg_score
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'amusement', emotion_amusement
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'anger', emotion_anger
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'annoyance', emotion_annoyance
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'approval', emotion_approval
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'caring', emotion_caring
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'confusion', emotion_confusion
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'curiosity', emotion_curiosity
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'desire', emotion_desire
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'disappointment', emotion_disappointment
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'disapproval', emotion_disapproval
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'disgust', emotion_disgust
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'embarrassment', emotion_embarrassment
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'excitement', emotion_excitement
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'fear', emotion_fear
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'gratitude', emotion_gratitude
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'grief', emotion_grief
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'joy', emotion_joy
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'love', emotion_love
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'nervousness', emotion_nervousness
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'neutral', emotion_neutral
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'optimism', emotion_optimism
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'pride', emotion_pride
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'realization', emotion_realization
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'relief', emotion_relief
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'remorse', emotion_remorse
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'sadness', emotion_sadness
    FROM film_emotions
    UNION ALL
    SELECT film_id, film_title, language_code, 'surprise', emotion_surprise
    FROM film_emotions
),

-- Self-join to create language pairs
language_pairs AS (
    SELECT
        a.film_id,
        a.film_title,
        a.language_code AS language_a,
        b.language_code AS language_b,
        a.emotion_type,
        a.avg_score AS avg_score_lang_a,
        b.avg_score AS avg_score_lang_b,
        (b.avg_score - a.avg_score) AS difference_score,
        CASE
            WHEN a.avg_score = 0 THEN NULL  -- Avoid division by zero
            ELSE ROUND(((b.avg_score - a.avg_score) / a.avg_score * 100), 4)
        END AS percent_difference
    FROM emotions_unpivoted a
    INNER JOIN emotions_unpivoted b
        ON a.film_id = b.film_id
        AND a.emotion_type = b.emotion_type
        AND a.language_code < b.language_code  -- Avoid duplicate pairs (EN-FR vs FR-EN)
)

SELECT
    film_id,
    film_title,
    language_a,
    language_b,
    emotion_type,
    avg_score_lang_a,
    avg_score_lang_b,
    difference_score,
    percent_difference,
    CASE
        WHEN percent_difference IS NULL THEN FALSE
        WHEN ABS(percent_difference) > 20 THEN TRUE
        ELSE FALSE
    END AS is_significant
FROM language_pairs
ORDER BY film_id, language_a, language_b, emotion_type
