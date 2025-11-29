{{
    config(
        materialized='table',
        description='Aggregated emotion vectors per film and language for similarity calculations'
    )
}}

WITH emotion_aggregates AS (
    SELECT
        fe.film_id,
        fe.language_code,

        -- Average emotion scores across all minutes (28 dimensions)
        AVG(fe.emotion_admiration) AS emotion_admiration,
        AVG(fe.emotion_amusement) AS emotion_amusement,
        AVG(fe.emotion_anger) AS emotion_anger,
        AVG(fe.emotion_annoyance) AS emotion_annoyance,
        AVG(fe.emotion_approval) AS emotion_approval,
        AVG(fe.emotion_caring) AS emotion_caring,
        AVG(fe.emotion_confusion) AS emotion_confusion,
        AVG(fe.emotion_curiosity) AS emotion_curiosity,
        AVG(fe.emotion_desire) AS emotion_desire,
        AVG(fe.emotion_disappointment) AS emotion_disappointment,
        AVG(fe.emotion_disapproval) AS emotion_disapproval,
        AVG(fe.emotion_disgust) AS emotion_disgust,
        AVG(fe.emotion_embarrassment) AS emotion_embarrassment,
        AVG(fe.emotion_excitement) AS emotion_excitement,
        AVG(fe.emotion_fear) AS emotion_fear,
        AVG(fe.emotion_gratitude) AS emotion_gratitude,
        AVG(fe.emotion_grief) AS emotion_grief,
        AVG(fe.emotion_joy) AS emotion_joy,
        AVG(fe.emotion_love) AS emotion_love,
        AVG(fe.emotion_nervousness) AS emotion_nervousness,
        AVG(fe.emotion_neutral) AS emotion_neutral,
        AVG(fe.emotion_optimism) AS emotion_optimism,
        AVG(fe.emotion_pride) AS emotion_pride,
        AVG(fe.emotion_realization) AS emotion_realization,
        AVG(fe.emotion_relief) AS emotion_relief,
        AVG(fe.emotion_remorse) AS emotion_remorse,
        AVG(fe.emotion_sadness) AS emotion_sadness,
        AVG(fe.emotion_surprise) AS emotion_surprise

    FROM {{ source('raw', 'film_emotions') }} fe
    GROUP BY fe.film_id, fe.language_code
)

SELECT
    ea.film_id,
    f.title AS film_title,
    ea.language_code,

    -- All 28 emotion dimensions
    ea.emotion_admiration,
    ea.emotion_amusement,
    ea.emotion_anger,
    ea.emotion_annoyance,
    ea.emotion_approval,
    ea.emotion_caring,
    ea.emotion_confusion,
    ea.emotion_curiosity,
    ea.emotion_desire,
    ea.emotion_disappointment,
    ea.emotion_disapproval,
    ea.emotion_disgust,
    ea.emotion_embarrassment,
    ea.emotion_excitement,
    ea.emotion_fear,
    ea.emotion_gratitude,
    ea.emotion_grief,
    ea.emotion_joy,
    ea.emotion_love,
    ea.emotion_nervousness,
    ea.emotion_neutral,
    ea.emotion_optimism,
    ea.emotion_pride,
    ea.emotion_realization,
    ea.emotion_relief,
    ea.emotion_remorse,
    ea.emotion_sadness,
    ea.emotion_surprise

FROM emotion_aggregates ea
JOIN {{ ref('stg_films') }} f ON ea.film_id = f.id
