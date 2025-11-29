{{
    config(
        materialized='table',
        description='Joins Kaggle box office data with emotion summaries for correlation analysis'
    )
}}

/*
Kaggle Emotion Correlation Mart
================================
Joins financial data (budget, revenue) with emotion analysis for each film-language pair.

Purpose: Enable Epic 5 correlation studies like "Do joyful films perform better at box office?"

Data Sources:
- stg_kaggle_films: Box office, budget, revenue, duration
- mart_film_emotion_summary: 28-dimension emotion vectors per film-language

Grain: One row per film-language pair (film_id + language_code)

Key Metrics:
- Top 7 emotions selected for analysis (joy, sadness, fear, anger, love, excitement, neutral)
- top_emotion: Dominant emotion (highest average score)
- emotion_diversity: Standard deviation across 7 emotions
*/

WITH kaggle_emotion_join AS (
    -- Join Kaggle box office data with emotion summaries
    -- One row per film-language pair where both datasets have data
    SELECT
        -- Film metadata from Kaggle
        k.film_id,
        k.title AS film_title,
        k.release_year,
        k.director,
        k.budget,
        k.revenue,
        k.duration,

        -- Language from emotion analysis
        e.language_code,

        -- Top 7 emotions for correlation analysis
        -- These emotions represent the most interpretable dimensions for box office correlation:
        -- - joy: Positive sentiment, family-friendly appeal
        -- - sadness: Emotional depth, critical acclaim indicator
        -- - fear: Suspense, adventure elements
        -- - anger: Conflict intensity
        -- - love: Romance, relationship themes
        -- - excitement: Action, engagement
        -- - neutral: Calm, contemplative tone
        e.emotion_joy,
        e.emotion_sadness,
        e.emotion_fear,
        e.emotion_anger,
        e.emotion_love,
        e.emotion_excitement,
        e.emotion_neutral

    FROM {{ ref('stg_kaggle_films') }} k
    INNER JOIN {{ ref('mart_film_emotion_summary') }} e
        ON k.film_id = e.film_id
),

emotion_stats AS (
    -- Pre-calculate statistics to avoid redundant computation
    -- This CTE improves performance and readability by computing mean and max once per row
    SELECT
        *,
        -- Calculate mean once for emotion_diversity formula
        (emotion_joy + emotion_sadness + emotion_fear + emotion_anger +
         emotion_love + emotion_excitement + emotion_neutral) / 7.0 AS emotion_mean,

        -- Calculate max score once for top_emotion determination
        GREATEST(
            emotion_joy,
            emotion_sadness,
            emotion_fear,
            emotion_anger,
            emotion_love,
            emotion_excitement,
            emotion_neutral
        ) AS max_emotion_score
    FROM kaggle_emotion_join
)

SELECT
    film_id,
    film_title,
    release_year,
    director,
    budget,
    revenue,
    duration,
    language_code,

    -- Top 7 emotion averages
    emotion_joy AS avg_emotion_joy,
    emotion_sadness AS avg_emotion_sadness,
    emotion_fear AS avg_emotion_fear,
    emotion_anger AS avg_emotion_anger,
    emotion_love AS avg_emotion_love,
    emotion_excitement AS avg_emotion_excitement,
    emotion_neutral AS avg_emotion_neutral,

    -- Derived metric 1: Top emotion (emotion with highest average score)
    -- Refactored to use pre-calculated max_emotion_score (eliminates 7 redundant GREATEST calls)
    CASE
        WHEN max_emotion_score = emotion_joy THEN 'joy'
        WHEN max_emotion_score = emotion_sadness THEN 'sadness'
        WHEN max_emotion_score = emotion_fear THEN 'fear'
        WHEN max_emotion_score = emotion_anger THEN 'anger'
        WHEN max_emotion_score = emotion_love THEN 'love'
        WHEN max_emotion_score = emotion_excitement THEN 'excitement'
        WHEN max_emotion_score = emotion_neutral THEN 'neutral'
        ELSE 'neutral'  -- Default fallback
    END AS top_emotion,

    -- Derived metric 2: Emotion diversity (standard deviation across 7 emotions)
    -- Refactored to use pre-calculated emotion_mean (eliminates 7 redundant mean calculations)
    -- Formula: STDDEV_POP = SQRT(SUM((x_i - mean)^2) / N)
    -- Higher values indicate more emotional variation; lower values indicate focus on one emotion
    ROUND(
        SQRT(
            (POW(emotion_joy - emotion_mean, 2) +
             POW(emotion_sadness - emotion_mean, 2) +
             POW(emotion_fear - emotion_mean, 2) +
             POW(emotion_anger - emotion_mean, 2) +
             POW(emotion_love - emotion_mean, 2) +
             POW(emotion_excitement - emotion_mean, 2) +
             POW(emotion_neutral - emotion_mean, 2)
            ) / 7.0
        ), 4
    ) AS emotion_diversity

FROM emotion_stats
ORDER BY release_year DESC, film_title, language_code
