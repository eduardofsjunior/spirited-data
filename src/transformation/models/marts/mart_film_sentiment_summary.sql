{{
    config(
        materialized='table',
        tags=['marts', 'sentiment', 'analytics']
    )
}}

/*
Sentiment Summary Mart
======================
Aggregates emotion data from subtitles to provide per-film sentiment metrics.

Purpose: Enable correlation analysis between emotional arcs and film success metrics.

Key Metrics:
- avg_compound_sentiment: Overall emotional tone (-1 to 1 scale)
- sentiment_variance: Emotional volatility throughout the film
- peak_positive/peak_negative: Highest/lowest emotional moments
- sentiment_trajectory: Overall arc direction (rising/falling/stable)

Business Questions:
- Do films with positive sentiment perform better at the box office?
- Is sentiment variance correlated with critic reception?
- Do rising arcs correlate with audience satisfaction?
*/

WITH film_emotions_base AS (
    SELECT
        film_slug,
        minute_offset,
        -- Calculate compound sentiment (positive emotions - negative emotions)
        (
            emotion_admiration + emotion_amusement + emotion_approval + emotion_caring +
            emotion_excitement + emotion_gratitude + emotion_joy + emotion_love +
            emotion_optimism + emotion_pride + emotion_relief
        ) - (
            emotion_anger + emotion_annoyance + emotion_disappointment + emotion_disapproval +
            emotion_disgust + emotion_embarrassment + emotion_fear + emotion_grief +
            emotion_nervousness + emotion_remorse + emotion_sadness
        ) AS compound_sentiment,
        emotion_neutral,
        dialogue_count
    FROM {{ source('raw', 'film_emotions') }}
    WHERE language_code = 'en'  -- Use English as canonical language for metrics
),

film_minute_bounds AS (
    SELECT
        film_slug,
        MAX(minute_offset) AS max_minute
    FROM film_emotions_base
    GROUP BY film_slug
),

film_sentiment_aggregates AS (
    SELECT
        fe.film_slug,
        AVG(fe.compound_sentiment) AS avg_compound_sentiment,
        STDDEV(fe.compound_sentiment) AS sentiment_variance,
        MAX(fe.compound_sentiment) AS peak_positive_sentiment,
        MIN(fe.compound_sentiment) AS peak_negative_sentiment,

        -- Find emotional range (distance between peak and trough)
        MAX(fe.compound_sentiment) - MIN(fe.compound_sentiment) AS emotional_range,

        -- Calculate trajectory (compare first 10% to last 10% of film)
        AVG(CASE
            WHEN fe.minute_offset <= (mb.max_minute * 0.1)
            THEN fe.compound_sentiment
        END) AS beginning_sentiment,
        AVG(CASE
            WHEN fe.minute_offset >= (mb.max_minute * 0.9)
            THEN fe.compound_sentiment
        END) AS ending_sentiment,

        -- Count data points for quality assessment
        COUNT(*) AS total_minutes,
        SUM(fe.dialogue_count) AS total_dialogue_lines,

        -- Identify predominant emotion zone
        AVG(fe.emotion_neutral) AS avg_neutral_score
    FROM film_emotions_base fe
    INNER JOIN film_minute_bounds mb ON fe.film_slug = mb.film_slug
    GROUP BY fe.film_slug
),

sentiment_trajectory_classification AS (
    SELECT
        *,
        CASE
            WHEN (ending_sentiment - beginning_sentiment) > 0.15 THEN 'rising'
            WHEN (ending_sentiment - beginning_sentiment) < -0.15 THEN 'falling'
            ELSE 'stable'
        END AS sentiment_trajectory,

        -- Classify overall emotional tone
        CASE
            WHEN avg_compound_sentiment > 0.1 THEN 'positive'
            WHEN avg_compound_sentiment < -0.1 THEN 'negative'
            ELSE 'neutral'
        END AS overall_emotional_tone
    FROM film_sentiment_aggregates
)

SELECT
    -- Extract film title from slug (strip language suffix)
    REGEXP_REPLACE(film_slug, '_[a-z]{2}$', '') AS film_slug_base,
    f.id AS film_id,
    f.title AS film_title,

    -- Sentiment metrics
    ROUND(st.avg_compound_sentiment, 4) AS avg_compound_sentiment,
    ROUND(st.sentiment_variance, 4) AS sentiment_variance,
    ROUND(st.peak_positive_sentiment, 4) AS peak_positive_sentiment,
    ROUND(st.peak_negative_sentiment, 4) AS peak_negative_sentiment,
    ROUND(st.emotional_range, 4) AS emotional_range,

    -- Trajectory metrics
    ROUND(st.beginning_sentiment, 4) AS beginning_sentiment,
    ROUND(st.ending_sentiment, 4) AS ending_sentiment,
    st.sentiment_trajectory,
    st.overall_emotional_tone,

    -- Quality metrics
    st.total_minutes AS data_points_count,
    st.total_dialogue_lines,
    ROUND(st.avg_neutral_score, 4) AS avg_neutral_score,

    -- Film metadata (for joining)
    f.release_year,
    f.director,
    f.rt_score

FROM sentiment_trajectory_classification st
LEFT JOIN {{ ref('stg_films') }} f
    ON LOWER(REPLACE(f.title, ' ', '_')) = REGEXP_REPLACE(st.film_slug, '_[a-z]{2}$', '')

WHERE st.total_minutes > 10  -- Filter out films with insufficient data
ORDER BY f.title
