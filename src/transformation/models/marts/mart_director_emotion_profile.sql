{{
    config(
        materialized='table',
        tags=['marts', 'emotion', 'analytics', 'director']
    )
}}

/*
Director Emotion Profile Mart
==============================
Aggregates emotion patterns by director to enable cross-director analysis
and identify director "signature" emotional styles.

Purpose: Enable Epic 5 director profile visualizations comparing emotional
         styles between directors (Miyazaki vs Takahata).

Key Metrics:
- avg_emotion_*: Average scores for all 28 GoEmotions dimensions
- top_emotion_1/2/3: Highest average emotions per director
- emotion_diversity: Standard deviation across 28 emotions
- career_span_years: Career length from first to last film

Business Questions:
- What are Miyazaki's signature emotions compared to Takahata?
- Which directors have the most emotionally diverse filmography?
- How do director emotion profiles correlate with critical success?
*/

WITH film_emotions_by_minute AS (
    -- Get per-minute emotion data with film metadata
    SELECT
        f.id AS film_id,
        f.title,
        f.director,
        f.release_year,
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
    WHERE fe.language_code = 'en'  -- Use English as canonical language
),

director_aggregates AS (
    -- Aggregate to director level
    SELECT
        director,
        COUNT(DISTINCT film_id) AS film_count,
        COUNT(*) AS total_minutes_analyzed,

        -- Calculate average for all 28 emotion dimensions
        ROUND(AVG(emotion_admiration), 4) AS avg_emotion_admiration,
        ROUND(AVG(emotion_amusement), 4) AS avg_emotion_amusement,
        ROUND(AVG(emotion_anger), 4) AS avg_emotion_anger,
        ROUND(AVG(emotion_annoyance), 4) AS avg_emotion_annoyance,
        ROUND(AVG(emotion_approval), 4) AS avg_emotion_approval,
        ROUND(AVG(emotion_caring), 4) AS avg_emotion_caring,
        ROUND(AVG(emotion_confusion), 4) AS avg_emotion_confusion,
        ROUND(AVG(emotion_curiosity), 4) AS avg_emotion_curiosity,
        ROUND(AVG(emotion_desire), 4) AS avg_emotion_desire,
        ROUND(AVG(emotion_disappointment), 4) AS avg_emotion_disappointment,
        ROUND(AVG(emotion_disapproval), 4) AS avg_emotion_disapproval,
        ROUND(AVG(emotion_disgust), 4) AS avg_emotion_disgust,
        ROUND(AVG(emotion_embarrassment), 4) AS avg_emotion_embarrassment,
        ROUND(AVG(emotion_excitement), 4) AS avg_emotion_excitement,
        ROUND(AVG(emotion_fear), 4) AS avg_emotion_fear,
        ROUND(AVG(emotion_gratitude), 4) AS avg_emotion_gratitude,
        ROUND(AVG(emotion_grief), 4) AS avg_emotion_grief,
        ROUND(AVG(emotion_joy), 4) AS avg_emotion_joy,
        ROUND(AVG(emotion_love), 4) AS avg_emotion_love,
        ROUND(AVG(emotion_nervousness), 4) AS avg_emotion_nervousness,
        ROUND(AVG(emotion_optimism), 4) AS avg_emotion_optimism,
        ROUND(AVG(emotion_pride), 4) AS avg_emotion_pride,
        ROUND(AVG(emotion_realization), 4) AS avg_emotion_realization,
        ROUND(AVG(emotion_relief), 4) AS avg_emotion_relief,
        ROUND(AVG(emotion_remorse), 4) AS avg_emotion_remorse,
        ROUND(AVG(emotion_sadness), 4) AS avg_emotion_sadness,
        ROUND(AVG(emotion_surprise), 4) AS avg_emotion_surprise,
        ROUND(AVG(emotion_neutral), 4) AS avg_emotion_neutral,

        -- Career span metrics
        MIN(release_year) AS earliest_film_year,
        MAX(release_year) AS latest_film_year,
        (MAX(release_year) - MIN(release_year)) AS career_span_years
    FROM film_emotions_by_minute
    WHERE director IS NOT NULL
    GROUP BY director
),

-- Calculate emotion diversity (standard deviation across 28 emotions)
director_with_diversity AS (
    SELECT
        *,
        ROUND(
            SQRT(
                (POW(avg_emotion_admiration - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_amusement - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_anger - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_annoyance - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_approval - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_caring - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_confusion - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_curiosity - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_desire - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_disappointment - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_disapproval - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_disgust - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_embarrassment - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_excitement - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_fear - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_gratitude - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_grief - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_joy - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_love - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_nervousness - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_optimism - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_pride - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_realization - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_relief - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_remorse - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_sadness - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_surprise - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2) +
                 POW(avg_emotion_neutral - (avg_emotion_admiration + avg_emotion_amusement + avg_emotion_anger + avg_emotion_annoyance + avg_emotion_approval + avg_emotion_caring + avg_emotion_confusion + avg_emotion_curiosity + avg_emotion_desire + avg_emotion_disappointment + avg_emotion_disapproval + avg_emotion_disgust + avg_emotion_embarrassment + avg_emotion_excitement + avg_emotion_fear + avg_emotion_gratitude + avg_emotion_grief + avg_emotion_joy + avg_emotion_love + avg_emotion_nervousness + avg_emotion_optimism + avg_emotion_pride + avg_emotion_realization + avg_emotion_relief + avg_emotion_remorse + avg_emotion_sadness + avg_emotion_surprise + avg_emotion_neutral) / 28.0, 2)
                ) / 28.0
            ), 4
        ) AS emotion_diversity
    FROM director_aggregates
),

-- Unpivot emotions to find top 3 (excluding neutral)
emotions_unpivoted AS (
    SELECT director, 'admiration' AS emotion_name, avg_emotion_admiration AS score FROM director_with_diversity
    UNION ALL SELECT director, 'amusement', avg_emotion_amusement FROM director_with_diversity
    UNION ALL SELECT director, 'anger', avg_emotion_anger FROM director_with_diversity
    UNION ALL SELECT director, 'annoyance', avg_emotion_annoyance FROM director_with_diversity
    UNION ALL SELECT director, 'approval', avg_emotion_approval FROM director_with_diversity
    UNION ALL SELECT director, 'caring', avg_emotion_caring FROM director_with_diversity
    UNION ALL SELECT director, 'confusion', avg_emotion_confusion FROM director_with_diversity
    UNION ALL SELECT director, 'curiosity', avg_emotion_curiosity FROM director_with_diversity
    UNION ALL SELECT director, 'desire', avg_emotion_desire FROM director_with_diversity
    UNION ALL SELECT director, 'disappointment', avg_emotion_disappointment FROM director_with_diversity
    UNION ALL SELECT director, 'disapproval', avg_emotion_disapproval FROM director_with_diversity
    UNION ALL SELECT director, 'disgust', avg_emotion_disgust FROM director_with_diversity
    UNION ALL SELECT director, 'embarrassment', avg_emotion_embarrassment FROM director_with_diversity
    UNION ALL SELECT director, 'excitement', avg_emotion_excitement FROM director_with_diversity
    UNION ALL SELECT director, 'fear', avg_emotion_fear FROM director_with_diversity
    UNION ALL SELECT director, 'gratitude', avg_emotion_gratitude FROM director_with_diversity
    UNION ALL SELECT director, 'grief', avg_emotion_grief FROM director_with_diversity
    UNION ALL SELECT director, 'joy', avg_emotion_joy FROM director_with_diversity
    UNION ALL SELECT director, 'love', avg_emotion_love FROM director_with_diversity
    UNION ALL SELECT director, 'nervousness', avg_emotion_nervousness FROM director_with_diversity
    UNION ALL SELECT director, 'optimism', avg_emotion_optimism FROM director_with_diversity
    UNION ALL SELECT director, 'pride', avg_emotion_pride FROM director_with_diversity
    UNION ALL SELECT director, 'realization', avg_emotion_realization FROM director_with_diversity
    UNION ALL SELECT director, 'relief', avg_emotion_relief FROM director_with_diversity
    UNION ALL SELECT director, 'remorse', avg_emotion_remorse FROM director_with_diversity
    UNION ALL SELECT director, 'sadness', avg_emotion_sadness FROM director_with_diversity
    UNION ALL SELECT director, 'surprise', avg_emotion_surprise FROM director_with_diversity
    -- Exclude neutral from top emotion ranking
),

top_emotions_ranked AS (
    SELECT
        director,
        emotion_name,
        score,
        ROW_NUMBER() OVER (PARTITION BY director ORDER BY score DESC) AS emotion_rank
    FROM emotions_unpivoted
),

top_3_emotions AS (
    SELECT
        director,
        MAX(CASE WHEN emotion_rank = 1 THEN emotion_name END) AS top_emotion_1,
        MAX(CASE WHEN emotion_rank = 1 THEN score END) AS top_emotion_1_score,
        MAX(CASE WHEN emotion_rank = 2 THEN emotion_name END) AS top_emotion_2,
        MAX(CASE WHEN emotion_rank = 2 THEN score END) AS top_emotion_2_score,
        MAX(CASE WHEN emotion_rank = 3 THEN emotion_name END) AS top_emotion_3,
        MAX(CASE WHEN emotion_rank = 3 THEN score END) AS top_emotion_3_score
    FROM top_emotions_ranked
    WHERE emotion_rank <= 3
    GROUP BY director
)

-- Final output combining all metrics
SELECT
    d.director,
    d.film_count,
    d.total_minutes_analyzed,

    -- All 28 emotion averages
    d.avg_emotion_admiration,
    d.avg_emotion_amusement,
    d.avg_emotion_anger,
    d.avg_emotion_annoyance,
    d.avg_emotion_approval,
    d.avg_emotion_caring,
    d.avg_emotion_confusion,
    d.avg_emotion_curiosity,
    d.avg_emotion_desire,
    d.avg_emotion_disappointment,
    d.avg_emotion_disapproval,
    d.avg_emotion_disgust,
    d.avg_emotion_embarrassment,
    d.avg_emotion_excitement,
    d.avg_emotion_fear,
    d.avg_emotion_gratitude,
    d.avg_emotion_grief,
    d.avg_emotion_joy,
    d.avg_emotion_love,
    d.avg_emotion_nervousness,
    d.avg_emotion_optimism,
    d.avg_emotion_pride,
    d.avg_emotion_realization,
    d.avg_emotion_relief,
    d.avg_emotion_remorse,
    d.avg_emotion_sadness,
    d.avg_emotion_surprise,
    d.avg_emotion_neutral,

    -- Top 3 emotions
    t.top_emotion_1,
    ROUND(t.top_emotion_1_score, 4) AS top_emotion_1_score,
    t.top_emotion_2,
    ROUND(t.top_emotion_2_score, 4) AS top_emotion_2_score,
    t.top_emotion_3,
    ROUND(t.top_emotion_3_score, 4) AS top_emotion_3_score,

    -- Diversity and career metrics
    d.emotion_diversity,
    d.earliest_film_year,
    d.latest_film_year,
    d.career_span_years

FROM director_with_diversity d
LEFT JOIN top_3_emotions t ON d.director = t.director
ORDER BY d.film_count DESC, d.director
