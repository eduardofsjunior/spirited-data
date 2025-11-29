{{
    config(
        materialized='table',
        description='Pre-computed cosine similarity matrix for all film pairs based on 27-dimension emotion vectors (excluding neutral)'
    )
}}

WITH emotion_vectors AS (
    SELECT
        film_id,
        film_title,
        language_code,
        -- 27 emotion dimensions (excluding neutral)
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
        emotion_optimism,
        emotion_pride,
        emotion_realization,
        emotion_relief,
        emotion_remorse,
        emotion_sadness,
        emotion_surprise
    FROM {{ ref('mart_film_emotion_summary') }}
),

film_pairs AS (
    SELECT
        a.film_id AS film_id_a,
        a.film_title AS film_title_a,
        b.film_id AS film_id_b,
        b.film_title AS film_title_b,
        a.language_code,

        -- Dot product (numerator)
        (a.emotion_admiration * b.emotion_admiration +
         a.emotion_amusement * b.emotion_amusement +
         a.emotion_anger * b.emotion_anger +
         a.emotion_annoyance * b.emotion_annoyance +
         a.emotion_approval * b.emotion_approval +
         a.emotion_caring * b.emotion_caring +
         a.emotion_confusion * b.emotion_confusion +
         a.emotion_curiosity * b.emotion_curiosity +
         a.emotion_desire * b.emotion_desire +
         a.emotion_disappointment * b.emotion_disappointment +
         a.emotion_disapproval * b.emotion_disapproval +
         a.emotion_disgust * b.emotion_disgust +
         a.emotion_embarrassment * b.emotion_embarrassment +
         a.emotion_excitement * b.emotion_excitement +
         a.emotion_fear * b.emotion_fear +
         a.emotion_gratitude * b.emotion_gratitude +
         a.emotion_grief * b.emotion_grief +
         a.emotion_joy * b.emotion_joy +
         a.emotion_love * b.emotion_love +
         a.emotion_nervousness * b.emotion_nervousness +
         a.emotion_optimism * b.emotion_optimism +
         a.emotion_pride * b.emotion_pride +
         a.emotion_realization * b.emotion_realization +
         a.emotion_relief * b.emotion_relief +
         a.emotion_remorse * b.emotion_remorse +
         a.emotion_sadness * b.emotion_sadness +
         a.emotion_surprise * b.emotion_surprise) AS dot_product,

        -- L2 norms (denominator components)
        SQRT(a.emotion_admiration * a.emotion_admiration +
             a.emotion_amusement * a.emotion_amusement +
             a.emotion_anger * a.emotion_anger +
             a.emotion_annoyance * a.emotion_annoyance +
             a.emotion_approval * a.emotion_approval +
             a.emotion_caring * a.emotion_caring +
             a.emotion_confusion * a.emotion_confusion +
             a.emotion_curiosity * a.emotion_curiosity +
             a.emotion_desire * a.emotion_desire +
             a.emotion_disappointment * a.emotion_disappointment +
             a.emotion_disapproval * a.emotion_disapproval +
             a.emotion_disgust * a.emotion_disgust +
             a.emotion_embarrassment * a.emotion_embarrassment +
             a.emotion_excitement * a.emotion_excitement +
             a.emotion_fear * a.emotion_fear +
             a.emotion_gratitude * a.emotion_gratitude +
             a.emotion_grief * a.emotion_grief +
             a.emotion_joy * a.emotion_joy +
             a.emotion_love * a.emotion_love +
             a.emotion_nervousness * a.emotion_nervousness +
             a.emotion_optimism * a.emotion_optimism +
             a.emotion_pride * a.emotion_pride +
             a.emotion_realization * a.emotion_realization +
             a.emotion_relief * a.emotion_relief +
             a.emotion_remorse * a.emotion_remorse +
             a.emotion_sadness * a.emotion_sadness +
             a.emotion_surprise * a.emotion_surprise) AS norm_a,

        SQRT(b.emotion_admiration * b.emotion_admiration +
             b.emotion_amusement * b.emotion_amusement +
             b.emotion_anger * b.emotion_anger +
             b.emotion_annoyance * b.emotion_annoyance +
             b.emotion_approval * b.emotion_approval +
             b.emotion_caring * b.emotion_caring +
             b.emotion_confusion * b.emotion_confusion +
             b.emotion_curiosity * b.emotion_curiosity +
             b.emotion_desire * b.emotion_desire +
             b.emotion_disappointment * b.emotion_disappointment +
             b.emotion_disapproval * b.emotion_disapproval +
             b.emotion_disgust * b.emotion_disgust +
             b.emotion_embarrassment * b.emotion_embarrassment +
             b.emotion_excitement * b.emotion_excitement +
             b.emotion_fear * b.emotion_fear +
             b.emotion_gratitude * b.emotion_gratitude +
             b.emotion_grief * b.emotion_grief +
             b.emotion_joy * b.emotion_joy +
             b.emotion_love * b.emotion_love +
             b.emotion_nervousness * b.emotion_nervousness +
             b.emotion_optimism * b.emotion_optimism +
             b.emotion_pride * b.emotion_pride +
             b.emotion_realization * b.emotion_realization +
             b.emotion_relief * b.emotion_relief +
             b.emotion_remorse * b.emotion_remorse +
             b.emotion_sadness * b.emotion_sadness +
             b.emotion_surprise * b.emotion_surprise) AS norm_b

    FROM emotion_vectors a
    CROSS JOIN emotion_vectors b
    WHERE a.language_code = b.language_code
      AND a.film_id != b.film_id  -- Exclude self-similarity
),

similarity_scores AS (
    SELECT
        film_id_a,
        film_id_b,
        film_title_a,
        film_title_b,
        language_code,
        dot_product / (norm_a * norm_b) AS similarity_score,
        ROW_NUMBER() OVER (
            PARTITION BY film_id_a, language_code
            ORDER BY (dot_product / (norm_a * norm_b)) DESC
        ) AS similarity_rank
    FROM film_pairs
)

SELECT
    film_id_a,
    film_id_b,
    film_title_a,
    film_title_b,
    language_code,
    similarity_score,
    similarity_rank
FROM similarity_scores
WHERE similarity_rank <= 10  -- Top 10 only
ORDER BY film_id_a, language_code, similarity_rank
