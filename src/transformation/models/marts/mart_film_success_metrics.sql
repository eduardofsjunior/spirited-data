{{
    config(
        materialized='table',
        tags=['marts', 'success', 'analytics']
    )
}}

/*
Film Success Metrics Mart
==========================
Consolidates multiple success indicators for Studio Ghibli films.

Purpose: Enable correlation studies between sentiment and various success metrics.

Metrics:
- Box office revenue (from Kaggle dataset)
- Critic scores (Rotten Tomatoes)
- Audience ratings (TMDB)
- Normalized z-scores for fair comparison across metric types

Business Questions:
- What predicts box office success?
- Do critics and audiences agree?
- Is sentiment a leading indicator of commercial performance?
*/

WITH film_base AS (
    SELECT
        f.id AS film_id,
        f.title,
        f.release_year,
        f.director,
        f.rt_score,
        k.revenue AS box_office_revenue,
        k.budget AS production_budget
    FROM {{ ref('stg_films') }} f
    LEFT JOIN {{ ref('stg_kaggle_films') }} k
        ON f.id = k.film_id
),

metric_statistics AS (
    SELECT
        AVG(box_office_revenue) AS avg_revenue,
        STDDEV(box_office_revenue) AS stddev_revenue,
        AVG(rt_score) AS avg_rt_score,
        STDDEV(rt_score) AS stddev_rt_score
    FROM film_base
    WHERE box_office_revenue IS NOT NULL  -- Only compute stats from films with data
),

normalized_metrics AS (
    SELECT
        fb.*,

        -- Z-scores for normalized comparison
        CASE
            WHEN fb.box_office_revenue IS NOT NULL AND ms.stddev_revenue > 0
            THEN (fb.box_office_revenue - ms.avg_revenue) / ms.stddev_revenue
            ELSE NULL
        END AS revenue_zscore,

        CASE
            WHEN fb.rt_score IS NOT NULL AND ms.stddev_rt_score > 0
            THEN (fb.rt_score - ms.avg_rt_score) / ms.stddev_rt_score
            ELSE NULL
        END AS critic_zscore,

        -- ROI calculation
        CASE
            WHEN fb.production_budget IS NOT NULL AND fb.production_budget > 0
            THEN (fb.box_office_revenue - fb.production_budget) / fb.production_budget
            ELSE NULL
        END AS return_on_investment,

        -- Success tier classification
        CASE
            WHEN fb.box_office_revenue >= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY box_office_revenue) FROM film_base WHERE box_office_revenue IS NOT NULL) THEN 'top_quartile'
            WHEN fb.box_office_revenue >= (SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY box_office_revenue) FROM film_base WHERE box_office_revenue IS NOT NULL) THEN 'above_median'
            WHEN fb.box_office_revenue >= (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY box_office_revenue) FROM film_base WHERE box_office_revenue IS NOT NULL) THEN 'below_median'
            ELSE 'bottom_quartile'
        END AS revenue_tier,

        CASE
            WHEN fb.rt_score >= 90 THEN 'certified_fresh'
            WHEN fb.rt_score >= 75 THEN 'fresh'
            WHEN fb.rt_score >= 60 THEN 'mixed'
            ELSE 'rotten'
        END AS critical_reception_tier

    FROM film_base fb
    CROSS JOIN metric_statistics ms
)

SELECT
    film_id,
    title,
    release_year,
    director,

    -- Raw success metrics
    rt_score AS critic_score,
    box_office_revenue,
    production_budget,
    return_on_investment AS roi,

    -- Normalized metrics (for correlation analysis)
    ROUND(revenue_zscore, 4) AS revenue_zscore,
    ROUND(critic_zscore, 4) AS critic_zscore,

    -- Classification tiers
    revenue_tier,
    critical_reception_tier,

    -- Data completeness flags
    CASE WHEN box_office_revenue IS NOT NULL THEN TRUE ELSE FALSE END AS has_revenue_data,
    CASE WHEN rt_score IS NOT NULL THEN TRUE ELSE FALSE END AS has_critic_data,

    -- Composite success score (average of available z-scores)
    ROUND(
        (COALESCE(revenue_zscore, 0) + COALESCE(critic_zscore, 0)) /
        NULLIF(
            CAST(CASE WHEN revenue_zscore IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) +
            CAST(CASE WHEN critic_zscore IS NOT NULL THEN 1 ELSE 0 END AS INTEGER),
            0
        ),
        4
    ) AS composite_success_score

FROM normalized_metrics
ORDER BY release_year DESC, title
