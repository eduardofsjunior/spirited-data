{{
    config(
        materialized='view',
        tags=['marts', 'analytics', 'correlation']
    )
}}

/*
Sentiment-Success Correlation Analysis View
============================================
Combines sentiment metrics with success indicators for correlation studies.

Purpose: Answer the core research question:
"Does sentiment arc or sentiment matrix correlate with revenue or critic reception?"

This view joins:
- Film sentiment summaries (emotional metrics)
- Success metrics (revenue, ratings, reception)

Enables queries like:
- "What is the correlation between avg_compound_sentiment and box_office_revenue?"
- "Do films with rising sentiment trajectories have better critic scores?"
- "Is sentiment variance associated with audience satisfaction?"

Use with correlate_metrics() RAG tool:
- correlate_metrics(metric_x="sentiment", metric_y="box_office")
- correlate_metrics(metric_x="sentiment", metric_y="rt_score")
*/

SELECT
    -- Film identifiers
    sent.film_id,
    sent.film_title,
    sent.release_year,
    sent.director,

    -- Sentiment metrics
    sent.avg_compound_sentiment,
    sent.sentiment_variance,
    sent.peak_positive_sentiment,
    sent.peak_negative_sentiment,
    sent.emotional_range,
    sent.sentiment_trajectory,
    sent.overall_emotional_tone,
    sent.beginning_sentiment,
    sent.ending_sentiment,

    -- Success metrics
    succ.critic_score,
    succ.box_office_revenue,
    succ.production_budget,
    succ.roi,
    succ.revenue_zscore,
    succ.critic_zscore,
    succ.revenue_tier,
    succ.critical_reception_tier,
    succ.composite_success_score,

    -- Data quality indicators
    sent.data_points_count AS sentiment_data_points,
    sent.total_dialogue_lines,
    succ.has_revenue_data,
    succ.has_critic_data,

    -- Computed correlation-ready flags
    CASE
        WHEN sent.avg_compound_sentiment IS NOT NULL
         AND succ.box_office_revenue IS NOT NULL
        THEN TRUE ELSE FALSE
    END AS has_sentiment_revenue_pair,

    CASE
        WHEN sent.avg_compound_sentiment IS NOT NULL
         AND succ.critic_score IS NOT NULL
        THEN TRUE ELSE FALSE
    END AS has_sentiment_critic_pair

FROM {{ ref('mart_film_sentiment_summary') }} sent
INNER JOIN {{ ref('mart_film_success_metrics') }} succ
    ON sent.film_id = succ.film_id

-- Only include films with valid sentiment data
WHERE sent.avg_compound_sentiment IS NOT NULL
  AND sent.data_points_count >= 10  -- Minimum data quality threshold

ORDER BY sent.release_year DESC, sent.film_title
