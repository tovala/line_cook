{{
    config(
        tags=['text_analytics'],
        materialized='incremental',
        unique_key='meal_sku_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH

target_reviews AS (
    SELECT
        reviews.meal_sku_id
        , reviews.term_id
        , skus.title as meal
        , MEDIAN(snowflake.cortex.sentiment(comment)) AS median_sentiment
        , ARRAY_AGG(comment) AS comments
        , COUNT(review_id) AS num_reviews
    FROM {{ ref('reviews') }}
    LEFT JOIN {{ ref('meal_skus') }} AS skus
        ON reviews.meal_sku_id = skus.meal_sku_id::INT
    WHERE comment IS NOT NULL
        AND reviews.term_id = ({{ latest_completed_term() }} - 1)
    GROUP BY 1,2,3
)

SELECT
    meal_sku_id
    , term_id
    , meal
    , num_reviews
    , comments
    , median_sentiment
    , {{ cortex_summarize('comments', 'summarize_positive_meal_feedback', 'claude-3-5-sonnet') }} AS positive_summary
    , {{ cortex_summarize('comments', 'summarize_negative_meal_feedback', 'claude-3-5-sonnet') }} AS negative_summary
    , {{ cortex_summarize('comments', 'recommend_meal_improvements', 'claude-3-5-sonnet') }} AS recommendations
FROM target_reviews
WHERE num_reviews > 1
