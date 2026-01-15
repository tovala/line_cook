{{
    config(
        tags=['text_analytics'],
        materialized='incremental',
        unique_key='term_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH

target_reviews AS (
    SELECT
        term_id
        , ARRAY_AGG(comment) AS comments
    FROM {{ ref('reviews') }}
    WHERE comment IS NOT NULL
        AND term_id = ({{ latest_completed_term() }} - 1)
    GROUP BY 1
)

SELECT 
    term_id
    , {{ cortex_summarize('comments', 'summarize_term_meal_reviews', 'claude-3-5-sonnet') }} AS term_meal_review_summary
FROM target_reviews
