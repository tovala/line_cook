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

menu_survey_responses AS (
    SELECT 
        customer_id
        , term_id
        , user_comment
    FROM {{ ref('menu_ratings') }} 
    WHERE user_comment IS NOT NULL
        AND LENGTH(user_comment) > 5
        AND term_id = ({{ latest_completed_term() }} + 1)
)

, agg_menu_survey_responses AS (
    SELECT 
        term_id
        , ARRAY_AGG(user_comment) AS agg_comments
    FROM menu_survey_responses
    GROUP BY 1
)

SELECT 
    term_id
    , {{ cortex_summarize('agg_comments', 'summarize_negative_menu_feedback', 'claude-3-5-sonnet') }} AS menu_survey_summary_negative
    , {{ cortex_summarize('agg_comments', 'summarize_positive_menu_feedback', 'claude-3-5-sonnet') }} AS menu_survey_summary_positive
FROM agg_menu_survey_responses
