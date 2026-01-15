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

skip_survey_freeform_responses AS (
    SELECT
        customer_id
        , term_id
        , skip_feedback
FROM {{ ref('skip_reasons') }}
WHERE skip_feedback IS NOT NULL 
    AND LENGTH(skip_feedback) > 5
    AND term_id = ({{ latest_completed_term() }} + 1)
)

, agg_skip_survey_responses AS (
    SELECT 
        term_id
        , ARRAY_AGG(skip_feedback) AS agg_responses
    FROM skip_survey_freeform_responses
    GROUP BY 1
)

SELECT 
    term_id
    , {{ cortex_summarize('agg_responses', 'summarize_skip_responses', 'claude-3-5-sonnet') }} AS skip_survey_summary
FROM agg_skip_survey_responses
