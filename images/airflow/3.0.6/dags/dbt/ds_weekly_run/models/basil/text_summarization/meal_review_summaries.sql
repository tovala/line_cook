{{
    config(
        tags=['text_analytics']
    )
}}

SELECT *
FROM {{ ref('meal_review_summarization') }}
