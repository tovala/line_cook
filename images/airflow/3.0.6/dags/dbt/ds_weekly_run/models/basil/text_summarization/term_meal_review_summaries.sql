{{
    config(
        tags=['text_analytics']
    )
}}

SELECT * FROM {{ ref('term_meal_review_summarization') }}
