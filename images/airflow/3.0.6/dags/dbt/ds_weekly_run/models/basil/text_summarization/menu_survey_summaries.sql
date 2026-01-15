{{
    config(
        tags=['text_analytics']
    )
}}

SELECT * FROM {{ ref('menu_survey_summarization') }} 
