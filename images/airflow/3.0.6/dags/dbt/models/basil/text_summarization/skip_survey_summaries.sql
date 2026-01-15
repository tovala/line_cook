{{
    config(
        tags=['text_analytics']
    )
}}

SELECT * FROM {{ ref('skip_survey_summarization') }}
