{{ config(
    alias='typeform_answers',
    materialized='incremental', 
    unique_key="answer_id") }}

SELECT 
  {{ hash_natural_key('landing_id', 'value:field:id::STRING') }} AS answer_id
  , form_id 
  , landing_id
  , value:field:id::STRING AS question_id
  , value:field:type::STRING AS answer_type -- values
  , value:type::STRING AS data_type  -- values
  , CASE WHEN data_type = 'boolean' 
        THEN value:boolean::BOOLEAN 
    END AS boolean_answer
  , CASE WHEN data_type = 'number' 
        THEN value:number::INTEGER
    END AS numeric_answer
  , CASE WHEN data_type = 'choice' AND value:choice:label IS NOT NULL
         THEN ARRAY_CONSTRUCT(value:choice:label)
         WHEN data_type = 'choices'
         THEN value:choices:labels
    END AS multiple_choice_answer
  , CASE WHEN data_type = 'choice'
         THEN value:choice:other::STRING
         WHEN data_type = 'choices'
         THEN value:choices:other::STRING
    END AS multiple_choice_other     
  , CASE WHEN data_type = 'email'
         THEN value:email::STRING
         WHEN data_type = 'phone_number'
         THEN value:phone_number::STRING
         WHEN data_type = 'text'
         THEN value:text::STRING
    END AS text_answer
  , CASE WHEN data_type = 'url'
         THEN value:url::STRING
    END AS url_answer
  , CASE WHEN data_type = 'file_url'
         THEN value:file_url::STRING
    END AS file_url_answer
  , CASE WHEN data_type = 'date'
         THEN value:date::DATETIME
    END AS date_answer
  , CASE WHEN data_type = 'multi_format'
         THEN value:multi_format:video_url::STRING
    END AS video_answer
  , upload_time 
FROM {{ table_reference('typeform_landings') }}, LATERAL FLATTEN (answers_json)
{% if is_incremental() %}
  WHERE upload_time >= (SELECT MAX(upload_time) FROM {{this}} )
{%- endif -%}
