{{ config(
    alias='typeform_surveys') }}

SELECT 
  _airbyte_raw_id AS id 
  , id::STRING AS form_id 
  , type AS survey_type 
  , title::STRING AS survey_title 
  , _links:display::STRING AS survey_link
  , TO_TIMESTAMP_TZ(created_at) AS created_at
  , TO_TIMESTAMP_TZ(published_at) AS published_at
  , TO_TIMESTAMP_TZ(last_updated_at) AS last_updated_at
  , fields AS question_fields 
  , _airbyte_extracted_at AS upload_time  
FROM {{ source('typeform', 'forms') }}
