{{ config(
      alias='typeform_landings',
      materialized='incremental', 
      unique_key="landing_id") }}


SELECT 
  form_id 
  , landing_id
  , token
  , response_id
  , response_type 
  , TO_TIMESTAMP_TZ(submitted_at) AS submitted_at 
  , TO_TIMESTAMP_TZ(landed_at) AS landed_at 
  , COALESCE({{ clean_string('hidden:userid::STRING') }}, 
             {{ clean_string('hidden:user_id::STRING') }},
             {{ clean_string('hidden:customerid::STRING') }}) AS userid 
  , {{ clean_string('hidden:email::STRING') }} AS email
  , {{ clean_string('hidden:selection::STRING') }} AS selection 
  , {{ clean_string('hidden:termid::STRING') }} AS termid
  , {{ clean_string('hidden:source::STRING') }} AS source 
  , {{ clean_string('hidden:mealsonlysincenov16::STRING') }} AS mealsonlysincenov16
  , {{ clean_string('metadata:browser::STRING') }} AS browser 
  , {{ clean_string('metadata:network_id::STRING') }} AS network_id
  , {{ clean_string('metadata:platform::STRING') }} AS platform
  , {{ clean_string('metadata:referrer::STRING') }} AS referrer
  , {{ clean_string('metadata:user_agent::STRING') }} AS user_agent  
  , answers AS answers_json 
  , hidden AS hidden_json 
  , metadata AS metadata_json 
  , _airbyte_extracted_at AS upload_time 
FROM {{ source('typeform', 'responses') }}
{% if is_incremental() %}
  WHERE upload_time >= (SELECT MAX(upload_time) FROM {{this}} )
{%- endif -%}