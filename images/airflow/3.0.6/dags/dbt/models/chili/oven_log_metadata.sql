{{
  config(
    materialized='incremental', 
    unique_key='date_key',
    tags=["metadata"],
    full_refresh = false,
    pre_hook="{% if is_incremental() %} DELETE FROM {{ this }} WHERE ingested_date = (SELECT MAX(ingested_date) FROM {{ this }}) {% endif %}"
  ) 
}}

SELECT 
  original_file_date 
  , updated::DATE AS ingested_date 
  , COALESCE(DATE_TRUNC('day', raw_data:timestamp::TIMESTAMP::DATE), DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(raw_data:eventTimeMs::NUMBER, 3)))) AS event_date 
  , COUNT(*) AS num_records 
  , COUNT(DISTINCT raw_data) AS num_distinct_records
  , {{ hash_natural_key('original_file_date', 'event_date', 'ingested_date') }} AS date_key
FROM {{ source('kinesis', 'oven_logs') }}
{% if is_incremental() %}
WHERE updated::DATE > (SELECT MAX(ingested_date) FROM {{ this }})
{% endif %}
GROUP BY 1, 2, 3
