{{ config(
  alias='tvsquared_responses',
  materialized='incremental',
  unique_key=['id'],
  tags=['tvsquared']) }}

SELECT
  {{ hash_natural_key('UserId', 'AdSpotID', 'usersessionid', 'spottime') }} AS id
  , {{ clean_string('AdSpotID') }} AS adspotid
  , {{ clean_string('UserId') }} AS userid
  , {{ clean_string('creative') }} AS creative
  , {{ clean_string('network') }} AS network
  , {{ clean_string('origin') }} AS origin
  , CAST({{ clean_string('probability') }} AS DECIMAL(38,18)) AS probability
  , {{ clean_string('referrer') }} AS referrer
  , {{ clean_string('region') }} AS region
  , CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_NTZ({{ clean_string('spottime') }})) AS spottime_utc
  , {{ clean_string('tv2mediatype') }} AS tv2mediatype
  , {{ clean_string('usersessionid') }} AS usersessionid
  , CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_NTZ({{ clean_string('visitdatetime') }})) AS visitdatetime_utc
  , _airbyte_extracted_at AS extracted 
  , _ab_source_file_url AS filename
FROM {{ source('tvsquared', 'response') }}
{% if is_incremental() %}
  WHERE extracted >= (SELECT MAX(extracted) FROM {{this}} )
{%- endif -%}
