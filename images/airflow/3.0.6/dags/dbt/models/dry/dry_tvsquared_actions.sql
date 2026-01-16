{{ config(
  alias='tvsquared_actions',
  materialized='incremental',
  unique_key=['id'],
  tags=['tvsquared']
  ) }}

SELECT
  {{ hash_natural_key('UserId', 'actiondatetime','action','AdSpotID') }} AS id
  , {{ clean_string('AdSpotID') }} AS adspotid
  , {{ clean_string('SessionRefID') }} AS sessionrefid
  , {{ clean_string('UserId') }} AS userid
  , {{ clean_string('action') }} AS action
  , TRY_TO_TIMESTAMP_NTZ({{ clean_string('actiondatetime') }}) AS actiondatetime
  , {{ clean_string('actionid') }} AS actionid
  , CAST({{ clean_string('actionprobability') }} AS DECIMAL(38,18)) AS actionprobability
  , CAST({{ clean_string('actionsessionprobability') }} AS DECIMAL(38,18)) AS actionsessionprobability
  , CAST({{ clean_string('attributedrevenue') }} AS DECIMAL(38,18)) AS attributedrevenue
  , {{ clean_string('creative') }} AS creative
  , {{ clean_string('customeruserref') }} AS customeruserref
  , {{ clean_string('network') }} AS network
  , {{ clean_string('origin') }} AS origin
  , {{ clean_string('product') }} AS product
  , {{ clean_string('referrer') }} AS referrer
  , {{ clean_string('region') }} AS region
  , TRY_TO_TIMESTAMP_NTZ({{ clean_string('spottime') }}) AS spottime
  , {{ clean_string('t_adspots_datagroup') }} AS t_adspots_datagroup
  , {{ clean_string('t_usersessionactions_promo') }} AS t_usersessionactions_promo
  , CAST({{ clean_string('totalactionrevenue') }} AS DECIMAL(10,2)) AS totalactionrevenue
  , {{ clean_string('tv2mediatype') }} AS tv2mediatype
  , {{ clean_string('usersessionid') }} AS usersessionid
  , TRY_TO_TIMESTAMP_NTZ({{ clean_string('visitdatetime') }}) AS visitdatetime
  , _airbyte_extracted_at AS extracted
  , _ab_source_file_url AS filename
FROM {{ source('tvsquared', 'action') }} 
{% if is_incremental() %}
  WHERE extracted >= (SELECT MAX(extracted) FROM {{this}} )
{%- endif -%}
