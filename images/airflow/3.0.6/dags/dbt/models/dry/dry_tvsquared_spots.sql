{{ config(
  alias='tvsquared_spots',
  materialized='incremental',
  unique_key=['spotid'],
  tags=['tvsquared']) }}

SELECT
  {{ clean_string('Channel') }} AS channel
  , {{ clean_string('Client') }} AS client
  , {{ clean_string('Country') }} AS country
  , {{ clean_string('Creative') }} AS creative
  , {{ clean_string('"DATA GROUP"') }} AS datagroup
  , {{ clean_string('Daypart') }} AS daypart
  , {{ clean_string('Hour') }} AS spot_hour
  , {{ clean_string('"PARENT CHANNEL"') }} AS parentchannel
  , {{ clean_string('Programme') }} AS programme
  , {{ clean_string('Quarter') }} AS spot_quarter
  , {{ clean_string('Region') }} AS region
  , {{ clean_string('Saleshouse') }} AS saleshouse
  , {{ clean_string('"SPOT LENGTH"') }} AS spotlength
  , {{ clean_string('Weekday') }} AS spot_weekday
  , {{ clean_string('Year') }} AS spot_year
  , {{ clean_string('"YEAR MONTH"') }} AS yearmonth
  , {{ clean_string('"YEAR MONTH DATE"') }} AS yearmonthdate
  , {{ clean_string('"YEAR MONTH WEEK"') }} AS yearmonthweek
  , {{ clean_string('"ZONE NAME"') }} AS zonename
  , {{ clean_string('completeregistration') }} AS completeregistration
  , TRY_TO_TIMESTAMP_NTZ({{ clean_string('datetime') }}) AS spot_datetime
  , {{ clean_string('"DEVICE:DESKTOP COMPLETEREGISTRATION"') }} AS devicedesktop_completeregistration
  , {{ clean_string('"DEVICE:DESKTOP RESPONSE"') }} AS devicedesktop_response
  , {{ clean_string('"DEVICE:MOBILE COMPLETEREGISTRATION"') }} AS devicemobile_completeregistration
  , {{ clean_string('"DEVICE:MOBILE RESPONSE"') }} AS devicemobile_response
  , {{ clean_string('"DEVICE:TABLET COMPLETEREGISTRATION"') }} AS devicetablet_completeregistration
  , {{ clean_string('"DEVICE:TABLET RESPONSE"') }} AS devicetablet_response
  , {{ clean_string('"DEVICE:UNKNOWN COMPLETEREGISTRATION"') }} AS deviceunknown_completeregistration
  , {{ clean_string('"DEVICE:UNKNOWN RESPONSE"') }} AS deviceunknown_response
  , {{ clean_string('response') }} AS response
  , {{ clean_string('"SPOT COUNT"') }} AS spotcount
  , {{ clean_string('spotid') }} AS spotid
  , _airbyte_extracted_at AS extracted
FROM {{ source('tvsquared', 'spots') }}
{% if is_incremental() %}
  WHERE extracted >= (SELECT MAX(extracted) FROM {{this}} )
{%- endif -%}
