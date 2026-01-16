
WITH deactivation_times AS (
    SELECT 
      _airtable_id AS key_id 
      , MAX(_airbyte_extracted_at) AS last_appeared_time
      , last_appeared_time = (SELECT MAX(_airbyte_extracted_at) FROM {{ source('airtable_feedback_collector', 'growth_keys') }}) AS is_active
    FROM {{ source('airtable_feedback_collector', 'growth_keys') }}
    GROUP BY 1
)
SELECT 
  _airtable_created_time::TIMESTAMP_TZ AS created_time 
  , _airtable_id AS key_id
  , {{ clean_string('key') }} AS key
  , COALESCE(implemented, FALSE) AS is_implemented
  , COALESCE(iterable, FALSE) AS is_in_iterable
  , COALESCE(ads, FALSE) AS is_in_ads
  , SPLIT({{ clean_string('old_key_names') }}, ',') AS deprecated_key_names
  , {{ clean_string('description') }} AS description 
  , category AS key_categories
  , analytics_attributes
  , growth_analytics_attributes
  , webpage AS key_webpages
  , dt.is_active 
  , CASE WHEN NOT dt.is_active
         THEN last_appeared_time
    END AS deactivation_time
FROM {{ source('airtable_feedback_collector', 'growth_keys') }} air
LEFT JOIN deactivation_times dt 
  ON _airtable_id = dt.key_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY key_id ORDER BY created_time DESC) = 1 
