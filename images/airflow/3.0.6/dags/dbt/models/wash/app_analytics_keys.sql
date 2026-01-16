
WITH deactivation_times AS (
    SELECT 
      _airtable_id AS key_id 
      , MAX(_airbyte_extracted_at) AS last_appeared_time
      , last_appeared_time = (SELECT MAX(_airbyte_extracted_at) FROM {{ source('airtable_feedback_collector', 'analytics_keys') }}) AS is_active
    FROM {{ source('airtable_feedback_collector', 'analytics_keys') }} 
    GROUP BY 1
)
SELECT 
  _airtable_created_time::TIMESTAMP_TZ AS created_time 
  , _airtable_id AS key_id
  , {{ clean_string('key') }} AS key
  , {{ clean_string('description') }} AS description 
  , REPLACE(REGEXP_REPLACE(LOWER(category), '[^a-zA-Z ]+', ''), ' ', '_') AS key_category
  , COALESCE(ios, FALSE) AS is_on_ios
  , COALESCE(android, FALSE) AS is_on_android
  , COALESCE(web, FALSE) AS is_on_web
  , analytics_attributes
  , dt.is_active 
  , CASE WHEN NOT dt.is_active
         THEN last_appeared_time
    END AS deactivation_time
FROM {{ source('airtable_feedback_collector', 'analytics_keys') }} air
LEFT JOIN deactivation_times dt 
  ON _airtable_id = dt.key_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY _airtable_id ORDER BY _airbyte_extracted_at DESC) = 1 