{{ config(alias='menu_feedback') }}

(SELECT 
   menu_rating_id
   , customer_id
   , rating_time
   , menu_rating
   , source_os
   , term_id
   , user_comment
 FROM {{ source('brine', 'historical_menu_ratings')}})

UNION ALL

(SELECT 
  _airtable_id AS menu_rating_id
  , userid AS customer_id
  , createdat::TIMESTAMP_TZ AS rating_time
  , rating AS menu_rating
  , {{ clean_string('source') }} AS source_os
  , termid AS term_id
  , {{ clean_string('usercomment') }} AS user_comment
FROM {{ source('airtable_feedback_collector', 'menu_feedback') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY _airtable_id ORDER BY _airbyte_extracted_at DESC) = 1)

