SELECT
  _airtable_id AS add_on_review_id
  , user_id AS customer_id
  , created_at::TIMESTAMP_TZ AS rating_time
  , {{ clean_string('prompt_id') }} AS prompt_id
  , {{ clean_string('prompt_subtitle') }} AS prompt_subtitle
  , {{ clean_string('prompt_title') }} AS prompt_title
  , rating
  , {{ clean_string('comment') }} AS comment
FROM {{ source('airtable_feedback_collector', 'add_on_reviews') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY _airtable_id ORDER BY _airbyte_extracted_at DESC) = 1