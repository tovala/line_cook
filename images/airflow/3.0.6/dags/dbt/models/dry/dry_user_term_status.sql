{{ dry_config('user_term_status') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('mealcountsallowed_id') }} AS mealcountsallowed_id
  , {{ clean_string('notes') }} AS notes
  , termid
  , updated
  , userid
  , {{ clean_string('subterm_id') }} AS subterm_id
  , locked_subterm
FROM {{ source('combined_api_v3', 'user_term_status') }}

{{ load_incrementally() }}
