{{ dry_config('user_term_status_audit', primary_key = None, load_mode='table', post_hooks=[]) }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('mealcountsallowed_id') }} AS mealcountsallowed_id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('operation') }} AS operation
  , stamp
  , termid
  , updated
  , userid
  , {{ clean_string('subterm_id') }} AS subterm_id
  , locked_subterm
FROM {{ source('combined_api_v3', 'user_term_status_audit') }}

{{ load_incrementally() }}
