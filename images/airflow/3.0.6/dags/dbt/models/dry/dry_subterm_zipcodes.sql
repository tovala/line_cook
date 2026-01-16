{{ dry_config('subterm_zipcodes') }}

SELECT
  available
  , available_for_new_users
  , available_for_prospects
  , created
  , default_subterm
  , {{ clean_string('id') }} AS id
  , {{ clean_string('marketing_lever_color') }} AS marketing_lever_color
  , {{ clean_string('marketing_lever_string') }} AS marketing_lever_string
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('subterm_id') }} AS subterm_id
  , updated
  , {{ clean_string('zipcode') }} AS zipcode
FROM {{ source('combined_api_v3', 'subterm_zipcodes') }}

{{ load_incrementally() }}