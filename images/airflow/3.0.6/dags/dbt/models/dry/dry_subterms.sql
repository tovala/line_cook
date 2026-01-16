{{ dry_config('subterms') }}

SELECT
  created
  , default_subterm
  , {{ clean_string('id') }} AS id
  , {{ clean_string('marketing_lever_color') }} AS marketing_lever_color
  , {{ clean_string('marketing_lever_string') }} AS marketing_lever_string
  , {{ clean_string('notes') }} AS notes
  , ship_date
  , ship_period
  , {{ clean_string('special_event') }} AS special_event
  , termid
  , updated
  , available
  , {{ clean_string('facility_network') }} AS facility_network
FROM {{ source('combined_api_v3', 'subterms') }}

{{ load_incrementally() }}
