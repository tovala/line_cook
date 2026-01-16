{{ dry_config('ops_guidance_events') }}

SELECT
    {{ clean_string('id') }} AS id
  , {{ clean_string('title') }} AS title
  , {{ clean_string('problem_description') }} AS problem_description
  , {{ clean_string('resolution_description') }} AS resolution_description
  , {{ clean_string('resolution_link') }} AS resolution_link
  , {{ clean_string('extra_links') }} AS extra_links
  , event_start_date
  , event_end_date
  , {{ clean_string('source_key') }} as source_key
  , created
  , updated

FROM {{ source('combined_api_v3', 'ops_guidance_events')}}

{{ load_incrementally() }}