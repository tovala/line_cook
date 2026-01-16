{{ dry_config('mealimages') }}

SELECT
  {{ clean_string('alt') }} AS alt
  , {{ clean_string('blurhash') }} AS blurhash
  , {{ clean_string('caption') }} AS caption
  , {{ clean_string('dominantcolor') }} AS dominantcolor
  , {{ clean_string('filename') }} AS filename
  , {{ clean_string('hash') }} AS hash
  , {{ clean_string('id') }} AS id
  , {{ clean_string('key') }} AS key
  , mealid
  , updated
  , uploaded
  , {{ clean_string('url') }} AS url
FROM {{ source('combined_api_v3', 'mealimages') }}

{{ load_incrementally() }}
