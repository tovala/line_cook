{{ dry_config('stripefingerprints') }}

SELECT
  created
  , {{ clean_string('customerid') }} AS customerid
  , {{ clean_string('fingerprint') }} AS fingerprint
  , {{ clean_string('id') }} AS id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('paymentsourceid') }} AS paymentsourceid
  , updated
  , userid
FROM {{ source('combined_api_v3', 'stripefingerprints') }}

{{ load_incrementally() }}