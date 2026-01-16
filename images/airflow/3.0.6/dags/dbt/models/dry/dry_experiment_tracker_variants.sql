{{ dry_config('experiment_tracker_variants', 
   tags=['experiment_tracker_v2']) 
}}

SELECT 
  {{ clean_string('experiment_id') }} AS experiment_id
  , {{ clean_string('id') }} AS id
  , {{ clean_string('order_identifier') }}::INTEGER AS order_identifier
  , variant_params::VARIANT::OBJECT AS variant_params
  , updated
  , created
FROM {{ source('experiment_tracker_v2', 'variants') }}
{{ load_incrementally() }}