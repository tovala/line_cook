{{ dry_config('experiment_tracker_experiments_seen', 
   tags=['experiment_tracker_v2']) 
}}

SELECT 
  {{ clean_string('id') }} AS id
  , {{ clean_string('experiment_id') }} AS experiment_id
  , TRY_TO_NUMBER(user_id::VARCHAR)::INTEGER AS user_id
  , {{ clean_string('variant_id') }} AS variant_id
  , {{ clean_string('additional_detail') }} AS additional_detail
  , sent_at
  , created 
FROM {{ source('experiment_tracker_v2', 'experiments_seen') }}
{{ load_incrementally(bookmark='created') }}