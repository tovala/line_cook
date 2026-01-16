{{ dry_config('experiment_tracker_experiments', 
   tags=['experiment_tracker_v2']) 
}}

SELECT 
  {{ clean_string('id') }} AS id
  , {{ clean_string('owner_identifier') }} AS owner_identifier
  , {{ clean_string('partition_method') }} AS partition_method
  , {{ clean_string('description') }} AS description
  , start_time
  , updated
  , created
FROM {{ source('experiment_tracker_v2', 'experiments') }}
{{ load_incrementally() }}