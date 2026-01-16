SELECT 
    id AS experiment_id
    , owner_identifier AS owner
    , partition_method
    , description
    , start_time AS experiment_start_time
FROM {{ table_reference('experiment_tracker_experiments') }}  