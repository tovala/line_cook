SELECT 
    id AS experiment_variant_assignment_id 
    , experiment_id
    , user_id AS customer_id 
    , variant_id
    , sent_at AS variant_seen_time
FROM {{ table_reference('experiment_tracker_experiments_seen') }}