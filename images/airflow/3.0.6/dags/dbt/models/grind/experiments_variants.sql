SELECT 
    id AS variant_id 
    , experiment_id
    , order_identifier AS variant_order
    , COALESCE(ARRAY_TO_STRING(PARSE_JSON(variant_params:userAspects), ','), 'control') AS variant_name
FROM {{ table_reference('experiment_tracker_variants') }}