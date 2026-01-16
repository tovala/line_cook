{{ dry_config('mise_production_meal_info', tags=['misevala'], meta={'source_schema':'misevala_v3', 'base_table':'production_meal_info'}) }}

SELECT 
  api_meal_code
  , api_meal_id
  , {{ clean_string('api_meal_title') }} AS api_meal_title
  , created
  , cycle
  , {{ clean_string('facility_network_id') }} AS facility_network_id
  , {{ clean_string('id') }} AS id
  , {{ clean_string('meal_type') }} AS meal_type
  , {{ clean_string('production_part_id') }} AS production_part_id
  , {{ clean_string('short_title') }} AS short_title
  , term_id
  , total_meals
  , updated
FROM {{ source('misevala_v3', 'production_meal_info') }}

{{ load_incrementally() }}