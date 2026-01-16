{{ dry_config('mise_production_parts', tags=['misevala'], meta={'source_schema':'misevala_v3', 'base_table':'production_parts'}) }}

SELECT 
  created
  , cycle
  , {{ clean_string('facility_network_id') }} AS facility_network_id
  , {{ clean_string('id') }} AS id
  , is_meal
  , {{ clean_string('part_id') }} AS part_id
  , {{ clean_string('part_title') }} AS part_title
  , {{ clean_string('part_version_id') }} AS part_version_id
  , tags
  , term_id
  , updated
FROM {{ source('misevala_v3', 'production_parts') }}

{{ load_incrementally() }}