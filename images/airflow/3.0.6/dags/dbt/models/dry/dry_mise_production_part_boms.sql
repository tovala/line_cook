{{ dry_config('mise_production_part_boms', tags=['misevala'], meta={'source_schema':'misevala_v3', 'base_table':'production_part_boms'}) }}

SELECT 
  created
  , {{ clean_string('base_sku') }} AS base_sku
  , {{ clean_string('id') }} AS id
  , {{ clean_string('parent_production_part_id') }} AS parent_production_part_id
  , {{ clean_string('part_id') }} AS part_id
  , {{ clean_string('part_title') }} AS part_title
  , {{ clean_string('part_version_id') }} AS part_version_id
  , {{ clean_string('production_part_id') }} AS production_part_id
  , qty_pounds
  , tags
  , total_weight_pounds
  , updated
  , yield_per_batch
  , yield_pounds
FROM {{ source('misevala_v3', 'production_part_boms') }}

{{ load_incrementally() }}