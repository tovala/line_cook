{{ dry_config('mise_part_pgs', tags=['misevala'], meta={'source_schema':'misevala_v3', 'base_table':'part_pgs'}) }}

SELECT 
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('part_id') }} AS part_id
  , {{ clean_string('purchased_good_id') }} AS purchased_good_id
  , updated
FROM {{ source('misevala_v3', 'part_pgs') }}

{{ load_incrementally() }}