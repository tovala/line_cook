{{ dry_config('mise_purchased_goods', tags=['misevala'], meta={'source_schema':'misevala_v3', 'base_table':'purchased_goods'}) }}

SELECT 
  active
  , {{ clean_string('base_sku') }} AS base_sku
  , {{ clean_string('category') }} AS category
  , created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('name') }} AS name
  , {{ clean_string('pick_group_flag') }} AS pick_group_flag
  , allergens
  , {{ clean_string('storage_location') }} AS storage_location
  , updated
  , {{ clean_string('zone') }} AS zone
FROM {{ source('misevala_v3', 'purchased_goods') }}

{{ load_incrementally() }}