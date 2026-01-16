{{ dry_config('mise_facility_network', tags=['misevala'], meta={'source_schema':'misevala_v3', 'base_table':'facility_network'}) }}

SELECT 
  created
  , {{ clean_string('display_name') }} AS display_name
  , {{ clean_string('display_theme') }} AS display_theme
  , {{ clean_string('id') }} AS id
  , {{ clean_string('title') }} AS title
  , updated
FROM {{ source('misevala_v3', 'facility_network') }}

{{ load_incrementally() }}