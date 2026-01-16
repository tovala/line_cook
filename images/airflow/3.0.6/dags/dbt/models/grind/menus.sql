
SELECT
  id AS menu_id
  , subterm_id
  , name AS menu_name
  , description AS menu_description
  , is_default
  , notes
FROM {{ table_reference('menus') }}
