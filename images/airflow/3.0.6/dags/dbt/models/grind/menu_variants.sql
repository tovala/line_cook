
SELECT
  st.term_id
  , mvb.subterm_id
  -- Get the variant_id from the filename as the group after the second underscore, or null if there isn't one
  -- example default filename: components_00746e81-1549-43a0-b9ca-66205fb2ad58.json
  -- example variant filename: components_00746e81-1549-43a0-b9ca-66205fb2ad58_00746e81-1549-43a0-b9ca-66205fb2ad58.json
  -- menus associated with deleted variants are archived, and have a filename ending in _archived.json
  -- example variant filename: components_00746e81-1549-43a0-b9ca-66205fb2ad58_00746e81-1549-43a0-b9ca-66205fb2ad58_archived.json
  , mvb.variant_id
  , {{ hash_menu_variant_id('st.subterm_id', 'mvb.variant_id')}} AS menu_variant_id
  , COALESCE(ev.variant_name, 'default') AS variant_name
  , e.description  AS experiment_name
  , mvb.variant_id IS NULL AS is_default_variant
  , REGEXP_LIKE(mvb.filename, '_archived\.json$') AS is_archived
FROM {{ ref('menu_variants_base') }} mvb
LEFT JOIN {{ ref('subterms')}} st
  ON st.subterm_id = mvb.subterm_id
LEFT JOIN {{ ref('experiments_variants')}} ev
  ON ev.variant_id = mvb.variant_id
LEFT JOIN {{ ref('experiments')}} e
  ON e.experiment_id = ev.experiment_id
