
SELECT 
  id AS subterm_id
  , termid AS term_id
  , default_subterm AS is_default 
  , ship_date AS subterm_ship_time
  , ship_period AS cycle 
  , facility_network
  , special_event IS NOT NULL AS is_special_event 
  , available AS is_available
  , subterm_ship_time < {{ current_timestamp_utc() }} AS has_shipped
FROM {{ table_reference('subterms') }}
