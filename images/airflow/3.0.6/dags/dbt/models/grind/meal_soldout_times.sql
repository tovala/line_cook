SELECT
    mse.id
    , mm.id AS menu_meal_id
    , mse.meal_id AS meal_sku_id
    , mse.subterm_id
    , mse.sold_out_count
    , mse.selections_count
    , st.termid
    , st.ship_period AS cycle
    , st.facility_network AS facility
    , mse.created AS event_time
FROM {{ table_reference('meal_soldout_event') }} mse
LEFT JOIN {{ table_reference('subterms') }} st 
  ON mse.subterm_id = st.id
INNER JOIN {{ table_reference('menus') }} mn 
  ON mn.subterm_id = mse.subterm_id
INNER JOIN {{ table_reference('menu_meals') }} mm 
  ON mm.meal_id = mse.meal_id 
  AND mn.id = mm.menu_id
-- Only include events where the meal has been marked as sold out
WHERE mse.sold_out_count = mse.selections_count

