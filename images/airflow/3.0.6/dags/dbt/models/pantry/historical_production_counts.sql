
SELECT 
  st.subterm_id 
  , wmc.term_id
  , wmc.meal_sku_id 
  , wmc.production_cd 
  , CASE WHEN cycle = 1 
         THEN cycle_1_production_count 
         WHEN cycle = 2
         THEN cycle_2_production_count
    END AS production_count 
  , NOT wmc.is_customer_facing AS is_frozen
FROM {{ source('brine', 'weekly_meal_counts') }} wmc
LEFT JOIN {{ ref('subterms') }} st 
  ON wmc.term_id = st.term_id
  AND wmc.facility_network = st.facility_network
WHERE wmc.meal_sku_id IS NOT NULL
  AND wmc.term_id >= 275
