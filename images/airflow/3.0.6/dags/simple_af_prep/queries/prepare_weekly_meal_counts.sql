INSERT INTO brine.weekly_meal_counts 
WITH recorded_meal_counts AS (
    SELECT DISTINCT
        pmi.term_id
        , mfn.title AS facility_network
        , msk.meal_sku_id
        , CONCAT(COALESCE(msk.title, ''), ' ', COALESCE(msk.subtitle, '')) AS meal
        , pmi.api_meal_code AS production_cd
        , COALESCE(MAX(CASE WHEN pmi.cycle = 1 THEN pmi.total_meals END), 0) AS cycle_1_production_count
        , COALESCE(MAX(case when pmi.cycle = 2 THEN pmi.total_meals END), 0) AS cycle_2_production_count
        , mtm.id IS NULL AS is_customer_facing
        , CONCAT(mfn.title, msk.meal_sku_id) AS pkey
    FROM dry.mise_production_meal_info pmi
    INNER JOIN dry.mise_facility_network mfn
        on pmi.facility_network_id = mfn.id
    INNER JOIN grind.meal_skus msk
        on pmi.api_meal_code = msk.production_cd
        and contains(msk.facility_network, mfn.title)
        and pmi.term_id = msk.term_id
    INNER JOIN dry.menu_meals mm
        ON msk.meal_sku_id = mm.meal_id
    INNER JOIN dry.menus me
        ON mm.menu_id = me.id
    INNER JOIN dry.subterms s
        ON me.subterm_id = s.id
    LEFT JOIN dry.mealtagmap mtm
        on msk.meal_sku_id = mtm.mealid
        and mtm.tagid = 53
    WHERE s.termid = %(current_term_id)s
    AND NOT EXISTS (
      SELECT 1
      FROM brine.weekly_meal_counts
      WHERE meal_sku_id = mm.meal_id
    )
    GROUP BY  1,2,3,4,5,8,9
)
SELECT
    term_id 
    , facility_network
    , meal_sku_id 
    , meal 
    , production_cd 
    , cycle_1_production_count
    , cycle_2_production_count
    , is_customer_facing
    , pkey 
FROM recorded_meal_counts
