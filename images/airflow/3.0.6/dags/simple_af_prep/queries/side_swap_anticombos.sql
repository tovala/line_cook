INSERT INTO brine.anticombinations 
SELECT 
    s.termid 
    , s.facility_network
    , s.ship_period AS cycle
    , mm1.meal_id AS meal_sku_id
    , CONCAT(m1.title, ' ', m1.subtitle) AS meal 
    , mm2.meal_id AS anticombination_meal_sku_id
    , CONCAT(m2.title, ' ', m2.subtitle) AS anticombo
    , ma.is_sideswap
FROM dry.menu_anticombos ma 
INNER JOIN dry.menus men
    ON ma.menu_id = men.id 
INNER JOIN dry.subterms s 
    ON men.subterm_id = s.id 
INNER JOIN dry.menu_meals mm1
    ON ma.menu_meal_id = mm1.id
INNER JOIN dry.menu_meals mm2
    ON ma.anti_combo_menu_meal_id = mm2.id
INNER JOIN dry.meals m1
    ON mm1.meal_id = m1.id 
INNER JOIN dry.meals m2 
    ON mm2.meal_id = m2.id
WHERE ma.is_sideswap 
AND NOT EXISTS (
  SELECT 1 
  FROM brine.anticombinations 
  WHERE meal_sku_id = m1.id 
  AND anticombination_meal_sku_id = m2.id
);