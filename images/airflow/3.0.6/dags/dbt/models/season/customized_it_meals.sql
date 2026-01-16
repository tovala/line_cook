
WITH menu_meal_anticombo_match AS (
  --match meal info to menu_anticombo pair at menu_meal level
  SELECT
    mmo1.meal_sku_id
    , mmo1.menu_meal_id
    , mmo1.title
    , mmo1.subtitle
    , ms1.protein_subtype
    , mmo2.meal_sku_id AS anticombo_meal_sku_id
    , mmo2.menu_meal_id AS anticombo_menu_meal_id
    , mmo2.title AS anticombo_title
    , mmo2.subtitle AS anticombo_subtitle
    , ms2.protein_subtype as anticombo_protein_subtype
    , ROW_NUMBER() OVER (PARTITION BY mmo1.menu_meal_id ORDER BY updated) + 1 AS non_primary_position -- using updated time to determine position
  FROM {{ ref('corrected_menu_anticombos') }} ma
  -- join to menu_anticombos' main menu_meal_id
  LEFT JOIN {{ ref('menu_meal_offerings') }} mmo1 
    ON ma.menu_meal_id = mmo1.menu_meal_id
  LEFT JOIN {{ ref('meals') }} ms1 
    ON mmo1.internal_meal_name = ms1.internal_meal_name
  -- join to menu_anticombos' secondary menu_meal_id  
  LEFT JOIN {{ ref('menu_meal_offerings') }} mmo2 
    ON ma.anti_combo_menu_meal_id = mmo2.menu_meal_id
  LEFT JOIN {{ ref('meals') }} ms2
    ON mmo2.internal_meal_name = ms2.internal_meal_name
),
sister_meal_pairs AS (
  -- find all sister meals for each menu_meal
  SELECT
    menu_meal_id
    , MAX(non_primary_position) AS customization_option_count -- figure out how many options to customize exist
    , LISTAGG(anticombo_meal_sku_id, '-') WITHIN GROUP (ORDER BY anticombo_meal_sku_id) AS sister_meal_sku_list -- for each menu_meal_id figure out all the "pairings meal_skus_id", not include itself (groupings if > 2 anticombos)
    , LISTAGG(anticombo_menu_meal_id, '--') WITHIN GROUP (ORDER BY anticombo_menu_meal_id) AS sister_menu_meal_list -- for each menu_meal_id figure out all the "pairings menu_meal_id", not include itself (groupings if > 2 anticombos)
    , LISTAGG(anticombo_protein_subtype, '--') WITHIN GROUP (ORDER BY anticombo_protein_subtype) AS protein_list
  FROM menu_meal_anticombo_match
  GROUP BY 1
),
customized_groups AS (
  -- do everything at the granularity of the PRIMARY menu meal offering
  SELECT
    mmo.menu_meal_id
    , mmo.meal_sku_id
    , mmo.term_id
    , mma.anticombo_menu_meal_id
    , mma.anticombo_meal_sku_id
    , smp.customization_option_count
    , mma.non_primary_position AS customized_position
    , CONCAT(mmo.meal_sku_id, '-', smp.sister_meal_sku_list) AS customize_it_meal_sku_group
    , CONCAT(mmo.menu_meal_id, '--', smp.sister_menu_meal_list) AS customize_it_menu_meal_group
    , CONCAT(mma.protein_subtype, '--', smp.protein_list) AS customize_it_protein_group
    , CONCAT(LISTAGG(mtm1.tagid, '--'), '--', LISTAGG(mtm2.tagid, '--')) as tag_group
    , MIN(CASE WHEN (mtm1.tagid = 88) OR (mtm2.tagid = 88) 
               THEN 'dessert'
               WHEN (mtm1.tagid = 65) OR (mtm2.tagid = 65) 
               THEN 'dual_serving'
               WHEN mmo.title = mma.anticombo_title AND mmo.subtitle <> mma.anticombo_subtitle 
               THEN 'side'
               WHEN (mmo.title <> mma.anticombo_title AND mmo.subtitle = mma.anticombo_subtitle) OR (mtm1.tagid = 94 OR mtm2.tagid = 94)
               THEN 'protein'
               WHEN (mt1.title LIKE 'BOXEXTRA%') OR (mt2.title LIKE 'BOXEXTRA%')
               THEN 'meal_extra'
               ELSE 'unknown'
          END ) AS customize_it_category 
    -- TODO: Write logic for customize_it_types eg.chicken salmon beef
    -- protein_subtype is pulling from dry.culinary_meal_tags and join to internal_meal level data on names. 
    -- Put this in once finish meal_sku and intenral_meal table
    , MIN(CASE WHEN mtm1.tagid = 94 OR mtm2.tagid = 94 
               THEN 'protein_choice'
               WHEN mmo.title <> mma.anticombo_title AND mmo.subtitle = mma.anticombo_subtitle THEN 'protein_swap'
          END ) AS customize_it_merchandising -- We only have 'protein_choice/protein_swap' for now. 
  FROM sister_meal_pairs smp
  LEFT JOIN {{ ref('menu_meal_offerings') }} mmo
    ON smp.menu_meal_id = mmo.menu_meal_id
  LEFT JOIN menu_meal_anticombo_match mma 
    ON mmo.menu_meal_id = mma.menu_meal_id
  LEFT JOIN {{ table_reference('mealtagmap') }} mtm1 
    ON mmo.meal_sku_id = mtm1.mealid
  LEFT JOIN {{ table_reference('mealtags') }} mt1
    ON mtm1.tagid = mt1.id  
  LEFT JOIN {{ table_reference('mealtagmap') }} mtm2 
    ON mma.anticombo_meal_sku_id = mtm2.mealid
  LEFT JOIN {{ table_reference('mealtags') }} mt2
    ON mtm2.tagid = mt2.id  
  -- Before Term 242, there were other menu offerings that resembled customized meals in terms of data. However, those were not "customize it meals" so we're excluding them.
  WHERE term_id > 242
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
union_table AS (
  (SELECT  -- Pull data for PRIMARY meal (note: we can do this because there will always be at least one primary meal for customized options, regardless of whether there are 2, 3, or however many customization options)
    menu_meal_id
    , meal_sku_id
    , customize_it_meal_sku_group
    , customize_it_menu_meal_group
    , customization_option_count
    , customize_it_category
    , customize_it_merchandising
    , customize_it_protein_group
    , 1 AS customized_position 
    , LISTAGG(tag_group, '--') as tag_group
  FROM customized_groups
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9)

UNION

  -- union primary meal with all anticombos to normalize granularity at an MMO_ID level
  (SELECT
    anticombo_menu_meal_id AS menu_meal_id
    , anticombo_meal_sku_id AS meal_sku_id
    , customize_it_meal_sku_group
    , customize_it_menu_meal_group
    , customization_option_count
    , customize_it_category
    , customize_it_merchandising
    , customize_it_protein_group
    , customized_position
    , tag_group
  FROM customized_groups)),

customized_category_set AS (
  SELECT
    menu_meal_id
    , meal_sku_id
    , customize_it_meal_sku_group
    , customize_it_menu_meal_group
    , customization_option_count
    , CASE WHEN ARRAY_CONTAINS('94'::VARIANT, SPLIT(tag_group, '--'))
         THEN 'protein'
         ELSE customize_it_category
      END AS customize_it_category
    , customize_it_merchandising
    , customize_it_protein_group
    , customized_position
    , tag_group
  FROM union_table
)

SELECT
  ccs.menu_meal_id
  , ccs.meal_sku_id
  , ccs.customize_it_meal_sku_group
  , ccs.customize_it_menu_meal_group
  , ccs.customization_option_count
  , ccs.customized_position 
  , ccs.tag_group
  , ccs.customize_it_category
  , CASE WHEN ccs.customize_it_category = 'dessert' AND ccs.customized_position = 2
         THEN 'dessert'
         WHEN ccs.customize_it_category = 'meal_extra' AND ccs.customized_position = 2
         THEN 'meal_extra'
    END AS meal_with_extra_or_dessert
  , ccs.customize_it_merchandising
  , CASE WHEN ccs.customize_it_category = 'protein' 
         THEN ccs.customize_it_protein_group
    END AS customize_it_protein_group
  , SUM(mme.menu_raw_engagement_rate) OVER (PARTITION BY ccs.customize_it_menu_meal_group) AS group_engagement_rate
  , mme.menu_raw_engagement_rate/NULLIF(group_engagement_rate, 0) AS take_rate
FROM customized_category_set ccs
LEFT JOIN {{ ref('menu_meal_raw_engagement_rates') }} mme 
  ON ccs.menu_meal_id = mme.menu_meal_id
  