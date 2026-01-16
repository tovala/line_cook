
( SELECT
   id
   , menu_id
   , menu_meal_id
   , anti_combo_menu_meal_id
   , is_sideswap
   , updated
 FROM {{ table_reference('menu_anticombos') }} ma
 WHERE id not in ('1e4a25e4-1297-4b8e-a507-0f7c47412f6e'))
UNION
(SELECT
   '1e4a25e4-1297-4b8e-a507-0f7c47412f6e' AS id
   , '9ebdef04-0c9a-4fef-8a5f-1012b4eddcfa' AS menu_id
   , '434a5970-38f7-4b5f-870c-027b97b16312' AS menu_meal_id
   , 'c215fda4-1c37-4a41-a6eb-251a4d8ce4dd' AS anti_combo_menu_meal_id
   , TRUE AS is_sideswap
   , CAST('2022-10-19 20:05:07.860 +0000' AS timestamp) AS updated)
