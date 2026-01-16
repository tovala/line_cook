
SELECT 
  afm.meal_sku_id
  -- Can adjust this in future to override the order in which meals are autofilled 
  , 1 AS autofill_priority
FROM {{ ref('autofillable_meals') }} afm 
