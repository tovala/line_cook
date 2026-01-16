
WITH parsed_meal_info AS (
  SELECT 
    m.id AS meal_sku_id
    , {{ clean_string('LOWER(v.value:key::STRING)') }} AS info_type
    , REGEXP_REPLACE({{ clean_string('LOWER(v.value:name::STRING)') }}, '^\\*') AS info_name
    , {{ clean_string('LOWER(v.value:unit::STRING)') }} AS meal_info_unit
    , TRY_TO_DOUBLE(v.value:value::STRING) AS meal_info_value
  FROM {{ table_reference('meals') }} m,
  LATERAL FLATTEN(input => m.nutritionalinfo) v)
SELECT 
  meal_sku_id
  , CASE WHEN info_type = 'allergn'
         THEN 'allergen'
         WHEN info_type = 'g0lden'
         THEN 'golden'
         ELSE info_type
    END AS meal_info_type
  , info_name AS raw_info_name
  , CASE WHEN info_name IN ('ch0lesterol', 'cholestrol')
         THEN 'cholesterol'
         WHEN info_name = 'smartpoint'
         THEN 'smartpoints'
         WHEN meal_info_type = 'allergen'
         THEN CASE WHEN info_name IN ('egg', 'eggs')
                   THEN 'eggs'
                   WHEN RLIKE(info_name, 'fish.*|trout') AND NOT info_name LIKE '%shrimp%' AND NOT info_name LIKE '%shellfish%'
                   THEN 'fish'
                   WHEN info_name LIKE 'shellfish%' AND NOT info_name LIKE '%shrimp%'
                   THEN 'shellfish' 
                   WHEN info_name LIKE '%shrimp%' 
                   THEN 'shrimp'
                   WHEN info_name LIKE 'tree nut%' OR info_name LIKE 'trees nuts%' OR info_name IN ('nuts (pistachio)', 'nut (pistachio)', 'coconut')
                        OR info_name LIKE 'lava cake may contain tree nuts%'
                   THEN 'tree nuts' 
                   WHEN info_name IN ('mik', 'milk')
                   THEN 'milk'
                   WHEN info_name = 'contains gluten'
                   THEN 'gluten'
                   WHEN info_name IN ('soy', 'soybean', 'soybeans') OR RLIKE(info_name, 'tortilla(.*)? may contain soy.')
                   THEN 'soy'
                   WHEN info_name IN ('wheat', 'dairy', 'sesame', 'peanuts')
                   THEN info_name
                   WHEN info_name = 'disclaimer: the corn nuts in this meal were produced in a facility that also produces nuts.'
                        OR info_name = 'the corn nuts in this meal were produced in a facility that also produces nuts.'
                   THEN 'corn|tree nuts'
                   WHEN info_name = 'gnocchi were produced in a facility that processes egg and milk. may contain soy.'
                   THEN 'eggs|milk|soy'
                   WHEN info_name = 'plantain chips contain canola (corn) oil and sunflower oil.'
                   THEN 'corn'
                   WHEN info_name = 'vegetable oil used in candied walnuts may contain peanut, cottonseed, soybean, and/or sunflower oil.'
                   THEN 'peanuts|soy|tree nuts'
                   WHEN info_name = 'cookie may contain peanuts and tree nuts.'
                   THEN 'peanuts|tree nuts'
                   WHEN info_name = 'cake may contain tree nuts and soy.'
                   THEN 'tree nuts|soy'
                   WHEN info_name = 'flatbread may contain soy, sesame, eggs, and coconut.'
                   THEN 'soy|sesame|eggs|coconut'
              END
         WHEN meal_info_type = 'golden' AND raw_info_name RLIKE '(.*) - (calories|total fat|carbs|protein)'
         THEN REGEXP_SUBSTR(raw_info_name, '(calories|total fat|carbs|protein)')
         ELSE info_name 
    END AS meal_info_name
  , CASE WHEN meal_sku_id = 4933 AND meal_info_unit = '6'
         THEN 'g'
         ELSE meal_info_unit
    END AS meal_info_unit
  , meal_info_value
FROM parsed_meal_info
WHERE info_type IS NOT NULL
  AND NOT RLIKE(info_name, 'high protein:.*|low calorie:.*')
