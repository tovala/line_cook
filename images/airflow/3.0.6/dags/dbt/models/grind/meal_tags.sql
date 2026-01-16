
SELECT 
    mmo.menu_meal_id
  , mtm.mealid as meal_sku_id
  , mt.display_mode
  , REPLACE(REGEXP_REPLACE(LOWER(TRIM(REGEXP_REPLACE(mt.title, '[^-a-zA-Z0-9 ]+'))), '[\\s]+', '_'),'-','_') AS meal_tag
  -- note:
  -- 1. REGEXP_REPLACE(title, '[^-a-zA-Z0-9 ]+'): This regular expression (regex) replace operation is removing all characters from the title string that are not a lowercase letter (a-z), an uppercase letter (A-Z), a number (0-9), a space ( ), or a hyphen (-).
  -- 2. TRIM(): This function removes any leading or trailing white spaces from the resulting string from step 1.
  -- 3. LOWER(): This function converts all the characters in the string from step 2 to lowercase.
  -- 4. REGEXP_REPLACE(..., '[\\s]+', '_'): This regex replace operation changes all spaces (including multiple spaces in a row) in the string from step 3 into single underscores (_).
  -- 5. REPLACE(...,'-','_'): This replaces all remaining hyphens (-) in the string from step 4 with underscores (_).
  , CASE WHEN meal_tag in ('add_on_box','add_on_meal','hidden_meal','contains_dessert_addon','premium','breakfast','2_servings_exclude_from_autofill') -- part of internal useful tags, using for auto-fill
         THEN 'internal_tags'
         WHEN meal_tag ILIKE '%boxextra%' -- all boxextras 
         THEN 'box_extras'
         WHEN mt.display_mode  <> 'hidden' -- all the tags that are visible to customers
         THEN 'cx_tags'
         ELSE 'other'
    END AS tag_category
FROM {{ table_reference('mealtags') }} mt 
INNER JOIN {{ table_reference('mealtagmap') }} mtm
 ON mt.id = mtm.tagid
INNER JOIN {{ ref('menu_meal_offerings') }} mmo
 ON mtm.mealid =  mmo.meal_sku_id
 