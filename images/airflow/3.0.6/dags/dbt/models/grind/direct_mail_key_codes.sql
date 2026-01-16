SELECT
  key_code
  -- lowercase all values since files come in various case types  
  , LOWER(mail_timeframe) AS mail_timeframe
  , LOWER(list_description) AS list_description
  , LOWER(mailer_count_type) AS mailer_count_type
  , LOWER(mail_variant) AS mail_variant
  , LOWER(test_variant) AS test_variant
  , COALESCE(quantity::INTEGER, 0) AS quantity

  -- break out parts of the list_description
  , CASE
      WHEN REGEXP_LIKE(LOWER(list_description), 'rea.*append.*','i') THEN 'internal email list'
      WHEN REGEXP_LIKE(LOWER(list_description), 'a1.*','i') THEN 'a1'
      WHEN REGEXP_LIKE(LOWER(list_description), 'universal performance.*','i') THEN 'up'
      WHEN REGEXP_LIKE(LOWER(SPLIT_PART(list_description,',',1)), '.*multis.*','i') THEN 'multi model'  
      WHEN REGEXP_LIKE(LOWER(list_description),'.*, seg.*','i') THEN SPLIT_PART(LOWER(list_description), ', seg', 1)
      WHEN REGEXP_LIKE(LOWER(list_description),'.* seg.*','i') THEN SPLIT_PART(LOWER(list_description), ' seg', 1)
      ELSE LOWER(SPLIT_PART(list_description, ',', 1))
    END AS model_type
  , CASE 
  --segment number for internal email list and multi model is defined by Finance
      WHEN model_type = 'internal email list' THEN '999'
      WHEN model_type = 'multi model' THEN '998'
      --null for any key codes with no segments
      WHEN list_description NOT ILIKE '%seg%' THEN NULL
      --differentiate for commas vs spaces
      WHEN list_description ILIKE '%segs%'
      THEN SPLIT_PART(TRIM(SPLIT_PART(LOWER(list_description), 'segs', 2)), ' ', 1)
      ELSE SPLIT_PART(TRIM(SPLIT_PART(LOWER(REGEXP_REPLACE(list_description,',','')),'seg',2)),' ',1)
    END AS segment_number
  , TRIM(SPLIT_PART(REGEXP_REPLACE(LOWER(list_description), ',', ''), CONCAT('seg ', segment_number), 2)) AS target_area
  , NULL AS model_number
  , NULL AS has_received_previous_year
FROM {{ source('sigma_input_tables', 'direct_mail_key_codes_output') }}
WHERE mail_timeframe NOT IN ('latemar2025','mar2025')
QUALIFY ROW_NUMBER() OVER (PARTITION BY key_code ORDER BY last_updated_at DESC) = 1
UNION
SELECT key_code
, LOWER(mail_timeframe)
, LOWER(list_description)
, LOWER(mailer_count_type)
, LOWER(mail_variant)
, LOWER(test_variant)
, quantity
, LOWER(model_type)
, segment_number
, LOWER(tier) as target_area
, model_number
, has_received_previous_year
FROM {{ source('sigma_input_tables', 'direct_mail_key_codes_output_post_mar_2025') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY key_code ORDER BY last_updated_at DESC) = 1