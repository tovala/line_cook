
SELECT DISTINCT
  LOWER({{ clean_string('raw_data:ORDER_ID') }}) AS oven_order_id
  , TRY_TO_NUMERIC({{ clean_string('raw_data:ACCOUNT_ID') }}) AS customer_id
  --remove spaces and underscores from promo_code field
  , REGEXP_REPLACE({{ clean_string('raw_data:PROMO_CODE') }},'[^a-zA-Z0-9]','') AS clean_promo_cd
  , LOWER(LEFT(clean_promo_cd, 3)) || REGEXP_SUBSTR(clean_promo_cd, '(\\d+)') AS mail_timeframe
  -- Retrieves the last part of the promo code (ex: JAN2026HDR)
  , LOWER(REGEXP_SUBSTR(clean_promo_cd, '[A-Z]+$')) AS file_suffix
  -- Determines if 'ml' or 'hd' is part of the suffix
  , REGEXP_SUBSTR(file_suffix, '(ml|hd)') AS variant_code
  , CASE WHEN variant_code = 'ml' THEN 'received_mailer'
         WHEN variant_code = 'hd' THEN 'holdout'
    END AS mailer_variant
  , {{ clean_string('raw_data:KEYCODE') }} AS key_code
FROM {{ source('chili', 'direct_mail_ccc') }}
WHERE raw_data:CHANNEL = 'R'     -- Only keep oven orders, not meal orders
  AND filename NOT LIKE '%NON_HITS%'
  AND NOT file_suffix = 'cml' --remove reactivation campaign from acquisition table