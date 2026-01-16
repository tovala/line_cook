
WITH uto AS (
  (SELECT 
     DISTINCT userid
   FROM {{ table_reference('products_purchase') }})
  UNION
  (SELECT 
     DISTINCT userid 
   FROM {{ table_reference('user_term_order') }}
   WHERE status = 'complete')   
  UNION 
  (SELECT 
     DISTINCT userid 
   FROM {{ table_reference('gift_cards') }}
  )
  UNION
  (SELECT 
     DISTINCT userid 
   FROM {{ table_reference('payment') }}
  )
  UNION
  (SELECT 
     DISTINCT userid 
   FROM {{ table_reference('stripesubscriptions') }}
   WHERE active
  )
  UNION
  (SELECT 
     DISTINCT userid 
   FROM {{ table_reference('meal_cashmoney') }}
  )
  UNION
  (SELECT 
     DISTINCT userid 
   FROM {{ table_reference('ovens_history') }}
  )
), first_active_time AS (
  SELECT 
    userid AS customer_id 
    , created AS status_start_time 
    , ROW_NUMBER() OVER (PARTITION BY userid ORDER BY created) AS row_num 
  FROM {{ table_reference('subscriptionactivity') }}
  WHERE active
)
, name_separation AS (
  SELECT
  lead_id 
  , name
  , CASE WHEN name NOT LIKE '%@%' 
         THEN NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(LOWER(name)), ' \+', ' '), '[^a-z ]', ''), '(( jr| sr| iii| ii| i)$)|^(dr |mrs |ms |mr )|deactivated', ''), '')
    END AS cleaned_name
  , INITCAP(SPLIT_PART(cleaned_name, ' ', 1)) AS first_name
  , CASE WHEN ARRAY_SIZE(SPLIT(cleaned_name, ' ')) > 1
         THEN INITCAP(SPLIT_PART(cleaned_name, ' ', -1))
    END AS last_name
  FROM {{ ref('leads') }}
  WHERE name IS NOT NULL
)
SELECT 
    l.lead_id AS customer_id
    , l.registration_time
    , l.name 
    , ns.first_name
    , ns.last_name
    , l.email
    , l.referral_cd
    , l.is_employee
    , a.zip_cd as default_zip_cd
    , COALESCE(ss.default_ship_period, 1) AS default_cycle 
    , a.delivery_dow as default_delivery_day
    , gs.customer_id IS NOT NULL AS is_grandfathered
    , ss.commitment AS on_commitment
    , COALESCE(gs.gets_free_delivery, FALSE) AS gets_free_delivery
    , ss.default_meal_count AS default_order_size
    , ss.double AS wants_double_autofill
    , COALESCE(l.email ILIKE '%deactivate%' OR l.name ILIKE '%deactivate%', FALSE) AS is_deactivated
    , ut.userid IS NOT NULL AS is_internal_account
    , ss.active AS is_active
    , ss.premium_meals_valid
    , COALESCE(ss.has_blacksheet_tray, FALSE) AS has_blacksheet_tray
    , dc.customer_id IS NOT NULL AS is_delinquent
    , fa.status_start_time AS first_activation_time
    , ut2.userid IS NOT NULL AS is_influencer
    , bse.customer_id IS NOT NULL AS is_on_break
    , bse.break_start_term
    , bse.break_planned_end_term
    -- TODO: customer_type 
    -- TODO: acquisition_channel
FROM uto 
INNER JOIN {{ ref('leads') }} l
  ON uto.userid = l.lead_id 
LEFT JOIN {{ ref('grandfathered_shipping') }} gs
  ON l.lead_id = gs.customer_id
LEFT JOIN {{ table_reference('stripesubscriptions') }} ss 
  ON l.lead_id = ss.userid 
LEFT JOIN {{ table_reference('usertags') }} ut 
  ON l.lead_id = ut.userid
  AND ut."TAG" = 'internal_account'
LEFT JOIN {{ table_reference('usertags') }} ut2 
  ON l.lead_id = ut2.userid
  AND ut2."TAG" = 'influencer'
LEFT JOIN {{ ref('addresses') }} a 
  ON l.lead_id = a.customer_id 
LEFT JOIN {{ ref('delinquent_customers') }} dc 
  ON l.lead_id = dc.customer_id
LEFT JOIN first_active_time fa 
  ON l.lead_id = fa.customer_id 
  AND fa.row_num = 1
LEFT JOIN name_separation ns
  ON l.lead_id = ns.lead_id
LEFT JOIN {{ ref('break_skip_events') }} bse 
  ON l.lead_id = bse.customer_id 
  AND bse.is_on_break
