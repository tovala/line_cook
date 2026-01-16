-- TO DO: flesh this out with considerably more information 
-- Includes customers who have had either a fulfilled meal order OR a fulfilled oven order
WITH latest_order AS (
  SELECT 
    customer_id 
    , cycle 
    , status
    , term_id
    , is_fulfilled
    , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS row_num
    , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_fulfilled DESC, order_time DESC) AS row_num_without_unfulfilled
    , COUNT(DISTINCT CASE WHEN is_fulfilled THEN meal_order_id END) OVER 
           (PARTITION BY customer_id ORDER BY order_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
      AS total_fullfilled_orders_count
    , MAX(order_time) OVER (PARTITION BY customer_id) AS latest_meal_order_time 
    , MAX(CASE WHEN is_fulfilled THEN order_time END) OVER (PARTITION BY customer_id) AS latest_fulfilled_meal_order_time 
  FROM {{ ref('meal_orders') }}
), gift_card_purchases AS (
  SELECT 
    buyer_customer_id 
    , COUNT(DISTINCT transaction_id) AS gift_card_purchase_count
  FROM {{ ref('gift_cards_summary') }}
  WHERE transaction_type = 'card_purchase'
  GROUP BY 1 
  HAVING gift_card_purchase_count > 0
), gift_card_redemptions AS (
  SELECT DISTINCT 
    redeemer_customer_id 
  FROM {{ ref('gift_cards_summary') }}
  WHERE transaction_type = 'meal_purchase'
), active_in_app AS (
  SELECT
    fstc.customer_id
    , BOOLOR_AGG(se.event_start_time >= DATEADD('day',-7,CURRENT_DATE)) AS has_recent_activity
    , MODE(se.page_os) AS most_common_os
  FROM {{ table_reference('session_events', 'grind') }} se
  INNER JOIN {{ table_reference('fullstory_session_to_customer', 'wash') }} fstc
    ON se.session_id = fstc.session_id
  GROUP BY 1
), nps_reccomendation_score AS (
  SELECT 
    customer_id
    , MAX_BY(response, response_time) AS nps_recommendation_score
  FROM {{ ref('all_typeform_responses') }}
  -- Question ID for "How likely are you to recommend Tovala to a friend or colleague?"
  WHERE question_id = 'amx3J1RXF3P3'
  GROUP BY customer_id
)
SELECT DISTINCT
  ac.customer_id
  , cu.is_internal_account
  , cu.on_commitment
  , cu.is_delinquent AS is_payment_delinquent 
  , cu.is_deactivated 
  , cu.is_on_break
  , cu.break_start_term
  , cu.break_planned_end_term
  , cu.email AS email
  , fmo.meal_order_id AS first_meal_order_id
  , fmo.order_size AS first_order_size
  , fus.default_order_size AS first_default_order_size
  , fmo.first_shipping_service
  , fmo.order_time AS first_meal_order_time
  , lo.latest_meal_order_time 
  , lo2.latest_fulfilled_meal_order_time
  -- Includes the first term if order is excluded (GMA/trialvala)
  , COALESCE(fmo.first_term_with_excluded, fmo.term_id) AS first_order_term_id
  , CASE WHEN NOT COALESCE(fmo.is_excluded_order, FALSE)
         THEN c.cohort
    END AS cohort
  , CASE WHEN NOT COALESCE(fmo.is_excluded_order, FALSE) 
         THEN cl.week_with_holidays - c.week_with_holidays + 1 
    END AS latest_cohort_week_with_holidays
  , CASE WHEN NOT COALESCE(fmo.is_excluded_order, FALSE) 
         THEN cl.week_without_holidays - c.week_without_holidays + 1 
    END AS latest_cohort_week_without_holidays
  , CASE WHEN NOT COALESCE(fmo.is_excluded_order, FALSE)
         THEN c.offset_with_holidays
    END AS offset_with_holidays
  , CASE WHEN NOT COALESCE(fmo.is_excluded_order, FALSE)
         THEN c.offset_without_holidays
    END AS offset_without_holidays
  , CASE WHEN NOT COALESCE(fmo.is_excluded_order, FALSE)
         THEN c.cohort_start_date
    END AS cohort_start_date
  , cpiv.cook_cohort_date 
  , COALESCE(cpiv.is_active_oven_user, FALSE) AS is_active_oven_user
  , ra.user_id IS NOT NULL AS is_excluded_from_retention
  , COALESCE(acm.is_active_free_dessert_for_life, FALSE) AS is_free_dessert_customer
  , acm.cancellation_time_free_dessert_for_life AS cancellation_time_free_dessert_for_life
  , COALESCE(fmo.is_excluded_order, FALSE) AS has_excluded_orders_only
  , lus.user_status AS latest_status 
  , COALESCE( -- User is inactive if they are paused/canceled OR they had 3 consecutive payment errors OR if they were payment delinquent in their latest order
      lus.user_status IN ('paused', 'canceled') OR lo.status = 'payment_delinquent', 
      FALSE
    ) AS is_inactive
  , CASE WHEN lus.user_status = 'paused'
         THEN lus.status_start_time
         ELSE NULL
    END AS current_pause_time
  , CASE WHEN lus.user_status = 'canceled'
         THEN lus.status_start_time
         ELSE NULL
    END AS current_cancel_time
  , lus.default_order_size AS latest_default_order_size
  , zsi.facility_network AS latest_default_facility_network
  , zsi.shipping_service AS latest_shipping_service 
  , zsi.shipping_company AS latest_shipping_company
  , zsi.transit_days AS latest_shipping_days
  , COALESCE(lus.cycle, CASE WHEN lo.is_fulfilled THEN lo.cycle END) AS latest_cycle
  , dtc.oven_order_id AS first_oven_order_id
  , COALESCE(dtc.dtc_ovens_purchase_count, 0) AS dtc_ovens_purchase_count 
  , dtc.first_dtc_purchase_time
  , CASE WHEN roh.oven_retailer IS NOT NULL 
         THEN roh.oven_retailer
         WHEN crc.customer_id IS NOT NULL 
         THEN crc.retail_channel
         ELSE 'dtc' 
    END AS first_oven_retailer
  , crc.customer_id IS NOT NULL OR roh.oven_retailer IS NOT NULL AS is_retail_user
  , COALESCE(roh.costco_show_code, crc.show_code) AS costco_show_code 
  , roh.start_time AS first_oven_registration_time
  , roh.serial_number AS first_registration_serial_number
  , fmo.meal_order_id IS NOT NULL AS is_subscription_customer
  , dtc.first_dtc_purchase_time IS NOT NULL AS is_dtc_oven_customer
  , COALESCE(dtc.first_oven_generation, roh.oven_generation) AS first_oven_generation
  , dtc.first_oven_referral_cd
  , dtc.first_oven_referral_cd IS NOT NULL AS is_referral
  , dtc.is_first_oven_affirm_order
  , dtc.first_oven_coupon_cd
  , dtc.is_first_oven_commitment_purchase
  , dtc.is_commitment_forgiven
  , dtc.first_oven_commitment_week_count
  , dtc.first_oven_first_orderable_term_id
  , dtc.first_oven_sale_price
  , dtc.first_oven_purchase_price
  , dtc.first_oven_charge_amount
  , dtc.is_spotlight_tv_customer
  , COALESCE(psel.pork_selection_count > 0, FALSE) AS eats_pork 
  , COALESCE(psel.tofu_selection_count > 0, FALSE) AS eats_tofu 
  , lus.wants_surcharged_autofill
  , lus.wants_breakfast_autofill
  , g.buyer_customer_id IS NOT NULL AS has_purchased_gift_card
  , gcr.redeemer_customer_id IS NOT NULL AS has_used_gift_card
  , COALESCE(cpiv.scan_the_store_completed_cook_count, 0) AS sts_cook_count
  , COALESCE(cpiv.chefs_recipe_completed_cook_count, 0) AS chefs_recipe_cook_count
  , lo2.term_id AS absolute_last_fulfilled_term_id
  , cmc.first_touch_channel
  , cmc.first_touch_channel_category
  , cmc.last_touch_channel
  , cmc.last_touch_channel_category
  , cmc.has_same_first_last_channel
  , lo2.total_fullfilled_orders_count
  , cpiv.latest_cook_time
  --The categorizations below were provided by the Acquisition team
  , CASE WHEN dtc.first_oven_referral_cd IS NOT NULL
         THEN 'Referral'
         WHEN cmc.last_touch_channel_category IN ('Affiliate', 'Digital')
         THEN 'Paid'
         WHEN cmc.last_touch_channel_category IN ('Direct', 'Organic Search', 'Organic Social', 'Other Referral')
         THEN 'Organic'
         WHEN cmc.last_touch_channel_category IN ('Email', 'SMS')
         THEN 'Owned'
         ELSE cmc.last_touch_channel_category
    END AS acquisition_type
  , dtc.is_direct_mail_oven
  , aia.most_common_os
  , mof.meal_arr AS first_order_value
  -- The first time a customer has interacted with Tovala is the least value of:
      -- 1. Their first non-excluded (i.e. non gma and non trial) meal order
      -- 2. Their first oven purchase
      -- 3. The first time they registered an oven
  , DATE_TRUNC(MINUTE, LEAST(
          COALESCE(fmo.order_time, '9999-12-31'), 
          COALESCE(dtc.first_dtc_purchase_time, '9999-12-31'), 
          COALESCE(roh.start_time, '9999-12-31'))) AS first_interaction_time
  , nps.nps_recommendation_score AS latest_nps_score
  -- Experian information
  , e.education_level
  , e.marital_status
  , e.household_adult_count
  , e.household_child_count 
  , e.likelihood_of_children
  , e.est_household_income_range
  , e.do_not_mail
  , e.is_direct_mail_responder
  , e.home_ownership
  , e.county_type
  , e.county_population
  , e.kitchen_purchase_prob
  , e.household_size
  , e.age
  , e.gender
  , e.has_moved_in_past_6_months 
  , e.bought_home_in_past_6_months 
  , e.became_new_parent_in_past_3_years 
  , e.is_fitness_enthusiast
  , e.is_gourmet_cook
  , e.buys_kitchen_aid_appliances
  , e.warehouse_club_membership_prob 
  , e.loyalty_card_prob
  , e.fast_food_prob
  , e.gourmet_cooking_prob
  , e.music_streaming_prob
  , e.healthy_living_prob
  , e.fitness_enthusiast_prob
  , e.dieting_prob
  , e.weight_conscious_prob
  , e.tech_adoption
  , e.grocery_spend_range
  , e.meal_kit_subscription
  , e.deal_seeker
  , e.quality_conscious
  , e.novelty_seeker
  , e.grocery_delivery_user
  , e.foodie 
  , e.high_organic_spend
  , e.high_grocery_delivery_spend
  , e.high_mealkit_spend
  , e.healthy_mealkit_service
  , e.big_box_costco_in_store
  , e.big_box_costco_frequent
  , e.high_grocery_spend
  , e.high_grocery_frequency
  , e.big_box_costco_high_spend
  , e.frequent_fast_food_diner
  , e.frequent_family_restaurant_diner
  , e.summer_break_travelers
  , e.occupation
  , bcs.bullseye_segment
  , bcs.bullseye_persona
FROM {{ ref('customer_facts_base') }} ac
INNER JOIN {{ ref('customers') }} cu 
  ON ac.customer_id = cu.customer_id 
LEFT JOIN {{ ref('first_meal_orders') }} fmo
  ON ac.customer_id = fmo.customer_id
LEFT JOIN latest_order lo 
  ON fmo.customer_id = lo.customer_id 
  AND lo.row_num = 1
LEFT JOIN {{ ref('cohorts') }} c
  ON fmo.term_id = c.term_id
LEFT JOIN {{ source('brine', 'customers_to_exclude_from_retention_analysis') }} ra 
  ON fmo.customer_id = ra.user_id
LEFT JOIN {{ ref('first_user_statuses') }} fus 
  ON ac.customer_id = fus.customer_id
LEFT JOIN {{ ref('latest_user_statuses') }} lus  
  ON ac.customer_id = lus.customer_id 
LEFT JOIN {{ ref('customer_dtc_ovens') }} dtc 
  ON ac.customer_id = dtc.customer_id
LEFT JOIN {{ ref('customer_protein_selection_counts') }} psel
  ON ac.customer_id = psel.customer_id
LEFT JOIN gift_card_purchases g 
  ON ac.customer_id = g.buyer_customer_id 
LEFT JOIN gift_card_redemptions gcr 
  ON ac.customer_id = gcr.redeemer_customer_id
LEFT JOIN latest_order lo2 
  ON fmo.customer_id = lo2.customer_id 
  AND lo2.row_num_without_unfulfilled = 1
LEFT JOIN {{ ref('customer_marketing_channel') }} cmc
  ON ac.customer_id = cmc.customer_id
LEFT JOIN {{ ref('experian_responses') }} e 
  ON ac.customer_id = e.customer_id 
LEFT JOIN active_in_app aia
  ON ac.customer_id = aia.customer_id
LEFT JOIN {{ ref('historic_oven_registrations') }} roh
  ON ac.customer_id = roh.customer_id
  AND roh.is_first_record_for_customer
LEFT JOIN {{ ref('cohorts') }} cl
  ON cl.cohort = (SELECT MAX(term_id) FROM {{ ref('terms') }} WHERE is_past_order_by)
  AND cl.week_without_holidays IS NOT NULL 
LEFT JOIN {{ table_reference('zipcode_shipping_information') }} zsi 
  ON cu.default_zip_cd = zsi.zipcode 
  AND default_subterm
LEFT JOIN {{ ref('meal_order_facts') }} mof 
  ON mof.meal_order_id = fmo.meal_order_id 
LEFT JOIN {{ ref('customer_oven_usage_pivot') }} cpiv
  ON ac.customer_id = cpiv.customer_id 
LEFT JOIN {{ ref('retail_customer_survey') }} crc 
  ON ac.customer_id = crc.customer_id
LEFT JOIN {{ ref('active_customer_memberships') }} acm
  ON ac.customer_id = acm.customer_id
LEFT JOIN {{ ref('bullseye_customer_segments') }} bcs
  ON ac.customer_id = bcs.customer_id
LEFT JOIN nps_reccomendation_score nps
  ON ac.customer_id = nps.customer_id
