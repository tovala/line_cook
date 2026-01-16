
WITH categorized_tags AS (
  SELECT 
    id AS ticket_id 
    , tags.value::STRING AS tag 
    , CASE
        WHEN tag IN ('prospective_customer', 'training_primary', 'account_help', 'ux_issue', 'gifting_inquiry_primary', 'oven_issue_primary', 'meal_refund', 'processed_order_changes', 'oven_order', 'air_fry_pro', 'steam_airfry_oven', 'marketplace', 'unsubscribe_request', 'no_response', 'no_answer_needed', 'business_development', 'service_recovery'
                     --Tags prior to FEB 2023 change below
                     , 'order_inquiry', 'oven_issue', 'food_inquiry', 'feedback_contact', 'leadership', 'techescalation', 'backend_team_escalation', 'ovenescalation', 'refund_pending', 'parts_shipment', 'unsubscribe_reqest', 'account_inquiry') 
        THEN 'primary'
        WHEN tag IN ('tracking_training', 'meal_plan_training', 'commitment_training', 'meal_selection_training', 'nutrition_detail_training', 'checkout_training', 'delivery_eduction_training', 'app_training', 'website_training', 'tovala_cash/gift_card_training', 'auto-select_training', 'air_fry_pro_training', 'steam_and_air_fry_training'
                     , 'oven_comparison_training', 'meal_plan_settings', 'meal_selection', 'oven_fan_issue', 'login_help', 'skip/pause_help', 'display_error__door', 'display_error__3-x', 'oven_return', 'cancel_meals', 'cutting-out', 'does_not_turn_on',  'profile_update', 'duplicate_account', 'missed_promotion/referral', 'oven_return/exchange_status', 'commitment_extension', 'commitment_buy_out', 'receipt', '2nd_oven_discount'
                     , 'air_fry_pro_feedback', 'steam and air_fry_feedback', 'web_bug', 'app_experience', 'menu_feedback', 'meal_feedback', 'cosmetic_damage', 'shipping_experience', 'carrier_fedex', 'carrier_ups', 'oven_light_issue', 'carrier_veho', 'carrier_uds', 'carrier_ontrac', 'carrier_axelhire', 'gift_card_issue', 'gift_bundle_issue', 'self-ship', 'scheduled_gift_delivery'
                     , 'delayed_meals_-_melted_ice', 'on_time_meals_-_melted_ice', 'missing/wrong_protein', 'single_vac-pack_damaged', 'smell_or_bakeout_emission', 'multiple_vac-pack_damaged', 'skip/pause_confusion', 'automatic_selection_accomodation', 'wrong_meal', 'missing_meal_component', 'damaged_meal_component', 'box_damaged', 'missing_full_box', 'carrier_misdelivery'
                     , 'open_tray', 'food_quality_refund', 'other_meal_refund', 'cycle_1', 'cycle_2', 'chicago', 'salt_lake_city', 'oven_electrical_issue', 'cancel_processed_order', 'order_payment_issue', 'order_status', 'update_delivery_date', 'oven_other', 'oven_lost', 'update_box_size', 'update_meal_selection', 'update_order_address', 'oven_order_cancel', 'broken_glass', 'oven_order_tracking', 'lost_oven'
                     , 'damaged_oven', 'missing_accessories', 'discoloration_afp', 'water_dripping_afp', 'steam_concern_afp', 'syncing_issue_afp', 'hardware_issue_afp', 'cooking_afp', 'cleaning_issue_afp', 'non_responsive_afp', 'scanner_issue_afp', 'syncing_issue', 'hardware_issue', 'cooking', 'oven_display_issue', 'cleaning_issue', 'no_power', 'no_power_afp', 'non_responsive'
                     , 'scanner_issue', 'accessory_quality', 'accessory_quality_afp', 'water_sensor_issue', 'oven_issue_resolved', 'courtesy_credit/education', 'oven_credit', 'oven_return_offered', 'oven_exchange_offered', 'oven_result_unknown', 'oven_customer_training', 'accessory_shipment', 'wildgrain', 'lou_malnatis', 'classic_comfort_box', 'achatz'
                     , 'panbury', 'hot_cakes', 'nuchas', 'balkan_bites', 'get_maine', 'holiday_box', 'callies_hlb', 'home_dough', 'pizza_cupcake', 'butcher_box', 'marketplace_order_issue', 'marketplace_order_update', 'marketplace_general_inquiry', 'marketplace_saf_cooking', 'marketplace_afp_cooking'
                     --Tags prior to FEB 2023 change below
                     , 'training', 'gifting_inquiry','mobile_app_issue', 'web_issue','cancel_oven_order','delayed_delivery','meal_tracking_-_on_time','automatic_selection_issue', 'skip/pause_issue','cancellation_by_customer','cancelled_by_tovala','meal_quality','meal_packaging_issue','packout_issue','payment_issue'
                     , 'oven_order_cancellation','oven_tracking', 'delivery_issue','process_education','no_action','credit','refund','cancel_meal_plan','return_oven/cancel','shipping_claim_meal','update_order','reroute_successful','reroute_unsuccessful','updated_shipping_info','updated_payment_info','syncing_isue', 'electrical'
                     , 'shipping_claim_oven','dietary_request','ingredients/nutritional_detail','food_quality','meal_size','oven_feedback','web/app_feedback','general_feedback','shipper_feedback_','general_inquiry','oven_basics','commitment_inquiry','meal_basics','checkout_help','promotional_inquiry')
        THEN 'secondary'
        WHEN tag IN ('cancel_accidental', 'cancel_delivery_issues', 'cancel_food_quality', 'cancel_meal_cost', 'cancel_meal_damage', 'cancel_meal_options', 'cancel_other', 'cancel_oven_issues', 'cancel_packaging', 'cancel_skip/auto_select_confusion', 'cancel_no_reason', 'email_login', 'payment', 'shipping_address', 'no_bluetooth_code'
                     , 'not_connecting_to_wifi', 'wifi_credentials/5_ghz', 'unknown_error', 'other_syncing', 'ios', 'android', 'new_oven', 'used_oven', 'fan_noise_afp', 'first_cycle_afp', 'bulb_light_cover_afp', 'error_code_afp', 'door_issue_afp', 'restarting_afp', 'other_hardware_afp', 'under_cooking_afp', 'uneven_cooking_afp', 'over_cooking_afp'
                     , 'strange_smell_afp', 'other_cooking_afp', 'fan_noise', 'door_issue', 'smell_during_first_cycle', 'bulb_light_cover', 'error_code', 'restarting', 'other_hardware', 'under_cooking', 'uneven/hotspot', 'over_cooking', 'strange_smell', 'other_cooking'
                     --Tags prior to FEB 2023 change below
                     , 'meal_plan_training','oven_feature_training','oven_ordering_training','checkout_training','cooking_tovala_meals_training','home_cooking_training','scan-to-cook_training','scheduled_gift_delivery','gift_card_issue','self-ship','gift_bundle_issue','ice_pack_rupture','insulation_failure','outer_box_damage'
                     , 'seal_failure','vacpack_damage','wrong_meal_sent','missing_meal','garnish_missing/incorrect','missing_main_dish/protein','missing_side_dish','missing_tray','Wifi credentials/5 ghz','Unknown Error','First','Resync', 'missing_part','gasket_issue','hidden_damage','insulation_sticking_out'
                     , 'wire_sticking_out','heating_instructions','recipes','low_sodium','vegan','vegetarian','protein_variety','save_preferences','larger_portions','pricing','gift_card','credit_card','referral','meals_only','affirm','stuck_in_checkout','gift_bundle','promotion_issue')
        THEN 'tertiary' 
        ELSE 'legacy' -- TODO: Josh Boock to instruct us what to do with "unknown" levels 
      END AS tag_level
  FROM {{ source('zendesk_support_v3', 'tickets') }},
  LATERAL FLATTEN(INPUT => tags) tags
)
SELECT
  ticket_id 
  , MAX(CASE WHEN tag_level = 'primary' THEN tag ELSE NULL END) AS primary_tag
  , MAX(CASE WHEN tag_level = 'secondary' THEN tag ELSE NULL END) AS secondary_tag
  , MAX(CASE WHEN tag_level = 'tertiary' THEN tag ELSE NULL END) AS tertiary_tag
  , MAX(CASE WHEN tag_level = 'legacy' THEN tag ELSE NULL END) AS legacy_tag
FROM categorized_tags
GROUP BY 1
