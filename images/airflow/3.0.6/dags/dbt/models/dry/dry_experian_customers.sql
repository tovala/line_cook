{{
  config(
    alias='experian_customers',
    materialized='incremental', 
    unique_key='customer_id'
  ) 
}}

SELECT 
  customer_id
  , updated 
  , {{ clean_string('raw_data:"224"') }}::STRING AS person_1_marital_status
  , {{ clean_string('raw_data:"891"') }}::STRING AS mail_objector
  , {{ clean_string('raw_data:"986"') }}::STRING AS mail_responder
  , {{ clean_string('raw_data:"4496"') }}::STRING AS homeowner_combined_homeowner_renter
  , {{ clean_string('raw_data:"4811"') }}::STRING AS behaviorbank_interest_in_fitness
  , {{ clean_string('raw_data:"4822"') }}::STRING AS behaviorbank_interest_in_gourmet_cooking
  , {{ clean_string('raw_data:"4858"') }}::STRING AS behaviorbank_kitchen_aids_small_appliances
  , {{ clean_string('raw_data:"6676"') }}::STRING AS children_presence_of_child_0_18
  , {{ clean_string('raw_data:"10769"') }}::STRING AS person_1_combined_age
  , {{ clean_string('raw_data:"10792"') }}::STRING AS person_1_gender
  , {{ clean_string('raw_data:"10952"') }}::STRING AS new_mover_indicator_last_6_months
  , {{ clean_string('raw_data:"11027"') }}::STRING AS new_homeowner_indicator_6m
  , {{ clean_string('raw_data:"16465"') }}::STRING AS new_parent_36m__indicator
  , {{ clean_string('raw_data:"24008"') }}::STRING AS estimated_household_income_range_code_v6
  , {{ clean_string('raw_data:"189"') }}::INTEGER AS person_1_education_model
  , {{ clean_string('raw_data:"866"') }}::INTEGER AS number_of_adults_in_living_unit
  , {{ clean_string('raw_data:"867"') }}::INTEGER AS number_of_children_in_living_unit
  , {{ clean_string('raw_data:"7504"') }}::INTEGER AS census_rural_urban_county_size_code
  , {{ clean_string('raw_data:"9105"') }}::INTEGER AS household_consumer_expenditures_kitchen
  , {{ clean_string('raw_data:"10105"') }}::INTEGER AS number_of_persons_in_living_unit
  , {{ clean_string('raw_data:"10797"') }}::INTEGER AS person_1_occupation_code
  , {{ clean_string('raw_data:"14699"') }}::INTEGER AS buyer_warehouse_club_members
  , {{ clean_string('raw_data:"14826"') }}::INTEGER AS act_int_eats_at_fast_food_restaurants
  , {{ clean_string('raw_data:"14830"') }}::INTEGER AS buyer_loyalty_card_user
  , {{ clean_string('raw_data:"16376"') }}::INTEGER AS act_int_gourmet_cooking
  , {{ clean_string('raw_data:"16389"') }}::INTEGER AS act_int_music_streaming
  , {{ clean_string('raw_data:"16399"') }}::INTEGER AS act_int_healthy_living
  , {{ clean_string('raw_data:"16400"') }}::INTEGER AS act_int_fitness_enthusiast
  , {{ clean_string('raw_data:"16401"') }}::INTEGER AS act_int_on_a_diet
  , {{ clean_string('raw_data:"16402"') }}::INTEGER AS act_int_weight_conscious
  , {{ clean_string('raw_data:"17028"') }}::INTEGER AS person_1_technology_adoption
  , {{ clean_string('raw_data:"23809"') }}::INTEGER AS person_1_scl_mda_mdl_subscription_boxes_food
  , {{ clean_string('raw_data:"23910"') }}::INTEGER AS person_1_truetouch_deal_seekers
  , {{ clean_string('raw_data:"23912"') }}::INTEGER AS person_1_truetouch_quality_matters
  , {{ clean_string('raw_data:"23915"') }}::INTEGER AS person_1_truetouch_novelty_seekers
  , {{ clean_string('raw_data:"28607"') }}::INTEGER AS cons_behavior_grocery_delivery
  , {{ clean_string('raw_data:"29982"') }}::INTEGER AS retail_shoppers_grocery_freq_spend
  , {{ clean_string('raw_data:"29983"') }}::INTEGER AS retail_shoppers_grocery_high_spend
  , {{ clean_string('raw_data:"29984"') }}::INTEGER AS retail_shoppers_meal_subscription_high_spend
  , {{ clean_string('raw_data:"29985"') }}::INTEGER AS retail_shoppers_online_grocery_del_high_spend
  , {{ clean_string('raw_data:"29986"') }}::INTEGER AS retail_shoppers_organic_grocery_high_spend
  , {{ clean_string('raw_data:"30070"') }}::INTEGER AS retail_shoppers_foodies
  , {{ clean_string('raw_data:"30494"') }}::INTEGER AS retail_shoppers_big_box_costco_high_spend
  , {{ clean_string('raw_data:"25579"') }}::INTEGER AS food_frequent_family_restaurant_diner
  , {{ clean_string('raw_data:"25580"') }}::INTEGER AS food_frequent_fast_food_diner
  , {{ clean_string('raw_data:"28578"') }}::INTEGER AS lm_summer_break_travelers
  , {{ clean_string('raw_data:"30428"') }}::STRING AS spend_range_grocery_stores
  , {{ clean_string('raw_data:"30491"') }}::INTEGER AS retail_shoppers_big_box_costco_in_store
  , {{ clean_string('raw_data:"30493"') }}::INTEGER AS retail_shoppers_big_box_costco_frequent
  , {{ clean_string('raw_data:"30530"') }}::INTEGER AS retail_shoppers_healthy_food_meal_kit_service
FROM {{ source('chili', 'experian_customers') }}
WHERE customer_id IS NOT NULL
{% if is_incremental() %}
  AND updated >= (SELECT MAX(updated) FROM {{this}} )
{%- endif -%}
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated DESC) = 1
