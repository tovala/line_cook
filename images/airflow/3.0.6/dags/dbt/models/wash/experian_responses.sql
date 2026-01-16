SELECT
    customer_id 
    , CASE
        WHEN person_1_education_model = '11' THEN 'high_school'
        WHEN person_1_education_model = '12' THEN 'some_college'
        WHEN person_1_education_model = '13' THEN 'undergrad'
        WHEN person_1_education_model = '14' THEN 'grad'
        WHEN person_1_education_model = '15' THEN 'less_than_hs'
        WHEN person_1_education_model = '51' THEN 'high_school_likely'
        WHEN person_1_education_model = '52' THEN 'some_college_likely'
        WHEN person_1_education_model = '53' THEN 'undergrad_likely'
        WHEN person_1_education_model = '54' THEN 'grad_likely'
        WHEN person_1_education_model = '55' THEN 'less_than_hs_likely'
        ELSE NULL
    END AS education_level
    , CASE
        WHEN person_1_marital_status like '_M' THEN 'married'
        WHEN person_1_marital_status = '5S' THEN 'single'
        ELSE NULL
    END AS marital_status
    , number_of_adults_in_living_unit AS household_adult_count
    , number_of_children_in_living_unit AS household_child_count 
    , CASE
        WHEN children_presence_of_child_0_18 = '00' THEN 'child_only_household'
        WHEN children_presence_of_child_0_18 = '1Y' THEN 'known'
        WHEN children_presence_of_child_0_18 IN ('5N', '5U') THEN 'unlikely'
        WHEN children_presence_of_child_0_18 = '5Y' THEN 'likely'
        ELSE NULL 
    END AS likelihood_of_children
    , CASE 
        WHEN estimated_household_income_range_code_v6 = 'A' THEN '$1,000-$14,999'
        WHEN estimated_household_income_range_code_v6 = 'B' THEN '$15,000-$24,999'
        WHEN estimated_household_income_range_code_v6 = 'C' THEN '$25,000-$34,999'
        WHEN estimated_household_income_range_code_v6 = 'D' THEN '$35,000-$49,999'
        WHEN estimated_household_income_range_code_v6 = 'E' THEN '$50,000-$74,999'
        WHEN estimated_household_income_range_code_v6 = 'F' THEN '$75,000-$99,999'
        WHEN estimated_household_income_range_code_v6 = 'G' THEN '$100,000-$124,999'
        WHEN estimated_household_income_range_code_v6 = 'H' THEN '$125,000-$149,999'
        WHEN estimated_household_income_range_code_v6 = 'I' THEN '$150,000-$174,999'
        WHEN estimated_household_income_range_code_v6 = 'J' THEN '$175,000-$199,999'
        WHEN estimated_household_income_range_code_v6 = 'K' THEN '$200,000-$249,999'
        WHEN estimated_household_income_range_code_v6 = 'L' THEN '$250k+'     
        ELSE NULL 
   END AS est_household_income_range
    , COALESCE(mail_objector = 'Y', FALSE) AS do_not_mail
    , COALESCE(mail_responder in ('M', 'Y'), FALSE) AS is_direct_mail_responder
    , CASE
        WHEN homeowner_combined_homeowner_renter = 'H' THEN 'homeowner'
        WHEN homeowner_combined_homeowner_renter = 'R' THEN 'renter'
        WHEN homeowner_combined_homeowner_renter = 'U' THEN 'unknown'
        WHEN homeowner_combined_homeowner_renter = 'T' THEN 'renter_likely'
        WHEN homeowner_combined_homeowner_renter in (7,8,9) THEN 'homeowner_likely'
        ELSE NULL 
    END AS home_ownership
    -- Note: Can use same zipcodes to impute for other customers!
    , CASE 
        WHEN census_rural_urban_county_size_code in (1,2,3) THEN 'metro'
        WHEN census_rural_urban_county_size_code in (4,6) THEN 'urban_adjacent'
        WHEN census_rural_urban_county_size_code in (5,7) THEN 'urban_nonadjacent'
        WHEN census_rural_urban_county_size_code in (8,9) THEN 'rural'
        ELSE NULL 
    END AS county_type
    , CASE 
        WHEN census_rural_urban_county_size_code = 1 THEN '1M+'
        WHEN census_rural_urban_county_size_code = 2 THEN '250k-1M'
        WHEN census_rural_urban_county_size_code = 3 THEN '<250k'
        WHEN census_rural_urban_county_size_code in (4,5) THEN '20k+'
        WHEN census_rural_urban_county_size_code in (6,7) THEN '2500-20k'
        WHEN census_rural_urban_county_size_code in (8,9) THEN '<2500'
        ELSE NULL 
    END AS county_population
    , ROUND(household_consumer_expenditures_kitchen/100, 2) AS kitchen_purchase_prob
    , number_of_persons_in_living_unit AS household_size
    , REGEXP_REPLACE(person_1_combined_age, '[^[:digit:]]', '')::INTEGER AS age
    , CASE 
        WHEN person_1_gender = 'M' THEN 'male'
        WHEN person_1_gender = 'F' THEN 'female'
        WHEN person_1_gender = 'U' THEN 'unknown'
        ELSE NULL 
     END AS gender
     , CASE 
        WHEN new_mover_indicator_last_6_months = 'Y' THEN true 
        WHEN new_mover_indicator_last_6_months = 'N' THEN false
        ELSE NULL
    END AS has_moved_in_past_6_months 
    , CASE 
        WHEN new_homeowner_indicator_6m = 'Y' THEN true 
        WHEN new_homeowner_indicator_6m = 'N' THEN false
        ELSE NULL
    END AS bought_home_in_past_6_months 
    , CASE 
        WHEN new_parent_36m__indicator = 'Y' THEN true
        WHEN new_parent_36m__indicator = 'N' THEN false
        ELSE NULL 
    END AS became_new_parent_in_past_3_years 
    , {{ experian_enthusiasm_bools('behaviorbank_interest_in_fitness') }} AS is_fitness_enthusiast
    , {{ experian_enthusiasm_bools('behaviorbank_interest_in_gourmet_cooking') }} AS is_gourmet_cook
    , {{ experian_enthusiasm_bools('behaviorbank_kitchen_aids_small_appliances') }} AS buys_kitchen_aid_appliances
    , {{ experian_probability('buyer_warehouse_club_members') }} AS warehouse_club_membership_prob 
    , {{ experian_probability('buyer_loyalty_card_user') }} AS loyalty_card_prob
    , {{ experian_probability('act_int_eats_at_fast_food_restaurants') }} AS fast_food_prob
    , {{ experian_probability('act_int_gourmet_cooking') }} AS gourmet_cooking_prob
    , {{ experian_probability('act_int_music_streaming') }} AS music_streaming_prob
    , {{ experian_probability('act_int_healthy_living') }} AS healthy_living_prob
    , {{ experian_probability('act_int_fitness_enthusiast') }} AS fitness_enthusiast_prob
    , {{ experian_probability('act_int_on_a_diet') }} AS dieting_prob
    , {{ experian_probability('act_int_weight_conscious') }} AS weight_conscious_prob
    , CASE 
        WHEN person_1_technology_adoption = 1 THEN 'wizard'
        WHEN person_1_technology_adoption = 2 THEN 'tech_savvy'
        WHEN person_1_technology_adoption = 3 THEN 'tech_informed'
        WHEN person_1_technology_adoption = 4 THEN 'novice'
        ELSE NULL
    END AS tech_adoption
    , CASE
        WHEN spend_range_grocery_stores = 'A' THEN '$1-$499'
        WHEN spend_range_grocery_stores = 'B' THEN '$500-$749'
        WHEN spend_range_grocery_stores = 'C' THEN '$750-$999'
        WHEN spend_range_grocery_stores = 'D' THEN '$1,000-$1,499'
        WHEN spend_range_grocery_stores = 'E' THEN '$1,500-$1,999'
        WHEN spend_range_grocery_stores = 'F' THEN '$2,000-$2,499'
        WHEN spend_range_grocery_stores = 'G' THEN '$2,500-$2,999'
        WHEN spend_range_grocery_stores = 'H' THEN '$3,000-$3,999'
        WHEN spend_range_grocery_stores = 'I' THEN '$4,000-$6,499'
        WHEN spend_range_grocery_stores = 'J' THEN '$6,500+'
        ELSE NULL
    END AS grocery_spend_range
    , {{ experian_likelihood('person_1_scl_mda_mdl_subscription_boxes_food') }} AS meal_kit_subscription
    , {{ experian_likelihood('person_1_truetouch_deal_seekers') }} AS deal_seeker
    , {{ experian_likelihood('person_1_truetouch_quality_matters') }} AS quality_conscious
    , {{ experian_likelihood('person_1_truetouch_novelty_seekers') }} AS novelty_seeker
    , {{ experian_likelihood('cons_behavior_grocery_delivery') }} AS grocery_delivery_user
    , {{ experian_likelihood('retail_shoppers_foodies') }} AS foodie 
    , {{ experian_likelihood('retail_shoppers_organic_grocery_high_spend') }} AS high_organic_spend
    , {{ experian_likelihood('retail_shoppers_online_grocery_del_high_spend') }} AS high_grocery_delivery_spend
    , {{ experian_likelihood('retail_shoppers_meal_subscription_high_spend') }} AS high_mealkit_spend
    , {{ experian_likelihood('retail_shoppers_healthy_food_meal_kit_service') }} AS healthy_mealkit_service
    , {{ experian_likelihood('retail_shoppers_big_box_costco_in_store') }} AS big_box_costco_in_store
    , {{ experian_likelihood('retail_shoppers_big_box_costco_frequent') }} AS big_box_costco_frequent
    , {{ experian_likelihood('retail_shoppers_grocery_high_spend') }} AS high_grocery_spend
    , {{ experian_likelihood('retail_shoppers_grocery_freq_spend') }} AS high_grocery_frequency
    , {{ experian_likelihood('retail_shoppers_big_box_costco_high_spend') }} AS big_box_costco_high_spend
    , {{ experian_likelihood('food_frequent_fast_food_diner') }} AS frequent_fast_food_diner
    , {{ experian_likelihood('food_frequent_family_restaurant_diner') }} AS frequent_family_restaurant_diner
    , {{ experian_likelihood('lm_summer_break_travelers') }} AS summer_break_travelers
    , CASE
        WHEN person_1_occupation_code = '00' THEN 'Unknown'
        WHEN person_1_occupation_code = '02' THEN 'Professional/Technical'
        WHEN person_1_occupation_code = '03' THEN 'Upper Management/Executive'
        WHEN person_1_occupation_code = '04' THEN 'Middle Management'
        WHEN person_1_occupation_code = '05' THEN 'Sales/Marketing'
        WHEN person_1_occupation_code = '06' THEN 'Clerical/Office'
        WHEN person_1_occupation_code = '07' THEN 'SkilledTrade/Machine/Laborer'
        WHEN person_1_occupation_code = '08' THEN 'Retired'
        WHEN person_1_occupation_code = '10' THEN 'Executive/Administrator'
        WHEN person_1_occupation_code = '11' THEN 'Self Employed'
        WHEN person_1_occupation_code = '12' THEN 'Professional Driver'
        WHEN person_1_occupation_code = '13' THEN 'Military'
        WHEN person_1_occupation_code = '14' THEN 'Civil Servant'
        WHEN person_1_occupation_code = '15' THEN 'Farming/Agriculture'
        WHEN person_1_occupation_code = '16' THEN 'Work From Home'
        WHEN person_1_occupation_code = '17' THEN 'Health Services'
        WHEN person_1_occupation_code = '18' THEN 'Financial Services'
        WHEN person_1_occupation_code = '21' THEN 'Teacher/Educator'
        WHEN person_1_occupation_code = '22' THEN 'Retail Sales'
        WHEN person_1_occupation_code = '23' THEN 'Computer Professional'
        WHEN person_1_occupation_code = '30' THEN 'Beauty'
        WHEN person_1_occupation_code = '31' THEN 'Real Estate'
        WHEN person_1_occupation_code = '32' THEN 'Architects'
        WHEN person_1_occupation_code = '33' THEN 'Interior Designers'
        WHEN person_1_occupation_code = '34' THEN 'Landscape Architects'
        WHEN person_1_occupation_code = '35' THEN 'Electricians'
        WHEN person_1_occupation_code = '36' THEN 'Engineers'
        WHEN person_1_occupation_code = '37' THEN 'Accountants/CPA'
        WHEN person_1_occupation_code = '38' THEN 'Attorneys'
        WHEN person_1_occupation_code = '39' THEN 'Social Worker'
        WHEN person_1_occupation_code = '40' THEN 'Counselors'
        WHEN person_1_occupation_code = '41' THEN 'Occupational Ther/Physical Ther'
        WHEN person_1_occupation_code = '42' THEN 'Speech Path./Audiologist'
        WHEN person_1_occupation_code = '43' THEN 'Psychologist'
        WHEN person_1_occupation_code = '44' THEN 'Pharmacist'
        WHEN person_1_occupation_code = '45' THEN 'Opticians/Optometrist'
        WHEN person_1_occupation_code = '46' THEN 'Veterinarian'
        WHEN person_1_occupation_code = '47' THEN 'Dentist/Dental Hygienist'
        WHEN person_1_occupation_code = '48' THEN 'Nurse'
        WHEN person_1_occupation_code = '49' THEN 'Doctors/Physicians/Surgeons'
        WHEN person_1_occupation_code = '50' THEN 'Chiropractors'
        WHEN person_1_occupation_code = '51' THEN 'Surveyors'
        WHEN person_1_occupation_code = '52' THEN 'Clergy'
        WHEN person_1_occupation_code = '53' THEN 'Insurance/Underwriters'
        WHEN person_1_occupation_code = '54' THEN 'Services/Creative'
        ELSE NULL
    END AS occupation
    
    -- TODO: Eventually flesh out occupation; Currently (7/6/22) NULL for 80%+ so not bothering with 45 lines of CASE/WHEN right now
FROM {{ table_reference('experian_customers') }}


