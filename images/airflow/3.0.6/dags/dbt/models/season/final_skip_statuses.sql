
SELECT 
  oss.skip_id 
  , oss.customer_id 
  , oss.term_id 
  , oss.skip_time
  , oss.unskip_time 
  , oss.skip_end_type AS final_status 
  , oss.is_forced_skip
  , oss.is_break_skip
  , oss.meal_order_id
  , CASE
        WHEN sr.answer IN (
            'I do not need meals this week', 
            'Budget/ financial reasons', 
            'I am not interested in the meals Tovala is offering this week'
            )
        OR sr.answer IS NULL
        THEN sr.answer
        ELSE 'Other' 
  END AS reason 
  , sr.customer_id IS NOT NULL AS took_skip_survey
  , sr.going_out_of_town
  , sr.going_out_to_eat
  , sr.ordering_delivery_or_takeout 
  , sr.cooking_another_meal_kit
  , sr.cooking_from_scratch
  , sr.eating_frozen_meals
  , sr.prefer_not_to_answer
  , sr.desired_more_healthy_options
  , sr.desired_more_comfort_options
  , sr.desired_more_lunch_options
  , sr.desired_more_vegetarian_options
  , sr.desired_better_dietary_restriction_fit
  , sr.desired_more_protein_variety
  , sr.desired_more_breakfast_options
  , sr.desired_more_cuisine_variety
  , sr.specified_low_sodium_options
  , sr.specified_low_calorie_options
  , sr.specified_low_carb_options
  , sr.specified_gluten_free_options
  , sr.specified_dairy_free_options
  , sr.specified_vegetables_options
  , sr.specified_salad_options
  , sr.specified_vegetarian_options
  , sr.skip_feedback
FROM {{ ref('ordinal_skip_statuses') }} oss
LEFT JOIN {{ ref('skip_reasons') }} sr 
  ON oss.customer_id = sr.customer_id 
  AND oss.term_id = sr.term_id 
WHERE oss.row_number = 1 
AND oss.term_id < {{ live_term() }}
AND oss.meal_order_id IS NOT NULL 
AND NOT EXISTS ( -- Do not count as "skipped" if they were paused (inactivity overrides skip)
  SELECT 1 
  FROM {{ ref('pauses') }}
  WHERE customer_id = oss.customer_id 
  AND oss.skip_time >= pause_time 
  AND oss.skip_time < COALESCE(unpause_time, '9999-12-31'::TIMESTAMP)
)
