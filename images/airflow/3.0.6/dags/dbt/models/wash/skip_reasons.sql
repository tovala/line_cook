
WITH latest_answer AS (
  SELECT 
    TRY_TO_NUMERIC(tl.userid) AS customer_id 
    , TRY_TO_NUMERIC(tl.termid) AS term_id 
    , tl.landing_id
    , tl.submitted_at AS survey_time
    , tl.platform 
    , tl.browser 
    , tl.user_agent
    , {{ typeform_mc_parser('C0Psz27rbS9e') }} AS main_skip_reason
    , {{ typeform_question_mapping('Going out of town','rUz0NzW8iaQK') }} AS going_out_of_town
    , {{ typeform_question_mapping('Going out to eat','rUz0NzW8iaQK') }} AS going_out_to_eat
    , {{ typeform_question_mapping('Cooking from another meal kit','rUz0NzW8iaQK') }} AS cooking_another_meal_kit
    , {{ typeform_question_mapping('Ordering delivery/ takeout','rUz0NzW8iaQK') }} AS ordering_delivery_or_takeout
    , {{ typeform_question_mapping('Cooking from scratch','rUz0NzW8iaQK') }} AS cooking_from_scratch
    , {{ typeform_question_mapping('Eating frozen meals','rUz0NzW8iaQK') }} AS eating_frozen_meals
    , {{ typeform_question_mapping('Prefer not to answer','rUz0NzW8iaQK') }} AS prefer_not_to_answer
    , {{ typeform_question_mapping('More healthy options','IBu4gcaP9rWH') }} AS desired_more_healthy_options
    , {{ typeform_question_mapping('More comfort options','IBu4gcaP9rWH') }} AS desired_more_comfort_options
    , {{ typeform_question_mapping('More lunch options','IBu4gcaP9rWH') }} AS desired_more_lunch_options
    , {{ typeform_question_mapping('More vegetarian options','IBu4gcaP9rWH') }} AS desired_more_vegetarian_options
    , {{ typeform_question_mapping('More dishes that fit my dietary restrictions','IBu4gcaP9rWH') }} AS desired_better_dietary_restriction_fit
    , {{ typeform_question_mapping('Wider protein variety','IBu4gcaP9rWH' ) }} AS  desired_more_protein_variety
    , {{ typeform_question_mapping('More breakfast options','IBu4gcaP9rWH' ) }} AS desired_more_breakfast_options
    , {{ typeform_question_mapping('Greater cuisine variety','IBu4gcaP9rWH' ) }} AS desired_more_cuisine_variety
    , {{ typeform_question_mapping('Low carb options','jjJ32mBOhF3A') }} AS specified_low_carb_options
    , {{ typeform_question_mapping('Low calorie options','jjJ32mBOhF3A') }} AS specified_low_calorie_options
    , {{ typeform_question_mapping('Vegetarian options','jjJ32mBOhF3A') }} AS specified_vegetarian_options
    , {{ typeform_question_mapping('Salad options','jjJ32mBOhF3A') }} specified_salad_options
    , {{ typeform_question_mapping('Vegetables in dishes','jjJ32mBOhF3A') }} AS specified_vegetables_options
    , {{ typeform_question_mapping('Gluten free options','jjJ32mBOhF3A') }} AS specified_gluten_free_options
    , {{ typeform_question_mapping('Dairy free options','jjJ32mBOhF3A') }} AS specified_dairy_free_options
    , {{ typeform_question_mapping('Low sodium options','jjJ32mBOhF3A') }} AS specified_low_sodium_options
    , NULLIF(LISTAGG(CASE WHEN ta.question_id = 'fuxqBuxxEqOC' THEN ta.text_answer END, '|'), '') AS skip_feedback
    , ROW_NUMBER() OVER (PARTITION BY customer_id, term_id ORDER BY tl.submitted_at DESC) AS nth
  FROM {{ table_reference('typeform_landings') }} tl 
  INNER JOIN {{ table_reference('typeform_answers') }} ta
    ON tl.landing_id = ta.landing_id
  WHERE tl.form_id = 'GBOKHQ'
  AND TRY_TO_NUMERIC(tl.userid) IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7
)
SELECT 
 customer_id 
  , term_id 
  , main_skip_reason AS answer
  , going_out_of_town
  , going_out_to_eat
  , cooking_another_meal_kit
  , ordering_delivery_or_takeout
  , cooking_from_scratch
  , eating_frozen_meals
  , prefer_not_to_answer
  , desired_more_healthy_options
  , desired_more_comfort_options
  , desired_more_lunch_options
  , desired_more_vegetarian_options
  , desired_better_dietary_restriction_fit
  , desired_more_protein_variety
  , desired_more_breakfast_options
  , desired_more_cuisine_variety
  , specified_low_sodium_options
  , specified_low_calorie_options
  , specified_low_carb_options
  , specified_gluten_free_options
  , specified_dairy_free_options
  , specified_vegetables_options
  , specified_salad_options
  , specified_vegetarian_options
  , skip_feedback
  , survey_time
  , platform 
  , browser
  , user_agent 
FROM latest_answer 
WHERE nth = 1 
