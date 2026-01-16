
--We are only pulling in the non-multiple answer type responses from typeform.
WITH latest_answer AS (
 	SELECT
	  TRY_TO_NUMERIC(tl.userid) AS customer_id
	  , tl.landing_id  AS survey_response_id
	  , tl.submitted_at AS survey_time
	  --How likely are you to recommend Tovala to a friend or family member?
	  , MAX(CASE WHEN ta.question_id = 'abhIbnYpiFy0'
	             THEN ta.numeric_answer
	        END) AS nps_response
	  --Including yourself, how many people currently *live in your household?*
	  , {{ typeform_mc_parser('HgsU22QApzk7') }} AS household_size
	  --The delivery of my meal boxes is timely and predictable
	  , MAX(CASE WHEN ta.question_id = 'xeRsG7X03Wwn'
	             THEN ta.numeric_answer
	        END) AS timely_delivery_score
	  --The meal boxes contain the correct items that I have ordered
	  , MAX(CASE WHEN ta.question_id = 'ZqsoWdOOB6bF'
	             THEN ta.numeric_answer
	        END) AS correct_items_score
	  --The ingredients in the box are fresh and usable
	  , MAX(CASE WHEN ta.question_id = 'RSKphATFCnDW'
	             THEN ta.numeric_answer
	        END) AS ingredient_freshness_score
	  --Unpacking and storing the meals I receive is efficient
	  , MAX(CASE WHEN ta.question_id = 'A0OzutqKDORr'
	             THEN ta.numeric_answer
	         END) AS unpackability_efficiency_score
	  --The meal packaging - the meal card, aluminum tins, proteins, garnishes, spices, etc - makes it easy to make and cook my Tovala meals
	  , MAX(CASE WHEN ta.question_id = 'kOEJ6mtCVWLo'
	             THEN ta.numeric_answer
	         END) AS packaging_convenience_enablement_score
	  --Disposing of the packaging (from the outer box, insulation and ice packs to the meal packaging) is convenient
	  , MAX(CASE WHEN ta.question_id = 'OyoXMjF9t60q'
	             THEN ta.numeric_answer
	        END) AS packaging_disposal_score
	  --Tovala’s Customer Support team is a helpful resource when I need assistance.
	  , MAX(CASE WHEN ta.question_id = '05MDIUjnpYPZ'
	             THEN ta.numeric_answer
	        END) AS cs_helpfulness_score
	  --Managing my weekly Tovala deliveries online is easy
	  , MAX(CASE WHEN ta.question_id = 'fJ5LOa5nkRE4'
	             THEN ta.numeric_answer
	        END) AS ease_of_order_management_score
	  --Browsing menus and selecting my meals is fun",
	  , MAX(CASE WHEN ta.question_id = 'f4u0g0bivWHI'
	             THEN ta.numeric_answer
	        END) AS browsing_menu_enjoyment_score
	  --I understand what the weekly cut off to select meals is, and what happens if I don’t select meals in time.
	  , MAX(CASE WHEN ta.question_id = '9spmftfDPXgw'
	             THEN ta.numeric_answer
	        END) AS order_deadline_understanding_score
	  --Tovala’s menus offer a large variety of meals relevant to me
	  , MAX(CASE WHEN ta.question_id = 'QOIS0jkRRRCD'
	             THEN ta.numeric_answer
	        END) AS menu_variety_score
	  --Tovala offers me high-quality and delicious meals
	  , MAX(CASE WHEN ta.question_id = '71tV1DUYJ1c6'
	             THEN ta.numeric_answer
	         END) AS meal_satisfaction_score
	  --I enjoy eating the meals I get from Tovala
	  , MAX(CASE WHEN ta.question_id = 'RzwP70WKBXxl'
	             THEN ta.numeric_answer
	        END) AS meal_enjoyment_score
	  --Tovala is a brand I trust
	  , MAX(CASE WHEN ta.question_id = 'mLFmJO3k6r0Z'
	             THEN ta.numeric_answer
	        END) AS brand_trust_score
	  --Tovala provides good value for the price
	  , MAX(CASE WHEN ta.question_id = 'hCCNpcmocgWU'
	             THEN ta.numeric_answer
	        END) AS value_score
	  --How likely are you to order Tovala meals in the near future?
	  , MAX(CASE WHEN ta.question_id = 'diqBPtDF57D7'
	             THEN ta.numeric_answer
	        END) AS likeliness_to_order_again
	  --Please rate how satisfied you are with the scan-to-cook feature?
	  , MAX(CASE WHEN ta.question_id = 'VxOIYj5VAbrC'
	             THEN ta.numeric_answer
	        END) AS sts_satisfaction_score
	  --Please rate how satisfied you are with the Chef's recipe feature?
	  , MAX(CASE WHEN ta.question_id = 'ms3mGZztW3Lw'
	             THEN ta.numeric_answer
	        END) AS chefs_recipe_satisfaction_score
	  --When it comes to the previous categories (delivery quality and management, meal storage, customer support, meal selection process) is there anything we can do to improve?
	  , NULLIF(LISTAGG(CASE WHEN ta.question_id = 'kByLsj2mSrY9' 
	                        THEN ta.text_answer
	                   END, '|'), '') AS operation_improvement_suggestions
	  --When it comes to the previous categories (menu variety, meal quality, price) is there anything we can do to improve?"
	  , NULLIF(LISTAGG(CASE WHEN ta.question_id = 'SBsYzwOVaUv4' 
	                        THEN ta.text_answer
	                   END, '|'), '') AS meal_improvement_suggestions
	  --What could we do to make the Chef's Recipes feature more useful for you? 
	  , NULLIF(LISTAGG(CASE WHEN ta.question_id = 'KS2WsmDFK85o' 
	                        THEN ta.text_answer
	                   END, '|'), '') AS chefs_recipe_improvement_suggestions
	  --Have we made you aware of our oven referral program?
	  , BOOLOR_AGG(CASE WHEN ta.question_id = 'pHfF4FRkOuEg'
	                    THEN ta.boolean_answer
	               END) AS is_aware_of_referral_program
	  --Have you ever referred someone to a Tovala oven through the referral program?
	  , BOOLOR_AGG(CASE WHEN ta.question_id = 'ExcC6YzTjqaS'
	                    THEN ta.boolean_answer
	               END) AS has_referred_someone
	FROM {{ table_reference('typeform_landings') }} tl
	INNER JOIN {{ table_reference('typeform_answers') }} ta
	  ON tl.landing_id = ta.landing_id
	WHERE tl.form_id = 'a7AO9gbm'
	  AND TRY_TO_NUMERIC(tl.userid) IS NOT NULL
	GROUP BY 1,2,3
	QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY survey_time DESC) = 1
)
SELECT 
  customer_id
  , survey_response_id
  , survey_time
  , nps_response
  , household_size
  , timely_delivery_score
  , correct_items_score
  , ingredient_freshness_score
  , unpackability_efficiency_score
  , packaging_convenience_enablement_score
  , packaging_disposal_score
  , cs_helpfulness_score
  , ease_of_order_management_score
  , browsing_menu_enjoyment_score
  , order_deadline_understanding_score
  , menu_variety_score
  , meal_satisfaction_score
  , meal_enjoyment_score
  , brand_trust_score
  , value_score
  , CASE WHEN likeliness_to_order_again = 5 THEN 'extremely_likely'
         WHEN likeliness_to_order_again = 4 THEN 'moderately_likely'
         WHEN likeliness_to_order_again = 3 THEN 'somewhat_likely'
         WHEN likeliness_to_order_again = 2 THEN 'moderately_unlikely'
         WHEN likeliness_to_order_again = 1 THEN 'extremely_unikely'
    END AS likeliness_to_order_again
  , sts_satisfaction_score
  , chefs_recipe_satisfaction_score
  , operation_improvement_suggestions
  , meal_improvement_suggestions
  , chefs_recipe_improvement_suggestions
  , is_aware_of_referral_program
  , has_referred_someone
FROM latest_answer
