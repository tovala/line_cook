
--Ingesting the responses to the Box 1 survey (leaving out the answers where multiple options can be selected) so we can analyze results more easily
WITH latest_answer AS (
  SELECT 
    TRY_TO_NUMERIC(tl.userid) AS customer_id
    , tl.landing_id AS survey_response_id
    , tl.submitted_at AS survey_time
    , MAX(CASE WHEN ta.question_id = 'zrAvD82N1Tbe'
               THEN ta.numeric_answer
          END) AS nps_response
    , BOOLOR_AGG(CASE WHEN ta.question_id = '3HAMCaYFF4Sp'
                      THEN ta.boolean_answer
                 END) AS has_used_mealkits
    , {{ typeform_mc_parser('tXAGVTt9oLLE') }} AS household_size
    , BOOLOR_AGG(CASE WHEN ta.question_id = 'I5XP0R1wKsr3'
                      THEN ta.boolean_answer
                 END) AS had_box_1_issue
    , BOOLOR_AGG(CASE WHEN ta.question_id = 'oGuIhQZD4AjG'
                      THEN ta.boolean_answer
                 END) AS had_contacted_cs
    , BOOLOR_AGG(CASE WHEN ta.question_id = 'PykXDRf847BM'
                      THEN ta.boolean_answer
                 END) AS ate_tovala_meal
    , BOOLOR_AGG(CASE WHEN ta.question_id = 'MhN3GC0wQbNz'
                      THEN ta.boolean_answer
                 END) AS rated_tovala_meal
    , BOOLOR_AGG(CASE WHEN ta.question_id = 'BfaOd0YOALSs'
                      THEN ta.boolean_answer
                 END) AS cooked_non_tovala_meal
    , MAX(CASE WHEN ta.question_id = 'S5ObGwtbQ1bZ'
               THEN ta.numeric_answer
          END) AS likeliness_to_order_again
    , BOOLOR_AGG(CASE WHEN ta.question_id = 'H8KvjilTEUFr'
                      THEN ta.boolean_answer
                 END) AS is_open_to_follow_up
    , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY survey_time DESC) AS nth
  FROM {{ table_reference('typeform_landings') }} tl
  INNER JOIN {{ table_reference('typeform_answers') }} ta
    ON tl.landing_id = ta.landing_id
  WHERE tl.form_id = 'kYJDCI9v'
    AND TRY_TO_NUMERIC(tl.userid) IS NOT NULL
  GROUP BY 1,2,3
)
SELECT 
  customer_id
  , survey_response_id
  , survey_time
  , nps_response
  , has_used_mealkits
  , household_size
  , had_box_1_issue
  , had_contacted_cs
  , ate_tovala_meal
  , rated_tovala_meal
  , cooked_non_tovala_meal
  , CASE WHEN likeliness_to_order_again = 5 THEN 'extremely_likely'
         WHEN likeliness_to_order_again = 4 THEN 'moderately_likely'
         WHEN likeliness_to_order_again = 3 THEN 'somewhat_likely'
         WHEN likeliness_to_order_again = 2 THEN 'moderately_unlikely'
         WHEN likeliness_to_order_again = 1 THEN 'extremely_unikely'
    END AS likeliness_to_order_again
  , is_open_to_follow_up
FROM latest_answer
WHERE nth = 1
