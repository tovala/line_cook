
SELECT 
  TRY_TO_NUMERIC(tl.userid) AS customer_id 
  , tl.landing_id
  , tl.submitted_at AS survey_time
  , tl.platform 
  , tl.browser 
  , tl.user_agent
  , MAX(CASE WHEN ta.question_id = 'sbtiVMJWUWof' THEN ta.text_answer END) AS customer_typed_reason
  , {{ typeform_mc_parser('taWrMZUJp9e6') }} AS will_order_in_future
  , {{ typeform_mc_parser('j0uS5xb034uG') }} AS cancel_reason
FROM {{ table_reference('typeform_landings') }} tl 
INNER JOIN {{ table_reference('typeform_answers') }} ta
  ON tl.landing_id = ta.landing_id
WHERE tl.form_id = 'b0XJUW'
  AND TRY_TO_NUMERIC(tl.userid) IS NOT NULL
GROUP BY 1,2,3,4,5,6
