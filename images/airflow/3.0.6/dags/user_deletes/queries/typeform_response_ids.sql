WITH all_responses AS (
  (SELECT 
     form_id
     , response_id
     , userid 
     , email
   FROM dry.typeform_landings 
   WHERE userid IN ({{ params.user_id_list }}))
  UNION 
  (SELECT 
     form_id
     , response_id
     , userid 
     , email
   FROM dry.typeform_landings 
   WHERE email IN ({{ params.email_list }})))
SELECT 
  CASE WHEN userid IS NOT NULL
       THEN userid 
       ELSE LOWER(email) 
  END AS user_identifier
  , form_id
  , ARRAY_TO_STRING(ARRAY_AGG(response_id), ', ')
FROM all_responses 
GROUP BY 1,2;