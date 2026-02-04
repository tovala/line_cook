WITH all_responses AS (
  (SELECT 
     form_id
     , response_id
     , userid 
     , email
   FROM dry.typeform_landings 
   WHERE userid IN ({{ ti.xcom_pull(task_ids='user_id_list', dag_id='user_deletes', key='return_value') }}, '1075968'))
  UNION 
  (SELECT 
     form_id
     , response_id
     , userid 
     , email
   FROM dry.typeform_landings 
   WHERE email IN ({{ ti.xcom_pull(task_ids='email_list', dag_id='user_deletes', key='return_value') }})))
SELECT 
  CASE WHEN userid IS NOT NULL
       THEN userid 
       ELSE LOWER(email) 
  END AS user_identifier
  , form_id
  , ARRAY_TO_STRING(ARRAY_AGG(response_id), ', ')
FROM all_responses 
GROUP BY 1,2;