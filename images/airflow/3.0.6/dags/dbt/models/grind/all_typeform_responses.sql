WITH answer_values AS (
  -- Pull all responses for non-multiple choice questions 
  SELECT 
    landing_id,
    question_id,
    CASE 
      WHEN boolean_answer IS NOT NULL THEN boolean_answer::STRING
      WHEN numeric_answer IS NOT NULL THEN numeric_answer::STRING
      WHEN text_answer IS NOT NULL THEN text_answer
      WHEN url_answer IS NOT NULL THEN url_answer
      WHEN file_url_answer IS NOT NULL THEN file_url_answer
      WHEN date_answer IS NOT NULL THEN date_answer::STRING
      WHEN f.value IS NOT NULL THEN f.value::STRING
      -- Typeform forms from before 2021 didn't keep the label in the form, so their responses on multiple choice questions are null
      ELSE NULL
    END AS answer_value,
    data_type,
    answer_type
  FROM {{ table_reference('typeform_answers') }}
  LEFT JOIN LATERAL FLATTEN(input => multiple_choice_answer, outer => TRUE) f
  WHERE (multiple_choice_answer IS NULL AND multiple_choice_other IS NULL)
   OR f.value IS NOT NULL

  UNION ALL

  -- Pull all responses for multiple choice questions 
  SELECT 
    landing_id,
    question_id,
    multiple_choice_other AS answer_value,
    data_type,
    answer_type
  FROM {{ table_reference('typeform_answers') }}
  WHERE multiple_choice_other IS NOT NULL
)

-- Final table combines all survey form details, question metadata, and response metadata
SELECT 
  tl.form_id
  , tf.form_title
  , tl.landing_id AS response_id
  , av.question_id
  , stg.title AS question_text
  , COALESCE(stg.question_type, av.answer_type) AS question_type
  , av.answer_value AS response
  , tl.submitted_at AS response_time
  , COALESCE(tl.response_type, 'completed') AS response_type
  , {{ hash_natural_key('tl.landing_id', 'av.question_id', 'COALESCE(av.answer_value, \'\')') }} AS response_fact_id
  , tl.email
  , TRY_TO_NUMBER(tl.userid) AS customer_id
  , tl.network_id
  -- Denotes the term id during which the Typeform response was submitted 
  , t.term_id AS submitted_term_id
FROM {{ table_reference('typeform_landings') }} tl
INNER JOIN answer_values av 
  ON tl.landing_id = av.landing_id
LEFT JOIN {{ ref('all_typeform_questions') }} stg
  ON av.question_id = stg.question_id
  AND tl.form_id = stg.form_id
LEFT JOIN {{ ref('all_typeform_forms') }} tf
  ON tl.form_id = tf.form_id
LEFT JOIN {{ ref('terms') }} t
  ON tl.submitted_at >= t.start_date
  AND tl.submitted_at < t.next_term_start_date
