WITH nested_fields AS (
  SELECT 
    form_id,
    f.value:id::STRING as question_id,
    f.value:title::STRING as title,
    f.value:type::STRING as type,
    upload_time
  FROM {{ table_reference('typeform_surveys')}},
  LATERAL FLATTEN(question_fields) base,
  LATERAL FLATTEN(base.value:properties:fields) f
  WHERE base.value:type::STRING IN ('matrix', 'group', 'contact_info', 'address')
)
, regular_questions AS (
  SELECT 
    form_id,
    value:id::STRING as question_id,
    value:title::STRING as title,
    value:type::STRING as type,
    upload_time
  FROM {{ table_reference('typeform_surveys')}},
  LATERAL FLATTEN(question_fields)
  WHERE value:type::STRING NOT IN ('matrix', 'group', 'contact_info', 'address')
)
, combined_questions AS (
  SELECT * FROM nested_fields
  UNION ALL
  SELECT * FROM regular_questions
)
, latest_questions AS (
  SELECT 
    form_id,
    question_id,
    title,
    type,
    upload_time
  FROM combined_questions
  QUALIFY ROW_NUMBER() OVER (PARTITION BY form_id, question_id ORDER BY upload_time DESC) = 1
)

SELECT 
  {{ hash_natural_key('form_id', 'question_id', 'upload_time') }} AS id
  , form_id
  , question_id
  , title
  , type as question_type
  , upload_time
FROM latest_questions 
