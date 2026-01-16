{{ config(
    alias='typeform_questions') }}

SELECT 
  {{ hash_natural_key('form_id','value:id::STRING', 'upload_time') }} AS id
  , form_id
  , {{ clean_string('value:id::STRING') }} AS question_id
  , index+1 as question_number
  , {{ clean_string('value:ref::STRING') }} AS question_ref
  , {{ clean_string('value:title::STRING') }} AS title
  , value AS raw_question_json
  , upload_time
FROM {{ table_reference('typeform_surveys')}}, LATERAL FLATTEN(question_fields) f
