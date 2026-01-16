
SELECT 
  {{ hash_natural_key('test_id', 'value::STRING') }} AS id
  , {{ clean_string('value::STRING') }} AS model_id
  , test_id
FROM {{ ref('current_tests') }}, LATERAL FLATTEN(input => models_tested)
