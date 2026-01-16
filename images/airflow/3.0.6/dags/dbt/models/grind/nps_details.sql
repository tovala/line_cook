-- Facts table for NPS survey responses by customer 
-- This is where business logic for NPS interpretations (e.g. NPS score, response type, etc) is defined

WITH nps_responses AS (
  -- Calculates NPS specific fields/values for each response 
  SELECT 
    landing_id AS response_id
    , MAX(numeric_answer) AS score
    , CASE WHEN score IN (9, 10) THEN 'promoter'
           WHEN score IN (7, 8) THEN 'passive' 
           WHEN score <= 6 THEN 'detractor' 
      END AS response_type
    , NULLIF(LISTAGG(CASE WHEN question_id IN ('sTpjxSsIjQe4', 'MuNzNSmpTg42')
                          THEN text_answer
                     END, '|'), '')  AS quote
  FROM {{ table_reference('typeform_answers') }}
  WHERE (form_id = 'SmBi5YTd' AND question_id IN ('amx3J1RXF3P3', 'MuNzNSmpTg42'))
    OR (form_id = 'uOnW1o' AND question_id IN ('m8OL3tAB7mdn', 'sTpjxSsIjQe4'))
  GROUP BY 1
)
SELECT
    tf.response_id
    , tf.customer_id
    , tf.response_time AS survey_time 
    -- NPS specific fields 
    , tf.form_id = 'SmBi5YTd' AS is_updated_survey
    , nps.score 
    , nps.response_type
    , nps.quote 
    -- Where does this response fall within n NPS surveys taken by the customer?
    , ROW_NUMBER() OVER (PARTITION BY tf.customer_id ORDER BY response_time DESC) AS nth_response
FROM {{ ref('all_typeform_responses') }} tf
LEFT JOIN nps_responses nps
  ON tf.response_id = nps.response_id
WHERE (tf.form_id = 'SmBi5YTd' OR tf.form_id = 'uOnW1o')
AND tf.customer_id IS NOT NULL 
-- Removing five edge cases with null NPS scores
AND tf.response_id NOT IN ('ikjkk8lxi1dn8zh6ikjkkk8b2u7oqan4',
                           'zg11tahytjd60bzg1rcz5igd1g7im20a',
                           'q7lu0zuxjvxxozqo4wjgq7lu6jou7qbc',
                           'davwn7qesntnriuaadavwn76xxo3cleu',
                           'n5oyh7hacm953n2v5hmn5oyh7haftrjy')