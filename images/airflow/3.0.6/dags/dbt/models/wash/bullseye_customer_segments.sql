WITH post_purchase_household_size AS (
  SELECT 
    customer_id
    , response
  FROM {{ ref('all_typeform_responses') }} 
  WHERE form_id IN ('lkCzSO','nSgG9qUa') --post purchase survey (versions before and after March 2025 PPS survey updates)
    AND question_id IN ('KYCBSUKxT77f','EaPYMkxUgGtO') --household number question (versions before and after March 2025 PPS survey updates)
  --only keep the most recent survey response
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY response_time DESC) = 1
)
SELECT 
  cfb.customer_id
  , CASE 
      WHEN er.age BETWEEN 30 AND 45 
        AND (pphs.response IN ('1', '2') OR (pphs.response IS NULL AND er.household_size IN (1, 2)))
        AND er.est_household_income_range IN ('$75,000-$99,999', '$100,000-$124,999', '$125,000-$149,999', '$150,000-$174,999', '$175,000-$199,999', '$200,000-$249,999', '$250k+')
        AND er.education_level IN ('some_college_likely', 'some_college', 'undergrad_likely', 'undergrad', 'grad', 'grad_likely')
        AND er.county_type = 'metro' 
      THEN 'savvy_urbanites'
      WHEN er.age BETWEEN 35 AND 55 
        AND (pphs.response IN ('3','3+','4+') OR (pphs.response IS NULL AND er.household_size >= 3))
        AND er.est_household_income_range IN ('$75,000-$99,999', '$100,000-$124,999', '$125,000-$149,999', '$150,000-$174,999', '$175,000-$199,999', '$200,000-$249,999', '$250k+')
        AND er.education_level IN ('some_college_likely', 'some_college', 'undergrad_likely', 'undergrad', 'grad', 'grad_likely')
        AND er.county_type IN ('metro', 'urban_adjacent') 
      THEN 'suburban_families'
      WHEN er.age >= 55 
        AND (pphs.response IN ('1', '2') OR (pphs.response IS NULL AND er.household_size <= 2 AND er.household_child_count = 0))
        AND er.est_household_income_range IN ('$75,000-$99,999', '$100,000-$124,999', '$125,000-$149,999', '$150,000-$174,999', '$175,000-$199,999', '$200,000-$249,999', '$250k+')
        AND er.education_level IN ('some_college_likely', 'some_college', 'undergrad_likely', 'undergrad', 'grad', 'grad_likely')
      THEN 'empty_nesters'
      ELSE 'other'
    END AS bullseye_segment
  , CASE
      WHEN bullseye_segment = 'savvy_urbanites' THEN 'Sam'
      WHEN bullseye_segment = 'suburban_families' THEN 'Jordan'
      WHEN bullseye_segment = 'empty_nesters' THEN 'Robin'
    END AS bullseye_persona
FROM {{ ref('customer_facts_base') }} cfb
LEFT JOIN {{ ref('experian_responses') }} er
  ON cfb.customer_id = er.customer_id
LEFT JOIN post_purchase_household_size pphs
  ON cfb.customer_id = pphs.customer_id