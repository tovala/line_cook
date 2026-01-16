-- This is the non-production version of affinity used for testing autofill

SELECT 
  ac.customer_id
  , am.meal_sku_id 
  , ac.term_id
  -- Included so we know the customer's setting at the time of their order 
  , ac.wants_double_autofill
  , CASE WHEN (am.contains_pork AND NOT ac.eats_pork)
               OR (am.is_surcharged AND NOT ac.wants_surcharged_autofill)
               OR (am.is_breakfast AND NOT ac.wants_breakfast_autofill)
         THEN 0 
         ELSE 100 
    END AS meal_affinity
FROM {{ ref('autofillable_customers') }} ac 
INNER JOIN {{ ref('autofillable_meals') }} am 
  ON ac.term_id = am.term_id
