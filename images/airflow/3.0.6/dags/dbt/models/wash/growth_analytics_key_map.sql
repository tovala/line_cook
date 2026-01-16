
SELECT 
  gak.key 
  , lf.value::STRING AS deprecated_key
FROM {{ ref('growth_analytics_keys' )}} gak,
LATERAL FLATTEN(deprecated_key_names) lf
