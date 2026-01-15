{% macro facebook_ads_name_json_cte(source_name, table_name) %}
-- CTE that adds the parsed ad name as an array, in the format ['$key:$value_', '$key:$value_', ...]
(WITH parsed_array AS 
(SELECT *
    , CASE 
            WHEN ad_name ILIKE 'at:%'
            THEN ad_name
            ELSE 'at:' || ad_name
    END AS clean_ad_name
    , REGEXP_EXTRACT_ALL(clean_ad_name, $$[[:alnum:]]{2}\:[^_:]*\_$$) AS ad_name_parts -- regex that generates the parsed array from ad name string (in the jump format)
FROM {{ source(source_name, table_name) }})
-- ARRAYS_TO_OBJECT takes 2 params (an array of keys and an array of corresponding values) and converts them to a single json object
-- in this case, each parameter is a TRANSFORM fn, operating on the ad name parsed array
-- 1st param: extracts keys (first set of 2 alpha numeric characters)
-- 2nd param: extracts values (gets all characters after the colon index, and removes trailing `_`)
SELECT *
, ARRAYS_TO_OBJECT(TRANSFORM(ad_name_parts, a -> REGEXP_SUBSTR(a, $$[[:alnum:]]{2}$$)), TRANSFORM(ad_name_parts, a -> TRIM(SUBSTR(a, 4), '_'))) AS ad_name_json
FROM parsed_array)

{% endmacro %}
