
-- TODO: extend this to parse other custom fields 

WITH flattened_custom_fields AS (
  SELECT 
    id AS ticket_id 
    , TRY_TO_NUMBER({{ clean_string('F.value:id::STRING') }}) AS custom_field_id
    , {{ clean_string('F.value:value::STRING') }} AS custom_field_value
  FROM {{ source('zendesk_support_v3', 'tickets') }}, 
    Table(Flatten({{ source('zendesk_support_v3', 'tickets') }}.custom_fields)) F
  WHERE custom_field_id IN (7656195453595, 1500000474661)
    AND custom_field_value IS NOT NULL)
SELECT 
  ticket_id 
  , MAX(CASE WHEN custom_field_id = 7656195453595 
             THEN TRY_TO_NUMBER(custom_field_value) 
        END) AS stella_score
  , SPLIT(MAX(CASE WHEN custom_field_id = 1500000474661
                   -- Regex: replaces all non-digit values with a single space then removes leading and trailing whitespace
                   THEN TRIM(REGEXP_REPLACE(custom_field_value, '[^[:digit:]]{1,}', ' '))
              END), ' ') AS term_ids
FROM flattened_custom_fields
GROUP BY 1
