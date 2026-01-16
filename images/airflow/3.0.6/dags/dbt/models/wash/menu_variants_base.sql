SELECT
  {{ clean_string('RAW_DATA:subTermID')}} AS subterm_id
  , filename
  , NULLIF(REGEXP_SUBSTR(filename, '^menu/components_.*?_(.+?)(_archived\.|_|\.json)$', 1, 1, 'e', 1), '') AS variant_id
  , updated
  , raw_data
FROM {{ source('chili', 'cdn_menu') }} cdn
-- exclude old variants with unsupported subterms
WHERE (raw_data:termID IS NULL OR TRY_TO_NUMBER(raw_data:termID::STRING) > 339)
-- get only the most version if there are changes
QUALIFY updated = MAX(updated) OVER (PARTITION BY filename)