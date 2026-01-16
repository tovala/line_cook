-- From Modus Direct, we pull creative/segment details per Customer ID    

SELECT 
    customer_id
    , impression_time
    , segment_name
    , tv_partner_name
    , isci
    , creative_name
    , total_seconds
FROM (
-- Customer data from the general customer file that Modus sends
SELECT 
    -- File incorrectly sets customer_id as order_number, so we change the field name here 
    order_number AS customer_id 
    , TRY_TO_TIMESTAMP(impression_timestamp) AS impression_time 
    -- Name of customer segment 
    , segment_name 
    -- Name of TV partner that Modus placed the ad on 
    , line_name AS tv_partner_name 
    -- TV Industry Standard Coding ID 
    -- Modus uses `TO` to indicate Tovala, an integer to indicate # of seconds, `H` as a standard ending 
    , SPLIT_PART(creative_name, '_', 1) AS isci 
    , CASE WHEN REGEXP_REPLACE(REPLACE(RTRIM(REPLACE(creative_name, isci, ''), '.mp4'), '_', ' '), ' \\d+\\w', '')=''
           THEN NULL 
           ELSE REGEXP_REPLACE(REPLACE(RTRIM(REPLACE(creative_name, isci, ''), '.mp4'), '_', ' '), ' \\d+\\w', '') 
      END AS creative_name  
    , REGEXP_SUBSTR(isci, '\\d+') AS total_seconds 
    , _ab_source_file_last_modified
FROM {{ source('modus', 'customer') }} 

UNION 

-- Customer data from the CRM customer file that Modus sends
SELECT 
    -- File incorrectly sets customer_id as order_number, so we change the field name here 
    order_number AS customer_id 
    , TRY_TO_TIMESTAMP(impression_timestamp) AS impression_time 
    -- Name of customer segment 
    , segment_name 
    -- Name of TV partner that Modus placed the ad on 
    , line_name AS tv_partner_name 
    -- TV Industry Standard Coding ID 
    -- Modus uses `TO` to indicate Tovala, an integer to indicate # of seconds, `H` as a standard ending 
    , SPLIT_PART(creative_name, '_', 1) AS isci 
    , CASE WHEN REGEXP_REPLACE(REPLACE(RTRIM(REPLACE(creative_name, isci, ''), '.mp4'), '_', ' '), ' \\d+\\w', '')=''
           THEN NULL 
           ELSE REGEXP_REPLACE(REPLACE(RTRIM(REPLACE(creative_name, isci, ''), '.mp4'), '_', ' '), ' \\d+\\w', '') 
      END AS creative_name  
    , REGEXP_SUBSTR(isci, '\\d+') AS total_seconds 
    , _ab_source_file_last_modified
FROM {{ source('modus', 'crm_customer') }} 
)
-- We dedupe once here because we only want one record per customer, even if they appear in both files 
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ab_source_file_last_modified DESC) = 1
