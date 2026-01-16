-- Within Airtable, the rma_request field in rma_requests shows up as the airtable_id for that table in oven_returns
-- This ensures that what appears in Airtable shows up in this table. 
WITH flattened_request_ids AS (
  SELECT 
    _airtable_id AS oven_returns_airtable_id 
    , value::VARCHAR AS rma_airtable_id 
  FROM {{ source('airtable_oven_inventory', 'oven_inventory') }}
  , LATERAL FLATTEN (rma_request)
), mapping_to_keys AS (
  SELECT 
    fri.oven_returns_airtable_id 
    , ARRAY_AGG(rma.rma_request::INTEGER) AS rma_request_id
  FROM flattened_request_ids fri 
  LEFT JOIN {{ source('airtable_oven_inventory', 'rma_request') }} rma 
    ON fri.rma_airtable_id = rma._airtable_id  
  GROUP BY 1
)
SELECT
  aoi._airtable_id AS oven_returns_airtable_id
  , {{ clean_string('aoi.oven_serial_number') }} AS serial_number
  , CASE WHEN LOWER({{ clean_string('aoi.hardware_id') }}) = 'ukn'
         THEN NULL 
         ELSE {{ clean_string('aoi.hardware_id') }}
    END AS oven_hardware_id
  -- This should always be a numeric unless it is N/A, so we convert it here 
  , TRY_TO_DECIMAL(aoi.oven_batch)::INTEGER AS batch_number
  , {{ clean_string('aoi.facility_assignment') }} AS facility_assignment
  , aoi.tsouth_location AS tovala_south_location	
  , aoi.date_received_in_tsouth AS tovala_south_received_date
  , aoi.date_leaving_tsouth AS tovala_south_leaving_date
  , mk.rma_request_id	
  , aoi.zendesk_ticket_number	
  , aoi.verified_request_reason
  , {{ clean_string('aoi."PURPOSE_OF_QA/RND_OVEN"') }} AS qa_oven_purpose	
  , {{ clean_string('aoi.oven_category') }} AS oven_category
  , aoi."R&D" AS is_r_and_d_oven 
  , aoi.reserved AS is_reserved 
  , aoi."REBOXED/REFURBISHED" AS is_reboxed_or_refurbished
  , aoi.lab_projects
  , {{ clean_string('aoi.notes') }} AS notes
  , aoi.customer_name
  , TRY_PARSE_JSON(aoi.print_sticker):url::STRING AS print_sticker_url
FROM {{ source('airtable_oven_inventory', 'oven_inventory') }} aoi
LEFT JOIN mapping_to_keys mk 
  ON aoi._airtable_id = mk.oven_returns_airtable_id
