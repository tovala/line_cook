SELECT
  rma._airtable_id AS rma_airtable_id
  , rma.rma_request AS rma_request_id
  , TRY_TO_DECIMAL({{ clean_string('rma.zendesk_ticket_number') }})::INTEGER AS zendesk_ticket_number
  , TRY_PARSE_JSON(rma.zendesk_link):url::STRING AS zendesk_url
  , TRY_TO_DECIMAL(rma.user_id)::INTEGER AS customer_id
  , TRY_PARSE_JSON(rma.glaze_link):url::STRING AS glaze_url
  , {{ clean_string('rma.customer_name') }} AS customer_name
  , {{ clean_string('rma.request_type') }} AS request_type
  , rma.request_date
  , {{ clean_string('rma.point_of_sale') }} AS point_of_sale
  , ori.serial_number
  , TRY_TO_DECIMAL(rma.oven_batch[0]::VARCHAR)::INTEGER AS batch_number
  , {{ clean_string('rma.returning_location') }} AS returning_location
  , CASE WHEN LOWER({{ clean_string('rma.tracking_number') }}) IN ('n/a', 'na', 'no return', 'ukn', 'n.a')
         THEN NULL 
         ELSE rma.tracking_number
    END AS tracking_number
  , TRY_PARSE_JSON(rma.shipping_status):url::STRING AS shipping_status_url
  , rma.date_received_in_tsouth[0]::DATE AS tovala_south_received_date
  , {{ clean_string('rma.reported_problem_details') }} AS reported_problem_details
  , rma.request_reason
  , {{ clean_string('rma.verified_request_reason') }} AS verified_request_reason
  , {{ clean_string('rma.oven_category') }} AS oven_category
  , rma.attribute_to
  , rma.needs_inspection
  , rma.needs_refund
  , {{ clean_string('rma."INSPECTION/REFUND_NOTES"') }} AS inspection_refund_notes
  , TRY_PARSE_JSON(rma.update_refund_status):url::STRING AS update_refund_status_url
  , rma.date_refunded
  , {{ clean_string('rma.refunding_cs_agent_name') }} AS refunding_cs_agent_name
  , TRY_PARSE_JSON(rma.edit_record):url::STRING AS edit_record_url 
FROM {{ source('airtable_oven_inventory', 'rma_request') }} rma
LEFT JOIN {{ ref('oven_return_inventory') }} ori 
  ON {{ clean_string('rma.serial_number[0]::STRING') }} = ori.oven_returns_airtable_id
