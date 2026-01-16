{{ dry_config('payment') }}

SELECT
  created
  , {{ clean_string('error_details') }} AS error_details
  , {{ clean_string('id') }} AS id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('status') }} AS status
  , REPLACE({{ clean_string('stripe_charge_id') }}, '\n', '') AS stripe_charge_id
  , {{ clean_string('stripe_customer_id') }} AS stripe_customer_id
  , updated
  , userid
  -- Checks that error details is proper JSON
  , CASE WHEN CHECK_JSON(error_details) IS NULL AND error_details IS NOT NULL
         THEN PARSE_JSON(error_details):"charge"::STRING
    END AS error_details_charge 
  , CASE WHEN CHECK_JSON(error_details) IS NULL AND error_details IS NOT NULL
         THEN PARSE_JSON(error_details):"code"::STRING
    END AS error_details_code
  , CASE WHEN CHECK_JSON(error_details) IS NULL AND error_details IS NOT NULL
         THEN PARSE_JSON(error_details):"message"::STRING
    END AS error_details_message 
  , CASE WHEN CHECK_JSON(error_details) IS NULL AND error_details IS NOT NULL
         THEN PARSE_JSON(error_details):"param"::STRING
    END AS error_details_param 
  , CASE WHEN CHECK_JSON(error_details) IS NULL AND error_details IS NOT NULL
         THEN PARSE_JSON(error_details):"request_id"::STRING
    END AS error_details_request_id
  , CASE WHEN CHECK_JSON(error_details) IS NULL AND error_details IS NOT NULL
         THEN PARSE_JSON(error_details):"status"::INTEGER
    END AS error_details_status 
  , CASE WHEN CHECK_JSON(error_details) IS NULL AND error_details IS NOT NULL
         THEN PARSE_JSON(error_details):"type"::STRING
    END AS error_details_type 
FROM {{ source('combined_api_v3', 'payment') }}

{{ load_incrementally() }}
