
WITH charge_events_parsed AS (
  SELECT 
    ex.id AS charge_id
    , value:id::STRING AS charge_event_id
    , {{ cents_to_usd('value:amount::INTEGER') }} AS transaction_amount
    , TO_TIMESTAMP_TZ(value:created) AS transaction_time
    , {{ cents_to_usd('value:fee::INTEGER') }} AS fee_amount
    , {{ cents_to_usd('value:fee_refunded::INTEGER') }} AS refunded_fee_amount
    , value:type::STRING AS transaction_type
  FROM {{ source('brine', 'affirm_charges_external') }} ex, LATERAL FLATTEN(events)
  WHERE ex.row_num = 1)
SELECT 
  cep.charge_id
  , aoi.order_item_id
  , cep.charge_event_id
  , CASE WHEN cep.transaction_type = 'capture' 
         THEN cep.transaction_amount
         ELSE -1*cep.transaction_amount
    END AS transaction_amount
  , cep.transaction_time
  , CASE WHEN cep.transaction_type = 'capture' 
         THEN cep.fee_amount
         ELSE -1*cep.refunded_fee_amount
    END AS fee_amount 
  , CASE WHEN cep.transaction_type = 'capture' 
         THEN 'charge'
         ELSE cep.transaction_type
    END AS transaction_type 
  , 'affirm' AS transaction_source
FROM charge_events_parsed cep
LEFT JOIN {{ ref('affirm_order_items') }} aoi 
  ON aoi.charge_id = cep.charge_id
WHERE cep.transaction_type IN ('capture', 'refund')
