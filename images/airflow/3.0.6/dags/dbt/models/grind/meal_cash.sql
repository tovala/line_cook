
SELECT
  id AS transaction_id
  , action AS action_category
  , {{ cents_to_usd('amount') }} AS meal_cash_amount
  , coupon_code AS coupon_cd
  , created AS transaction_time
  , notes
  , payment_id
  , purchase_id
  , type AS type_category
  , userid AS customer_id
  , cs_reason
  , {{ parse_zendesk_ticket_id('cs_zd_ticket') }} AS cs_ticket_id
  , cs_agent
  , cs_affected_mealids
  , voided
FROM {{ table_reference('meal_cashmoney') }}
