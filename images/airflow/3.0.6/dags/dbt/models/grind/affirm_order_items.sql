
SELECT
  {{ hash_natural_key('ex.id', 'af.id', 'COALESCE(af.sku, ex.item_sku)') }} AS order_item_id
  , COALESCE(af.sku, ex.item_sku) AS item_sku 
  , ex.id AS charge_id
  , ex.item_qty
  -- Occasionally, tax amount comes through as dollars instead of cents
  , CASE WHEN ex.tax_amount_cents IN (10, 12, 15)
         THEN ex.tax_amount_cents
         ELSE COALESCE({{ cents_to_usd('ex.tax_amount_cents') }}, 0) 
    END AS tax_amount
  , -1*COALESCE({{ cents_to_usd('ex.discount_amount_cents') }}, 0) AS discount_amount
  , CASE WHEN ex.unit_price_cents = 0
         THEN {{ cents_to_usd('ex.amount_cents') }} - discount_amount - tax_amount
         ELSE {{ cents_to_usd('ex.unit_price_cents') }}
    END AS unit_price_amount
  , ex.discount_display_name AS coupon_cd
FROM {{ source('brine', 'affirm_charges_external') }} ex
LEFT JOIN {{ table_reference('affirm_charges') }} af 
  ON ex.id = af.chargeid
WHERE ex.row_num = 1
