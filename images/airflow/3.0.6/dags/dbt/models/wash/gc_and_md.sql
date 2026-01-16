
WITH marketing_discounts AS 
(SELECT 
   DISTINCT gift_card_id 
   FROM {{ table_reference('gift_cards_activity') }} 
 WHERE TYPE = 'marketing')
SELECT 
  g.id AS discount_id
  , g.redeem_code AS redemption_cd
  , CASE WHEN g.redeemed_by IS NOT NULL 
         THEN g.redeemed_by
    END AS redeemer_customer_id 
  , CASE WHEN g.userid IS NOT NULL 
         THEN g.userid 
    END AS buyer_customer_id
  , g.buyer_email
  , g.created AS generated_time
  , md.gift_card_id IS NOT NULL AS is_marketing_discount
  , rc.retail_channel as credit_type
FROM {{ table_reference('gift_cards') }} g 
LEFT JOIN marketing_discounts md 
ON g.id = md.gift_card_id
LEFT JOIN {{ table_reference('reactivation_credits') }} rc
ON g.id = rc.gift_card_id
WHERE ((g.userid IS NULL OR g.userid IN (SELECT customer_id FROM {{ ref('customers') }}))
  OR (g.redeemed_by IS NULL OR g.redeemed_by IN (SELECT customer_id FROM {{ ref('customers') }})))
  AND (COALESCE(buyer_email,'') NOT ILIKE '%@testvala%') 
  -- Incorrectly added outliers
  AND NOT g.id IN ('ca02dc39-4301-fc4d-3075-eba7bf3cb82e', 
                   '63b63887-bb1b-50c7-8779-1728975d6336')
