
SELECT 
  ga.id AS transaction_id
  , ga.gift_card_id AS discount_id
  , {{ cents_to_usd('ga.amount_cents') }} AS transaction_amount
  , CASE WHEN (ga.action RLIKE '.*ReactivationCampaign.*Promotion' OR ga.action RLIKE('.*IterableWebhook.*')) AND ga.amount_cents >= 0
       THEN 'reactivation_promo_credit'
       WHEN ga.action = 'tovala_referrer_credit' AND amount_cents > 0
       THEN 'referral_credit'
       WHEN (ga.action IN ('QVC PROMOTION', 'qvc_via_cs', 'Taste of Tovala V2', 'Taste of Tovala V1', 'Rental-100Prepaid',
                           'CardPurchase-ProductPromotion', 'ProductPurchasePromotion') 
             OR (ga.action IN ('ProductPurchase', 'CardPurchase') AND ga.type = 'prepay'))
            AND ga.amount_cents > 0 
       THEN 'prepay_credit'
       WHEN ga.action IN ('CardPurchase', 'CardPurchase-ChargeForSubscription') AND ga.amount_cents > 0 
       THEN 'card_purchase'
       WHEN ga.action ILIKE 'coupon_code-amzn-%'
       THEN 'amazon_credit'
       WHEN ga.action ILIKE 'coupon_code-tvqa-%'
       THEN 'qvc_credit'
       WHEN ga.action ILIKE 'coupon_code-cstc-%'
       THEN 'costco_credit'
       WHEN ((ga.action ILIKE 'coupon_code-%' AND NOT ga.action ILIKE 'coupon-code-amzn-tvla%') 
              OR ga.action = 'cs_mealcash' 
              OR ga.action = 'ProductPurchase' 
              OR ga.action = 'marketing' 
              OR ga.action LIKE 'mealcredit_migration%' 
              OR ga.action LIKE 'thirdpartyvendor%'
            ) 
         AND type = 'marketing' AND ga.amount_cents >= 0
       THEN 'marketing_discount_credit'
       WHEN ga.action ILIKE 'Coupon Code Credit -%' AND ga.amount_cents > 0
       THEN 'coupon_credit'
       WHEN ga.action IN ('marketing', 'OtherMarketing') AND ga.amount_cents > 0 
       THEN 'other_marketing_credit'
       WHEN ((ga.action = 'MealDiscountApplied'AND (ga.type = 'refund' OR ga.created < '2020-11-10'))
              OR ga.action = 'Refund') AND ga.amount_cents > 0
       THEN 'purchase_refund'
       WHEN ga.action = 'MealDiscountApplied' AND ga.amount_cents < 0
       THEN 'meal_purchase'
       WHEN ga.action IN ('BundleCreditPurchase', 'ProductPurchasedAsGift')
       THEN 'giftflow_purchase'
       WHEN ga.action = 'ProductPurchase' AND ga.amount_cents < 0
       THEN 'product_purchase'
       WHEN ga.action = 'CardRefund' AND ga.amount_cents < 0
       THEN 'card_refund'
       WHEN ga.action IN ('ReactivationCampaign-Removal', 'FreeDessertforLife-Removal') AND ga.amount_cents <= 0
       THEN 'promotion_removal'
       WHEN ga.action IN ('RefundFixForFinance', 'Adjustment', 'removal') OR stripe_orderid = 'removal'
       THEN 'manual_adjustment'
       WHEN ga.action = 'officeuser_replenish'
       THEN 'office_purchase'
       WHEN ga.action ILIKE '%freedessert%' AND ga.amount_cents >=0
       THEN 'free_dessert_promo_credit'
  END AS transaction_type 
  -- This is included for testing
  , ga.type AS secondary_type
  , CASE WHEN ga.action = 'cs_mealcash' AND transaction_type = 'marketing_discount_credit' 
         THEN TRUE
         ELSE FALSE
    END AS is_customer_service_credit
  , ga.payment_id AS payment_id
  , ga.created AS transaction_time
  , ga.cs_reason as cs_reason
  -- TO DO: work with backend to ensure this is actually being used for stripe charge id instead of notes
  , CASE WHEN ga.stripe_orderid ILIKE 'ch_%' THEN ga.stripe_orderid END AS stripe_charge_id
FROM {{ table_reference('gift_cards_activity') }} ga
INNER JOIN {{ ref('gc_and_md') }} g ON g.discount_id = ga.gift_card_id
