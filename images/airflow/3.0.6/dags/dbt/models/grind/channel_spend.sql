-- Consolidated channel spend table that joins offline and online Marketing spend at the daily level. 
-- Using Marketing-defined channel dimensions, we pull in daily spend from Sigma input tables or API.
-- This serves as the base table for Marketing Channel Spend analyses. 

-- Referrals and Meal Discounts are input as channel dimensions, but we pull actual spend from our internal data tables
WITH referrals AS (
    SELECT 
        CONVERT_TIMEZONE('America/Chicago',transaction_time)::DATE AS spend_date 
        , 'Referral' AS channel_name
        , 'Tovala' AS partner 
        , 'grind.meal_cash' AS data_source
        , SUM(meal_cash_amount) AS total_spend
        , MAX(transaction_time) AS last_updated_at 
    FROM {{ ref('meal_cash') }}
    WHERE type_category = 'tovala_referrer_credit'
    GROUP BY ALL  
)
, meal_discounts AS (
    SELECT 
        CONVERT_TIMEZONE('America/Chicago',transaction_time)::DATE AS spend_date
        , 'Meal Discounts' AS channel_name
        , 'Tovala' AS partner 
        , 'season.marketing_discounts' AS data_source
        , SUM(transaction_amount) AS total_spend
        , MAX(transaction_time) AS last_updated_at 
    FROM {{ ref('marketing_discounts') }}
    WHERE transaction_type = 'marketing_discount_credit' 
    GROUP BY ALL  
)

-- Here we bring together our offline and online spend into the base table for all channel spend 
-- CTV data will appear as an estimate in offline spend, and then actual spend will show up in online spend on a day lag
SELECT
    c.channel_name
    , c.partner 
    , c.channel_category 
    , c.expense_category
    , c.channel_owner
    , c.media_type
    , c.is_active 
    , c.notes 
    -- Combining fields from all sources (online, offline, referrals, meal discounts)
    , COALESCE(o.data_source, off.data_source, ltv.update_cadence_type, r.data_source, md.data_source) AS source
    , source='sigma_input_table' AS is_manual 
    , COALESCE(off.update_cadence_type, ltv.update_cadence_type, 'ETL') AS update_cadence_type
    , COALESCE(off.update_cadence, 'Daily') AS update_cadence 
    , COALESCE(off.allocation_method, 'Straightline') AS allocation_method 
    , COALESCE(off.is_recurring, FALSE) AS is_recurring_term
    , COALESCE(off.recurring_term_months, 0) AS recurring_term_months
    , CASE WHEN off.allocation_method='Direct Mail 50 Day Curve'
           THEN 50 
           ELSE COALESCE(off.amortization_days, 1) 
      END AS amortization_days
    , CASE WHEN o.report_date IS NOT NULL 
           THEN FALSE 
           ELSE COALESCE(off.is_estimate, FALSE) 
      END AS is_estimated    
    -- Correct Linear TV datetime from Sigma for the 7 hours difference from the actual date input
    , cal.calendar_date::DATE AS spend_date
    , CASE WHEN is_estimated
           THEN do.term_start_date::DATE 
           ELSE COALESCE(o.report_date, do.term_start_date, DATEADD(day, 1, ltv.spend_date), r.spend_date, md.spend_date)::DATE 
      END AS term_start_date 
    -- Amortized or recurring spend will be applied over a range of time rather than on one date 
    , CASE WHEN is_recurring_term 
           THEN DATEADD(month, recurring_term_months, term_start_date)::DATE
           WHEN off.allocation_method='Direct Mail 50 day curve'
           THEN DATEADD(day, 50, term_start_date)
           WHEN is_estimated AND amortization_days>0 
           THEN DATEADD(day, amortization_days, term_start_date) 
           ELSE term_start_date
      END AS term_end_date 
    -- We define spend term as the number of days that a spend should be distributed over 
    , COALESCE(DATEDIFF(day, term_start_date, term_end_date), 1) AS spend_term_days
    , CASE WHEN NOT is_estimated 
           THEN COALESCE(o.total_spend, do.daily_spend, ltv.total_spend, r.total_spend, md.total_spend)::FLOAT 
           WHEN is_estimated 
           THEN do.daily_spend::FLOAT
      END AS daily_channel_spend 
    , COALESCE(o.total_in_platform_conversions, 0) AS total_in_platform_conversions
    -- Invoice number can currently only be pulled if Marketing inputs it manually 
    , off.invoice_number 
    , COALESCE(o.last_updated_at, do.last_updated_at, off.last_updated_at, ltv.last_updated_at, r.last_updated_at, md.last_updated_at) AS last_updated_at 
    , CASE WHEN NOT is_estimated AND o.report_date IS NOT NULL 
           THEN 'ETL'
           ELSE COALESCE(off.last_updated_by, ltv.last_updated_by, 'ETL')  
      END AS last_updated_by
FROM {{ ref('channel_dimensions') }} c
CROSS JOIN {{ source('brine', 'dim_calendar') }} cal
LEFT JOIN {{ ref('offline_channel_spend_daily') }} do
    ON c.channel_name=do.channel_name 
    AND c.partner=do.partner
    AND cal.calendar_date::DATE=do.spend_date::DATE
LEFT JOIN {{ ref('offline_channel_spend') }} off
    ON do.channel_name=off.channel_name 
    AND do.partner=off.partner
    AND do.term_start_date::DATE=DATEADD(day, 1, off.spend_date)::DATE
LEFT JOIN {{ ref('online_channel_spend') }} o 
    ON c.channel_name=o.channel_name 
    AND c.partner=o.partner
    AND cal.calendar_date::DATE=o.report_date::DATE
LEFT JOIN {{ ref('linear_tv_orders') }} ltv
    ON c.channel_name=ltv.channel_name 
    AND c.partner=ltv.partner
    AND cal.calendar_date::DATE=DATEADD(day, 1, ltv.spend_date)::DATE
LEFT JOIN referrals r 
    ON c.channel_name=r.channel_name 
    AND c.partner=r.partner
    AND cal.calendar_date::DATE=r.spend_date::DATE
LEFT JOIN meal_discounts md 
    ON c.channel_name=md.channel_name 
    AND c.partner=md.partner
    AND cal.calendar_date::DATE=md.spend_date::DATE
WHERE cal.calendar_date>='2025-09-01'
AND COALESCE(o.report_date, do.term_start_date, DATEADD(day, 1, ltv.spend_date), r.spend_date, md.spend_date) IS NOT NULL 
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.channel_name, c.partner, cal.calendar_date ORDER BY COALESCE(o.last_updated_at, off.last_updated_at, ltv.last_updated_at, r.last_updated_at, md.last_updated_at) DESC) = 1

-- We union together historicals from brine.new_media_spend_by_channel for all data prior to the launch of the new D2C tracker on 9/1/2025 
-- This brine table was created from grind.media_spend_by_channel, which was previously the source of truth for media spend 
UNION 

SELECT 
    channel_name 
    , partner 
    , channel_category
    , expense_category
    , channel_owner
    , media_type
    , NULL AS is_active
    , NULL AS notes 
    , 'Historical Google Sheet' AS source  
    , TRUE AS is_manual 
    , NULL AS update_cadence_type
    , NULL AS update_cadence
    , NULL AS allocation_method
    , NULL AS is_recurring_term
    , NULL AS recurring_term_months
    , NULL AS amortization_days
    , FALSE AS is_estimated
    , date AS spend_date 
    , date AS term_start_date 
    , NULL AS term_end_date 
    , 1 AS spend_term_days
    , SUM(spend) AS daily_channel_spend
    , 0 AS total_in_platform_conversions
    , NULL AS invoice_number 
    , '2025-11-05' AS last_updated_at 
    , 'emccarthy@tovala.com' AS last_updated_by 
FROM {{ source('brine', 'new_media_spend_by_channel') }} 
WHERE spend_date<'2025-09-01'
GROUP BY ALL 