-- Our offline spend has distinct allocation methods that informs how we distribute spend per day 
-- We run these daily calculations per allocation method and then rejoin them all together in a singular table 
-- This allows us to combine offline and online spend at the daily level 


-- Straightline spend should be distributed over the total number of amortization days 
WITH straightline AS (
    SELECT 
        cal.calendar_date
        , off.channel_name
        , off.partner
        -- Correct datetime from Sigma for the 7 hours difference from the actual date input
        , DATEADD(day, 1, off.spend_date) AS term_start_date
        , off.last_updated_at
        , off.total_spend / off.amortization_days as daily_spend
    FROM {{ source('brine', 'dim_calendar') }} cal
    INNER JOIN {{ ref('offline_channel_spend') }} off
      ON cal.calendar_date >= off.spend_date
      AND cal.calendar_date <= DATEADD('day', off.amortization_days, off.spend_date)
    WHERE off.allocation_method = 'Straightline'
)
-- Direct Mail has a specific 50 day curve of % shares to break up the total spend 
, direct_mail AS (
    SELECT 
        cal.calendar_date
        , off.channel_name
        , off.partner
        -- Correct datetime from Sigma for the 7 hours difference from the actual date input
        , DATEADD(day, 1, off.spend_date) AS term_start_date
        , off.total_spend 
        , off.last_updated_at
        , ROW_NUMBER() OVER (PARTITION BY channel_name, partner, spend_date ORDER BY calendar_date ASC) AS day_num 
    FROM {{ source('brine', 'dim_calendar') }} cal
    INNER JOIN {{ ref('offline_channel_spend') }} off
      ON cal.calendar_date >= off.spend_date
      AND cal.calendar_date <= DATEADD('day', 50, off.spend_date)
    WHERE off.allocation_method = 'Direct Mail 50 day curve'
)
-- Monthly distributions must be distributeed over the total months in the recurring term 
-- Then, we break that down to the daily level 
, monthly AS (
    SELECT 
        cal.calendar_date
        , off.channel_name
        , off.partner
        -- Correct datetime from Sigma for the 7 hours difference from the actual date input
        , DATEADD(day, 1, off.spend_date) AS term_start_date
        , off.last_updated_at
        , off.total_spend/(cal.no_days_in_month*COALESCE(off.recurring_term_months,1)) AS daily_spend
    FROM {{ source('brine', 'dim_calendar') }} cal
    INNER JOIN {{ ref('offline_channel_spend') }} off
      ON cal.calendar_date >= off.spend_date
      AND cal.calendar_date <= DATEADD('month', COALESCE(off.recurring_term_months, 1), off.spend_date)
    WHERE off.allocation_method = 'Monthly distribution'
) 
SELECT * 
FROM ( 
    SELECT 
        calendar_date AS spend_date 
        , channel_name
        , partner
        , term_start_date
        , daily_spend
        , last_updated_at
    FROM straightline 
    UNION 
    SELECT 
        calendar_date AS spend_date 
        , channel_name
        , partner
        , MIN(term_start_date) AS term_start_date
        , SUM(dm.total_spend*dc.pct_share/100) AS daily_spend 
        , MAX(last_updated_at) AS last_updated_at
    FROM direct_mail dm
    LEFT JOIN  {{ source('brine', 'direct_mail_curve') }} dc
        ON dm.day_num = dc.day_num 
    GROUP BY ALL 
    UNION 
    SELECT 
        calendar_date AS spend_date 
        , channel_name
        , partner
        , term_start_date
        , daily_spend 
        , last_updated_at
    FROM monthly )
QUALIFY ROW_NUMBER() OVER (PARTITION BY channel_name, partner, spend_date ORDER BY last_updated_at DESC) = 1