SELECT
    customer_id
    , MAX(CASE WHEN membership_group ILIKE 'free_dessert_for_life%'
               THEN is_active END)::BOOLEAN AS is_active_free_dessert_for_life
    -- If the customer is not a free dessert for life customer, then we want to know when they last cancelled
    , CASE 
        WHEN MAX(CASE WHEN membership_group ILIKE 'free_dessert_for_life%'
                      THEN is_active END)::BOOLEAN = false 
        THEN MAX(CASE WHEN membership_group ILIKE 'free_dessert_for_life%'
                      AND NOT is_active THEN cancellation_time END)
        END AS cancellation_time_free_dessert_for_life

FROM {{ ref('membership_group_customers')}}
GROUP BY customer_id
