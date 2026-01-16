
WITH dow_int_table AS (
  SELECT
    *
    , CASE
        WHEN ship_day_of_the_week = 'Sunday' THEN 0 
        WHEN ship_day_of_the_week = 'Monday' THEN 1 
        WHEN ship_day_of_the_week = 'Tuesday' THEN 2
        WHEN ship_day_of_the_week = 'Wednesday' THEN 3
        WHEN ship_day_of_the_week = 'Thursday' THEN 4
        WHEN ship_day_of_the_week = 'Friday' THEN 5
        WHEN ship_day_of_the_week = 'Saturday' THEN 6
     END AS dow_int
  FROM {{ table_reference('zipcode_shipping_information') }}
),
delivery_day_int_table AS (
  SELECT 
      *
      , (dow_int + estimated_delivery_days) AS delivery_day_int
  FROM dow_int_table
)
SELECT
    id
    , zipcode AS zip_cd
    , ship_day_of_the_week
    , ship_period AS cycle
    , default_subterm AS is_default_cycle
    , available AS is_available
    , estimated_delivery_days 
    , CASE
        WHEN delivery_day_int = 0 THEN 'Sunday'
        WHEN delivery_day_int = 1 THEN 'Monday'
        WHEN delivery_day_int = 2 THEN 'Tuesday'
        WHEN delivery_day_int = 3 THEN 'Wednesday'
        WHEN delivery_day_int = 4 THEN 'Thursday'
        WHEN delivery_day_int = 5 THEN 'Friday'
        WHEN delivery_day_int = 6 THEN 'Saturday'
    END AS delivery_day
FROM delivery_day_int_table
