-- COMBINED OVEN SALES
-- "combined_oven_sales_projections" in order_projections.py

/* 
In the current version of order_projections.py 
that Daniel wrote, he looks for a combined input that includes
oven sales actuals, oven sales projections for the current year, 
and oven sales projections for future years. The former two inputs
can be pulled from Snowflake automatically (via the code I provided in 
the other comments and which I've written as CTEs below), 
but the latter is a csv input from the user. 

Daniel had already written some code to combine these inputs 
into allspice.combined_oven_sales_projections
(https://github.com/tovala/spice_rack/pull/530/changes#diff-96b29913b1b2571460cfcbeeb52875cba345f81a136a84afc3ab167261fb976f)
but I don't think that PR ever got deployed. 
Below is a revised version of that code. 

Note to myself (Minna): Check with Peter on the logic for monthly vs. daily inputs...
this matches what I see in Excel but I'm not sure it makes sense. */

with
actual_oven_sales as (
SELECT 
  sale_date_chicago
  , SUM(oven_sales) AS oven_sales
FROM sage.RTT_OVEN_SALES
GROUP BY 1
ORDER BY sale_date_chicago DESC
)

, projected_oven_sales as (
SELECT 
  CONVERT_TIMEZONE('America/Chicago', projection_date)::DATE AS projection_date_chicago
  , projected_oven_sales
FROM grind.demand_planning_oven_projections
WHERE projection_date_chicago >= current_date()
ORDER BY projection_date_chicago DESC
)

, combined_projections as (
    select 
        splits.term_id
        , case
            when (splits.d2c_holiday = 1) then iff(current_projections.projected_oven_sales is not null, 
            current_projections.projected_oven_sales, future_projections.d2c)
            else null end as d2c_holiday
        , case
            when (splits.d2c_non_holiday = 1) then iff(current_projections.projected_oven_sales is not null, 
            current_projections.projected_oven_sales, future_projections.d2c)
            else null end as d2c_not_holiday
        , case
            when (splits.sale = 1) then iff(current_projections.projected_oven_sales is not null, 
            current_projections.projected_oven_sales, future_projections.d2c)
            else null end as sale
        , future_projections.amazon
        , future_projections.costco
        , future_projections.other
    from salt.cohort_model_daily_oven_sales_projections as future_projections
    left join (
        select projection_date_chicago, projected_oven_sales from projected_oven_sales
        union all 
        select sale_date_chicago, oven_sales from actual_oven_sales
        ) as current_projections
        on future_projections.date::date = current_projections.projection_date_chicago::date 
    left join salt.cohort_model_daily_oven_d2c_sales_splits as splits
        on future_projections.date::date = splits.date::date
)

, agg_combined_projections as (
    select 
        term_id
        , sum(d2c_holiday)::int as d2c_holiday
        , sum(d2c_not_holiday)::int as d2c_not_holiday
        , sum(sale)::int as sale
    from combined_projections
    group by 1
)

select 
    combined_projections.term_id
    , agg_combined_projections.d2c_not_holiday
    , agg_combined_projections.d2c_holiday
    , agg_combined_projections.sale
    , combined_projections.amazon
    , combined_projections.costco
    , combined_projections.other
from combined_projections
left join agg_combined_projections
    on combined_projections.term_id = agg_combined_projections.term_id