-- ACTUAL OVEN SALES

/* 
In the current process, Emily pulls this weekly from 
the "Growth Team Sales Projections" Gsheet
(https://docs.google.com/spreadsheets/d/17XbFp-Kmy1bL3W6aQDOLrzrtmTg_Ry2NRhEbC6wb_j0/edit?usp=sharing). 
She copies all new actuals since the last cohort model update, 
excluding the current date, and pastes them into column S on the 
"Monthly Oven Orders (input)" tab of the Excel version of the 
cohort model.

In the Gsheet that these inputs are coming from, 
the daily actuals are sourced from the below Retool query 
(https://tovala.retool.com/apps/Demand%20Planning/Aggregated%20Oven%20Sales). 

Note that by pulling the actuals directly from the query below,
these actuals may differ slightly from what is in the Excel
version of the cohort model (any cases where historical
data was restated). */

SELECT 
  sale_date_chicago
  , SUM(oven_sales) AS oven_sales
FROM sage.RTT_OVEN_SALES
-- Added WHERE statement to the original Retool query
WHERE sale_date_chicago < current_date()
GROUP BY 1
ORDER BY sale_date_chicago DESC