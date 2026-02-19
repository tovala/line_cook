-- PROJECTED OVEN SALES (CURRENT YEAR)

/* 
In the current process, Emily pulls this weekly from 
the "Growth Team Sales Projections" Gsheet
(https://docs.google.com/spreadsheets/d/17XbFp-Kmy1bL3W6aQDOLrzrtmTg_Ry2NRhEbC6wb_j0/edit?usp=sharing). 
She starts from today's date, 
copies the full remaining forecast, 
and pastes them below the actuals in column S 
on the "Monthly Oven Orders (input)" tab of the 
Excel version of the cohort model.

In the Gsheet that these inputs are coming from, 
these projections are owned and input by BK,
based on the latest oven sales forecast for the 
current year plus any ad hoc tweaks he decides
to make for demand planning purposes.

The data team is currently ingesting the data 
from this Gsheet ("Data Team Ingestion" tab)
and modeling into grind.demand_planning_oven_projections. */

SELECT 
  CONVERT_TIMEZONE('America/Chicago', projection_date)::DATE AS projection_date_chicago
  , projected_oven_sales
FROM grind.demand_planning_oven_projections
WHERE projection_date_chicago >= current_date()
ORDER BY projection_date_chicago DESC