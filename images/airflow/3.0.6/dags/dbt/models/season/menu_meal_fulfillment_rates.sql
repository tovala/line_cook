WITH selection_counts AS (
  SELECT
    msa.term_id
    , msa.facility_network
    , msa.cycle
    , msa.menu_meal_id
    , msa.meal_sku_id
    , msa.internal_meal_id
    , SUM(msa.meal_fulfilled_count) AS menu_meal_selection_count
    , SUM(msa.meal_fulfilled_total_count) AS subterm_meal_selection_count
  FROM {{ ref('meal_selection_agg') }} msa
  GROUP BY 1,2,3,4,5,6
)
SELECT
  menu_meal_id
  , meal_sku_id
  , internal_meal_id
  , term_id
  , facility_network
  , cycle
  , menu_meal_selection_count / subterm_meal_selection_count AS menu_fulfilled_pct
  -- fulfilled_pct of a meal in subterm/menu level(in a specific facility_network, cycle,and term)
  , SUM(menu_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id, cycle) 
    / 
    SUM(subterm_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id, cycle) 
    AS cycle_fulfilled_pct
  -- fulfilled_pct of a meal in cycle level(sum across facilities, in a specific cycle, and term)
  -- Using to compareing between cycles)
  , SUM(menu_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id, facility_network) 
    / 
    SUM(subterm_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id, facility_network) 
    AS facility_fulfilled_pct
  --  fulfilled_pct of a meal in facility level(sum across cycles, in a specific facility, and term)
  -- Using to compareing  fulfilled_pct between facility
  , SUM(menu_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id) 
    / 
    SUM(subterm_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id) 
    AS term_fulfilled_pct
  --  fulfilled_pct of a version of internal meal in term level(sum across all cycles, facilities, in a specific term.
  -- This number is using to know the total  fulfilled_pct of a same verion of a meal. Ignore all small changes eg. price difference and etc)
FROM selection_counts
