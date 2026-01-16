WITH selection_counts AS (
  SELECT
    msa.term_id
    , msa.facility_network
    , msa.cycle
    , msa.menu_meal_id
    , msa.meal_sku_id
    , msa.internal_meal_id
    , SUM(CASE WHEN cts.cohort_week_without_holidays < 5 THEN msa.meal_selection_count END) AS new_cohort_menu_meal_selection_count
    , SUM(CASE WHEN cts.cohort_week_without_holidays < 5 THEN msa.selection_total_count END) AS new_cohort_subterm_meal_selection_count
    , SUM(CASE WHEN cts.cohort_week_without_holidays > 10 THEN msa.meal_selection_count END) AS old_cohort_menu_meal_selection_count
    , SUM(CASE WHEN cts.cohort_week_without_holidays > 10 THEN msa.selection_total_count END) AS old_cohort_subterm_meal_selection_count
  FROM {{ ref('meal_selection_agg') }} msa
  LEFT JOIN {{ ref('production_meal_tags') }} pmt
    ON msa.menu_meal_id = pmt.menu_meal_id
  LEFT JOIN {{ ref('customer_term_summary') }} cts
    ON msa.customer_id = cts.customer_id
    AND msa.term_id = cts.term_id
  WHERE NOT pmt.was_frozen_meal
  GROUP BY 1,2,3,4,5,6
), cohort_eng AS (
  SELECT
    menu_meal_id
    , meal_sku_id
    , internal_meal_id
    , term_id
    , facility_network
    , cycle

  -- NEW COHORT
    , new_cohort_menu_meal_selection_count / new_cohort_subterm_meal_selection_count AS new_cohort_menu_raw_engagement_rate 
    -- Engagement rate of a meal in subterm/menu level(in a specific facility_network, cycle,and term)
    , SUM(new_cohort_menu_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id) 
      / 
      SUM(new_cohort_subterm_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id) 
      AS new_cohort_term_raw_engagement_rate
    -- Engagement rate of a version of internal meal in term level(sum across all cycles, facilities, in a specific term.
    -- This number is using to know the total engagement of a same verion of a meal. Ignore all small changes eg. price difference and etc)

  -- OLD COHORT
    , old_cohort_menu_meal_selection_count / old_cohort_subterm_meal_selection_count AS old_cohort_menu_raw_engagement_rate 
    , SUM(old_cohort_menu_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id) 
      / 
      SUM(old_cohort_subterm_meal_selection_count) OVER (PARTITION BY term_id, internal_meal_id) 
      AS old_cohort_term_raw_engagement_rate
  FROM selection_counts
)
SELECT
  menu_meal_id
  , meal_sku_id
  , old_cohort_menu_raw_engagement_rate
  , new_cohort_menu_raw_engagement_rate
  , old_cohort_menu_raw_engagement_rate / NULLIFZERO(new_cohort_menu_raw_engagement_rate) - 1 AS cohort_engagement_rate_ratio_menu
  , old_cohort_term_raw_engagement_rate
  , new_cohort_term_raw_engagement_rate
  , old_cohort_term_raw_engagement_rate / NULLIFZERO(new_cohort_term_raw_engagement_rate) - 1 AS cohort_engagement_rate_ratio_term
FROM cohort_eng
