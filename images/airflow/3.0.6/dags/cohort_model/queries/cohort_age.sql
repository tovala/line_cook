SELECT DISTINCT 
  cohort 
  , term_id 
  , (cohort_week_without_holidays - 1) as cohort_age
FROM season.customer_term_summary
WHERE cohort_week_without_holidays IS NOT NULL
ORDER BY 1,2;