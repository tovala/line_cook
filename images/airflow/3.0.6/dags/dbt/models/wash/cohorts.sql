
WITH terms_without_holidays AS 
(SELECT 
   term_id
   , start_date AS cohort_start_date
   , RANK() OVER (ORDER BY term_id) AS week_without_holidays
   , 1 - week_without_holidays AS offset_without_holidays
 FROM {{ ref('terms') }}
 WHERE NOT is_company_holiday),
terms_with_holidays AS 
(SELECT 
   term_id
   , start_date AS term_start_date
   , RANK() OVER (ORDER BY term_id) AS week_with_holidays
   , 1 - week_with_holidays AS offset_with_holidays
 FROM {{ ref('terms') }})
SELECT
  t1.term_id
  , t1.term_start_date
  , t1.week_with_holidays
  , t1.offset_with_holidays
  , t2.week_without_holidays
  , COALESCE(t2.offset_without_holidays, LEAD(t2.offset_without_holidays) IGNORE NULLS OVER (ORDER BY t1.term_id)) AS offset_without_holidays
  , COALESCE(t2.term_id, LEAD(t2.term_id) IGNORE NULLS OVER (ORDER BY t1.term_id)) AS cohort
  , COALESCE(t2.cohort_start_date, LEAD(t2.cohort_start_date) IGNORE NULLS OVER (ORDER BY t1.term_id)) AS cohort_start_date
FROM terms_with_holidays AS t1 
LEFT JOIN terms_without_holidays AS t2
ON t1.term_id = t2.term_id