-- We only have calculated cohort age in customer_terms_summary for past terms. In order to set up a matrix of cohort age for all future terms to be predicted,
-- 1. get the last calculated age for existing cohorts
-- 2. set future cohorts (to be calculated as determined by the aggregate retention curves table) with first active term
-- 2. set up array of all terms to be predicted
-- 3. compute cohort age for each future predicted term (future prediction term id - last known term id + cohort age at last known term)
-- result is one row per cohort with a cohort age array of length n where n is the number of terms to be predicted 
WITH last_known_age AS (SELECT 
  cohort
  , term_id AS last_known_term
  , cohort_age AS last_known_term_age
FROM "{{ params.runtime_schema_prefix }}_{{ run_id }}".COHORT_AGE 
WHERE term_id = (SELECT MAX(term_id) FROM "{{ params.runtime_schema_prefix }}_{{ run_id }}".COHORT_AGE)),
all_model_cohorts AS (
SELECT 
    COALESCE(lka.cohort, ft.cohort) as cohort
    , COALESCE(last_known_term, ft.cohort) AS last_known_term
    , COALESCE(last_known_term_age, 0) AS last_known_term_age
FROM "{{ params.runtime_schema_prefix }}_{{ run_id }}".FUTURE_COHORT_INITIAL_ORDER_PREDICTIONS AS ft 
FULL JOIN last_known_age AS lka ON ft.cohort = lka.cohort)
SELECT cohort,
TRANSFORM(AS_ARRAY({{ params.projection_terms_array }}), ter INT -> (ter - last_known_term) + last_known_term_age) AS future_term_cohort_age
FROM all_model_cohorts WHERE cohort IS NOT NULL ORDER BY cohort;