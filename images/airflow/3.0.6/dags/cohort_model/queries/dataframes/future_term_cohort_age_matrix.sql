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
FROM "{{ params.runtime_schema_prefix }}_{{ run_id }}".COHORT_AGE WHERE term_id = (SELECT MAX(term_id) FROM "COHORT_MODEL_manual__2026-04-08T16:02:48.528246+00:00".COHORT_AGE)),
all_model_cohorts AS (
SELECT 
    arc.cohort
    , COALESCE(last_known_term, arc.cohort) AS last_known_term
    , COALESCE(last_known_term_age, 0) AS last_known_term_age
FROM "{{ params.runtime_schema_prefix }}_{{ run_id }}".AGGREGATE_ORDER_RETENTION_CURVES as arc 
FULL JOIN last_known_age AS lka ON arc.cohort = lka.cohort)
SELECT cohort,
TRANSFORM((SELECT array_agg(term_id) 
    FROM mugwort.future_terms 
    WHERE term_id <= (SELECT MAX(term_id) FROM mugwort.combined_oven_sales)), ter INT -> (ter - last_known_term) + last_known_term_age) AS future_term_cohort_age
FROM all_model_cohorts WHERE cohort IS NOT NULL ORDER BY cohort;