CREATE OR REPLACE TABLE {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".combined_cohort_characteristics_data 
AS
  SELECT 
    rcd.id AS curve_definition_id
    ,rcd.price_bucket_id AS price_bucket_id
    , rcd.purchase_month AS purchase_month
    , rcd.on_commitment AS on_commitment
    , pmm.cohort AS cohort
    , CASE 
      WHEN rcd.on_commitment::BOOLEAN 
      THEN ocm.commitment
      WHEN NOT rcd.on_commitment::BOOLEAN THEN ocm.non_commitment
    END AS on_commitment_ratio
    , CASE 
      WHEN rcd.price_bucket_id = 1 THEN pbm."1"
      WHEN rcd.price_bucket_id = 2 THEN pbm."2"
      WHEN rcd.price_bucket_id = 3 THEN pbm."3"
      ELSE NULL
    END AS price_bucket_ratio
    , CASE 
      WHEN NOT rcd.on_commitment::BOOLEAN AND rcd.price_bucket_id = 4 THEN cmur.unknown_cohort_characteristics
      WHEN rcd.on_commitment::BOOLEAN AND rcd.price_bucket_id = 4 THEN NULL -- this isn't modeled in the current retention curves (on commitment but unknown purchase price)
      ELSE cmur.known_cohort_characteristics
    END AS known_unknown_multiplier
    , COALESCE(on_commitment_ratio * price_bucket_ratio * known_unknown_multiplier, known_unknown_multiplier) AS percentage_of_cohort -- if known characteristics, use the calculated percent of cohort, else use unkown characteristics multiplier
  FROM yarrow.retention_curve_definitions AS rcd
  JOIN yarrow.purchase_month_mix AS pmm ON rcd.purchase_month = pmm."FIRST MONTH" -- by definition, the mix tables need to have one entry per cohort
  JOIN yarrow.on_commitment_mix AS ocm ON ocm.cohort = pmm.cohort
  JOIN yarrow.price_bucket_mix AS pbm ON pbm.cohort = pmm.cohort
  JOIN yarrow.cohort_mix_unknown_ratio AS cmur ON cmur.cohort = pmm.cohort;