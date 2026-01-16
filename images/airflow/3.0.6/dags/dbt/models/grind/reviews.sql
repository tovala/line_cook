
WITH legacy_duped_reviews AS (
  SELECT 
    {{ hash_natural_key('mr.userid', 'mr.mealid', 'mr.created') }} AS review_id
    , mr.userid AS customer_id
    , mr.mealid AS meal_sku_id
    , msk.is_premium
    , msk.term_id AS term_id
    , roh.oven_generation
    , mr.created AS review_time
    , mr.rating AS rating_binary
    , mr.comment
    , {{ parse_subroutine('mr.comment') }} AS subroutine
    , CASE 
        WHEN mr.medium ILIKE 'robo_binary%' THEN 'Android'
        WHEN mr.medium ILIKE 'web%' THEN 'Web'
        ELSE mr.medium 
      END AS rating_medium
    , mr.valid AS is_valid
    , COALESCE(mr.side_rating, 0) AS side_rating
    , mr.side_comment
    , 'legacy' AS rating_type
  FROM {{ table_reference('mealreviews') }} mr
  INNER JOIN {{ table_reference('users') }} u 
    ON mr.userid = u.id 
  INNER JOIN {{ ref('meal_skus') }} msk 
    ON mr.mealid = msk.meal_sku_id
  LEFT JOIN {{ ref('historic_oven_registrations') }} roh
    ON mr.userid = roh.customer_id
    AND mr.created >= roh.start_time
    AND mr.created <= COALESCE(roh.end_time, '9999-12-31')
  WHERE mr.valid
    AND mr.rating IN (0,1,-1)
    AND msk.title IS NOT NULL
    AND rating_medium IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mr.userid, meal_sku_id, subroutine ORDER BY review_time DESC) = 1
),
-- handle legacy reviews
aggregated_reviews AS ( -- get array of all chips per review
  SELECT
    cookable_review_id 
    , ARRAY_AGG(cookable_review_chip_id) AS all_chips
  FROM {{ table_reference('cookable_review_chip_selections') }}
  GROUP BY 1
),
level_one AS ( -- Get all level 1 chips
  SELECT 
    id AS parent_id
    , name AS meal_part
    , sentiment
  FROM {{ table_reference('cookable_review_chips') }} 
  WHERE parent IS NULL
),
level_two AS ( -- Get all level 2 chips
  SELECT 
    id AS child_id
    , name AS chip_selection
  FROM {{ table_reference('cookable_review_chips') }} 
  WHERE parent IS NOT NULL
),
thumbs_duped_reviews AS (
  SELECT DISTINCT -- Note: Running distinct until there's a timestamp we can use on cookable_review_chip_selections to only pick the most recent PUT from the web/apps
    {{ hash_natural_key('cr.userid', 'cmr.mealid', 'cr.created') }} AS review_id
    , cr.userid AS customer_id 
    , cmr.mealid AS meal_sku_id 
    , msk.is_premium 
    , msk.term_id
    , roh.oven_generation
    , cr.created AS review_time
    , CASE 
        WHEN cr.rating_type = 'thumbs' THEN 
        CASE 
          WHEN cr.rating_int = 100 THEN 1 
          WHEN cr.rating_int = 0   THEN -1 
          ELSE 0 
        END
      END AS rating_binary
    , CASE 
        WHEN cr.rating_type = 'stars' THEN 
        CASE 
          WHEN cr.rating_int = 100 THEN 5 
          WHEN cr.rating_int = 80  THEN 4
          WHEN cr.rating_int = 60  THEN 3
          WHEN cr.rating_int = 40  THEN 2
          WHEN cr.rating_int = 20  THEN 1 
          ELSE 0 
        END
      END AS rating_stars
    -- Note: We had an iOS bug where if a customer left "Anything else?" blank, we captured that as a comment. Accounting for that here
    , CASE WHEN cr.comment = 'Anything else?' THEN NULL ELSE COMMENT END AS comment
    , {{ parse_subroutine('cr.comment') }} AS subroutine
    , CASE 
        WHEN cr.platform ILIKE 'robo_binary%' THEN 'Android'
        WHEN cr.platform ILIKE 'web%' THEN 'Web'
        ELSE cr.platform
      END AS rating_medium
    , TRUE AS is_valid -- Assumes all chip-based reviews are valid
    -- Note: We are no longer collecting "side" ratings and comments
    , 0 AS side_rating 
    , NULL AS side_comment
    , cr.rating_type
    , lo.meal_part AS reviewed_part
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Good Value') AS was_good_value
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Great Taste/Flavor') AS was_great_taste_flavor
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Perfect Portion Size') AS was_perfect_portion_size
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Bad Value') AS was_bad_value
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Bland') AS was_bland
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Not Filling') AS was_not_filling
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') IN ('Over Cooked', 'Overcooked')) AS was_overcooked
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') IN ('Spoiled/Damaged', 'Spoiled items')) AS was_spoiled_damaged
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Tasted Bad') AS tasted_bad
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') IN ('Under Cooked', 'Undercooked')) AS was_undercooked
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Missing Items') AS had_missing_items
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Taste') AS feedback_taste
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Other') AS feedback_other
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Portion Size') AS feedback_portion_size
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Ingredient quality') AS feedback_ingredient_quality
    , BOOLOR_AGG(COALESCE(l2.chip_selection, '') = 'Texture') AS feedback_texture
  -- Note: This doesn't account for reviews on Assist, STS etc. 
  FROM {{ table_reference('cookable_reviews') }} cr 
  INNER JOIN {{ table_reference('cookable_meal_reviews') }} cmr 
    ON cr.id = cmr.cookable_review_id 
  LEFT JOIN {{ table_reference('cookable_review_chip_selections') }} crcs 
    ON cr.id = crcs.cookable_review_id
  LEFT JOIN {{ table_reference('cookable_review_chips') }} crc 
    ON crcs.cookable_review_chip_id = crc.id 
  LEFT JOIN {{ ref('meal_skus') }} msk 
    ON cmr.mealid = msk.meal_sku_id 
  LEFT JOIN {{ ref('historic_oven_registrations') }} roh
    ON cr.userid = roh.customer_id
    AND cr.created >= roh.start_time
    AND cr.created <= COALESCE(roh.end_time, '9999-12-31')
  LEFT JOIN aggregated_reviews ar 
    ON cr.id = ar.cookable_review_id
  LEFT JOIN level_one lo 
    ON ARRAY_CONTAINS(lo.parent_id::VARIANT, all_chips)
  LEFT JOIN level_two l2 
    ON ARRAY_CONTAINS(l2.child_id::VARIANT, all_chips)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16, meal_part, meal_sku_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cr.userid, meal_sku_id ORDER BY review_time DESC) = 1
)
-- TODO: There will be "stars_duped_reviews" here soon...

-- Join legacy and new reviews
SELECT 
  review_id
  , customer_id
  , meal_sku_id
  , is_premium
  , term_id
  , oven_generation
  , review_time
  , rating_binary
  , NULL AS rating_stars
  , comment
  , rating_type
  , subroutine 
  , rating_medium
  , is_valid
  , side_rating
  , side_comment
  , CASE
    WHEN rating_binary <> 0 AND side_rating <> 0 THEN 'Entire Meal'
    WHEN rating_binary <> 0 AND side_rating = 0 THEN 'Main'
    WHEN rating_binary = 0 AND side_rating <> 0 THEN 'Side'
    ELSE NULL
  END AS reviewed_part
  -- Set all tags to NULL (didn't exist in legacy reviews)
  , NULL AS was_good_value
  , NULL AS was_great_taste_flavor
  , NULL AS was_perfect_portion_size
  , NULL AS was_bad_value
  , NULL AS was_bland
  , NULL AS was_not_filling
  , NULL AS was_overcooked
  , NULL AS was_spoiled_damaged
  , NULL AS tasted_bad
  , NULL AS was_undercooked
  , NULL AS had_missing_items
  , NULL AS feedback_taste
  , NULL AS feedback_other
  , NULL AS feedback_portion_size
  , NULL AS feedback_ingredient_quality
  , NULL AS feedback_texture
FROM legacy_duped_reviews
UNION 
SELECT 
  review_id 
  , customer_id
  , meal_sku_id 
  , is_premium 
  , term_id 
  , oven_generation
  , review_time 
  , rating_binary
  , rating_stars 
  , comment 
  , rating_type 
  , subroutine 
  , rating_medium
  , is_valid
  , side_rating
  , side_comment
  , reviewed_part
  , was_good_value
  , was_great_taste_flavor
  , was_perfect_portion_size
  , was_bad_value
  , was_bland
  , was_not_filling
  , was_overcooked
  , was_spoiled_damaged
  , tasted_bad
  , was_undercooked
  , had_missing_items
  , feedback_taste
  , feedback_other
  , feedback_portion_size
  , feedback_ingredient_quality
  , feedback_texture
FROM thumbs_duped_reviews t
WHERE term_id IS NOT NULL -- Exclude test reviews (TODO: Not 100% certain these are all test reviews; some might be QVC)
AND review_time > '2021-02-18' -- Only look at things AFTER we released this
AND NOT EXISTS ( -- Exclude legacy reviews (already handled above)
  SELECT 1 
  FROM legacy_duped_reviews 
  WHERE customer_id = t.customer_id 
  AND meal_sku_id = t.meal_sku_id 
  AND term_id = t.term_id
)
AND rating_medium <> 'development'

