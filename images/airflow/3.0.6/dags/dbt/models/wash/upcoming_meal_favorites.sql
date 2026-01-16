--This query checks the meals that are on the upcoming menu, all positive customer reviews, currently active but skipped customers for the upcoming term
--  and compiles a list of 1: the most recently positively reviewed meal on the upcoming menu and 2: the first positively reviewed meal on the upcoming menu by customer id
--  so that we can send an unskip reminder that targets customers by meals they've previously rated positively
WITH upcoming_menu_meals AS (
SELECT
  DISTINCT(title) -- Need distinct to get rid of meals that show multiple times
  , subtitle
  , term_id
FROM {{ ref('meal_skus')}}
WHERE term_id = (SELECT MIN(term_id) FROM {{ ref('terms')}} WHERE is_live)
AND meal_skus.facility_network = 'chicago_slc'
)
, reviews AS (
  SELECT
    sku.title
    , sku.subtitle
    , r.meal_sku_id
    , r.term_id
    , r.customer_id
    , r.review_time
    , ROW_NUMBER() OVER (PARTITION BY customer_id, sku.title||sku.subtitle ORDER BY review_time ASC) AS first_review
    , ROW_NUMBER() OVER (PARTITION BY customer_id, sku.title||sku.subtitle ORDER BY review_time DESC) AS most_recent_review
  FROM {{ ref('reviews')}} r
  LEFT JOIN {{ ref('meal_skus')}} sku
    ON r.meal_sku_id = sku.meal_sku_id
  WHERE (RATING_BINARY = 1 or RATING_STARS = 5)
    AND is_valid
)
, agg_review AS (
  SELECT
    title
    , subtitle
    , customer_id
    , review_time
    , first_review AS total_review_count
  FROM reviews
  WHERE most_recent_review = 1
)
, customers AS (
  SELECT
    fts.customer_id
    , c.email
    , fts.term_id
  FROM {{ ref('future_term_summary')}} fts
  LEFT JOIN {{ ref('customers')}} c
    ON fts.customer_id = c.customer_id
  WHERE fts.latest_user_status = 'active'
    AND NOT fts.is_internal_account
    AND NOT fts.is_employee
    AND fts.term_id = (SELECT MIN(term_id) FROM {{ ref('terms')}} WHERE is_live)
)
, intermediate_reviews AS (
  SELECT
    DISTINCT(ar.title||' '||ar.subtitle) AS meal_name
    , ar.customer_id
    , c.email
    , umm.term_id
    , ar.title
    , ar.subtitle
    , ar.review_time
    , ar.total_review_count
    , COUNT(DISTINCT meal_name) OVER (PARTITION BY ar.customer_id) AS meals_reviewed_positively_on_menu
    , ROW_NUMBER() OVER (PARTITION BY ar.customer_id ORDER BY review_time DESC) AS latest_positively_reviewed_meal
    , ROW_NUMBER() OVER (PARTITION BY ar.customer_id ORDER BY review_time ASC) AS first_positively_reviewed_meal
  FROM upcoming_menu_meals umm
  LEFT JOIN agg_review ar
    ON umm.title||umm.subtitle = ar.title||ar.subtitle
  INNER JOIN customers c
    ON ar.customer_id = c.customer_id
)
--Meal 1 = most recent positively rated meal on upcoming menu
--Meal 2 = first positively rated meal on upcoming menu
, cleanup AS (
  SELECT
    ir.customer_id
    , ir.email
    , ir.term_id
    , MAX(CASE WHEN ir.latest_positively_reviewed_meal = 1 THEN ir.title END) AS fave_meal_1_title
    , MAX(CASE WHEN ir.latest_positively_reviewed_meal = 1 THEN ir.subtitle END) AS fave_meal_1_subtitle
    , MAX(CASE WHEN ir.latest_positively_reviewed_meal = 1 THEN ir.review_time END) AS fave_meal_1_review_time
    --If only 1 meal was rated, meal 1 and meal 2 will be equal, so make meal 2 NULL
    , MAX(CASE WHEN ir.meals_reviewed_positively_on_menu <> 1 AND ir.first_positively_reviewed_meal = 1 THEN ir.title END) AS fave_meal_2_title
    , MAX(CASE WHEN ir.meals_reviewed_positively_on_menu <> 1 AND ir.first_positively_reviewed_meal = 1 THEN ir.subtitle END) AS fave_meal_2_subtitle
    , MAX(CASE WHEN ir.meals_reviewed_positively_on_menu <> 1 AND ir.first_positively_reviewed_meal = 1 THEN ir.review_time END) AS fave_meal_2_review_time
    , ir.meals_reviewed_positively_on_menu
  FROM intermediate_reviews ir
  GROUP BY 1,2,3,10
  //HAVING fave_meal_1_title <> COALESCE(fave_meal_2_title, '')
)
--If meal 2 = meal 1, make meal 2 NULL
SELECT
  customer_id
  , email
  , term_id
  , fave_meal_1_title
  , fave_meal_1_subtitle
  , fave_meal_1_review_time
  , CASE WHEN fave_meal_1_title = fave_meal_2_title THEN NULL ELSE fave_meal_2_title END AS fave_meal_2_title
  , CASE WHEN fave_meal_1_title = fave_meal_2_title THEN NULL ELSE fave_meal_2_subtitle END AS fave_meal_2_subtitle
  , CASE WHEN fave_meal_1_title = fave_meal_2_title THEN NULL ELSE fave_meal_2_review_time END AS fave_meal_2_review_time
  , meals_reviewed_positively_on_menu
FROM cleanup