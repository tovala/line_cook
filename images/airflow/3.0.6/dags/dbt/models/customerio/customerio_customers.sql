-- !!!!!!!IMPORTANT!!!!!!
-- If you make any changes to this model - you MUST also make sure the changes are reflected in the SQL within the customerio sync https://fly.customer.io/workspaces/194449/pipelines/sources/94954/syncs/3922
-- If you're bringing in dates/times, they have to be converted from epoch time in the cio sync SQL
WITH customer_review_aggs AS (
  SELECT 
    customer_id 
    , review_time
    , rating_stars
    , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY review_time ASC) AS row_number
    , AVG(rating_stars) OVER (PARTITION BY customer_id ORDER BY review_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_rating_stars_all_time
    , AVG(rating_stars) OVER (PARTITION BY customer_id ORDER BY review_time DESC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg_rating_stars_last_5
  FROM {{ ref('reviews') }}
  WHERE rating_stars IS NOT NULL
  GROUP BY 1,2,3
),

ranked_terms AS (
  SELECT
    term_id
    , ROW_NUMBER() OVER (ORDER BY order_by_time) AS term_rank
  FROM {{ ref('terms') }}
  WHERE NOT is_past_order_by  
),

upcoming_orders_summary AS (
  SELECT
    cf.customer_id
    , {{ order_summary_by_term_rank(1) }}
    , {{ order_summary_by_term_rank(2) }}
    , {{ order_summary_by_term_rank(3) }}

  FROM {{ ref('customer_facts')}} cf
  LEFT JOIN {{ ref('future_term_summary')}} fts
    ON fts.customer_id = cf.customer_id 
  LEFT JOIN ranked_terms rt 
    ON fts.term_id = rt.term_id
  GROUP BY ALL
),

next_term_status AS (
  SELECT 
    customer_id 
    , NTH_VALUE(term_id, 1) OVER (PARTITION BY customer_id ORDER BY term_id) AS next_term_to_close
    , NTH_VALUE(term_id, 2) OVER (PARTITION BY customer_id ORDER BY term_id) AS next_2_terms_to_close
    , NTH_VALUE(term_id, 3) OVER (PARTITION BY customer_id ORDER BY term_id) AS next_3_terms_to_close
    , NTH_VALUE(latest_user_status, 1) OVER (PARTITION BY customer_id ORDER BY term_id) AS next_term_to_close_status
    , NTH_VALUE(latest_user_status, 2) OVER (PARTITION BY customer_id ORDER BY term_id) AS next_2_terms_to_close_status
    , NTH_VALUE(latest_user_status, 3) OVER (PARTITION BY customer_id ORDER BY term_id) AS next_3_terms_to_close_status
    , NTH_VALUE(is_fully_selected, 1) OVER (PARTITION BY customer_id ORDER BY term_id) AS next_term_to_close_fully_selected
    , NTH_VALUE(is_break_skip, 1) OVER (PARTITION BY customer_id ORDER BY term_id) AS sf_t_0_is_break_skip
    , NTH_VALUE(is_break_skip, 2) OVER (PARTITION BY customer_id ORDER BY term_id) AS sf_t_1_is_break_skip
    , NTH_VALUE(is_break_skip, 3) OVER (PARTITION BY customer_id ORDER BY term_id) AS sf_t_2_is_break_skip
    , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY term_id) AS row_number
  FROM {{ ref('future_term_summary') }} 
  WHERE NOT is_past_order_by
),

customer_term_summary_details AS (
  SELECT
    customer_id
    , running_total_fulfilled_order_count AS total_fulfilled_orders_count
    , consecutive_fulfilled_count
    , is_off_inactive
    , consecutive_skip_count
  FROM {{ ref('customer_term_summary') }}
  WHERE is_most_recent_record
),

delivery_estimate AS (
  SELECT
    customer_id
    , MAX_BY(estimated_delivery_date, term_id) AS estimated_delivery_date
  FROM {{ ref('meal_shipments') }}
  GROUP BY customer_id
)

SELECT
  c.customer_id
  , c.email AS customer_email
  , c.is_deactivated AS sf_is_deactivated
  , c.is_on_break AS sf_is_on_break

  --free dessert flags
  , cf.is_free_dessert_customer AS sf_is_free_dessert_customer
  , cf.cancellation_time_free_dessert_for_life AS sf_cancellation_time_free_dessert_for_life

  --commitment flags
  --this total fulfilled orders count is more inclusive, including orders across any stripe fingerprint associated with the customer
  , cc.total_fulfilled_orders_count AS sf_total_fulfilled_orders_count_commitment
  , cc.commitment_orders_remaining AS sf_commitment_orders_remaining
  , cc.commitment_status AS sf_commitment_status

  --upcoming orders info
  , cf.latest_status = 'active' AS sf_ce_active_status
  , uos.sf_t_0_number_meals_selected AS sf_ce_t_0_number_meals_selected
  , uos.sf_t_0_meals_selected_status AS sf_ce_t_0_meals_selected_status
  , uos.sf_t_0_skip_status AS sf_ce_t_0_skip_status
  , uos.sf_t_1_number_meals_selected AS sf_ce_t_1_number_meals_selected
  , uos.sf_t_1_meals_selected_status AS sf_ce_t_1_meals_selected_status
  , uos.sf_t_1_skip_status AS sf_ce_t_1_skip_status
  , uos.sf_t_2_number_meals_selected AS sf_ce_t_2_number_meals_selected
  , uos.sf_t_2_meals_selected_status AS sf_ce_t_2_meals_selected_status
  , uos.sf_t_2_skip_status AS sf_ce_t_2_skip_status

  --customer latest status & customer_facts info
  , cf.latest_status AS sf_latest_status
  , cf.is_internal_account AS sf_is_internal_account
  , cf.current_pause_time AS sf_latest_pause_time
  , cf.current_cancel_time AS sf_latest_cancel_time
  , cf.latest_nps_score AS sf_nps_recommendation_score

  --delivery estimate
  , de.estimated_delivery_date AS sf_estimated_delivery_date

  --term flags
  , nts.next_term_to_close AS sf_next_term_to_close
  , nts.next_2_terms_to_close AS sf_next_2_terms_to_close
  , nts.next_3_terms_to_close AS sf_next_3_terms_to_close

  -- customer's next term to close information
  , nts.next_term_to_close_status AS sf_next_term_to_close_status
  , nts.next_term_to_close_fully_selected AS sf_next_term_to_close_fully_selected
  , nts.next_2_terms_to_close_status AS sf_next_2_terms_to_close_status
  , nts.next_3_terms_to_close_status AS sf_next_3_terms_to_close_status
  , nts.sf_t_0_is_break_skip
  , nts.sf_t_1_is_break_skip
  , nts.sf_t_2_is_break_skip

  -- upcoming meal favorites
  , umf.fave_meal_1_title AS sf_nttc_meal_favorites_1_title
  , umf.fave_meal_1_subtitle AS sf_nttc_meal_favorites_1_subtitle
  , umf.fave_meal_1_review_time AS sf_nttc_meal_favorites_1_review_time
  , umf.fave_meal_2_title AS sf_nttc_meal_favorites_2_title
  , umf.fave_meal_2_subtitle AS sf_nttc_meal_favorites_2_subtitle
  , umf.fave_meal_2_review_time AS sf_nttc_meal_favorites_2_review_time
  , umf.meals_reviewed_positively_on_menu AS sf_nttc_meal_favorites_meals_reviewed_positively_on_menu

  -- total fulfilled orders count
  , cts.total_fulfilled_orders_count AS sf_total_fulfilled_orders_count
  , cts.consecutive_fulfilled_count AS sf_consecutive_fulfilled_count
  , cts.is_off_inactive AS sf_off_inactive_flag
  , cts.consecutive_skip_count AS sf_consecutive_skip_count

  -- reactivation eligible flag
  , rec.customer_id IS NOT NULL AS sf_reactivation_eligible_flag

  -- review info
  , cr.customer_id IS NOT NULL AS sf_has_reviewed_flag
  , cr.avg_rating_stars_all_time AS sf_avg_rating_stars_all_time
  , cr.avg_rating_stars_last_5 AS sf_avg_rating_stars_last_5

  -- referral info
  , r.referral_number AS sf_referral_count

  , CURRENT_TIMESTAMP AS sf_last_updated

FROM {{ ref('customers')}} c
LEFT JOIN {{ ref('customer_facts')}} cf
  ON cf.customer_id = c.customer_id
LEFT JOIN {{ ref('commitment_customers')}} cc
  ON c.customer_id = cc.customer_id
LEFT JOIN next_term_status nts
  ON nts.customer_id = c.customer_id
  AND nts.row_number = 1
LEFT JOIN {{ ref('upcoming_meal_favorites')}} umf
  ON umf.customer_id = c.customer_id
  AND umf.term_id = nts.next_term_to_close
LEFT JOIN delivery_estimate de
  ON de.customer_id = c.customer_id
LEFT JOIN customer_term_summary_details cts
  ON cts.customer_id = c.customer_id
LEFT JOIN {{ ref('reactivation_eligible_customers')}} rec
  ON rec.customer_id = c.customer_id
LEFT JOIN customer_review_aggs cr
  ON cr.customer_id = c.customer_id
  AND cr.row_number = 1
LEFT JOIN {{ ref('referrals')}} r
  ON r.referrer_customer_id = c.customer_id
  AND r.is_most_recent_record
LEFT JOIN upcoming_orders_summary uos
  ON uos.customer_id = c.customer_id
WHERE c.email IS NOT NULL
