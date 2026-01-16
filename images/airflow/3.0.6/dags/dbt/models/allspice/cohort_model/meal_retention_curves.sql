{{
  config(
    tags=['cohort_model'],
  )
}}

-- Customer start month
{% set start_months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'] %}

-- Customer commitment flag
{% set commitments = [true, false] %}

-- Customer oven price bucket lower & upper bounds
{% set price_buckets = [[0, 60], [61, 97], [98, 1000], [-1001, -1]] %}

-- Total number of iterations
{% set total_iterations = (start_months | length) * (commitments | length) * (price_buckets | length) %}

-- Iterator counter
{% set counter = namespace(value=1) %}

WITH

/*
Iterate through each possible combination of
• Start month (x12)
• commitment flag (x2)
• Oven price bucket (x4)
yields a total of 96 iterations
*/

{% for start_month in start_months %}
  {% for commitment in commitments %}
    {% for bucket in price_buckets %}

      -- If the this is the 2nd+ iteration, start with a comma
      {% if counter.value > 1 %} , {% endif %}

      base_{{ counter.value }} AS (
        SELECT
          cts.cohort
          , cts.term_id
          , cts.cohort_week_without_holidays AS order_week
          , COUNT(DISTINCT CASE WHEN NOT cts.is_atypical_retention_week THEN cts.customer_id END) AS cohort_count
          , COUNT(DISTINCT CASE WHEN NOT cts.is_atypical_retention_week AND cts.is_fulfilled THEN cts.meal_order_id END) AS fulfilled_orders
          , SUM(CASE WHEN NOT cts.is_atypical_retention_week THEN cf.first_order_size END) AS first_meal_count
          , SUM(CASE WHEN NOT cts.is_atypical_retention_week AND cts.is_fulfilled THEN cts.order_size END) AS fulfilled_meals
          , ROW_NUMBER() OVER (PARTITION BY cts.cohort ORDER BY cts.term_id) AS adjusted_order_week
        FROM {{ ref('customer_term_summary') }} as cts
        LEFT JOIN {{ ref('customer_facts') }} as cf
          ON cts.customer_id = cf.customer_id
        WHERE cts.cohort NOT IN (155,156,203,207,215,216,219,224,234,235)
          AND cts.term_id NOT IN (155,156,203,207,215,216,219,224,234,235)
          AND cts.is_after_cohort_term
          AND NOT cts.is_excluded_from_retention
          AND NOT cts.is_holiday_term
          AND NOT cts.is_internal_account
          AND cf.cohort_start_date >= '2021-01-01'
          AND LOWER(MONTHNAME(cf.cohort_start_date)) = '{{ start_month }}'
          AND COALESCE(cf.is_first_oven_commitment_purchase, FALSE) = {{ commitment }}
          AND COALESCE(cf.first_oven_purchase_price, -1000) >= {{ bucket[0] }}
          AND COALESCE(cf.first_oven_purchase_price, -1000) < {{ bucket[1] }}
        GROUP BY 1,2,3
        )

      , facts_{{ counter.value }} AS (
        SELECT
          b.cohort
          , b.term_id
          , b.adjusted_order_week
          , b.cohort_count
          , b.first_meal_count
          , b.fulfilled_orders
          , b.fulfilled_meals
          FROM base_{{ counter.value }} as b
        )

      , aggs_{{ counter.value }} AS (
        SELECT
          f.adjusted_order_week
          , SUM(f.first_meal_count) AS cohort_meal_count
          , SUM(f.fulfilled_meals) AS meal_count
        FROM facts_{{ counter.value }} as f
        GROUP BY 1
      )

      , retention_{{ counter.value }} as (
        SELECT
          adjusted_order_week
          , ZEROIFNULL(DIV0NULL(meal_count, cohort_meal_count)) AS order_retention_{{ start_month }}_{{ commitment }}_{{ bucket[0] | replace('-', '_') }}
        FROM aggs_{{ counter.value }}
        WHERE adjusted_order_week <= 208
        ORDER BY 1
      )

      -- Increment the iteration counter
      {% set counter.value = counter.value + 1 %}

    {% endfor %}
  {% endfor %}
{% endfor %}

/*
Below is the final select statement that needs to reference the CTEs above.
*/

-- Reset the iteration counter to 1
{% set counter.value = 1 %}

{% for start_month in start_months %}
  {% for commitment in commitments %}
    {% for bucket in price_buckets %}

      -- If this is the first iteration, reference the base table
      {% if counter.value == 1 %}
        select

        -- Nested loop through all 96 retention CTEs that grabs the relevant field from each one
        {% for iter in range(total_iterations) %}
          {% if loop.first %}
            retention_{{ iter + 1 }}.*
          {% endif %}
          {% if not loop.first %}
          , retention_{{ iter + 1 }}.* exclude adjusted_order_week
          {% endif %}
        {% endfor %}

        from retention_{{ counter.value }}

      {% endif %}

      -- All the join statements are created below
      {% if counter.value > 1 %}

        left join retention_{{ counter.value }}
          on retention_1.adjusted_order_week = retention_{{ counter.value }}.adjusted_order_week

      {% endif %}

      -- Increment the iteration counter
      {% set counter.value = counter.value + 1 %}

    {% endfor %}
  {% endfor %}
{% endfor %}
