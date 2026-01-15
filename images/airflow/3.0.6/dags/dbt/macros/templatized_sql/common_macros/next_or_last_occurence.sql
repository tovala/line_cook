{%- macro next_or_last_occurence(lead_or_lag, row_filter, field) -%}
  {{ lead_or_lag }}(CASE WHEN {{ row_filter }} AND NOT COALESCE(mo.is_gma_or_trial_order, FALSE) AND t.term_id >= cf.cohort
                         THEN {{ field }} 
                    END) IGNORE NULLS OVER (PARTITION BY ft.customer_id ORDER BY t.term_id) 
{%- endmacro -%}