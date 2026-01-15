{% macro order_summary_by_term_rank(term_rank) %} 
  MAX(CASE WHEN rt.term_rank = {{ term_rank }} THEN fts.total_meals_count END) AS sf_t_{{ term_rank - 1 }}_number_meals_selected
  , MAX(CASE WHEN rt.term_rank = {{ term_rank }} THEN
          CASE
            WHEN fts.total_meals_count = 0 THEN 'none'
            WHEN fts.total_meals_count < fts.actual_order_size THEN 'partial'
            ELSE 'full'
          END
        END) AS sf_t_{{ term_rank - 1 }}_meals_selected_status
  , MAX(CASE WHEN rt.term_rank = {{ term_rank }} THEN fts.is_skipped END) AS sf_t_{{ term_rank - 1 }}_skip_status
{% endmacro %}