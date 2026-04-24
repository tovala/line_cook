SELECT 
  'TERM_' || term_id AS term_id
FROM mugwort.future_terms 
WHERE term_id BETWEEN {{ ti.xcom_pull(task_ids='get_projection_start_term', dag_id='cohort_model', key='return_value') }} AND {{ ti.xcom_pull(task_ids='get_projection_end_term', dag_id='cohort_model', key='return_value') }}
AND NOT is_skipped_term;