SET num_batch_size = {{ params.batch_size }};
SET current_batch = {{ ti.map_index }} + 1;
SELECT 
  '{' || LISTAGG('"rec' || COALESCE(NULLIFZERO(row_number%$num_batch_size),$num_batch_size) || '":"' || request_body || '"', ',') WITHIN GROUP (ORDER BY row_number) || '}'
FROM brine.experian_customers_temp
WHERE row_number BETWEEN ({{ params.batch_size }} * $current_batch) - {{ params.batch_size }} + 1 AND {{ params.batch_size }} * $current_batch;