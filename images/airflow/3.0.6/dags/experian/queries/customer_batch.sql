SET num_batch_size = {{ params.batch_size }};
SELECT 
  '{' || LISTAGG('"rec' || COALESCE(NULLIFZERO(row_number%$num_batch_size),$num_batch_size) || '":"' || request_body || '"', ',') WITHIN GROUP (ORDER BY row_number) || '}'
FROM brine.experian_customers_temp
WHERE row_number BETWEEN ({{ params.batch_size }} * {{ ti.map_index }}) - {{ params.batch_size }} + 1 AND {{ params.batch_size }} * {{ ti.map_index }};