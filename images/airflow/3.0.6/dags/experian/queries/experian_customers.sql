SELECT 
  '{' || LISTAGG('"rec' || row_number || '":"' || request_body || '"', ',') || '}'
FROM brine.experian_customers_temp
WHERE row_number >= 0 AND row_number < 10;