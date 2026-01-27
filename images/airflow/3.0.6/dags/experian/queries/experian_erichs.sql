SELECT 
  LISTAGG(id::STRING, ',') AS erich_values
FROM brine.experian_erichs ORDER BY id;