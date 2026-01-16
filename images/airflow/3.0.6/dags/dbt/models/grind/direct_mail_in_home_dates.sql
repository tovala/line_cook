SELECT 
  LOWER(mail_timeframe) AS mail_timeframe
  , CONVERT_TIMEZONE('UTC', in_home_date)::DATE AS in_home_date
  , spend
  , offer
FROM {{ source('sigma_input_tables', 'in_home_date_output_table') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY mail_timeframe ORDER BY last_updated_at DESC) = 1