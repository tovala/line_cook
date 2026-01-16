-- All channel spend line items are submitted via a form in Sigma by Marketing team 
-- DEA has communicated to Marketing that spend can only be input for channels that exist in wash.channel_dimensions  
-- This model should contain all offline spend details that have been manually added in Sigma  

SELECT
  id AS sigma_id
  , TRIM(channel_name) AS channel_name 
  , TRIM(partner) AS partner 
  , update_cadence
  , allocation_method 
  , allocation_method='Straightline' AS is_straightline
  , allocation_method='Monthly distribution' AS is_monthly
  -- If Allocation Method is Monthly distribution, we say that yes it should have a recurring term period
  -- Cast yes/no drop down values as a boolean 
  , CASE WHEN is_monthly 
         THEN is_recurring_term::BOOLEAN  
    END AS is_recurring
  -- Total number of months that a recurring term spend item should be distributed over 
  , CASE WHEN is_recurring
         THEN recurring_term_period_total_months::FLOAT 
    END AS recurring_term_months
  -- If Allocation Method is Straightline, we use the input amortization days. Otherwise, we reset it to null.  
  , CASE WHEN is_straightline
         THEN amortization_days::FLOAT 
    END AS amortization_days 
  , spend_date
  , TRUNC(total_spend, 2) AS total_spend 
  , NULLIF(invoice_number, 'No') AS invoice_number
  -- Cast yes/no drop down values as a boolean 
  , is_estimate::BOOLEAN AS is_estimate 
  -- Set data source and update cadence to indicate this data is pulled from Sigma 
  , 'sigma_input_table' AS data_source 
  , 'manual_input' AS update_cadence_type
  , last_updated_at
  , last_updated_by
FROM {{ source('sigma_input_tables', 'channel_spend_details') }}