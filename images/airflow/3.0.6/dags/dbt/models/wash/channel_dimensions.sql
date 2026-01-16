-- All channel dimensions are defined in an input table in Sigma by Marketing team 
-- DEA has communicated to Marketing that any channels *not* input into this table will not be recognized in our database 
-- We should expect all existing channels to show up here with corresponding dimensions
-- However, there will be separate spend tables for channels with offline vs online spend (e.g. manual input vs API). 

SELECT
  id as sigma_id
  , TRIM(channel_name) AS channel_name
  , channel_category 
  , TRIM(partner) AS partner 
  , expense_category 
  , channel_owner 
  , media_type 
  -- Cast yes/no drop down values as a boolean 
  , is_active::BOOLEAN AS is_active 
  , last_updated_at
  , last_updated_by
  , notes
FROM {{ source('sigma_input_tables', 'channel_dimensions') }}
WHERE channel_name IS NOT NULL 
-- Pull the most recent row for every Channel/Partner/Category combination 
QUALIFY ROW_NUMBER() OVER (PARTITION BY channel_name, channel_category, partner ORDER BY last_updated_at DESC) = 1