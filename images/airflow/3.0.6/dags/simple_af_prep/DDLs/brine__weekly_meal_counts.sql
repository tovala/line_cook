-- TODO: Do we ever have to worry about the meal being different across CYCLES? If so, do something about that below

-- Create new table 
CREATE OR REPLACE TABLE brine.weekly_meal_counts_new (
    term_id INTEGER
    , facility_network STRING
    , meal_sku_id INTEGER
    , meal STRING 
    , production_cd INTEGER
    , cycle_1_production_count INTEGER
    , cycle_2_production_count INTEGER
    , is_customer_facing BOOLEAN
    , pkey STRING 
); 

-- Read in data from old table 
INSERT INTO brine.weekly_meal_counts_new
SELECT 
    term_id 
    , 'chicago' 
    , meal_sku_id 
    , meal 
    , production_cd 
    , cycle_1_production_count_chi 
    , cycle_2_production_count_chi 
    , is_customer_facing 
    , CONCAT(facility_network, meal_sku_id)
FROM brine.weekly_meal_counts;

-- Add a row for T256
INSERT INTO brine.weekly_meal_counts_new
SELECT 
    term_id 
    , 'slc'
    , meal_sku_id 
    , meal 
    , production_cd 
    , cycle_1_production_count_chi 
    , cycle_2_production_count_chi 
    , is_customer_facing 
FROM brine.weekly_meal_counts
WHERE term_id = 256;

-- Drop old table 
DROP TABLE brine.weekly_meal_counts; 

-- Rename old table namespace with new table 
ALTER TABLE brine.weekly_meal_counts_NEW RENAME TO brine.weekly_meal_counts;

-- Grant permissions
GRANT SELECT, UPDATE, INSERT, DELETE ON brine.weekly_meal_counts TO ROLE technical;
