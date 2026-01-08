-- Create new table
CREATE OR REPLACE TABLE brine.new_anticombinations (
    term_id INTEGER
    , facility_network STRING 
    , cycle INTEGER 
    , meal_sku_id INTEGER 
    , meal STRING 
    , anticombination_meal_sku_id INTEGER 
    , anticombo STRING 
    , is_sideswap BOOLEAN 
);

-- Backfill new table 
INSERT INTO brine.new_anticombinations
SELECT DISTINCT 
    term_id 
    , 'chicago' -- hardcode to Chicago for < T256 (was the only option)
    , NULL -- leaving cycle null cuz there was never a difference before T256
    , meal_sku_id 
    , meal
    , anticombination_meal_sku_id 
    , anticombo 
    , is_sideswap
FROM brine.anticombinations;

-- Adding T256 data
UPDATE brine.new_anticombinations SET ship_period = 1 WHERE term_id = 256;
INSERT INTO brine.new_anticombinations 
VALUES
    (256, 'chicago', 2, 2091, 'BBQ Chicken Breast with Mac & Cheese', 3092, 'BBQ Chicken Breast with Slaw-Style Broccoli', TRUE), 
    (256, 'slc', 2, 2091, 'BBQ Chicken Breast with Mac & Cheese', 3092, 'BBQ Chicken Breast with Slaw-Style Broccoli', TRUE);

-- Drop old table 
DROP TABLE brine.anticombinations; 

-- Rename new table to old table name
ALTER TABLE brine.new_anticombinations RENAME TO brine.anticombinations;

-- Ensure access
GRANT ALL ON TABLE brine.anticombinations TO ROLE technical;
