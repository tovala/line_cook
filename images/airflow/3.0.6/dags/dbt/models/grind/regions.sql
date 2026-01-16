
SELECT DISTINCT
    zipcode AS zip_cd 
    , state 
    , county 
    , CASE
        WHEN state IN ('WA', 'OR', 'CA') THEN 'west_coast'
        WHEN state IN ('NV', 'AZ', 'NM', 'TX') THEN 'southwest'
        WHEN state IN ('ID', 'MT', 'WY', 'UT', 'CO') THEN 'rocky_mountain'
        WHEN state IN ('ND', 'SD', 'NE', 'KS', 'OK') THEN 'great_plains'
        WHEN state IN ('MN', 'WI', 'IA', 'MI', 'MO', 'IL', 'IN', 'OH') THEN 'midwest'
        WHEN state IN ('AR', 'LA', 'MS', 'AL', 'GA', 'FL', 'SC', 'TN', 'KY', 'NC', 'WV', 'VA') THEN 'south'
        WHEN state IN ('NY', 'PA', 'NJ', 'DE', 'MD', 'DC') THEN 'mid_atlantic'
        WHEN state IN ('ME', 'VT', 'NH', 'MA', 'RI', 'CT') THEN 'new_england'
        WHEN state = 'AK' THEN 'alaska'
        WHEN state = 'HI' THEN 'hawaii'
        ELSE NULL
    END AS region
    , latitude
    , longitude
FROM {{ source('brine', 'zipcode_details') }} 
