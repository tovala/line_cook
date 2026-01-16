
SELECT
  COALESCE(b.zip, z.zipcode) AS zip_cd
  , COALESCE(z.city, b.city) AS city
  , COALESCE(z.county, b.county) AS county
  , COALESCE(z.state, b.state) AS state
  , CASE WHEN COALESCE(b.timezone, z.timezone) LIKE 'America/%' 
              OR COALESCE(b.timezone, z.timezone) = 'Pacific/Honolulu'
         THEN COALESCE(b.timezone, z.timezone)
         WHEN st.timezone <> 'Mixed'
         THEN st.timezone
  END AS timezone
FROM {{ source('brine', 'timezonebyzipcode') }} b 
FULL OUTER JOIN {{ source('brine', 'zipcode_details') }} z 
ON b.zip = z.zipcode
LEFT JOIN {{ source('brine', 'state_timezones') }} st 
ON COALESCE(z.state, b.state) = st.state_abbreviation
