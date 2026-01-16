SELECT
  TRY_TO_NUMERIC(spotid)::INTEGER AS spot_id
  , CASE 
      WHEN region LIKE 'US/east-coast/%' THEN 'America/New_York'
      WHEN region LIKE 'US/central/%' THEN 'America/Chicago'
      WHEN region LIKE 'US/mountain/%' THEN 'America/Denver'
      WHEN region LIKE 'US/west-coast/%' THEN 'America/Los_Angeles'
      ELSE 'America/Chicago' -- default to Central if region is unknown
    END AS source_timezone
  , CONVERT_TIMEZONE(
      'America/Chicago', 
    source_timezone,
    spot_datetime
  ) AS spot_datetime_local
  , CONVERT_TIMEZONE(
    'America/Chicago', 
    'UTC',
    spot_datetime
  ) AS spot_datetime_utc
  , spot_datetime
  , region
  , daypart AS day_part
  , channel AS tv_channel
  , parentchannel AS parent_tv_channel
  , saleshouse AS sales_house
  , creative
  , extracted AS updated
FROM {{ table_reference('tvsquared_spots') }}
