SELECT 
  session_id
  , base_url AS landing_url
  , base_page AS landing_page
  , base_path AS landing_path
  , page_utm_campaign AS marketing_campaign
  , page_utm_ad_id AS marketing_ad_id
  , page_utm_content AS marketing_content
  -- Organized, nested CASE statements to determine 'channel' using page_utm_medium, page_utm_source, and page_utm_campaign
  -- WHEN statements in each level are alphabetized for ease in reading or editing
  -- Classification from broadest to most specific is page_utm_medium --> page_utm_medium --> page_utm_source
  -- one classification type per nested level (i.e. should not have a WHEN that co-mingles medium & source, or campaign & medium)
  -- Return a value at the broadest level possible (i.e. don't specify campaign if medium & source are sufficient to classify)
  , COALESCE(
    {{marketing_channels_no_medium('page_utm_source', 'page_utm_campaign')}}
    , {{ marketing_channels('page_utm_medium', 'page_utm_source', 'page_utm_campaign') }}
    , 'Uncategorized') AS channel
  , CASE
      -- direct matches first
      WHEN channel in ('Direct','Email','SMS')
      THEN channel
      WHEN channel IN ('Instagram','YouTube', 'Girlboss', 'Affiliate Influencer')
      THEN 'Influencer'
      WHEN channel = 'Facebook Organic'
      THEN 'Organic Social'
      WHEN channel = 'Ampush Retainer'
      THEN 'Other'
      WHEN channel IN ('Customer Service Commission','Meal Discounts','Referral Direct')
      THEN 'Owned'
      WHEN channel IN ('Jennifer Hudson', 'Real Simple', 'Right At Home', 'Valpak', 'Zulily')
      THEN 'Print'
      WHEN channel = 'Audacy'
      THEN 'Radio'
      -- if no direct match, then try search_string partial matches
      WHEN {{ search_string('channel', ('affiliate.*|dmi.*|impact radius platform fee|mysubscriptionaddiction')) }}
      THEN 'Affiliate'
      WHEN {{ search_string('channel', ('.*organic search.*')) }}
      THEN 'Organic Search'
      WHEN {{ search_string('channel', ('Ad Theorent|Agency Fee|Amobee|.*Brand Search|Co-op Commerce|Facebook|Google.*|Jeeng|Live Intent|Morning Brew|Pinterest|Podcast|ReadySet|Reddit|Snapchat|Steelhouse Remarketing|Taboola|TikTok|WellPut|Google Youtube|Google Shopping|Rokt')) }}
      THEN 'Digital'
      WHEN {{ search_string('channel', ('Direct Mail.*')) }}
      THEN 'Direct Mail'
      WHEN {{ search_string('channel', ('FreeWheel.*|.*TV.*|Pacific Media|Steelhouse|Xfinity|Roku|Spotlight|CTV|Linear TV')) }}
      THEN 'TV'
      ELSE 'Uncategorized'
    END AS channel_category
FROM {{ table_reference('page_visits', 'grind') }} 
WHERE is_first_in_session