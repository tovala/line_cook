{%- macro marketing_channels_no_medium(source,  campaign) -%}
CASE -- Source-level when undefined medium criteria (sources with multiple, random, irrelevant mediums)
    WHEN {{ source }} ILIKE 'affiliate'
    THEN 'Affiliate Commission'
    WHEN {{ source }} ILIKE 'affiliate-influencer'
    THEN 'Affiliate Influencer'
    WHEN {{ source }} ILIKE 'applovin'
    THEN 'Applovin'
    WHEN {{ source }} ILIKE '%coopcommerce'
    THEN 'Co-op Commerce'
    WHEN {{ source }} ILIKE '%facebook'
    THEN
        CASE -- Campaign-level when undefined medium criteria, source: '%facebook'
            WHEN {{ campaign }} ILIKE 'profilelink'
            THEN 'Facebook Organic Social'
            ELSE 'Facebook'
        END
    WHEN {{ source }} ILIKE 'google_discovery'
    THEN 'Google Discovery'
    WHEN {{ source }} ILIKE 'influencer'
    THEN 'Influencer'
    WHEN {{ source }} ILIKE 'juice-media'
    THEN 
        CASE -- Campaign-level when undefined medium criteria, source: 'juice-media'
            WHEN {{ campaign }} ILIKE '%ctv%'
            THEN 'CTV'
            ELSE 'Juice Media'
        END
    WHEN {{ source }} ILIKE 'pinterest'
    THEN 'Pinterest'
    WHEN {{ source }} ILIKE 'reddit'
    THEN 'Reddit'
    WHEN {{ source }} ILIKE 'snapchat'
    THEN 'Snapchat'
    WHEN {{ source }} ILIKE 'taboola'
    THEN 'Taboola'
    WHEN {{ source }} ILIKE 'tiktok%'
    THEN 'TikTok'
    ELSE
        CASE -- Campaign-level when undefined medium, source criteria (campaigns with multiple, random, irrelevant mediums and/or sources)
            WHEN {{ campaign}} ILIKE 'bw-coop%'
            THEN 'Direct Mail'
            WHEN {{ campaign }} ILIKE 'ctrpassword'
            THEN 'Email'
        END
END
{%- endmacro -%}