-- Stardard categorization for marketing channels when  medium,  source, and  campaign are available and useful (default categorization)
{%- macro marketing_channels(medium, source, campaign) -%}
CASE -- Medium-level
    WHEN {{ medium }} IS NULL
    THEN
        CASE --Source-level for  medium: null
            WHEN {{ source }} IS NULL
            THEN
                CASE -- Campaign-level for  medium: null,  source: null
                    WHEN {{ search_string('referer_url', ('.*(google\\.).*')) }}
                    THEN 'Google Organic Search'
                    WHEN {{ search_string('referer_url', ('.*(bing\\.).*')) }}
                    THEN 'Bing Organic Search'
                    WHEN {{ search_string('referer_url', ('.*(yahoo\\.).*')) }}
                    THEN 'Yahoo Organic Search'
                    WHEN {{ search_string('referer_url', ('.*(facebook\\.).*')) }}
                    THEN 'Facebook Organic'
                    WHEN (({{ search_string("COALESCE(referer_url,'tovala.')", ('.*(tovala\\.).*')) }}) OR (referer_url IS NULL AND page_utm_content IS NULL))
                    THEN 'Direct'
                END
            WHEN {{ source }} ILIKE 'facebook'
            THEN 'Facebook'
            WHEN {{ source }} ILIKE 'google discovery'
            THEN 'Google Discovery'
            WHEN {{ source }} ILIKE 'rokt'
            THEN 'Rokt'
            WHEN {{ source }} ILIKE 'tvscientific' 
            THEN 'Peacock'
        END
    WHEN {{ medium }} ilike '%adset.name%'
    THEN 'Facebook'
    WHEN {{ medium }} ILIKE 'cpc'
    THEN
        CASE -- Source-level for  medium: 'cpc'
            WHEN {{ source }} ILIKE 'bing'
            THEN
                CASE -- Campaign-level for  medium: 'cpc',  source: 'bing'
                    WHEN {{ search_string(campaign, ('.*_brand.*')) }}
                    THEN 'Bing Brand Search'
                    WHEN {{ search_string(campaign, ('.*_non-brand.*')) }}
                    THEN 'Bing Non Brand Search'
                END
            WHEN {{ source }} ILIKE 'google%'
            THEN
                CASE -- Campaign-level for  medium: cpc,  source: 'google%'
                    WHEN  ({{ search_string(campaign, ('.*pmax.*')) }} AND {{ search_string(campaign, ('.*_brand.*')) }})
                    THEN 'Google Performance Max Brand'
                    WHEN  {{ search_string(campaign, ('.*pmax.*')) }}
                    THEN 'Google Performance Max Non Brand'
                    WHEN  ({{ search_string(campaign, ('.*_brand.*')) }} OR LOWER({{ campaign }}) = 'm6brand')
                    THEN 'Google Brand Search'
                    WHEN  {{ search_string(campaign, ('.*display.*')) }}
                    THEN 'Google Display'
                    WHEN ( (({{ search_string(campaign, ('.*_non-brand.*|.*2ndpresence.*|.*search_us_dsa_all_all.*|FALSE|pmax_bfcm2022-us_prospecting')) }}) OR (  {{ campaign }} = '') OR (  {{ campaign }} = 'pm_gsearch_ovensales_nb')))
                    THEN 'Google Non Brand Search'
                    WHEN {{ campaign }} ILIKE '%video%'
                    THEN 'Google YouTube'
                    WHEN {{ campaign }} ILIKE '%shopping%'
                    THEN 'Google Shopping'
                    WHEN {{ search_string(campaign, ('.*demandgen.*')) }}
                    THEN 'Google DemandGen'
                    WHEN {{ search_string(campaign, ('.*discovery.*')) }}
                    THEN 'Google Discovery'
                END
        END
    WHEN {{ medium }} ILIKE 'cpm'
    THEN
        CASE -- Source-level for  medium: 'cpm'
            WHEN   {{ source }} ILIKE 'adtheorent'
            THEN 'Ad Theorent'
        END
    WHEN {{ medium }} ILIKE 'digital%'
    THEN
        CASE -- Source-level for  medium: 'digital%'
            WHEN {{ source }} ILIKE 'audacy'
            THEN 'Audacy'
            WHEN {{ source }} ILIKE 'rokt'
            THEN 'Rokt'
        END
    WHEN {{ medium }} ILIKE 'direct_mail'
    THEN 'Valpak'
    WHEN {{ medium }} ILIKE 'display%'
    THEN
        CASE -- Source-level for  medium: 'display%'
            WHEN {{ source }} ILIKE 'amazon'
            THEN 'Amazon'
        END
    WHEN {{ medium }} ILIKE 'email'
    THEN 'Email'
    WHEN {{ medium }} ILIKE 'influencer'
    THEN 'Influencer'
    WHEN {{ medium }} ILIKE 'link'
    THEN
        CASE -- Source-level for  medium: 'link'
            WHEN   {{ source }} ILIKE 'cj'
            THEN
                CASE -- Campaign-level for  medium: 'link',  source: 'cj'
                    WHEN   {{ campaign }} ILIKE '3637436'
                    THEN 'Affiliate Commission'
                END
        END
    WHEN {{ medium }} ILIKE 'organic%'
    THEN
        CASE -- Source-level for  medium: 'organic%'
            WHEN   {{ source }} ILIKE 'social' OR   {{ source }} ILIKE 'instagram'
            THEN 'Instagram'
        END
    WHEN {{ medium }} ILIKE 'paid%'
    THEN
        CASE -- Source-level for  medium: 'paid%'
            WHEN {{ source }} ILIKE 'girlboss'
            THEN 'Girlboss'
            WHEN {{ source }} ILIKE 'jeeng'
            THEN 'Jeeng'
            WHEN {{ source }} ILIKE 'liveintent'
            THEN 'Live Intent'
            WHEN {{ source }} ILIKE 'morningbrew'
            THEN 'Morning Brew'
            WHEN {{ source }} ILIKE 'msa'
            THEN 'Facebook'
            WHEN {{ source }} ILIKE 'wellput'
            THEN 'WellPut'
        END
    WHEN {{ medium }} ILIKE 'print'
    THEN
        CASE -- Source-level for  medium: 'print'
            WHEN {{ source }} ILIKE 'qrcode'
            THEN
                CASE -- Campaign-level for  medium: 'print',  source: 'qrcode'
                    WHEN {{ campaign }} ILIKE 'jhudgiveaway'
                    THEN 'Jennifer Hudson'
                END
        END
    WHEN {{ medium }} ILIKE 'qrcode'
    THEN
        CASE -- Source-level for  medium: 'qrcode'
            WHEN {{ source }} ILIKE 'print'
            THEN
                CASE -- Campaign-level for  medium: 'qrcode',  source: 'print'
                    WHEN {{ campaign }} ILIKE 'realsimple'
                    THEN 'Real Simple'
                    WHEN {{ campaign }} ILIKE 'rightathome'
                    THEN 'Right At Home'
                    WHEN {{ campaign }} ILIKE 'zulily'
                    THEN 'Zulily'
                END 
        END
    WHEN {{ medium }} ILIKE 'sms'
    THEN 'SMS'
    WHEN {{ medium }} ILIKE '%tv'
    THEN
        CASE -- Source-level for  medium: '%tv'
            WHEN {{ source }} ILIKE 'effectv' AND {{ search_string(campaign, ('x1voiceplus')) }}
            THEN 'Xfinity'
            WHEN {{ source }} ILIKE 'mynt'
            THEN 'Linear TV'
            WHEN {{ source }} ILIKE '%spotlight%'
            THEN 'Spotlight'
            WHEN {{ source }} ILIKE 'steelhouse' AND {{ search_string(campaign, ('rmktovalactv|televisioncampaigngroup.*')) }}
            THEN 'Steelhouse'
            WHEN {{ source }} ILIKE 'roku'
            THEN 'Roku'
        END
    WHEN {{ medium }} ILIKE 'whitelisting'
    THEN
        CASE -- Source-level for  medium: 'whitelisting'
            WHEN {{ source }} ILIKE 'msa'
            THEN 'Facebook'
        END
    WHEN {{ medium }} ILIKE 'youtube'
    THEN
        CASE -- Source-level for  medium: 'youtube'
            WHEN {{ source }} ILIKE 'google'
            THEN 'Google YouTube'
        END
END
{%- endmacro -%}