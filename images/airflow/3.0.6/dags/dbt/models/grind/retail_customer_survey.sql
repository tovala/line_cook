SELECT 
 ut.userid AS customer_id 
 , LOWER({{ clean_string('ut.data:channel') }}) AS retail_channel
 , cr.show_code
 , {{ clean_string('ut.data:phone') }} AS phone_number
FROM {{ table_reference('usertags') }} ut
LEFT JOIN {{ source('mold', 'costco_roadshows') }} cr
 ON {{ clean_string('ut.data:location') }} = cr.location_number
 AND ut.updated >= cr.roadshow_start_date 
 AND ut.updated < COALESCE(cr.next_roadshow_start_date, '9999-12-31')
WHERE ut.tag = 'retail_user'
 AND LOWER({{ clean_string('ut.data:channel') }}) NOT IN ('i don\'t have one!', 'other', 'tovala.com', 'tovala')