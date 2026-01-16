{{ config(alias='shipment_file') }}

WITH ranked_files AS (
  SELECT DISTINCT 
    term_id
    , COALESCE(cycle_id,1) AS cycle_id
    , COALESCE(CASE WHEN term_id < 256 THEN 'chicago' END, facility_network) AS facility_network
    , filename
    , upload_time
    , DENSE_RANK() OVER (PARTITION BY term_id, 
                                      COALESCE(cycle_id,1), 
                                      COALESCE(CASE WHEN term_id < 256 THEN 'chicago' END, facility_network) 
                         ORDER BY upload_time DESC) AS file_rank
  FROM {{ source('chili', 'shipment_file') }}) 
SELECT 
  cf.term_id
  , rf.cycle_id
  , rf.facility_network
  , cf.filename
  , TRY_TO_NUMBER({{ clean_string('raw_data:userid::STRING') }}) AS userid
  , {{ clean_string('raw_data:email::STRING') }} AS email
  , {{ clean_string('raw_data:shipping_name::STRING') }} AS shipping_name
  , {{ clean_string('raw_data:shipping_line1::STRING') }} AS shipping_line1
  , {{ clean_string('raw_data:shipping_line2::STRING') }} AS shipping_line2
  , {{ clean_string('raw_data:shipping_zipcode::STRING') }} AS shipping_zipcode
  , {{ clean_string('raw_data:shipping_city::STRING') }} AS shipping_city
  , {{ clean_string('raw_data:shipping_state::STRING') }} AS shipping_state
  , {{ clean_string('raw_data:country::STRING') }} AS country
  , {{ clean_string('raw_data:phone::STRING') }} AS phone
  , {{ clean_string('raw_data:address_rdi::STRING') }} AS address_rdi
  , TRY_TO_NUMBER({{ clean_string('raw_data:subscription_type::STRING') }}) AS subscription_type
  , TRY_TO_NUMBER({{ clean_string('raw_data:"100"::STRING') }}) AS nf_100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"200"::STRING') }}) AS nf_200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"300"::STRING') }}) AS nf_300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"400"::STRING') }}) AS nf_400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"500"::STRING') }}) AS nf_500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"600"::STRING') }}) AS nf_600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"700"::STRING') }}) AS nf_700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"800"::STRING') }}) AS nf_800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"900"::STRING') }}) AS nf_900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1000"::STRING') }}) AS nf_1000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1100"::STRING') }}) AS nf_1100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1200"::STRING') }}) AS nf_1200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1300"::STRING') }}) AS nf_1300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1400"::STRING') }}) AS nf_1400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1500"::STRING') }}) AS nf_1500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1600"::STRING') }}) AS nf_1600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1700"::STRING') }}) AS nf_1700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1800"::STRING') }}) AS nf_1800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"1900"::STRING') }}) AS nf_1900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2000"::STRING') }}) AS nf_2000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2100"::STRING') }}) AS nf_2100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2200"::STRING') }}) AS nf_2200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2300"::STRING') }}) AS nf_2300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2400"::STRING') }}) AS nf_2400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2500"::STRING') }}) AS nf_2500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2600"::STRING') }}) AS nf_2600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2700"::STRING') }}) AS nf_2700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2800"::STRING') }}) AS nf_2800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"2900"::STRING') }}) AS nf_2900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3000"::STRING') }}) AS nf_3000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3100"::STRING') }}) AS nf_3100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3200"::STRING') }}) AS nf_3200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3300"::STRING') }}) AS nf_3300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3400"::STRING') }}) AS nf_3400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3500"::STRING') }}) AS nf_3500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3600"::STRING') }}) AS nf_3600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3700"::STRING') }}) AS nf_3700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3800"::STRING') }}) AS nf_3800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"3900"::STRING') }}) AS nf_3900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4000"::STRING') }}) AS nf_4000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4100"::STRING') }}) AS nf_4100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4200"::STRING') }}) AS nf_4200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4300"::STRING') }}) AS nf_4300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4400"::STRING') }}) AS nf_4400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4500"::STRING') }}) AS nf_4500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4600"::STRING') }}) AS nf_4600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4700"::STRING') }}) AS nf_4700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4800"::STRING') }}) AS nf_4800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"4900"::STRING') }}) AS nf_4900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5000"::STRING') }}) AS nf_5000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5100"::STRING') }}) AS nf_5100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5200"::STRING') }}) AS nf_5200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5300"::STRING') }}) AS nf_5300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5400"::STRING') }}) AS nf_5400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5500"::STRING') }}) AS nf_5500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5600"::STRING') }}) AS nf_5600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5700"::STRING') }}) AS nf_5700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5800"::STRING') }}) AS nf_5800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"5900"::STRING') }}) AS nf_5900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6000"::STRING') }}) AS nf_6000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6100"::STRING') }}) AS nf_6100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6200"::STRING') }}) AS nf_6200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6300"::STRING') }}) AS nf_6300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6400"::STRING') }}) AS nf_6400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6500"::STRING') }}) AS nf_6500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6600"::STRING') }}) AS nf_6600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6700"::STRING') }}) AS nf_6700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6800"::STRING') }}) AS nf_6800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"6900"::STRING') }}) AS nf_6900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7000"::STRING') }}) AS nf_7000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7100"::STRING') }}) AS nf_7100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7200"::STRING') }}) AS nf_7200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7300"::STRING') }}) AS nf_7300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7400"::STRING') }}) AS nf_7400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7500"::STRING') }}) AS nf_7500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7600"::STRING') }}) AS nf_7600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7700"::STRING') }}) AS nf_7700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7800"::STRING') }}) AS nf_7800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"7900"::STRING') }}) AS nf_7900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8000"::STRING') }}) AS nf_8000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8100"::STRING') }}) AS nf_8100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8200"::STRING') }}) AS nf_8200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8300"::STRING') }}) AS nf_8300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8400"::STRING') }}) AS nf_8400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8500"::STRING') }}) AS nf_8500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8600"::STRING') }}) AS nf_8600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8700"::STRING') }}) AS nf_8700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8800"::STRING') }}) AS nf_8800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"8900"::STRING') }}) AS nf_8900
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9000"::STRING') }}) AS nf_9000
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9100"::STRING') }}) AS nf_9100
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9200"::STRING') }}) AS nf_9200
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9300"::STRING') }}) AS nf_9300
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9400"::STRING') }}) AS nf_9400
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9500"::STRING') }}) AS nf_9500
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9600"::STRING') }}) AS nf_9600
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9700"::STRING') }}) AS nf_9700
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9800"::STRING') }}) AS nf_9800
  , TRY_TO_NUMBER({{ clean_string('raw_data:"9900"::STRING') }}) AS nf_9900
  , CASE WHEN {{ clean_string('raw_data:new_food_customer_y_n::STRING') }} ILIKE 'Y'
        THEN TRUE
        WHEN {{ clean_string('raw_data:new_food_customer_y_n::STRING') }} ILIKE 'N'
        THEN FALSE 
   END AS new_food_customer_y_n
  , {{ clean_string('raw_data:orderfulfillmentid::STRING') }} AS orderfulfillmentid
  , {{ clean_string('raw_data:meal_selection::STRING') }} AS meal_selection
  , {{ date_unfucker(clean_string('raw_data:shipdate::STRING')) }} AS shipdate
  , {{ clean_string('raw_data:shipping_company::STRING') }} AS shipping_company
  , {{ clean_string('raw_data:shipping_service::STRING') }} AS shipping_service
  , {{ clean_string('raw_data:shipping_origin::STRING') }} AS shipping_origin
  , {{ clean_string('raw_data:internal_account_type::STRING') }} AS internal_account_type
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:internal_account_shipped::STRING') }}) AS internal_account_shipped
  , {{ clean_string('raw_data:insulation_type::STRING') }} AS insulation_type
  , {{ clean_string('raw_data:box_extras::STRING') }} AS box_extras
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:all_meals_autoselected::STRING') }}) AS all_meals_autoselected
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:do_not_replace::STRING') }}) AS do_not_replace
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:is_employee::STRING') }}) AS is_employee
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:production_employee::STRING') }}) AS production_employee
  , {{ clean_string('raw_data:facility::STRING') }} AS facility
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:influencer_free_order::STRING') }}) AS influencer_free_order
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:vip_user::STRING') }}) AS vip_user
FROM {{ source('chili', 'shipment_file') }} cf
INNER JOIN ranked_files rf 
ON cf.filename = rf.filename 
WHERE rf.file_rank = 1
