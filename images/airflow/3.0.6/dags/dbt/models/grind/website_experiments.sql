SELECT
  {{ hash_natural_key('s.session_id', 'fe.evt_experiment_id_str') }} AS website_experiment_key
  , s.session_id
  , s.fullstory_user_id
  , fe.evt_experiment_id_str AS experiment_id
  , fe.evt_variation_name_str AS variant_name
   , ARRAY_SIZE(SPLIT(variant_name, '|'))>=5 AS has_variant_name_parts
  , CASE WHEN has_variant_name_parts
        THEN SPLIT_PART(variant_name, '|', 1)
        END AS variant_type
  , CASE WHEN has_variant_name_parts
        THEN SPLIT_PART(variant_name, '|', 2)
        END AS variant_price
  , CASE WHEN has_variant_name_parts
        THEN SPLIT_PART(variant_name, '|', 3)
        END AS variant_page
  , CASE WHEN has_variant_name_parts
        THEN SPLIT_PART(variant_name, '|', 4)
        END AS variant_target_audience
  , CASE WHEN has_variant_name_parts
        THEN SPLIT_PART(variant_name, '|', 5)
        END AS variant_extra_information
  , fe.evt_experiment_name_str AS experiment_name
  , ARRAY_SIZE(SPLIT(experiment_name, '|'))>=6 AS has_experiment_name_parts
  , CASE WHEN has_experiment_name_parts
        THEN SPLIT_PART(experiment_name, '|', 1)
        END AS experiment_start_date
  , CASE WHEN has_experiment_name_parts
        THEN SPLIT_PART(experiment_name, '|', 2) 
        END AS experiment_type
  , CASE WHEN has_experiment_name_parts
        THEN SPLIT_PART(experiment_name, '|', 3)
        END AS experiment_page
  , CASE WHEN has_experiment_name_parts
        THEN SPLIT_PART(experiment_name, '|', 4)
        END AS experiment_channel
  , CASE WHEN has_experiment_name_parts
        THEN SPLIT_PART(experiment_name, '|', 5)
        END AS experiment_test_type
  , CASE WHEN has_experiment_name_parts
        THEN SPLIT_PART(experiment_name, '|', 6)
        END AS experiment_extra_information
  , MIN(fe.eventstart) AS first_view_time
FROM {{ table_reference('fullstory_events') }} fe
INNER JOIN {{ ref('sessions') }} s
  ON fe.sessionid = s.session_id
WHERE fe.eventcustomname = 'VWO'
GROUP BY website_experiment_key, s.session_id, s.fullstory_user_id, fe.evt_experiment_id_str, fe.evt_variation_name_str, fe.evt_experiment_name_str
--Remove any sessions that saw more than one variant. As of 3/19/24, only 6 had seen multiple (< 0.002%)
QUALIFY COUNT(variant_name) OVER (PARTITION BY s.session_id, fe.evt_experiment_id_str ORDER BY s.session_start_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 1