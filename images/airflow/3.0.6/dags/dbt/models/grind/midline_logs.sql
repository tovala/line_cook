{{ 
    config(
        materialized='incremental',
        unique_key='midline_log_id',
        tags=["thyme_incremental"]
    ) 
}}

WITH midline_test AS
(
  SELECT
    {{ hash_natural_key('raw_data::STRING')}} AS midline_log_id
    , {{ clean_string('raw_data:channel::STRING') }} AS channel
    , TRY_TO_NUMERIC({{ clean_string('raw_data:limitMax::STRING') }}, 10, 1) AS limitmax
    , TRY_TO_NUMERIC({{ clean_string('raw_data:limitMin::STRING') }}, 10, 1) AS limitmin
    , SPLIT_PART({{clean_string('raw_data:results::STRING')}}, '.', 2) AS results
    , MAX(
      CASE WHEN {{clean_string('raw_data:testName::STRING')}} ='Oven Result' 
           THEN SPLIT_PART({{clean_string('raw_data:results::STRING')}}, '.', 2) 
      END) OVER (PARTITION BY {{clean_string('raw_data:timestamp::STRING')}}) AS oven_results
    , CASE WHEN {{clean_string('raw_data:testName::STRING')}} <> 'Oven Result' 
           THEN SPLIT_PART({{clean_string('raw_data:results::STRING')}}, '.', 2) 
      END AS step_results
    , SPLIT_PART({{clean_string('raw_data:testCondition::STRING')}}, '.', 2) AS testcondition
    , REGEXP_REPLACE({{clean_string('raw_data:testName::STRING')}}, '[^\\x00-\\x7F]+', '') AS testname
    -- to remove non-English characters that is not belong to ASCII
    , CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP({{ clean_string('raw_data:timeStart::STRING') }})) AS timestart 
    , CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP({{ clean_string('raw_data:timeFinish::STRING') }})) AS timefinish
    , {{clean_string('raw_data:timestamp::STRING')}} AS unix_timestamp
    , PARSE_JSON(raw_data:samples) AS samples_json
    , updated
    , test_run_time
  FROM {{ source('chili', 'midline')}}
  {% if is_incremental() %} 
  WHERE updated > (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
), midline_samples AS
(
  SELECT 
    midline_log_id
    , channel
    , limitmax
    , limitmin
    , testcondition
    , testname
    , timestart
    , timefinish
    , step_results
    , COALESCE(oven_results, 'CANCELLED') AS oven_results
    -- for ovens being tested before 2/14/2024 will be nulls, need to mark as cancelled
    , unix_timestamp
    , samples_json
    , updated
    , test_run_time
    , f.index AS sample_index
    , f.value:timestamp::FLOAT AS sample_timestamp
    , flattened_measurements.value as sample_values
  FROM midline_test,
      LATERAL FLATTEN(input => PARSE_JSON(samples_json)) f,
      LATERAL FLATTEN(input => f.value:measurements) AS flattened_measurements
  WHERE f.index < 2 and step_results IS NOT NULL
  --we only want the top 2 samples and index starts from 0, here we can flat only index 0 and 1
)
SELECT  
  midline_log_id
  , channel
  , limitmax
  , limitmin
  , testcondition
  , testname
  , timestart
  , timefinish
  , step_results
  , oven_results
  , unix_timestamp
  , samples_json
  , updated
  , test_run_time
  {{ midline_samples([0, 1], ['voltage_channel_1', 'voltage_channel_2', 'current_channel_1', 'current_channel_2', 'power_channel_1', 'power_channel_2']) }}
FROM midline_samples
GROUP BY ALL
