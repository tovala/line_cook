{% macro unique_cook_cycle_id() %}
    CASE WHEN raw_data:cookCycleID::STRING IS NOT NULL 
         THEN {{ hash_natural_key(clean_string('raw_data:cookCycleID::STRING'), "COALESCE(deviceid, '')", "COALESCE(sessionid, '')") }} 
    END AS unique_cook_cycle_id
{% endmacro %}