-- Returns a list of access configs that should be granted to every role

{%- macro global_access_configs() -%}
    {%- set config_string = 
    '{"global_grants" :
        [
            {
            "resources": ["MASALA"],
            "generic_privilege_type": "database_usage"
            }
        ]
    }' -%}
    {{ return(fromjson(config_string)) }}
{%- endmacro -%}