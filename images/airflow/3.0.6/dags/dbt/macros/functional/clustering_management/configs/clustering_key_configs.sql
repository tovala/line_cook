-- Returns a list of clustering configs that can be used to alter clustering keys
{%- macro clustering_key_configs() -%}
    {%- set config_string = '''
    {
        "clustering_keys": [
            {
                "schema": "season",
                "table": "refund_facts",
                "keys": ["cs_refund_time"]
            },
            {
                "schema": "season",
                "table": "customer_term_summary",
                "keys": ["cohort_start_date"]
            }
            ,
            {
                "schema": "grind",
                "table": "oven_events",
                "keys": ["updated"]
            }  
        ]
    }
    ''' -%}
    {{ return(fromjson(config_string)) }}
{%- endmacro -%}