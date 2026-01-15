-- Returns a dicitonary of generic types of permissions

{%- macro generic_access_configs() -%}
    {%- set config_string = 
    '{
        "create_table":
            [
                {
                "privilege": "CREATE TABLE",
                "resource_types": ["SCHEMA"]
                }
            ],
        "database_usage":
            [
                {
                "privilege": "USAGE",
                "resource_types": ["DATABASE"]
                }
            ],
        "full_database_read":
            [
                {
                "privilege": "USAGE",
                "resource_types": ["DATABASE", "ALL SCHEMAS IN DATABASE"]
                },
                {
                "privilege": "SELECT",
                "resource_types": ["ALL TABLES IN DATABASE", "ALL VIEWS IN DATABASE"]
                }
            ],
        "full_schema_create":
            [
                {
                "privilege": "CREATE STAGE",
                "resource_types": ["SCHEMA"]
                },
                {
                "privilege": "CREATE TABLE",
                "resource_types": ["SCHEMA"]
                },
                {
                "privilege": "CREATE VIEW",
                "resource_types": ["SCHEMA"]
                }
            ],
        "full_schema_read":
            [
                {
                "privilege": "USAGE",
                "resource_types": ["SCHEMA"]
                },
                {
                "privilege": "SELECT",
                "resource_types": ["ALL TABLES IN SCHEMA", "FUTURE TABLES IN SCHEMA"]
                }
            ],
        "full_schema_write":
            [
                {
                "privilege": "DELETE",
                "resource_types": ["ALL TABLES IN SCHEMA", "FUTURE TABLES IN SCHEMA"]
                },
                {
                "privilege": "INSERT",
                "resource_types": ["ALL TABLES IN SCHEMA", "FUTURE TABLES IN SCHEMA"]
                },
                {
                "privilege": "UPDATE",
                "resource_types": ["ALL TABLES IN SCHEMA", "FUTURE TABLES IN SCHEMA"]
                }
            ],
        "partial_schema_write":
            [
                {
                "privilege": "INSERT",
                "resource_types": ["ALL TABLES IN SCHEMA", "FUTURE TABLES IN SCHEMA"]
                }
            ]          
    }' -%}
    {{ return(fromjson(config_string)) }}
{%- endmacro -%}