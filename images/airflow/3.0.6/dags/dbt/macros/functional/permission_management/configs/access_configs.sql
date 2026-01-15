-- Returns a dictionary of all configurations specified in the macro
-- New permissions added to this file will automatically be granted in the next production dbt run

{%- macro access_configs() -%}
    {%- set config_string = 
    '[
        {
            "grantee_name": "ANALYST", 
            "grantee_type": "ROLE",
            "grants": 
            [
                {
                "resources": ["BRINE", "DRY", "GRIND", "MOLD", "SANDBOX", "SAGE", "SEASON", "WASH"],
                "generic_privilege_type": "full_schema_read"
                },
                {
                "resources": ["SANDBOX"],
                "generic_privilege_type": "create_table"
                }
            ]
        },
        {
            "grantee_name": "CUSTOMERIO_RETL", 
            "grantee_type": "ROLE",
            "grants": 
            [
                {
                "resources": ["CUSTOMERIO"],
                "generic_privilege_type": "full_schema_read"
                }
            ]
        },
        {
            "grantee_name": "ETL", 
            "grantee_type": "ROLE",
            "grants":
            [
                {
                "resources": ["MASALA"],
                "generic_privilege_type": "full_database_read"
                },
                {
                "resources": ["SANDBOX"],
                "generic_privilege_type": "create_table"
                }
            ]
        },
        {
            "grantee_name": "GLOBAL_CATALYST_SHARE", 
            "grantee_type": "SHARE",
            "grants": 
            [
                {
                "resources": ["MASALA"],
                "access_and_resource_types": 
                [
                    {
                    "privilege": "USAGE",
                    "resource_types": ["DATABASE"]
                    }
                ]
                },
                {
                "resources": ["FLORET"],
                "access_and_resource_types": 
                [
                    {
                    "privilege": "USAGE",
                    "resource_types": ["SCHEMA"]
                    },
                    {
                    "privilege": "SELECT",
                    "resource_types": ["ALL TABLES IN SCHEMA"]
                    }
                ]
                }
            ]
        },
        {
            "grantee_name": "LOOKER", 
            "grantee_type": "ROLE",
            "grants": 
            [
                {
                "resources": ["BASIL", "BRINE", "BURDOCK", "FLORET", "GRIND", "MOLD", "SAGE", "SANDBOX", "SEASON", "SIGMA_INPUT_TABLES"],
                "generic_privilege_type": "full_schema_read"
                },
                {
                "resources": ["SIGMA_INPUT_TABLES"],
                "generic_privilege_type": "full_schema_create"
                }
            ]
        },
        {
            "grantee_name": "RETOOL", 
            "grantee_type": "ROLE",
            "grants": 
            [
                {
                "resources": ["BRINE", "GRIND", "RETOOL", "SAGE", "SEASON", "WASH"],
                "generic_privilege_type": "full_schema_read"
                },
                {
                "resources": ["BRINE"],
                "generic_privilege_type": "full_schema_write"
                },
                {
                "resources": ["RETOOL"],
                "generic_privilege_type": "partial_schema_write"
                }
            ]
        },
        {
            "grantee_name": "SOFTWARE", 
            "grantee_type": "ROLE",
            "grants": 
            [
                {
                "resources": ["BRINE", "PANTRY"],
                "generic_privilege_type": "full_schema_read"
                }
            ]
        },
        {
            "grantee_name": "TECHNICAL", 
            "grantee_type": "ROLE",
            "grants":
            [
                {
                "resources": ["MASALA"],
                "generic_privilege_type": "full_database_read"
                },
                {
                "resources": ["SANDBOX"],
                "generic_privilege_type": "create_table"
                },
                {
                "resources": ["BRINE"],
                "access_and_resource_types": 
                [
                    {
                    "privilege": "INSERT",
                    "resource_types": ["ALL TABLES IN SCHEMA"]
                    },
                    {
                    "privilege": "TRUNCATE",
                    "resource_types": ["ALL TABLES IN SCHEMA"]
                    }
                ]
                },
                {
                "resources": ["BRINE.CSV"],
                "access_and_resource_types": 
                [
                    {
                    "privilege": "USAGE",
                    "resource_types": ["FILE FORMAT"]
                    }
                ]
                },
                {
                "resources": ["BRINE.CSV_SKIPHEADER"],
                "access_and_resource_types": 
                [
                    {
                    "privilege": "USAGE",
                    "resource_types": ["FILE FORMAT"]
                    }
                ]
                }
            ]
        }
    ]' -%}
    {{ return(fromjson(config_string)) }}
{%- endmacro -%}