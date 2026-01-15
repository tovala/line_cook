-- Grants all permissions specified in the configs/access_configs.sql file 

{%- macro run_permission_grants() -%}
    -- Step 1: Remove access to PUBLIC role
    {{ revoke_public_access() }}
    -- Step 2: Ensure schemas managed by dbt exist
    {{ verify_schemas() }}
    -- Step 3: Fetch access configurations from file
    {%- set config_dict = access_configs() -%}
    -- Step 4: Fetch generic access configurations from file
    {%- set generic_configs = generic_access_configs() -%}
    -- Step 5: Fetch global access grants from file
    {%- set global_configs = global_access_configs() -%}
    -- Step 6: Iterate and grants permissions accordingly
    {%- for grantee in config_dict -%}
        {%- set all_grants = grantee['grants'] + global_configs['global_grants'] -%}
        {%- for grant in all_grants-%}
            {%- if grant['generic_privilege_type'] -%}
                {%- set access_read_dict = generic_configs[grant['generic_privilege_type']]-%}
            {% else %}
                {%- set access_read_dict = grant['access_and_resource_types'] -%}
            {% endif %}
            {%- for ar_type in access_read_dict -%}
                {{ alter_access_on(ar_type['privilege'], ar_type['resource_types'], grant['resources'], grantee['grantee_type'], grantee['grantee_name'])}}
            {%- endfor -%}
        {%- endfor -%}
    {%- endfor -%}
{%- endmacro -%}