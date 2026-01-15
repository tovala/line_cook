-- For every specified resource and resource type combination, creates a GRANT or REVOKE statement of the following format:
    -- GRANT (PRIVILEGE) ON (RESOURCE TYPE) (NAME OF RESOURCE) TO (TYPE OF GRANTEE) (NAME OF GRANTEE) (GRANT OPTION)
    -- REVOKE (PRIVILEGE) ON (RESOURCE TYPE) (NAME OF RESOURCE) FROM (TYPE OF GRANTEE) (NAME OF GRANTEE) (REVOKE OPTION)
    -- See Snowflake documentation for possible variable values

{%- macro alter_access_on(privilege, resource_types, resources, grantee_type, grantee_name, optional_qualifier = '', grant_or_revoke = 'GRANT') -%}
    {%- if grant_or_revoke == 'GRANT' -%}
        {%- set preposition = 'TO' -%}
    {%- elif grant_or_revoke == 'REVOKE' -%}
        {%- set preposition = 'FROM' -%}
    {%- endif -%}
    {%- for r in resources -%}
        {%- for r_type in resource_types -%}
            {% set query %}
                {{grant_or_revoke}} {{privilege}} ON {{r_type}} {{r}} {{ preposition }} {{grantee_type}} {{grantee_name}} {{optional_qualifier}};
            {% endset %}
            {{ query }}
        {%- endfor -%}
    {%- endfor -%}
{%- endmacro -%}