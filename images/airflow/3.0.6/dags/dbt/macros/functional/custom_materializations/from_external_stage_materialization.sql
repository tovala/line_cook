-- NOTE: these are the sources referenced in creating this materialization
-- 1. Specific guide on loading from S3 using DBT http://mamykin.com/posts/fast-data-load-snowflake-dbt/
-- 2. Materialization documentation for DBT: https://docs.getdbt.com/docs/guides/creating-new-materializations/

{% macro external_stage(path='') %}
    @__STAGE_TOKEN__{{path}}
{% endmacro %}

{% macro ensure_external_stage(stage_name, s3_url, stage_file_format_string, temporary=False) %}
    {{ log('Making external stage: ' ~ [stage_name, s3_url, file_format, temporary] | join(', ')) }}
    CREATE OR REPLACE STAGE {{ 'TEMPORARY' if temporary }} {{ stage_name }}
        URL='{{ s3_url }}'
        CREDENTIALS=(aws_key_id='{{ env_var("SF_AWS_KEY") }}' aws_secret_key='{{ env_var("SF_AWS_SECRET") }}')
        FILE_FORMAT = {{ stage_file_format_string }};
    GRANT OWNERSHIP ON STAGE {{ stage_name }} TO ROLE ETL REVOKE CURRENT GRANTS;
{% endmacro %}

{% materialization from_external_stage, adapter='snowflake' -%}
    -- 1. GET CONFIGS - this determines which configs the materialization requires and which are optional.
    -- Required: schema, stage_url, stage_file_format_string
    -- Optional: alias, stage_name, copy_into_options, can_refresh
    {%- set identifier = model['alias'] -%}
    {%- set stage_name = config.get('stage_name', default=identifier ~ '_stage') -%}

    {%- set schema = config.require('schema') -%}
    {%- set stage_url = config.require('stage_url') -%}
    {%- set file_format_string = config.require('stage_file_format_string') -%}
    {%- set copy_into_options = config.get('copy_into_options', default='') -%}
    {%- set can_refresh = config.get('can_refresh', default=False) -%}

    -- 2. Set up.
    -- a. Set the schema - this is snowflake specific, a schema is required to create a stage even though the stage isn't created in a schema
    {%- call statement() -%}
        USE SCHEMA {{ schema }};
    {%- endcall -%}

    -- b. Create a stage if there isn't one already, o/w replace the stage (this appears to refresh the contents without actually making a new stage and triggering reloads)
    {%- call statement() -%}
        {{ ensure_external_stage(stage_name, stage_url, file_format_string, temporary=False) }}
    {%- endcall -%}

    -- c. Set 'old_relation' to the old version of this materialization and 'target_relation' to the new version
    -- full_refresh_mode - is it possible to refresh (can_refresh) and is the full refresh flag being passed 
    -- exists_as_table - table exists and is a table 
    -- should_drop - if full_refresh_mode OR doesn't exist yet
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}

    {%- set full_refresh_mode = (flags.FULL_REFRESH == True and can_refresh == True) -%}
    {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
    {%- set should_drop = (full_refresh_mode or not exists_as_table) -%}

    -- d. Drop old table IFF should_drop is set to true above
    {% if old_relation is none -%}
        -- noop
    {%- elif should_drop -%}
        {{ adapter.drop_relation(old_relation) }}
        {%- set old_relation = none -%}
    {%- endif %}

    -- 3. Build the table.
    -- a. Runs pre_hooks outside of commit
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- b. Run pre_hooks inside of commit
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    -- c. Ensure landing schema exists
    {%- call statement() -%}
        CREATE SCHEMA IF NOT EXISTS {{ schema }};
    {%- endcall -%}

    -- d. IF there is no table or we are in full_refresh_mode, build the table
    {% if full_refresh_mode or old_relation is none -%}
        -- Create an empty table with columns as specified in sql.
        -- We append a unique invocation_id to ensure no files are actually loaded, and an empty row set is returned,
        -- which serves as a template to create the table.
        {%- call statement() -%}
            CREATE OR REPLACE TABLE {{ target_relation }} AS (
                {{ sql | replace('__STAGE_TOKEN__', stage_name ~ '/' ~ invocation_id) }}
            )
        {%- endcall -%}
    {%- endif %}

    -- e. Perform the main load operation using COPY INTO 
    -- See https://docs.snowflake.net/manuals/user-guide/data-load-considerations-load.html 
    -- See https://docs.snowflake.net/manuals/user-guide/data-load-transform.html 
    {%- call statement('main') -%}
        -- TODO: Figure out how to deal with the ordering of columns changing in the model sql... 
        COPY INTO {{ target_relation }}
        FROM (
            {{ sql | replace('__STAGE_TOKEN__', stage_name)}}
        ) {{ copy_into_options }}
    {% endcall %}

    -- f. Run post_hooks within commit
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- g. `COMMIT` happens here
    {{ adapter.commit() }}

    -- h. Run post_hooks outside of commit
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    -- 4. Add to relations metadata
    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}