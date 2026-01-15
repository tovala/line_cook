
{% macro copy_into_s3(stage_name, filename_base, file_format_string, where_clause='1=1') %}
  COPY INTO @{{ stage_name }}/{{ filename_base }}
  FROM (
    SELECT OBJECT_CONSTRUCT(*) 
    FROM {{ this }} 
    WHERE {{ where_clause }}) 
  CREDENTIALS=(aws_key_id='{{ env_var("SF_AWS_KEY") }}' aws_secret_key='{{ env_var("SF_AWS_SECRET") }}')
  FILE_FORMAT = {{ file_format_string }};
{% endmacro %}

{% macro upload_to_s3(stage_name, s3_bucket, where_clause='1=1') %}

  {%- if target.name == 'prod' -%}
    {%- call statement() -%}
      USE SCHEMA {{ this.schema }};
    {%- endcall -%}
    -- s3_url = s3://tovala-data-engineering/table_offloads/{{table_name}}/{{table_name}}-{{timestamp}}.gz
    {%- set stage_url = s3_bucket + '/table_offloads/' + this.table -%}
    {%- set file_format_string = "(type = 'JSON' compression = GZIP)" -%}

    -- 1. Create the stage for loading data into S3 -- repurposed from the from_external_stage materialization definition
    {{ ensure_external_stage(stage_name, stage_url, file_format_string) }}

    -- 2. Copy into the S3 
    {%- set filename_base = this.table + '__' + run_started_at.strftime("%Y-%m-%d-%H:%M:%S") + '__' -%}
    {{ copy_into_s3(stage_name, filename_base, file_format_string, where_clause) }}
  {%- endif -%}

{% endmacro %}