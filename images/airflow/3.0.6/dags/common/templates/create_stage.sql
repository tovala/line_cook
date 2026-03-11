CREATE STAGE IF NOT EXISTS {{ params.database }}.{{ params.schema }}.{{ params.stage }} 
    URL = {{ params.s3_url }}
    -- Storage integration objects should only be created once within Snowflake. 
    -- Re-running/replacing them will require an update to AWS role Trust relationships > Trusted entities policy.
    STORAGE_INTEGRATION = {{ params.storage_integration }}
    
    {% if params.file_format_name %}
    FILE_FORMAT = '{{ params.database}}.{{ params.schema }}.{{ params.file_format_name }}'
    {% else %}
    FILE_FORMAT = ( TYPE = {{ params.file_format }} )
    {% endif %}
    
    COPY_OPTIONS = ( ON_ERROR = 'continue' );