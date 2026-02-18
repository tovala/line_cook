CREATE STAGE IF NOT EXISTS {{ params.database }}.{{ params.schema }}.{{ params.stage }} 
    URL = {{ params.s3_url }}
    -- Storage integration objects should only be created once within Snowflake. 
    -- Re-running/replacing them will require an update to AWS role Trust relationships > Trusted entities policy.
    STORAGE_INTEGRATION = {{ params.storage_integration }}
    FILE_FORMAT = ( TYPE = {{ params.file_format }} )
    COPY_OPTIONS = ( ON_ERROR = 'continue' );