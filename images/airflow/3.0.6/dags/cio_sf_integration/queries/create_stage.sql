CREATE OR REPLACE STAGE {{ params.parent_database }}.{{ params.schema_name }}.{{ params.stage_name }} 
    URL = {{ params.url }}
    -- Storage integration objects should only be created once within Snowflake. 
    -- Re-running/replacing them will require an update to AWS role Trust relationships > Trusted entities policy.
    STORAGE_INTEGRATION = {{ params.storage_integration }}
    FILE_FORMAT = ( TYPE = {{ params.file_type }} )
    COPY_OPTIONS = ( ON_ERROR = 'continue' );
