CREATE OR REPLACE STAGE %(parent_database)s.%(schema_name)s.%(stage_name)s
    URL = %(url)s 
    -- Storage integration objects should only be created once within Snowflake. 
    -- Re-running/replacing them will require an update to AWS role Trust relationships > Trusted entities policy.
    STORAGE_INTEGRATION = %(storage_integration)s
    FILE_FORMAT = ( TYPE = %(file_type)s ) 
    COPY_OPTIONS = ( ON_ERROR = 'continue' );
