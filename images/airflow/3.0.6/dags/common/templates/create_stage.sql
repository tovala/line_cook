CREATE OR REPLACE STAGE {{ params.database }}.{{ params.schema }}.{{ params.stage }}
    URL = {{ params.s3_url }}
    -- Note: STORAGE_INTEGRATION objects themselves should only be created once.
    -- Re-running/replacing them requires updating the AWS role's trust relationships.
    -- This stage just references the existing integration, so recreating the stage is safe.
    STORAGE_INTEGRATION = {{ params.storage_integration }}
    FILE_FORMAT = ( {{ params.file_format }} )
    COPY_OPTIONS = ( ON_ERROR = 'continue' );