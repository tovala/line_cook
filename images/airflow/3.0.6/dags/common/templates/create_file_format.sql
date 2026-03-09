USE SCHEMA {{ params.database }}.{{ params.schema }};

CREATE FILE FORMAT IF NOT EXISTS {{ params.file_format_name }}
  TYPE = {{ params.file_format }}
  {{ params.file_format_options }};