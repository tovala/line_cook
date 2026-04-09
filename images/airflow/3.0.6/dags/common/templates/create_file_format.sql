CREATE FILE FORMAT IF NOT EXISTS {{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}
  TYPE = {{ params.file_format }}
  {{ params.file_format_options }};