{%- macro load_incrementally_supermetrics(refresh_cadence) -%}
-- Supermetrics transfer have a refresh cadence (<=30 days), this reloads all refreshed days plus a buffer
{% if is_incremental() %}
WHERE report_date >= (SELECT DATEADD('day', FLOOR(-{{ refresh_cadence }}*1.2), MAX(report_date)) FROM {{this}} )
  -- In case supermetrics is backfilled via the UI
  OR report_date < (SELECT MIN(report_date) FROM {{this}} )
{%- endif -%}
{%- endmacro -%}