# Creating a new chili DAG

Loads files from S3 into Snowflake via an external stage. Reference: [chili_box_fillometer_dag.py](chili_box_fillometer_dag.py).

## 1. sous_chef — add the storage integration

In `sous_chef/data-infrastructure/sf_storage_integration.tf`:

```hcl
module "<name>_storage_integration" {
  source         = "./modules/snowflake_storage_integration_aws"
  prefix         = "<name>"  # → SF integration `<NAME>_STORAGE_INTEGRATION`
  s3_bucket_name = ...
  s3_read_prefix = ["<prefix>/*"]

  providers = { aws = aws.main }  # only for main-account buckets (e.g. misevala)
}
```

For data-account buckets (most `tovala-*` ones), pass `s3_bucket_name = module.<bucket>.bucket_name` and drop the `providers` block.

```
terraform apply -target=module.<name>_storage_integration
```

## 2. line_cook — create the DAG

Copy [chili_box_fillometer_dag.py](chili_box_fillometer_dag.py); update `dag_id`, `COLUMNS`, and `chili_params(...)`. Keep `schedule=None` — the parent `chili` DAG (cron `0 */3 * * *`, every 3 hours) triggers all chili child DAGs. Append the new `dag_id` to `CHILD_DAGS` in [chili_dag.py](chili_dag.py).

For Slack notifications, see [../experian/experian_chili_load_dag.py](../experian/experian_chili_load_dag.py).

- `file_format` is just the type name, defaults to `'JSON'` — usually omit
- For extra format options (e.g. `STRIP_OUTER_ARRAY = TRUE`), pass `file_format_name='<name>'` in `chili_params(...)` and `chiliLoad(file_format_options='<options>')`. This creates/uses a Snowflake-named file format object — see [chili_box_fillometer_dag.py](chili_box_fillometer_dag.py).
- `pattern` is matched bucket-relative; escape `.` with raw strings: `r'.../[0-9]+\.json'`

## 3. Validate

Trigger manually (the first run creates the destination table; use `full_refresh = true` later if you need to recreate it). Confirm with `SELECT COUNT(*) FROM MASALA.CHILI_V2.<table>`.

If COPY INTO processes records but loads 0, the stage's `ON_ERROR = 'continue'` is hiding errors — rerun manually with `ON_ERROR = 'ABORT_STATEMENT'` to surface them.

## Migrating from spice_rack

`spice_rack/models/chili/<table>.sql` `config(...)` maps to `chili_params`:

| spice_rack | line_cook |
|---|---|
| `stage_url` | `s3_url` |
| `stage_file_format_string` | `file_format` (just the TYPE); for extras use `file_format_name` + `chiliLoad(file_format_options=...)` |
| `stage_name` | `stage` (append `_stage`) |
| `copy_into_options` PATTERN | `pattern` (escape `.`) |
| SELECT block | `COLUMNS` |
| model filename | `table` |

Schema moves from `chili` → `CHILI_V2` (permanent). After the line_cook DAG is verified in prod:

1. Delete `spice_rack/models/chili/<table>.sql` and `<table>.yml`
2. Add `MASALA.CHILI_V2.<table>` as a dbt `source` (port the column tests from the original `.yml` so they keep running)
3. Update downstream consumers: `ref('<table>')` → `source('chili_v2', '<table>')`
