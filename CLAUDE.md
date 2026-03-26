# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**line_cook** is Tovala Data Team's local development environment for Apache Airflow 3.0.6, mirroring AWS MWAA production. DAGs are developed and tested locally via Docker, then deployed to S3 via GitHub Actions on merge to `master`.

## Common Commands

All Docker commands run from `images/airflow/3.0.6/`.

```bash
# Start local MWAA environment (Airflow UI at localhost:8080)
cd images/airflow/3.0.6 && ./run.sh

# Test requirements installation
./run.sh test-requirements

# Test startup script
./run.sh test-startup-script

# Reset Airflow database
docker compose -f docker-compose-resetdb.yaml up

# Set up Python virtual environments for IDE support
python3 create_venvs.py --target development
python3 create_venvs.py --target development --recreate  # rebuild from scratch
```

### Linting & Type Checking

Dev tools are installed in the venv created by `create_venvs.py`:

```bash
ruff check .          # linting
ruff format .         # formatting
pyright               # type checking
pydocstyle            # docstring style
```

### Testing

```bash
pytest                        # run all tests
pytest tests/path/to/test.py  # run single test file
pytest -k "test_name"         # run specific test
```

## Architecture

### DAG Layer (`images/airflow/3.0.6/dags/`)

DAGs use the `@dag` decorator pattern and are organized by domain (e.g., `experian/`, `direct_mail/`, `user_deletes/`). Each domain directory typically contains:
- A main `*_dag.py` file
- Task group modules (`*_task_group.py`)
- Domain-specific config/variables
- SQL queries in a `queries/` subdirectory

**Shared utilities** live in `dags/common/`:
- **`slack_notifications.py`** — `bad_boy()` / `good_boy()` callbacks for failure/success Slack alerts; `slack_param()` for channel config. Used by virtually all DAGs.
- **`chili.py`** — Abstraction for Snowflake data loading. `chili_params()` generates standard parameters, `chili_macros()` provides SQL generation functions, `chiliLoad()` is a reusable task group for stage→check→insert/create flows.
- **`sql_operator_handlers.py`** — Handler functions passed to `SQLExecuteQueryOperator`: `fetch_single_result()`, `fetch_stupid_list()` (generates dummy list to control parallelism batch size), `fetch_results_array()`.
- **`dbt_cosmos_config.py`** / **`dbt_custom_operators.py`** — Centralized dbt project config and operator wrappers for the spice_rack dbt project.

**Import pattern**: DAGs import using module-relative paths from the `dags/` root (e.g., `from common.chili import chiliLoad`). This works because Docker mounts `dags/` to `/usr/local/airflow/dags/` which is on PYTHONPATH.

### MWAA Runtime (`images/airflow/3.0.6/python/mwaa/`)

The container runtime that mirrors AWS MWAA behavior:
- **`entrypoint.py`** — Single entry for all container commands (scheduler, worker, webserver, hybrid, migrate-db)
- **`execute_command.py`** — Routes commands to subprocess managers; scheduler runs dag-processor + triggerer as sub-processes
- **`config/`** — Environment setup, Airflow/Celery/DB/SQS configuration generation
- **`subprocess/`** — Process management with pluggable health check conditions (DB reachable, timeout, sidecar health, task monitoring)

### Docker Setup (`images/airflow/3.0.6/`)

- `build.sh` generates Dockerfiles from Jinja2 templates and builds multiple image variants
- `run.sh` detects container runtime (finch/podman/docker), manages Fernet key caching, and orchestrates builds + compose up
- Three-container architecture: scheduler (+ dag-processor + triggerer), Celery worker, webserver, plus Postgres and SQS

### Deployment

GitHub Actions (`.github/workflows/`) deploy on push to `master`:
- DAGs → `s3://tovala-airflow-prod/dags` (excluding `spice_rack/`)
- Requirements → `s3://tovala-airflow-prod/requirements.txt`
- Startup script → `s3://tovala-airflow-prod/startup.sh`

## Environment Variables

Required in `~/.zshrc` (see README for full list):
- `MWAA_LOCAL_DEV` — username identifier for local dev
- `TOVALA_DATA_REGION`, `TOVALA_DATA_AWS_ACCOUNT_ID`, `TOVALA_DATA_AWS_KEY`, `TOVALA_DATA_AWS_SECRET` — AWS credentials
- Python 3.12 must be on PATH

## Key Patterns

- DAG schedules always use `timezone='America/Chicago'` in `CronTriggerTimetable`
- DAGs use `@task_group` for logical grouping; task groups return dicts with outputs for downstream consumption
- Dynamic parallelism uses `.partial().expand()` or `.expand_kwargs()` with controlled cardinality via `fetch_stupid_list()`
- SQL templating uses `template_searchpath` pointing to `dags/common/templates/` with `user_defined_macros` for Jinja2 injection
- Triggered DAGs (schedule=None) are invoked via `TriggerDagRunOperator` from parent DAGs
- **Never call `Variable.get()` at DAG function body level** — the `@dag` function body runs on every scheduler parse cycle (~30s), so `Variable.get()` there causes repeated DB queries. Instead, call `Variable.get()` inside `@task` functions (runtime only), or use Jinja `{{ var.value.variable_name }}` in operator parameters
- Cross-account AWS access uses an Airflow connection (`aws_main_account`) with `assume_role` to the main Tovala account; all AWS hooks/operators reference it via `aws_conn_id`
