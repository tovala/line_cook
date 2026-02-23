#!/bin/sh

export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"

python3 -m venv "${DBT_VENV_PATH}"

${DBT_VENV_PATH}/bin/pip install dbt-snowflake

chmod +x ${AIRFLOW_HOME}/dags/direct_mail/scripts/txt_to_gz.py