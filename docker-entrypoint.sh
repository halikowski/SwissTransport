#!/usr/bin/env bash

# Make sure the script stops if any command fails
set -e

# Export AIRFLOW_HOME
export AIRFLOW_HOME=/opt/airflow
export PYTHONPATH=$PYTHONPATH:/opt/airflow/scripts

# Initialize the database if not already initialized
if [ "$1" = "webserver" ]; then
    airflow db init
fi

# Execute the given command (either webserver or scheduler)
exec airflow "$@"
