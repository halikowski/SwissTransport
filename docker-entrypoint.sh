#!/usr/bin/env bash

# Make sure the script stops if any command fails
set -e

# Load environment variables from .env file
set -o allexport
source $AIRFLOW_HOME/.env
set -o allexport

# Create the SnowSQL config file, set account credentials
mkdir -p ~/.snowsql
cat <<EOF > ~/.snowsql/config
[connections]
accountname = $SNOWFLAKE_ACCOUNT
username = $SNOWFLAKE_USER
password = $SNOWFLAKE_PASSWORD
warehousename = $SNOWFLAKE_WAREHOUSE
dbname = $SNOWFLAKE_DATABASE
schemaname = $SNOWFLAKE_SCHEMA
rolename = $SNOWFLAKE_ROLE
EOF

# Export AIRFLOW_HOME
export AIRFLOW_HOME=/opt/airflow
export PYTHONPATH=$PYTHONPATH:/opt/airflow/scripts

# Initialize the database if not already initialized
if [ "$1" = "webserver" ]; then
    airflow db init
fi

# Execute the given command (either webserver or scheduler)
exec airflow "$@"
