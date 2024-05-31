from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import sys
from snowflake.snowpark import Session
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/scripts')

# Import curated schema functions
from scripts.curated import (
    update_curated_transport, update_curated_accessibility, update_curated_vehicles,
    update_curated_transport_types, update_curated_line_data, update_curated_business,
    update_curated_operators, update_curated_stop_municipality, update_curated_occupancy,
    update_curated_parkings
)

# Import consumption schema functions
from scripts.consumption import (
    update_transport_fact, update_transport_types_dim, update_operators_dim, update_parking_dim,
    update_vehicles_dim, update_lines_dim, update_stops_dim, update_accessibility_dim, update_occupancy_dim,
    update_municipality_dim, update_business_types_dim
)

# Setup Snowflake connection
hook = SnowflakeHook('snowflake_default')  # 'snowflake_default' is the name of the connection created in Airflow
conn = hook.get_conn()
sf_session = Session.builder.config("connection", conn).create()

# DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_curated_consumption',
    default_args=default_args,
    description='Curated and Consumption data update for SWISS_TRANSPORT database',
    schedule_interval=None,  # Triggered by completion of daily DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

curated_functions = [
    update_curated_transport, update_curated_accessibility, update_curated_vehicles,
    update_curated_transport_types, update_curated_line_data, update_curated_business,
    update_curated_operators, update_curated_stop_municipality, update_curated_occupancy,
    update_curated_parkings
]
curated_tasks = []

# Create task for each curated schema function
for func in curated_functions:
    curated_tasks.append(
        PythonOperator(
            task_id=f'{func.__name__}',
            python_callable=func,
            op_kwargs={'session': sf_session},
            dag=dag,
        )
    )

consumption_functions = [
    update_transport_fact, update_transport_types_dim, update_operators_dim, update_parking_dim,
    update_vehicles_dim, update_lines_dim, update_stops_dim, update_accessibility_dim, update_occupancy_dim,
    update_municipality_dim, update_business_types_dim
]
consumption_tasks = []

# Create task for each consumption schema function
for func in consumption_functions:
    consumption_tasks.append(
        PythonOperator(
            task_id=f'{func.__name__}',
            python_callable=func,
            op_kwargs={'session': sf_session},
            dag=dag,
        )
    )

# Create Task groups
curated_group = TaskGroup(group_id='curated_tasks', dag=dag)
consumption_group = TaskGroup(group_id='consumption_tasks', dag=dag)

# Task dependencies
with curated_group:
    for task in curated_tasks:
        task

with consumption_group:
    for curated_task in curated_tasks:
        for task in consumption_tasks:
            curated_task >> task
