from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import json
import os
import sys

sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/scripts')

from scripts.utensils import get_snowpark_session

# Import the combined task functions
from transport_etl_tasks import (
    download_and_process_file, ingest_file_to_snowflake, load_data_to_table, cleanup_file
)
from scripts.curated import (
    update_curated_transport, update_curated_accessibility, update_curated_vehicles,
    update_curated_transport_types, update_curated_line_data, update_curated_business,
    update_curated_operators, update_curated_stop_municipality, update_curated_occupancy,
    update_curated_parkings
)
from scripts.consumption import (
    update_transport_fact, update_transport_types_dim, update_operators_dim, update_parking_dim,
    update_vehicles_dim, update_lines_dim, update_stops_dim, update_accessibilty_dim, update_occupancy_dim,
    update_municipality_dim, update_business_types_dim
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['email@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transport_data_etl',
    default_args=default_args,
    description='ETL for Swiss public transport data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

sf_session = get_snowpark_session()

with open('/opt/airflow/config/config.json', 'r') as f:
    config = json.load(f)

raw_tasks = []

for frequency, categories in config.items():
    files_directory = categories['files_directory']
    for category in categories['categories']:
        category_name = category['name']
        dataset_link = category['dataset_link']
        file_position = category['file_position']
        file_format = category['file_format']
        destination = category['destination']
        load_func = category['load_function']

        download_file_task = PythonOperator(
            task_id=f'download_{load_func}_{frequency}',
            python_callable=download_and_process_file,
            op_kwargs={
                'category_name': category_name,
                'dataset_link': dataset_link,
                'file_position': file_position,
                'file_format': file_format,
                'files_directory': files_directory
            },
            dag=dag,
        )

        file_sensor_task = FileSensor(
            task_id=f'wait_for_{load_func}_{frequency}',
            filepath=os.path.join(files_directory,
                                  f'{{{{ task_instance.xcom_pull(task_ids="{download_file_task.task_id}") }}}}'),
            poke_interval=30,
            timeout=300,
            dag=dag,
            )

        ingest_file_task = PythonOperator(
            task_id=f'ingest_{load_func}_{frequency}',
            python_callable=ingest_file_to_snowflake,
            op_kwargs={
                'filename': "{{ task_instance.xcom_pull(task_ids='" + download_file_task.task_id + "') }}",
                'files_directory': files_directory,
                'destination': destination
            },
            dag=dag,
        )

        load_data_task = PythonOperator(
            task_id=f'load_{load_func}_{frequency}',
            python_callable=load_data_to_table,
            op_kwargs={
                'sf_session': sf_session,
                'load_func': load_func,
                'destination': destination
            },
            dag=dag,
        )

        cleanup_task = PythonOperator(
            task_id=f'cleanup_{load_func}_{frequency}',
            python_callable=cleanup_file,
            op_kwargs={
                'file_path': os.path.join(files_directory,
                                          f'{{{{ task_instance.xcom_pull('
                                          f'task_ids="{ingest_file_task.task_id}", key="filename") }}}}')
            },
            dag=dag,
        )

        download_file_task >> file_sensor_task >> ingest_file_task >> load_data_task >> cleanup_task
        raw_tasks.extend([download_file_task, file_sensor_task, ingest_file_task, load_data_task, cleanup_task])

# Curated tasks
curated_functions = [
    update_curated_transport, update_curated_accessibility, update_curated_vehicles,
    update_curated_transport_types, update_curated_line_data, update_curated_business,
    update_curated_operators, update_curated_stop_municipality, update_curated_occupancy,
    update_curated_parkings
]
curated_tasks = []

for func in curated_functions:
    curated_tasks.append(
        PythonOperator(
            task_id=f'{func.__name__}',
            python_callable=func,
            op_kwargs={'session': sf_session},
            dag=dag,
        )
    )


# Consumption tasks
consumption_functions = [
    update_transport_fact, update_transport_types_dim, update_operators_dim, update_parking_dim,
    update_vehicles_dim, update_lines_dim, update_stops_dim, update_accessibilty_dim, update_occupancy_dim,
    update_municipality_dim, update_business_types_dim
]
consumption_tasks = []

for func in consumption_functions:
    consumption_tasks.append(
        PythonOperator(
            task_id=f'{func.__name__}',
            python_callable=func,
            op_kwargs={'session': sf_session},
            dag=dag,
        )
    )


# Dependencies for curated and consumption tasks
for task in curated_tasks:
    for raw_task in raw_tasks:
        raw_task >> task

for task in consumption_tasks:
    for curated_task in curated_tasks:
        curated_task >> task
