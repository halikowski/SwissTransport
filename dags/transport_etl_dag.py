from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import json
import os
import sys
from snowflake.snowpark import Session
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Added file paths for unpredicted path errors which have once happened when trying to access 'scripts' folder
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/scripts')

# Import the combined task functions
from transport_etl_tasks import (
    download_and_process_file, ingest_file_to_snowflake, load_data_to_table, cleanup_file
)

# Import raw schema functions
from scripts.raw import (
    load_raw_transport, load_raw_line_data, load_raw_accessibility_1,
    load_raw_accessibility_2, load_raw_stop_data, load_raw_operators,
    load_raw_bike_parking_data, load_raw_transport_subtypes, load_raw_parking_data,
    load_raw_transport_types, load_raw_occupancy_data, load_raw_toilets
)

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

# DAG creation
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
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

# Establish Snowflake connection
hook = SnowflakeHook('snowflake_default')
conn = hook.get_conn()
sf_session = Session.builder.config("connection", conn).create()

# Function mapping for calling appropriate function, which name is specified in config.json file as type str
function_mapping = {
    "load_raw_transport": load_raw_transport,
    "load_raw_line_data": load_raw_line_data,
    "load_raw_accessibility_1": load_raw_accessibility_1,
    "load_raw_accessibility_2": load_raw_accessibility_2,
    "load_raw_stop_data": load_raw_stop_data,
    "load_raw_operators": load_raw_operators,
    "load_raw_bike_parking_data": load_raw_bike_parking_data,
    "load_raw_transport_subtypes": load_raw_transport_subtypes,
    "load_raw_parking_data": load_raw_parking_data,
    "load_raw_transport_types": load_raw_transport_types,
    "load_raw_occupancy_data": load_raw_occupancy_data,
    "load_raw_toilets": load_raw_toilets
}

# Create DAG tasks according to config.json file - separate tasks for each file & each frequency
with open('/opt/airflow/config/config.json', 'r') as f:
    config = json.load(f)

raw_tasks = []

for frequency, categories in config.items():
    dl_directory = categories['dl_directory']
    for category in categories['categories']:
        category_name = category['name']
        dataset_link = category['dataset_link']
        file_position = category['file_position']
        file_format = category['file_format']
        destination = category['destination']
        load_func_name = category['load_function']
        # Get function object using function_mapping dict
        load_func = function_mapping.get(load_func_name)
        dl_sub_folder = category['dl_sub_folder']
        # Set different download directory for each file to prevent filename retrieval errors when
        # running parallel downloads
        files_directory = os.path.join(dl_directory, dl_sub_folder)

        # Task for finding,downloading specified file from the Open Swiss Transport Data Website.
        # File format processing and minor changes are applied if necessary, depending on the file
        download_file_task = PythonOperator(
            task_id=f'download_{load_func_name}_{frequency}',
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

        # Sensor task waiting for the file to appear in download directory, using xcom to achieve this
        file_sensor_task = FileSensor(
            task_id=f'wait_for_{load_func_name}_{frequency}',
            filepath=os.path.join(files_directory,
                                  f'{{{{ task_instance.xcom_pull(task_ids="{download_file_task.task_id}") }}}}'),
            poke_interval=30,
            timeout=300,
            dag=dag,
            )

        # Ingestion task for putting data into Snowflake internal stage, using SnowSQL CLI.
        # Filename is captured from preceding tasks
        ingest_file_task = PythonOperator(
            task_id=f'ingest_{load_func_name}_{frequency}',
            python_callable=ingest_file_to_snowflake,
            op_kwargs={
                'filename': "{{ task_instance.xcom_pull(task_ids='" + download_file_task.task_id + "') }}",
                'files_directory': files_directory,
                'destination': destination
            },
            dag=dag,
        )

        # Task for loading data from interal stage to the raw schema table, using Snowpark
        # and separate load function for each ingested file
        load_data_task = PythonOperator(
            task_id=f'load_{load_func_name}_{frequency}',
            python_callable=load_data_to_table,
            op_kwargs={
                'sf_session': sf_session,
                'load_func': load_func,
                'destination': destination
            },
            dag=dag,
        )

        # Task for removing downloaded file from download directory once it's successfully
        # loaded to Snowflake & the preceding load task is completed
        cleanup_task = PythonOperator(
            task_id=f'cleanup_{load_func_name}_{frequency}',
            python_callable=cleanup_file,
            op_kwargs={
                'file_path': os.path.join(files_directory,
                                          f'{{{{ task_instance.xcom_pull('
                                          f'task_ids="{ingest_file_task.task_id}", key="filename") }}}}')
            },
            dag=dag,
        )

        # Set task dependencies
        download_file_task >> file_sensor_task >> ingest_file_task >> load_data_task >> cleanup_task
        raw_tasks.extend([download_file_task, file_sensor_task, ingest_file_task, load_data_task, cleanup_task])


curated_functions = [
    update_curated_transport, update_curated_accessibility, update_curated_vehicles,
    update_curated_transport_types, update_curated_line_data, update_curated_business,
    update_curated_operators, update_curated_stop_municipality, update_curated_occupancy,
    update_curated_parkings
]
# Curated tasks creation
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

consumption_functions = [
    update_transport_fact, update_transport_types_dim, update_operators_dim, update_parking_dim,
    update_vehicles_dim, update_lines_dim, update_stops_dim, update_accessibility_dim, update_occupancy_dim,
    update_municipality_dim, update_business_types_dim
]
# Consumption tasks creation
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


# Set dependencies for curated and consumption tasks
for task in curated_tasks:
    for raw_task in raw_tasks:
        raw_task >> task

for task in consumption_tasks:
    for curated_task in curated_tasks:
        curated_task >> task
