from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
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

# DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

schedule_intervals = {
    'daily': '@daily',
    'weekly': '@weekly',
    'monthly': '@monthly',
    'yearly': '@yearly',
}


def create_dag(frequency: str, schedule_interval: str):
    """
    Function used for dynamic DAG generation.
    Instead of keeping every similar DAG in a separate file, they are generated with various frequencies
    and tasks in one file, according to the config parameters ('config.json' file).
    """

    dag_id = f'transport_data_etl_{frequency}'

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'ETL for SWISS_TRANSPORT database ({frequency})',
        schedule_interval=schedule_interval,
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )

    # Create Snowflake connection
    hook = SnowflakeHook('snowflake_default')  # 'snowflake_default' is the name of the connection created in Airflow
    conn = hook.get_conn()
    sf_session = Session.builder.config("connection", conn).create()

    # Function name mapping for further usage
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

    # Read config file to obtain parameters for each DAG
    with open('/opt/airflow/config/config.json', 'r') as f:
        config = json.load(f)

    categories = config[frequency]['categories']
    dl_directory = config[frequency]['dl_directory']

    # Loop over each website category, create separate tasks for each file
    for category in categories:
        category_name = category['name']
        dataset_link = category['dataset_link']
        file_position = category['file_position']
        file_format = category['file_format']
        destination = category['destination']
        load_func_name = category['load_function']
        load_func = function_mapping.get(load_func_name)
        dl_sub_folder = category['dl_sub_folder']
        files_directory = os.path.join(dl_directory, dl_sub_folder)

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

        file_sensor_task = FileSensor(
            task_id=f'wait_for_{load_func_name}_{frequency}',
            filepath=os.path.join(files_directory,
                                  f'{{{{ task_instance.xcom_pull(task_ids="{download_file_task.task_id}") }}}}'),
            poke_interval=30,
            timeout=300,
            dag=dag,
        )

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

    # Task for triggering the update_curated_consumption DAG right after the daily DAG runs successfully
    if frequency == 'daily':
        trigger_task = TriggerDagRunOperator(
            task_id='trigger_task',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            trigger_dag_id='update_curated_consumption',
            dag=dag
        )
    # Task dependencies
        download_file_task >> file_sensor_task >> ingest_file_task >> load_data_task >> cleanup_task >> trigger_task
    else:
        download_file_task >> file_sensor_task >> ingest_file_task >> load_data_task >> cleanup_task

    return dag

# Generate DAGs for each frequency - dynamic DAG generation
for frequency, schedule_interval in schedule_intervals.items():
    dag_id = f'transport_data_etl_{frequency}'
    globals()[dag_id] = create_dag(frequency, schedule_interval)