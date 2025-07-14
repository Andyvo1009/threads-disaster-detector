from airflow import DAG

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys
import os
from airflow.providers.standard.operators.bash import BashOperator
import time
# Add project directory to Python path
sys.path.append('/home/andy_68/PythonProjects/big-data-project')

# Import your existing functions
from threads_crawl import extract_threads_post_by_class
from kafka_stream import kafka_streaming
from snowflake_intergration import snowflake_integration

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}
dag = DAG(
    dag_id='disaster_pipeline_daily',
    default_args=default_args,
    description='Daily disaster data pipeline',
    schedule='0 6 * * *',  # ✅ Replaced schedule_interval with schedule
    catchup=False,
    tags=['disaster', 'pipeline'],
)


# Python callable functions for the PythonOperators
def run_threads_crawl(**context):
    """Run threads crawling"""
    print("Starting Threads crawling...")
    extract_threads_post_by_class("https://www.threads.com/search?q=disaster&serp_type=default")
    print("Crawling completed.")
    return "Crawling completed"

def run_kafka_stream(**context):
    """Run kafka streaming"""
    # Removed time.sleep(6) - if Kafka is started by this DAG,
    # the start_kafka task should ensure it's ready.
    # If Kafka is external, this sleep is unnecessary.
    print("Starting Kafka streaming...")
    time.sleep(6)  # Ensure Kafka is ready before streaming
    kafka_streaming()
    print("Kafka streaming completed.")
    return "Kafka streaming completed"

def run_snowflake_process(**context):
    """Run snowflake processing"""
    print("Starting Snowflake processing...")
    # Ensure snowflake_integration is a function call.
    # If snowflake_test.py runs on import, refactor it to be a function.
    snowflake_integration()
    print("Snowflake processing completed.")
    return "Snowflake processing completed"

# Define Python tasks
crawl_task = PythonOperator(
    task_id='crawl_threads',
    python_callable=run_threads_crawl,
    dag=dag,
)

kafka_task = PythonOperator(
    task_id='kafka_stream',
    python_callable=run_kafka_stream,
    dag=dag,
)

snowflake_task = PythonOperator(
    task_id='snowflake_process',
    python_callable=run_snowflake_process,
    dag=dag,
)

# --- Set Task Dependencies (Corrected) ---
# Assuming Zookeeper must start before Kafka, and both must be up before crawling.
# Kafka streaming and Snowflake processing can run in parallel after crawling.


crawl_task >> [kafka_task, snowflake_task]