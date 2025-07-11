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
start_zookeeper = BashOperator(
    task_id='start_zookeeper',
    bash_command='nohup /home/andy_68/tmp/kafka_2.13-3.8.1/bin/zookeeper-server-start.sh /home/andy_68/tmp/kafka_2.13-3.8.1/config/zookeeper.properties > zookeeper.out 2>&1 &',
    dag=dag,
)

start_kafka = BashOperator(
    task_id='start_kafka',
    bash_command='sleep 3 && nohup  /home/andy_68/tmp/kafka_2.13-3.8.1/bin/kafka-server-start.sh  /home/andy_68/tmp/kafka_2.13-3.8.1/config/server.properties > kafka.out 2>&1 &',
    dag=dag,
)

# Create DAG


def run_threads_crawl(**context):
    """Run threads crawling"""
    extract_threads_post_by_class("https://www.threads.com/search?q=disaster&serp_type=default")
    return "Crawling completed"

def run_kafka_stream(**context):
    """Run kafka streaming"""
    time.sleep(6)
    kafka_streaming()
    return "Kafka streaming completed"

def run_snowflake_process(**context):
    """Run snowflake processing"""
    # The snowflake_test.py will run when imported and executed
    snowflake_integration()
    return "Snowflake processing completed"

# Define tasks
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

# Set task dependencies
[start_zookeeper, start_kafka, crawl_task]
crawl_task >> [kafka_task, snowflake_task]