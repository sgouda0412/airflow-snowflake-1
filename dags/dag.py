from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from smart_file_sensor import SmartFileSensor
import os
from datetime import timedelta
from airflow.utils.dates import days_ago
from datetime import datetime
import sys
import pandas as pd
import csv
from json import dumps
import logging
from pymongo import MongoClient
from airflow.utils.task_group import TaskGroup

dag_path = '/opt/airflow/dags' #os.getcwd()

def delete_symbols():
    df = pd.read_csv(f'{dag_path}/tiktok_google_play_reviews.csv')
    df.content = df.content.replace('(?:[^\w]+)|(?:[0-9])+', ' ', regex=True).str.strip()
    df.to_csv(f'{dag_path}/processed_data.csv')

def fill_nan():
    df = pd.read_csv(f'{dag_path}/processed_data.csv')
    df = df.fillna('-')
    df = df.replace(r'^\s*$', '-', regex=True)
    df.to_csv(f'{dag_path}/processed_data.csv')

def sort_vals():
    df = pd.read_csv(f'{dag_path}/processed_data.csv')
    df = df.sort_values(by='at')
    df.to_csv(f'{dag_path}/processed_data.csv')

def upload_data():
    connection = MongoClient("mongodb://root:root@18243ac5b65b") # explanation: mongodb://{login}:{password}{CONTAINER_NAME} change CONTAINER_NAME only
    logging.info("Connected to mongo db")

    df = pd.read_csv(f'{dag_path}/processed_data.csv')
    df.index = df.index.map(str)
    data = df.to_dict(orient='records')
    logging.info("Data read")

    db = connection['tiktok']
    collection = db['reviews']
    collection.insert_many(data)
    logging.info("data uploaded")

default_args = {
    'owner':'airflow',
    'start_date': days_ago(3) 
}
    
sensor = SmartFileSensor(
    task_id='file_sensor',
    poke_interval=30,
    # mode='reschedule', # to avoid deadlock
    filepath=f'{dag_path}/tiktok_google_play_reviews.csv',
    fs_conn_id="file_system"
)

with DAG(  
    dag_id="airflow_project",
    default_args=default_args,
    description='Data pipeline dag',
    doc_md='*DAG which reads data, transforms it and loads it into the db*',
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False,
) as dag:

    with TaskGroup(group_id='data_cleaning') as data_cleaning:
        task1 = PythonOperator(task_id='delete_symbols', python_callable=delete_symbols)
        task2 = PythonOperator(task_id='fill_nan', python_callable=fill_nan)
        task3 = PythonOperator(task_id='sort_vals', python_callable=sort_vals)
        task1 >> task2 >> task3

upload = PythonOperator(
    task_id='upload_data',
    python_callable=upload_data,
    dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "dag executed"',
    dag=dag,
)

sensor >> data_cleaning >> upload >> notify
