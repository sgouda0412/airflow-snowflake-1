from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import os
from datetime import timedelta
from airflow.utils.dates import days_ago
from datetime import datetime
import sys
import pandas as pd
import csv
from json import dumps
import logging
#from pymongo import MongoClient

dag_path = '/opt/airflow/dags' #os.getcwd()

def data_cleaning():
    df = pd.read_csv(f'{dag_path}/tiktok_google_play_reviews.csv')
    df.fillna(value='-')
    df = df.sort_values(by='created_date')
    df.content = df.content.replace('(?:[^\w]+)|(?:[0-9])+', ' ', regex=True).str.strip()
    df.to_csv(f'{dag_path}/proccessed_data.csv')

# def upload_data():
#     connection = MongoClient("mongodb://localhost:27017/")
#     logging.info("Connected to mongo db read")

#     df = pd.read_csv(f'{dag_path}/proccessed_data.csv')
#     df.index = df.index.map(str)
#     data = df.to_dict(orient='records')
#     logging.info("Data read")

#     db = connection['tiktok']
#     collection = db['reviews']
#     collection.insert_many(data)
#     logging.info("data uploaded")


default_args = {
    'owner':'airflow',
    'start_date': days_ago(3) 
}

dag = DAG(  
    dag_id="airflow_project",
    default_args=default_args,
    description='Data pipeline dag',
    doc_md='*DAG which reads data, transforms it and loads it into the db*',
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False,
)
    
sensor = FileSensor(
    task_id='file_sensor',
    poke_interval=30,
    mode='reschedule', # to avoid deadlock
    filepath=f'{dag_path}/tiktok_google_play_reviews.csv'
)

clean = PythonOperator(
    task_id='clean_data',
    python_callable=data_cleaning,
    dag=dag
)

# upload = PythonOperator(
#     task_id='upload_data',
#     python_callable=upload_data,
#     dag=dag
# )

notify = BashOperator(
    task_id="notify",
    bash_command='echo "dag executed"',
    dag=dag,
)

sensor >> clean >> notify  #upload >> notify