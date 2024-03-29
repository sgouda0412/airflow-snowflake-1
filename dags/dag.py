from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from smart_file_sensor import SmartFileSensor
from datetime import timedelta
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
from json import dumps
import os
import logging
from airflow.utils.task_group import TaskGroup
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from dotenv import load_dotenv
load_dotenv()

dag_path = '/opt/airflow/dags' #os.getcwd()

password = os.getenv("PASSWORD")
user = os.getenv("USER")
account = os.getenv("ACCOUNT")

def connect_to_snowflake():
    con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
)

def create_tables_and_streams():
    con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse='MY_WH',
    database='MY_DB',
    schema='MY_SCHEMA'
)
    cursor = con.cursor()
    cursor.execute('CREATE OR REPLACE TABLE RAW_TABLE(_id VARCHAR(), IOS_App_Id INT, Title VARCHAR(), Developer_Name VARCHAR(), Developer_IOS_Id FLOAT, IOS_Store_Url VARCHAR(), Seller_Official_Website VARCHAR(), Age_Rating VARCHAR(), Total_Average_Rating VARCHAR(), Total_Number_of_Ratings VARCHAR(), Average_Rating_For_Version VARCHAR(), Number_of_Ratings_For_Version VARCHAR(), Original_Release_Date VARCHAR(), Current_Version_Release_Date VARCHAR(), Price_USD DECIMAL, Primary_Genre VARCHAR(), All_Genres VARCHAR(), Languages VARCHAR(), Description VARCHAR());')
    cursor.execute('CREATE OR REPLACE TABLE STAGE_TABLE(_id VARCHAR(), IOS_App_Id INT, Title VARCHAR(), Developer_Name VARCHAR(), Developer_IOS_Id FLOAT, IOS_Store_Url VARCHAR(), Seller_Official_Website VARCHAR(), Age_Rating VARCHAR(), Total_Average_Rating VARCHAR(), Total_Number_of_Ratings VARCHAR(), Average_Rating_For_Version VARCHAR(), Number_of_Ratings_For_Version VARCHAR(), Original_Release_Date VARCHAR(), Current_Version_Release_Date VARCHAR(), Price_USD DECIMAL, Primary_Genre VARCHAR(), All_Genres VARCHAR(), Languages VARCHAR(), Description VARCHAR());')
    cursor.execute('CREATE OR REPLACE STREAM RAW_STREAM ON TABLE RAW_TABLE;')
    cursor.execute('CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE STAGE_TABLE;')
    cursor.close()

def upload_data_to_raw():
    df = pd.read_csv(f'{dag_path}/763K_plus_IOS_Apps_Info.csv', index_col=False) # you can use demo.csv for test which is essentially just 100 first rows of the original data 
    df.reset_index(inplace=True, drop=True)
    df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse='MY_WH',
    database='MY_DB',
    schema='MY_SCHEMA'
)
    success, num_chunks, num_rows, output = write_pandas(con, df, table_name="RAW_TABLE", quote_identifiers=False)
    print(str(success) + ', ' + str(num_chunks) + ', ' + str(num_rows))
    con.close()

def raw_stream_into_stage_table():
    con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse='MY_WH',
    database='MY_DB',
    schema='MY_SCHEMA'
)
    cursor = con.cursor()
    cursor.execute('''INSERT INTO STAGE_TABLE(_id, IOS_App_Id, Title, Developer_Name, Developer_IOS_Id, IOS_Store_Url, Seller_Official_Website, Age_Rating, Total_Average_Rating, Total_Number_of_Ratings, Average_Rating_For_Version, Number_of_Ratings_For_Version, Original_Release_Date, Current_Version_Release_Date, Price_USD, Primary_Genre, All_Genres, Languages, Description)
                    SELECT _id, IOS_App_Id, Title, Developer_Name, Developer_IOS_Id, IOS_Store_Url, Seller_Official_Website, Age_Rating, Total_Average_Rating, Total_Number_of_Ratings, Average_Rating_For_Version, Number_of_Ratings_For_Version, Original_Release_Date, Current_Version_Release_Date, Price_USD, Primary_Genre, All_Genres, Languages, Description
                    FROM RAW_TABLE;''')
    cursor.close()

def stage_stream_into_master_table():
    con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse='MY_WH',
    database='MY_DB',
    schema='MY_SCHEMA'
)
    cursor = con.cursor()
    cursor.execute('CREATE OR REPLACE TABLE MASTER_TABLE(_id VARCHAR(), IOS_App_Id INT, Title VARCHAR(), Developer_Name VARCHAR(), Developer_IOS_Id FLOAT, IOS_Store_Url VARCHAR(), Seller_Official_Website VARCHAR(), Age_Rating VARCHAR(), Total_Average_Rating VARCHAR(), Total_Number_of_Ratings VARCHAR(), Average_Rating_For_Version VARCHAR(), Number_of_Ratings_For_Version VARCHAR(), Original_Release_Date VARCHAR(), Current_Version_Release_Date VARCHAR(), Price_USD DECIMAL, Primary_Genre VARCHAR(), All_Genres VARCHAR(), Languages VARCHAR(), Description VARCHAR());')

    cursor.execute('''INSERT INTO MASTER_TABLE(_id, IOS_App_Id, Title, Developer_Name, Developer_IOS_Id, IOS_Store_Url, Seller_Official_Website, Age_Rating, Total_Average_Rating, Total_Number_of_Ratings, Average_Rating_For_Version, Number_of_Ratings_For_Version, Original_Release_Date, Current_Version_Release_Date, Price_USD, Primary_Genre, All_Genres, Languages, Description)
                    SELECT _id, IOS_App_Id, Title, Developer_Name, Developer_IOS_Id, IOS_Store_Url, Seller_Official_Website, Age_Rating, Total_Average_Rating, Total_Number_of_Ratings, Average_Rating_For_Version, Number_of_Ratings_For_Version, Original_Release_Date, Current_Version_Release_Date, Price_USD, Primary_Genre, All_Genres, Languages, Description
                    FROM STAGE_STREAM;''')
    cursor.close()

default_args = {
    'owner':'airflow',
    'start_date': days_ago(3) 
}
    
sensor = SmartFileSensor(
    task_id='file_sensor',
    poke_interval=30,
    filepath=f'{dag_path}/763K_plus_IOS_Apps_Info.csv',
    fs_conn_id="file_system"
)

with DAG(  
    dag_id="airflow_project",
    default_args=default_args,
    description='Data pipeline dag',
    doc_md='*DAG which reads data, transforms it and uploads it to snowflake*',
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False,
) as dag:

    with TaskGroup(group_id='data_uploading') as data_cleaning:
        task1 = PythonOperator(task_id='check_connection_to_snowflake', python_callable=connect_to_snowflake)
        task2 = PythonOperator(task_id='create_tables_and_streams', python_callable=create_tables_and_streams)
        task3 = PythonOperator(task_id='upload_data_to_raw', python_callable=upload_data_to_raw)
        task4 = PythonOperator(task_id='raw_stream_into_stage_table', python_callable=raw_stream_into_stage_table)
        task5 = PythonOperator(task_id='stage_stream_into_master_table', python_callable=stage_stream_into_master_table)

        task1 >> task2 >> task3 >> task4 >> task5

notify = BashOperator(
    task_id="notify",
    bash_command='echo "dag executed"',
    dag=dag,
)

sensor >> data_cleaning >> notify