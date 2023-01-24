FROM apache/airflow:latest
# COPY requirements.txt /requirements.txt
# RUN pip install --requirement /requirements.txt # я в тильте: почему через requerements не работает, а просто RUN работает?
RUN pip install dotenv
RUN pip install pandas
RUN pip install snowflake-connector-python[pandas]
RUN pip install snowflake-sqlalchemy
RUN pip install apache-airflow-providers-snowflake