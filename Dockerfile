FROM apache/airflow:latest
# COPY requirements.txt /requirements.txt
# RUN pip install --requirement /requirements.txt # я в тильте: почему через requerements не работает, а просто RUN работает?
RUN pip install pandas
RUN pip install pymongo
RUN pip install apache-airflow-providers-mongo