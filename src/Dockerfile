# FROM python:3.6-alpine3.7
FROM python:3.9

# RUN pip install numpy /usr/local/airflow/dags/
# RUN pip install pandas
# RUN pip install pymongo
COPY ../tiktok_google_play_reviews.csv /opt/airflow/
COPY ./requirements.txt .
RUN pip install --requirement requirements.txt
RUN pip install pymongo

# ADD requirements.txt .
# RUN pip install -r requirements.txt