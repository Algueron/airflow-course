FROM apache/airflow:3.0.6

COPY my-sdk /opt/airflow/my-sdk

RUN pip install -e /opt/airflow/my-sdk
