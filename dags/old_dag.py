from airflow.models import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def extract_data(ti: TaskInstance):
    ti.xcom_push(key='data', value=42)

def transform_data(ti: TaskInstance):
    data = ti.xcom_pull(task_ids='task_a', key='data')
    ti.xcom_push(key='data', value=data * 2)

with DAG(dag_id='old_dag', start_date=datetime(2025, 1, 1), schedule="@daily", tags=['old']):

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=extract_data,
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=transform_data,
    )

    task_a >> task_b