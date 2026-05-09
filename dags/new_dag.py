from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="new_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    tags=["new"]
)
def new_dag():

    @task
    def extract_data():
        return 42
    
    @task
    def transform_data(data):
        return data * 2
    
    result = extract_data()
    transform_data(result)

new_dag()
