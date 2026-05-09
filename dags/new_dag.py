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
        return {
            'input_a': 42,
            'input_b': 43
        }
    
    @task
    def transform_data(data):
        data['input_a'] = data['input_a'] * 2
        data['input_b'] = data['input_b'] * 3
        return data
    
    result = extract_data()
    transform_data(result)

new_dag()
