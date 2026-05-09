from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="new_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    tags=["new"]
)
def new_dag():

    @task(multiple_outputs=True)
    def extract_data():
        return {
            'input_a': 42,
            'input_b': 43
        }
    
    @task
    def transform_a(input_a):
        return input_a * 2
    
    @task
    def transform_b(input_b):
        return input_b * 3
    
    values = extract_data()
    transform_a(values['input_a'])
    transform_b(values['input_b'])

new_dag()
