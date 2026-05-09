from airflow.sdk import dag, task, Param
from datetime import datetime

@dag(
    dag_id="new_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    tags=["new"],
    params={
        "extra_input": Param(
            default=10,
            type="integer",
            description="This extra input is used to multiply input_a",
            section="Important Parameters",
            minimum=1,
            maximum=100
        )
    }
)
def new_dag():

    @task(multiple_outputs=True)
    def extract_data():
        return {
            'input_a': 42,
            'input_b': 43
        }
    
    @task
    def transform_a(input_a, params=None): # The None default value is just to indicate this is a built-in Airflow parameter
        extra_input = params['extra_input']
        return input_a * 2 * extra_input
    
    @task
    def transform_b(input_b):
        return input_b * 3
    
    values = extract_data()
    transform_a(values['input_a'])
    transform_b(values['input_b'])

new_dag()
