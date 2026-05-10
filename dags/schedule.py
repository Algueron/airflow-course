from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True
)
def schedule():

    @task
    def my_task(data_interval_start: datetime = None, data_interval_end: datetime = None, logical_date: datetime = None):
        print(f"data_interval_start: {data_interval_start}")
        print(f"data_interval_end: {data_interval_end}")
        print(f"logical_date: {logical_date}")
    
    my_task()

schedule()
