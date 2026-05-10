from airflow.sdk import Asset, dag, task
from pendulum import datetime

file_a = Asset(
    uri="file:///tmp/file_a.txt",
    name="file_a",
    extra={
        "description": "File A contains the data to be sent"
    }
)

file_b = Asset(
    uri="file:///tmp/file_b.txt",
    name="file_b",
    extra={
        "description": "File B contains the data to be sent"
    }
)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    max_active_runs=1
)
def sender():

    @task(outlets=[file_a])
    def my_task():
        raise Exception("This is a test error")

    @task(outlets=[file_b])
    def my_task_2():
        print("This is a test print")
    
    my_task()
    my_task_2()

sender()
