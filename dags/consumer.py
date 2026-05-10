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
    schedule=[file_a, file_b],
    max_active_runs=1
)
def consumer():

    @task
    def consume_file_a():
        pass

    consume_file_a()
        
consumer()
