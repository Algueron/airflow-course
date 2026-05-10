from airflow.sdk import Asset, dag, task, AssetAlias
from pendulum import datetime

alias_name = "alias_file_a"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[AssetAlias(alias_name)],
    max_active_runs=1
)
def consumer():

    @task
    def consume_file_a():
        pass

    consume_file_a()
        
consumer()
