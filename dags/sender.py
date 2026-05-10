from airflow.sdk import Asset, dag, task, AssetAlias, Metadata
from pendulum import datetime

alias_name = "alias_file_a"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    max_active_runs=1
)
def sender():

    @task(outlets=[AssetAlias(alias_name)])
    def create_file():
        path: str = "/tmp/file_a.txt"
        with open(path, "w") as f:
            f.write("This is a test print")
        yield Metadata(
            asset=Asset(
                name="file_a",
                uri=f"file://{path}",
                extra={
                    "description": "File contains the data to be sent"
                }
            ),
            alias=AssetAlias(alias_name)
        )
    
    create_file()

sender()
