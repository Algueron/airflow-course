from airflow.sdk import dag, task, task_group, setup, teardown
from datetime import datetime


@task
def fetch_currency(currency: str):
    import requests
    response = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={currency}&vs_currencies=usd")
    return response.json()[currency]['usd']


@task
def write_price(currency:str, price: float, workspace: str) -> str:
    import json
    from pathlib import Path
    out = Path(workspace) / f"{currency}.json"
    out.write_text(json.dumps({"currency": currency, "usd": price}))
    return str(out)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    tags=['new'],
    default_args={
        "retries": 2
    }
)
def crypto_dag():

    @setup
    def create_workspace(**context):
        import os
        from pathlib import Path

        base = Path(f"/tmp/airflow_workspaces") / context["dag"].dag_id / context["run_id"]
        os.makedirs(base, exist_ok=True)
        return str(base)
    

    @teardown(on_failure_fail_dagrun=True)
    def cleanup_workspace(path: str) -> None:
        import shutil
        shutil.rmtree(path, ignore_errors=True)


    @task_group(
        default_args={
            "pool": "fetch_crypto_prices"
        }
    )
    def fetch_crypto_prices():

        return {
            "btc": fetch_currency.override(task_id="fetch_bitcoin")(currency="bitcoin"),
            "eth": fetch_currency.override(task_id="fetch_ethereum", priority_weight=100)(currency="ethereum"),
            "sol": fetch_currency.override(task_id="fetch_solana")(currency="solana")
        }
    

    @task_group
    def write_prices(prices: dict, workspace: str):

        return {
            "path_btc": write_price.override(task_id="write_bitcoin")(currency="bitcoin", price=prices["btc"], workspace=workspace),
            "path_eth": write_price.override(task_id="write_ethereum")(currency="ethereum", price=prices["eth"], workspace=workspace),
            "path_sol": write_price.override(task_id="write_solana")(currency="solana", price=prices["sol"], workspace=workspace)
        }


    @task_group
    def process_data(prices):

        @task
        def calculate_average(prices):
            return (prices['btc'] + prices['eth'] + prices['sol']) / 3
        
        @task.bash
        def print_average(average):
            return f"echo 'The average price is {average}'"
        
        average = calculate_average(prices)
        print_average(average)


    @task
    def final_report():
        print("Crypto data pipeline completed successfully")


    ws = create_workspace()
    with cleanup_workspace(ws):
        prices = fetch_crypto_prices()
        writes = write_prices(prices, ws)
        processed = process_data(prices)
        [processed, *list(writes.values())] >> final_report()


crypto_dag()
