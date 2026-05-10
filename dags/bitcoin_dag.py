from airflow.sdk import dag, task, task_group
from datetime import datetime


@task
def fetch_currency(currency: str):
    import requests
    response = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={currency}&vs_currencies=usd")
    return response.json()[currency]['usd']


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    tags=['new'],
    default_args={
        "retries": 2
    }
)
def my_dag():

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


    prices = fetch_crypto_prices()
    process_data(prices) >> final_report()


my_dag()
