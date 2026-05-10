from airflow.sdk import dag, task, task_group
from datetime import datetime

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

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
        @task
        def fetch_bitcoin():
            import requests
            response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd")
            return response.json()['bitcoin']['usd']

        @task
        def fetch_ethereum():
            import requests
            response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd")
            return response.json()['ethereum']['usd']

        @task
        def fetch_solana():
            import requests
            response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd")
            return response.json()['solana']['usd']

        return {
            "btc": fetch_bitcoin(),
            "eth": fetch_ethereum(),
            "sol": fetch_solana()
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
