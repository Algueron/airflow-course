from airflow.sdk import dag, task
from datetime import datetime

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    tags=['new']
)
def my_dag():

    @task
    def fetch_bitcoin_price():
        import requests
        response = requests.get(API)
        return response.json()['bitcoin']['usd']
    
    @task.bash
    def print_bitcoin_price(bitcoin_price):
        return f"echo '{bitcoin_price}'"
    
    @task
    def add_bitcoin_cap(bitcoin_price):
        import requests
        response = requests.get(f"{API}&include_market_cap=true")
        market_cap = response.json()['bitcoin']['usd_market_cap']
        return {
            "market_cap": market_cap,
            "bitcoin_price": bitcoin_price
        }
    
    add_bitcoin_cap(print_bitcoin_price(fetch_bitcoin_price()))

my_dag()
