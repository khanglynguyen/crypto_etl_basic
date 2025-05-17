import pendulum
import requests
import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# define the list of symbols you want to fetch
SYMBOLS = '["BTCUSDT","ETHUSDT","SOLUSDT"]'
BINANCE_API = "https://api.binance.com/api/v3/ticker/24hr"

# this DAG takes 24h price statistic
# instantiate the DAG
default_args = {
    'owner':'airflow',
}
@dag(
    default_args= default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,5,14, tz="UTC"),
    catchup=False,
    tags=['etl','crypto'],
)

def taskflow_api_etl():
    @task()
    def extract_data_24hr():
        params = {
            "symbols": SYMBOLS
        }

        response = requests.get(BINANCE_API, params=params)
        #response.raise_for_status()

        raw_data = response.json()
        return raw_data

    @task()
    def transform_data_24hr(raw_data):
    # get specific data
        get_key = ["symbol", "priceChange", "lastPrice", "openPrice",
            "highPrice", "lowPrice", "volume",
            "openTime", "closeTime"]
        trans_data_list = []

        for crypto in raw_data:
            # need to let the value be a list to avoid adding additional index for the dataframe
            get_raw_data = {key:[crypto[key]] for key in get_key if key in crypto}
            df = pd.DataFrame(data=get_raw_data, columns=get_key)

            # transform numeric
            numeric_Cols = ["priceChange", "lastPrice", "openPrice", "highPrice", "lowPrice", "volume"]
            for col in numeric_Cols:
                df[col] = pd.to_numeric(df[col])
            
            # transform date and time
            df["openTime"] = pd.to_datetime(df["openTime"], unit="ms")
            df["closeTime"] = pd.to_datetime(df["closeTime"], unit="ms")
            trans_data_list.append(df)

        return trans_data_list
    
    @task()
    def load_data_24hr(trans_data_list):
        for crypto in trans_data_list:
            # generate table name
            table_name = f"{crypto['symbol'].replace("0","").replace(" ","")}"

            # establish connection to load
            postgres_hook = PostgresHook(postgres_conn_id="crypto_connection")
            engine = postgres_hook.get_sqlalchemy_engine()

            crypto.to_sql(
                table_name,
                con=engine,
                if_exists="append",
                index=False
                #chunksize=1000
            )
    extracted_data = extract_data_24hr()
    transformed_data = transform_data_24hr(extracted_data)
    load_data_24hr(transformed_data)

crypto_etl_dag = taskflow_api_etl()
