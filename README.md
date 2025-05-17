# crypto_etl_basic
In this basic project, we fetch data about a specific set of crypto such as Bitcoin, Ethereum and Solana and transform the data to approriate data, then save them to a database. ETL functions include:
# 1_Extract task:
+ Fetch data from Binance api
# 2_Transform task:
+ Select the data information that we need (e.g, lastPrice, openPrice, closePrice)
+ Create a DataFrame from the fetched JSON format data
+ Apply simple type transformation to the data (change str to int, datetime)
# 3_Load task:
+ Load the DataFrame into tables in Postgres database using the native function .toSQL()

# Additional requirements:
+ Main code usage for the pipeline: Python. Main libraries: pandas (for DataFrame interaction and transformation), requests( for interacting with API)
+ Read through the Binance API document
+ Install Airflow on Docker
+ Edit yaml file of Docker to include Postgres information for installation of PgAdmin
+ Create database in Postgres
+ Check for Docker container of Postgres and check for host I.P
+ Apply Postgres connection in Airflow
+ Upload DAG file to Airflow
+ Use of TaskAPI for simpler task flow, task definition and upstream/downstream of data
