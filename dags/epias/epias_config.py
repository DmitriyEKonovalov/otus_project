import os
from dotenv import load_dotenv
import psycopg2
import clickhouse_driver

load_dotenv()

# PostgreSQL connection parameters
PG_USER = os.getenv('POSTGRES_USER', "admin")
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', "")
PG_HOST = 'postgres'
PG_PORT = os.getenv('POSTGRES_PORT')
PG_DB = os.getenv('POSTGRES_DB')
PG_CONN = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB)

# ClickHouse connection parameters
CH_USER = os.getenv('CLICKHOUSE_USER')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CH_HOST = 'clickhouse'
CH_PORT = os.getenv('CLICKHOUSE_PORT')
CH_DB = os.getenv('CLICKHOUSE_DB')
CH_CONN = clickhouse_driver.Client(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD, database=CH_DB)


# настройки для получения данных из API
EPIAS_USERNAME = os.getenv('EPIAS_USERNAME')
EPIAS_PASSWORD = os.getenv('EPIAS_PASSWORD')
LOGIN_URL = r"https://giris.epias.com.tr/cas/v1/tickets"
LOGIN_HEADER = {
    "Content-Type": r"application/x-www-form-urlencoded",
    # "Accept": "application/json"
    "Accept": r"text/plain"
}

# настройки для получения данных
REQUEST_DATA_HEADER = {
    "Accept-Language": "EN",
    "Accept": r"application/json",
    "Content-Type": r"application/json",
    "TGT": None  # Добавить тикет в заголовок после авторизации
}

DEBUG = True
