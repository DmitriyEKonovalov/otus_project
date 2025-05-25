from contextlib import closing
from psycopg2.extras import execute_batch
import requests
from datetime import datetime as dt, timedelta
import pytz

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from epias.epias_config import PG_CONN, CH_CONN
from epias.epias_config import LOGIN_URL, LOGIN_HEADER, EPIAS_USERNAME, EPIAS_PASSWORD, REQUEST_DATA_HEADER
from epias.epias_config import DEBUG

from epias.epias_common import login_to_epias
from epias.epias_common import get_epias_data
from epias.epias_common import create_pg_table_if_not_exists
from epias.epias_common import check_table_is_empty
from epias.epias_common import insert_to_pg_table
from epias.epias_common import create_ch_table
from epias.epias_common import clear_ch_table
from epias.epias_common import get_datamart_data_from_pg
from epias.epias_common import insert_to_ch_table
from epias.epias_common import ValidationModelT

from epias.epias_gas_prices import GRP_URL, GRP_TABLE, GRP_SCHEMA, GRP_DDL, EpiasGasPriceModel
from epias.epias_gas_trade_volume import GTV_URL, GTV_SCHEMA, GTV_TABLE, GTV_DDL, EpiasGasTradeVolumeModel
from epias.epias_mart import MART_SCHEMA, MART_TABLE, DDL_MART, BUILD_MART_SQL


def login_epias(**context):
    tgt = login_to_epias(url=LOGIN_URL,  login=EPIAS_USERNAME, password=EPIAS_PASSWORD, headers=LOGIN_HEADER)
    return tgt


def get_epias_prices(**context):
    """Получение и сохраннение данных из раздела GAS REFERENCE PRICE"""
    ti = context['ti']
    tgt = ti.xcom_pull(task_ids='login_epias')
    REQUEST_DATA_HEADER["TGT"] = tgt

    # Проверяем целевую таблицу для GRP
    create_pg_table_if_not_exists(GRP_DDL, PG_CONN)

    # определяем интервал, за который нужно получить данные, если таблица пустая, загружаем данные за 360 дней
    # и формируем заголовок запроса
    dag_start_date = context['data_interval_start']
    dag_end_date = context['data_interval_end']

    is_empty = check_table_is_empty(GRP_SCHEMA, GRP_TABLE, PG_CONN)
    start_date = dag_start_date - timedelta(days=360) if is_empty or DEBUG else dag_start_date
    end_date = dag_end_date

    # body = {
    #     "startDate": pytz.timezone("Europe/Moscow").localize(start_date).replace(microsecond=0).isoformat(),
    #     'endDate': pytz.timezone("Europe/Moscow").localize(end_date).replace(microsecond=0).isoformat(),
    # }
    body = {
        "startDate": start_date.replace(microsecond=0).isoformat(),
        'endDate': end_date.replace(microsecond=0).isoformat(),
    }

    print(f"Получаю данные в интервале {start_date} - {end_date}")
    data = get_epias_data(url=GRP_URL, headers=REQUEST_DATA_HEADER, params={}, body=body, validation_class=EpiasGasPriceModel)

    print(f"Cохраняю данные в таблицу {GRP_SCHEMA}.{GRP_TABLE}")
    insert_to_pg_table(data=data, schema=GRP_SCHEMA, table=GRP_TABLE, conn=PG_CONN)


def get_epias_volumes(**context):
    """Получение и сохраннение данных из раздела GAS TRADE VOLUMES"""
    ti = context['ti']
    tgt = ti.xcom_pull(task_ids='login_epias')
    REQUEST_DATA_HEADER["TGT"] = tgt

    # определяем интервал, за который нужно получить данные, если таблица пустая, загружаем данные за 360 дней
    # и формируем заголовок запроса
    dag_start_date = context['data_interval_start']
    dag_end_date = context['data_interval_end']

    create_pg_table_if_not_exists(GTV_DDL, PG_CONN)
    is_empty = check_table_is_empty(GTV_SCHEMA, GTV_TABLE, PG_CONN)

    start_date = dag_start_date - timedelta(days=360) if is_empty or DEBUG else dag_start_date
    end_date = dag_end_date

    # body = {
    #     "startDate": pytz.timezone("Europe/Moscow").localize(start_date).replace(microsecond=0).isoformat(),
    #     'endDate': pytz.timezone("Europe/Moscow").localize(end_date).replace(microsecond=0).isoformat(),
    # }

    body = {
        "startDate": start_date.replace(microsecond=0).isoformat(),
        'endDate': end_date.replace(microsecond=0).isoformat(),
    }

    print(f"Получаю данные в интервале {start_date} - {end_date}")
    data = get_epias_data(url=GTV_URL, headers=REQUEST_DATA_HEADER, params={}, body=body, validation_class=EpiasGasTradeVolumeModel)

    print(f"Cохраняю данные в таблицу {GTV_SCHEMA}.{GTV_TABLE}")
    insert_to_pg_table(data=data, schema=GTV_SCHEMA, table=GTV_TABLE, conn=PG_CONN)


def build_mart(**context):
    """Строит витрину и перекладывает ее в clickhouse"""
    create_ch_table(DDL_MART, CH_CONN)
    clear_ch_table(MART_SCHEMA, MART_TABLE, CH_CONN)
    mart_data, mart_columns = get_datamart_data_from_pg(BUILD_MART_SQL, PG_CONN)
    insert_to_ch_table(mart_data, mart_columns, MART_SCHEMA, MART_TABLE, CH_CONN)


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='epias_etl',
    default_args=default_args,
    description='Получает данные с EPIAS записывает в БД и собирает витрину для визуализации',
    schedule_interval='0 9 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    begin = EmptyOperator(task_id='begin')

    login_epias_task = PythonOperator(task_id='login_epias', python_callable=login_epias, provide_context=True)

    get_epias_prices_task = PythonOperator(
        task_id='get_epias_prices',
        python_callable=get_epias_prices,
        provide_context=True,
    )

    get_epias_volumes_task = PythonOperator(
        task_id='get_epias_volumes',
        python_callable=get_epias_volumes,
        provide_context=True,
    )

    build_mart_task = PythonOperator(
        task_id='build_mart',
        python_callable=build_mart,
        provide_context=True,
    )

    end = EmptyOperator(task_id='end')

    # Определяем порядок выполнения
    begin >> login_epias_task
    login_epias_task >> get_epias_prices_task >> build_mart_task >> end
    login_epias_task >> get_epias_volumes_task >> build_mart_task >> end


if __name__ == "__main__":
    dag.test()

    # _tgt = login_to_epias(url=LOGIN_URL,  login=EPIAS_USERNAME, password=EPIAS_PASSWORD, headers=LOGIN_HEADER)
    # REQUEST_DATA_HEADER["TGT"] = _tgt
    #
    # # GRP
    # create_pg_table_if_not_exists(GRP_DDL, PG_CONN)
    # _is_empty = check_table_is_empty(GRP_SCHEMA, GRP_TABLE, PG_CONN)
    #
    # _start_date = dt(2024, 6, 1) if _is_empty or DEBUG else dt.now() - timedelta(days=2)
    # _end_date = dt.now() - timedelta(days=1)
    # _body = {
    #     "startDate": pytz.timezone("Europe/Moscow").localize(_start_date).replace(microsecond=0).isoformat(),
    #     'endDate': pytz.timezone("Europe/Moscow").localize(_end_date).replace(microsecond=0).isoformat(),
    # }
    #
    # _grp_data = get_epias_data(url=GRP_URL, headers=REQUEST_DATA_HEADER, params={}, body=_body, validation_class=EpiasGasPriceModel)
    # insert_to_pg_table(data=_grp_data, schema=GRP_SCHEMA, table=GRP_TABLE, conn=PG_CONN)
    #
    # # GTV
    # create_pg_table_if_not_exists(GTV_DDL, PG_CONN)
    # _is_empty = check_table_is_empty(GTV_SCHEMA, GTV_TABLE, PG_CONN)
    #
    # _start_date = dt(2025, 1, 1) if _is_empty or DEBUG else dt.now() - timedelta(days=2)
    # _end_date = dt.now() - timedelta(days=1)
    # _body = {
    #     "startDate": pytz.timezone("Europe/Moscow").localize(_start_date).replace(microsecond=0).isoformat(),
    #     'endDate': pytz.timezone("Europe/Moscow").localize(_end_date).replace(microsecond=0).isoformat(),
    # }
    #
    # _gtv_data = get_epias_data(url=GTV_URL, headers=REQUEST_DATA_HEADER, params={}, body=_body, validation_class=EpiasGasTradeVolumeModel)
    # insert_to_pg_table(data=_gtv_data, schema=GTV_SCHEMA, table=GTV_TABLE, conn=PG_CONN)
    #
    # # BUILD MART
    # create_ch_table(DDL_MART, CH_CONN)
    # clear_ch_table(MART_SCHEMA, MART_TABLE, CH_CONN)
    # _mart_data, _mart_columns = get_datamart_data_from_pg(BUILD_MART_SQL, PG_CONN)
    # insert_to_ch_table(_mart_data, _mart_columns, MART_SCHEMA, MART_TABLE, CH_CONN)
    #

    # def get_dates(**kwargs):
    #     ti = kwargs['ti']
    #
    #     # Получаем даты из контекста
    #     start_date: datetime = kwargs['data_interval_start']
    #     end_date: datetime = kwargs['data_interval_end']
    #
    #     # Конвертируем в ISO формат
    #     start_iso = start_date.isoformat()
    #     end_iso = end_date.isoformat()
    #
    #     print(f"Start date: {start_iso}")
    #     print(f"End date: {end_iso}")
    #
    #     # Отправляем в XCom
    #     ti.xcom_push(key='start_date', value=start_iso)
    #     ti.xcom_push(key='end_date', value=end_iso)
    #
    #
    # # Функция-заглушка
    # def run_process(**kwargs):
    #     ti = kwargs['ti']
    #
    #     # Получаем значения из XCom
    #     start_iso = ti.xcom_pull(task_ids='get_dates', key='start_date')
    #     end_iso = ti.xcom_pull(task_ids='get_dates', key='end_date')
    #
    #     print(f"Process running with dates:")
    #     print(f"Start: {start_iso}")
    #     print(f"End: {end_iso}")

