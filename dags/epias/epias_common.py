from contextlib import closing
from dotenv import load_dotenv
import os
import requests
from http import HTTPStatus
import requests
import json
from pydantic import BaseModel
from typing import Type, TypeVar
import io
import csv
import psycopg2
from psycopg2.extras import execute_batch

ValidationModelT = TypeVar('ValidationModelT', bound='BaseModel')


def login_to_epias(url, login, password, headers):
    """Логинится к порталу EPIAS, и возвращает тикет/токен для авторизации."""
    params = {"username": login, "password": password}
    response = requests.post(url, headers=headers, params=params)

    if response.status_code not in (HTTPStatus.OK, HTTPStatus.CREATED):
        raise ValueError(f"Тикет для логина не получен. статус запроса {response.status_code}, ответ {response}")

    tgt = response.content.decode("utf-8")
    return tgt


def get_epias_data(
        url: str,
        headers: dict,
        params: dict,
        body: dict,
        validation_class: Type[ValidationModelT],
        delimiter: str = ";"
) -> list[ValidationModelT | None]:
    """
    Получает данные из API EPIAS и валидирует, через класс модели.
    Возвращает список экземпляров модели.
    """
    response = requests.post(url, headers=headers, params=params, data=json.dumps(body))
    # csv_data = io.StringIO(text_data)
    # reader = csv.DictReader(csv_data, delimiter=delimiter)
    # dataset = list(reader)
    data = response.json()
    validated_dataset = [validation_class(**row) for row in response.json()["items"]]
    return validated_dataset


def create_pg_table_if_not_exists(ddl: str, conn):
    """Создает таблицу в PG из DDL скрипта."""
    with conn, conn.cursor() as cursor:
        cursor.execute(ddl)
        conn.commit()


def check_table_is_empty(schema, table, conn):
    """Проверяет наличие записей в таблице. Возвращает True, если в ней нет ни одной записи"""
    with conn, conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {schema}.{table} LIMIT 1")
        data = cursor.fetchall()
        if len(data) == 0:
            return True
    return False


def insert_to_pg_table(data: list[ValidationModelT | None], schema: str, table: str, conn):
    """Вставляет набор данных в таблицу в PG."""
    if not data:
        return

    fields = data[0].model_fields_set
    fields_names = ", ".join(fields)
    fields_subst = ", ".join([f"%({field_name})s" for field_name in fields])
    insert_query = f"""INSERT INTO {schema}.{table} ({fields_names}) VALUES ({fields_subst})"""
    data = [dict(row) for row in data]

    with conn, conn.cursor() as cursor:
        execute_batch(cursor, insert_query, data)
        conn.commit()


def create_ch_table(ddl, conn):
    """Создает таблицу в Clickhouse."""
    conn.execute(ddl)


def clear_ch_table(schema, table, conn):
    """Очищает таблицу в Clickhouse."""
    clear_mart_sql = f"""ALTER TABLE {schema}.{table} DELETE WHERE 1=1;"""
    conn.execute(clear_mart_sql)


def get_datamart_data_from_pg(sql, conn):
    """Получить данные для витрины из PG"""
    with conn, conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return rows, columns


def insert_to_ch_table(data: list[tuple | None], columns, schema, table, conn):
    """Вставляет данные в таблицу CH."""
    insert_query = f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES"
    conn.execute(insert_query, data)
