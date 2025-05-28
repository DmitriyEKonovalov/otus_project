#!/bin/bash
set -e

# Проверяем, существует ли файл SQLite (или можно проверить Postgres через pg_isready)
if [ ! -f /opt/airflow/airflow.db ]; then
  echo "База не найдена. Инициализируем..."
  airflow db init

  echo "Создаем пользователя по умолчанию"
  airflow users create \
    --username airflow \
    --password airflow \
    --firstname Air \
    --lastname Flow \
    --role Admin \
    --email admin@example.com
else
  echo "База уже существует. Пропускаем инициализацию."
fi

pip install clickhouse-driver

# Выполняем переданную команду (webserver/scheduler)
exec airflow "$@"
