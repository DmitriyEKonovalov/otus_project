"""
    EPIAS GRP Trade Volume
    Настройки, описание форматов, моделей и таблиц для сбора данных из раздела GRP Trade Volume
    https://seffaflik.epias.com.tr/natural-gas-service/v1/markets/sgp/data/total-trade-volume

"""
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Any
from datetime import datetime as dt

# параметры получения данных с сайта
GTV_URL = r'https://seffaflik.epias.com.tr/natural-gas-service/v1/markets/sgp/data/total-trade-volume'


# таблица для сохранения сырых данных в PG
GTV_SCHEMA, GTV_TABLE = "public", "gas_trade_volume"
GTV_DDL = f"""
    CREATE TABLE IF NOT EXISTS {GTV_SCHEMA}.{GTV_TABLE} (
        gas_day timestamptz NOT NULL
    ,   trade_volume NUMERIC NULL 
    ,   id uuid default gen_random_uuid()
    ,   created_dt timestamptz default now()
    ,   CONSTRAINT gas_trade_volume_pk PRIMARY KEY (id)
    );
"""


class EpiasGasTradeVolumeModel(BaseModel):
    """Модель для валидации и преобразования строк CSV-файла для daily trade volume (из API)."""
    gas_day: str = Field(alias="gasDay")
    trade_volume: float | None = Field(alias="tradeVolume")
