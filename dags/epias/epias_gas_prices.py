"""
    EPIAS GAS PRICES
    Настройки, описание форматов, моделей и таблиц для сбора данных из раздела Daily Reference Price
    https://seffaflik.epias.com.tr/natural-gas-service/v1/markets/sgp/data/daily-reference-price

"""
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Any
from datetime import datetime as dt

# параметры получения данных с сайта
GRP_URL = r'https://seffaflik.epias.com.tr/natural-gas-service/v1/markets/sgp/data/daily-reference-price'

# таблица для сохранения сырых данных в PG
GRP_SCHEMA, GRP_TABLE = "public", "gas_reference_prices"
GRP_DDL = """
    CREATE TABLE IF NOT EXISTS public.gas_reference_prices (
        gas_day timestamptz NOT NULL
    ,   ref_price_eur_tcm NUMERIC NULL
    ,   ref_price_tl_tcm NUMERIC NULL 
    ,   ref_price_usd_tcm NUMERIC NULL
    ,   ref_price_usd_mmbtu NUMERIC NULL
    ,   id uuid default gen_random_uuid()
    ,   created_dt timestamptz default now()
    ,   CONSTRAINT gas_reference_prices_pk PRIMARY KEY (id)
    );
"""


class EpiasGasPriceModel(BaseModel):
    """Модель для валидации и преобразования строк CSV-файла для daily reference price (из API)."""
    gas_day: str = Field(alias='date')
    ref_price_eur_tcm: float | None = Field(alias='grfEur')
    ref_price_tl_tcm: float | None = Field(alias='grfTl')
    ref_price_usd_tcm: float | None = Field(alias='grfUsd')
    ref_price_usd_mmbtu: float | None = Field(alias='grfUsdMmBtu')

    # @field_validator("gas_day", mode="before")
    # @classmethod
    # def convert_date(cls, value: Any):
    #     """Проверяет, что дата в нужном формате без конвертации"""
    #     try:
    #         # value_dt = dt.strptime(value, "%d.%m.%Y")
    #         value_dt = dt.fromisoformat(value)
    #         validated_value = value_dt.strftime("%Y-%m-%d")
    #         return validated_value
    #     except Exception as err:
    #         raise ValidationError(err)

    # @field_validator("ref_price_tl_tcm", "ref_price_usd_tcm", "ref_price_eur_tcm", "ref_price_usd_mmbtu", mode="before")
    # @classmethod
    # def convert_numeric(cls, value: Any):
    #     validated_value = value.replace(".", "").replace(",", ".")
    #     try:
    #         validated_value = float(validated_value)
    #         return validated_value
    #     except Exception as err:
    #         raise ValidationError(err)
