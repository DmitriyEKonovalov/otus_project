from epias.epias_gas_trade_volume import GTV_SCHEMA, GTV_TABLE
from epias.epias_gas_prices import GRP_SCHEMA, GRP_TABLE

MART_SCHEMA, MART_TABLE = "default", "epias_mart"

DDL_MART = f"""
CREATE TABLE IF NOT EXISTS {MART_SCHEMA}.{MART_TABLE} (   
    gas_day Date
,   ref_price_eur_tcm Decimal(38, 10)
,   ref_price_tl_tcm Decimal(38, 10)
,   ref_price_usd_tcm Decimal(38, 10)
,   trade_volume Decimal(38, 10)
,   ts DateTime DEFAULT now()
) 
ENGINE = ReplacingMergeTree(ts)
ORDER BY gas_day;
"""

BUILD_MART_SQL = f"""
WITH 
    grp_last AS (
        SELECT 
            gas_day::date
        ,   ref_price_eur_tcm 
        ,   ref_price_tl_tcm 
        ,   ref_price_usd_tcm
        ,   ref_price_usd_mmbtu
        ,   ROW_NUMBER() OVER (PARTITION BY gas_day ORDER BY created_dt) AS rn
        FROM {GRP_SCHEMA}.{GRP_TABLE} grp 
    ),
    gtv_last AS (
        SELECT 
            gas_day::date
        ,   trade_volume 
        ,   ROW_NUMBER() OVER (PARTITION BY gas_day ORDER BY created_dt) AS rn
        FROM  {GTV_SCHEMA}.{GTV_TABLE} gtv 
    )
SELECT
    grp.gas_day 
,   grp.ref_price_eur_tcm 
,   grp.ref_price_tl_tcm 
,   grp.ref_price_usd_tcm
,   gtv.trade_volume
FROM grp_last grp
    LEFT JOIN gtv_last gtv USING (gas_day)
WHERE grp.rn = 1 AND gtv.rn = 1
"""

