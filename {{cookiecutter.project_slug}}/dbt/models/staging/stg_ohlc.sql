{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS stg_ohlc_pkey;",
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (symbol, timestamp);"
    ]
) }}

select
    symbol,
    timestamp,
    MAX(open::numeric(20,8)) as open,
    MAX(high::numeric(20,8)) as high,
    MAX(low::numeric(20,8)) as low,
    MAX(close::numeric(20,8)) as close,
    MAX(volume::numeric(20,8)) as volume,
    MAX(ingested_at) as ingested_at
from {{ ref('raw_ohlc') }}
where symbol is not null
GROUP BY symbol, timestamp