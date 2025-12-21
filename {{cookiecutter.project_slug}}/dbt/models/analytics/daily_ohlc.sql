{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS daily_ohlc_pkey;",
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (symbol, day);"
    ]
) }}

select
    symbol,
    date_trunc('day', timestamp) as day,
    (array_agg(open ORDER BY timestamp))[1] as open,
    max(high) as high,
    min(low) as low,
    (array_agg(close ORDER BY timestamp DESC))[1] as close,
    sum(volume) as volume
from {{ ref('stg_ohlc') }}
group by symbol, date_trunc('day', timestamp)