{{ config(
    materialized='incremental',
    unique_key=['symbol', 'timestamp'],
    on_schema_change='ignore',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_raw_ohlc_symbol_timestamp ON {{ this }} (symbol, timestamp DESC)"
    ]
) }}

-- This query returns nothing, but preserves the existing table
{% if is_incremental() %}
  -- Incremental mode: does NOTHING, preserves data
  select * from {{ this }} where false

{% else %}
  -- First creation : Create structure
  select
    cast(null as integer) as id,
    cast(null as varchar(20)) as symbol,
    cast(null as timestamp with time zone) as timestamp,
    cast(null as numeric(20,8)) as open,
    cast(null as numeric(20,8)) as high,
    cast(null as numeric(20,8)) as low,
    cast(null as numeric(20,8)) as close,
    cast(null as numeric(20,8)) as volume,
    cast(null as timestamp with time zone) as ingested_at
  where false

{% endif %}