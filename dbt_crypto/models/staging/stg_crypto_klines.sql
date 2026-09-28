{{ config(materialized='table') }}

with raw_klines as (
    select *
    from read_parquet('data/lake/klines/**/*.parquet', hive_partitioning = true)
)

select
    symbol,
    cast("timestamp" as timestamp) as candle_timestamp,
    cast(cast("timestamp" as timestamp) as date) as candle_date,
    cast("open" as double) as open_price,
    cast(high as double) as high_price,
    cast(low as double) as low_price,
    cast("close" as double) as close_price,
    cast(volume as double) as volume_crypto,
    cast(quote_asset_volume as double) as volume_usdt,
    cast(number_of_trades as bigint) as trades_count,
    cast(price_change_pct as double) as price_change_pct

from raw_klines
where "close" > 0 and volume > 0