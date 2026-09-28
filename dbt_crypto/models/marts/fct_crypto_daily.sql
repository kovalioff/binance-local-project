{{ config(materialized='table') }}

with ranked_candles as (
    select
        candle_date as trade_date,
        symbol,
        high_price,
        low_price,
        volume_crypto,
        volume_usdt,
        trades_count,
        {{ window_first('open_price', 'symbol, candle_date', 'candle_timestamp', 'asc') }} as daily_open,
        {{ window_first('close_price', 'symbol, candle_date', 'candle_timestamp', 'desc') }} as daily_close
    from {{ ref('stg_crypto_klines') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['symbol', 'trade_date']) }} as crypto_daily_id,
    trade_date,
    symbol,
    daily_open as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    daily_close as close_price,
    round(sum(volume_crypto), 2) as volume_crypto,
    round(sum(volume_usdt), 2) as volume_usdt,
    sum(trades_count) as trades_count,
    round(((daily_close - daily_open) / nullif(daily_open, 0)) * 100, 2) as return_pct,
    round(((max(high_price) - min(low_price)) / nullif(min(low_price), 0)) * 100, 2) as volatility_pct
from ranked_candles
group by trade_date, symbol, daily_open, daily_close