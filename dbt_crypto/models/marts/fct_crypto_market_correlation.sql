{{ config(materialized='table') }}

select
    trade_date,
    {{ dbt_utils.pivot(
        column='symbol',
        values=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
        then_value='close_price',
        prefix='price_'
    ) }},
    {{ dbt_utils.pivot(
        column='symbol',
        values=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
        then_value='volume_usdt',
        prefix='volume_'
    ) }}
from {{ ref('fct_crypto_daily') }}
group by trade_date
order by trade_date