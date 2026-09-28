{{ config(materialized='table') }}

with distinct_dates as (
    select distinct candle_date as date_day
    from {{ ref('stg_crypto_klines') }}
)

select
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(isodow from date_day) as day_of_week,
    case
        when extract(isodow from date_day) in (6, 7) then true
        else false
    end as is_weekend
from distinct_dates