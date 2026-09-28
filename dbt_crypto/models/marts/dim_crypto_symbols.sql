{{ config(materialized='table') }}

with symbols_seed as (
    select *
    from {{ ref('crypto_symbols') }}
)

select
    symbol,
    base_asset,
    quote_asset,
    asset_name,
    category
from symbols_seed