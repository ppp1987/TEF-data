{{ config(materialized='table') }}

with source_data as (

    SELECT DATE(a.date) AS date,
           (a.open_interest - b.insitutional_buy) as pop_investors_buy,
           (a.open_interest - b.insitutional_sell) as pop_investors_sell,
           (b.insitutional_sell - b.insitutional_buy) as pop_investors_value,
           round(100*((b.insitutional_sell - b.insitutional_buy)/a.open_interest), 1) as pop_investors_ratio

    FROM TFE_data.fut_daily_market_roprot_mtx a
    LEFT JOIN TFE_data.fut_contracts_date_mfx b ON a.date = b.date

)

select *
from source_data
order by date