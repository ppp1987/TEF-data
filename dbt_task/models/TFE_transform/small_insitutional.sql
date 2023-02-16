{{ config(materialized='table') }}

with source_data as (

    SELECT DATE(a.date) AS date,
           (b.foreign_investors - (a.buyer_top5 - a.seller_top5) + b.investment_trust) as small_insitutional
    FROM TFE_data.large_trader_fut_qry a
    LEFT JOIN TFE_data.fut_contracts_date_tfx b ON a.date = b.date

)

select *
from source_data
order by date
