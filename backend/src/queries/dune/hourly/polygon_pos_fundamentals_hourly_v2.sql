-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121754 (https://dune.com/queries/9121754), generated from 6715560.
with price_pol as (
    select 
        date_trunc('day', timestamp) as day,
        max(price) as price
    from prices.day
    where blockchain = 'ethereum' and contract_address in (0x455e53cbb86018ac2b8092fdcd39d8444affc3f6, 0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0) -- POL, MATIC
    and timestamp < date_trunc('hour', now())
    and timestamp >= from_iso8601_timestamp('{{start_time}}') - interval '25' hour
    GROUP BY 1
),

price_eth as (
    select 
        date_trunc('day', timestamp) as day,
        price
    from prices.day
    where blockchain = 'ethereum' and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- ETH
    and timestamp < date_trunc('hour', now())
    and timestamp >= from_iso8601_timestamp('{{start_time}}') - interval '25' hour
),

prices as (
    select 
        pol.day,
        pol.price as price_pol,
        eth.price as price_eth,
        pol.price/eth.price as pol_per_eth
    from price_pol pol
    inner join price_eth eth on pol.day = eth.day
)

select
    date_trunc('hour', block_time) as "timestamp",
    'polygon_pos' as origin_key,
    count(*) FILTER (WHERE gas_price > 0) as txcount,
    count(*) as txcount_plain,
    count(*) as txcount_comparison,
    SUM(gas_used) / (60 * 60) AS gas_per_second,
    --COUNT(distinct "from") AS daa,
    SUM((gas_used / POWER(10,18) * cast(gas_price as DOUBLE)) * pol_per_eth) AS fees_paid_eth,
    approx_percentile((gas_used / POWER(10,18) * cast(gas_price as DOUBLE)) * pol_per_eth, 0.5) FILTER (WHERE gas_price > 0) as txcosts_median_eth,
    count(distinct "from") as daa
from polygon.transactions t
left join prices p on t.block_date = p.day 
where block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date) -- for better partitioning
    and block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM polygon.transactions WHERE block_date >= current_date - interval '1' day)
    and block_time >= from_iso8601_timestamp('{{start_time}}')
group by 1