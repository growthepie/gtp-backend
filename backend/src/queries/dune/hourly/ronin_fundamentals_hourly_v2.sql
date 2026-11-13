-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121755 (https://dune.com/queries/9121755), generated from 6776063.
with price_ron as (
    select 
        date_trunc('day', timestamp) as day,
        max(price) as price
    from prices.day
    where blockchain = 'ronin' and contract_address in (0xe514d9deb7966c8be0ca922de8a064264ea6bcd4) -- RON
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
        ron.day,
        ron.price as price_ron,
        eth.price as price_eth,
        ron.price/eth.price as ron_per_eth
    from price_ron ron
    inner join price_eth eth on ron.day = eth.day
)

select
    date_trunc('hour', block_time) as "timestamp",
    'ronin' as origin_key,
    --count(*) FILTER (WHERE gas_price > 0) as txcount,
    count(*) as txcount,
    count(*) as txcount_plain,
    count(*) as txcount_comparison,
    SUM(gas_used) / (60 * 60) AS gas_per_second,
    --COUNT(distinct "from") AS daa,
    SUM((gas_used * (gas_price / 1e18) + coalesce(l1_fee / 1e18, 0)) * ron_per_eth) AS fees_paid_eth,
    approx_percentile((gas_used * (gas_price / 1e18) + coalesce(l1_fee / 1e18, 0)) * ron_per_eth, 0.5) FILTER (WHERE gas_price > 0) as txcosts_median_eth,
    count(distinct "from") as daa
from ronin.transactions t
left join prices p on t.block_date = p.day 
where block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date) -- for better partitioning
    and block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM ronin.transactions WHERE block_date >= current_date - interval '1' day)
    and block_time >= from_iso8601_timestamp('{{start_time}}')
group by 1