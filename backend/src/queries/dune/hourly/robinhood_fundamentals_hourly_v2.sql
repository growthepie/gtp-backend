-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121756 (https://dune.com/queries/9121756), generated from 7922085.
select
    date_trunc('hour', block_time) as "timestamp",
    'robinhood' as origin_key,
    count(*) FILTER (WHERE gas_used > 0) as txcount,
    count(*) as txcount_plain,
    count(*) as txcount_comparison,
    SUM(gas_used - gas_used_for_l1) / (60 * 60) AS gas_per_second,
    --COUNT(distinct "from") AS daa,
    SUM((gas_used / POWER(10,18) * cast(effective_gas_price as DOUBLE)) + coalesce (l1_fee / POWER(10,18),0)) AS fees_paid_eth,
    approx_percentile((gas_used / POWER(10,18) * cast(effective_gas_price as DOUBLE)) + coalesce(l1_fee / POWER(10,18),0), 0.5) FILTER (WHERE gas_price > 0) as txcosts_median_eth,
    count(distinct "from") as daa
from robinhood.transactions
where block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date) -- for better partitioning
    and block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM robinhood.transactions WHERE block_date >= current_date - interval '1' day)
    and block_time >= from_iso8601_timestamp('{{start_time}}')
group by 1