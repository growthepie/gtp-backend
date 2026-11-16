-- v2 (incremental): returns complete days >= start_date (YYYY-MM-DD) up to the last day Dune has fully ingested
-- (block_date < date_trunc('day', max(block_time))). Airflow passes start_date = last day loaded in our DB + 1.
-- Dune query 9134312 (https://dune.com/queries/9134312), generated from 7922138.
-- Column order here is important! Will be split up later

with price_eth as (
    select 
        date_trunc('day', timestamp) as day,
        price
    from prices.day
    where blockchain = 'ethereum' and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    and timestamp < current_date
    and timestamp >= CAST('{{start_date}}' AS date)
)


SELECT
  block_date AS day,
  "to" AS address,
  ROW(
    SUM((gas_used / POWER(10,18) * CAST(effective_gas_price AS DOUBLE)) + coalesce(l1_fee,0) / POWER(10,18)),              -- gas_fees_eth
    ROUND(
      SUM((gas_used / POWER(10,18) * price * CAST(effective_gas_price AS DOUBLE)) + coalesce(l1_fee,0) / POWER(10,18) * price),
      4
    ),                                                                                                -- gas_fees_usd
    COUNT(*),                                                                                         -- txcount
    COUNT(DISTINCT "from"),                                                                           -- daa
    SUM(CASE WHEN success THEN 1 ELSE 0 END) * 1.0 / COUNT(*),                                        -- success_rate
    approx_percentile(
      (gas_used / POWER(10,18) * CAST(effective_gas_price AS DOUBLE)) + coalesce(l1_fee,0) / POWER(10,18),
      0.5
    ),                                                                                                -- median_tx_fee
    SUM(gas_used-gas_used_for_l1)                                                                                     -- gas_used
  ) AS metrics
FROM robinhood.transactions t
left join price_eth p on t.block_date = p.day 
WHERE block_date < (SELECT least(current_date, CAST(date_trunc('day', max(block_time)) AS date)) FROM robinhood.transactions WHERE block_date >= current_date - interval '2' day)
  AND block_date >= CAST('{{start_date}}' AS date)
  AND "to" IS NOT NULL
  AND "data" IS NOT NULL
  AND gas_price > 0
GROUP BY 1,2
--Having count(*) > 3