-- v2 (incremental): returns complete days >= start_date (YYYY-MM-DD) up to the last day Dune has fully ingested
-- (block_date < date_trunc('day', max(block_time))). Airflow passes start_date = last day loaded in our DB + 1.
-- Dune query 9134313 (https://dune.com/queries/9134313), generated from 6474479.
-- Column order here is important! Will be split up later
with price_pol as (
    select 
        date_trunc('day', timestamp) as day,
        max(price) as price
    from prices.day
    where blockchain = 'ethereum' and contract_address in (0x455e53cbb86018ac2b8092fdcd39d8444affc3f6, 0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0) -- POL, MATIC
    and timestamp < current_date
    and timestamp >= CAST('{{start_date}}' AS date)
    GROUP BY 1
),

price_eth as (
    select 
        date_trunc('day', timestamp) as day,
        price
    from prices.day
    where blockchain = 'ethereum' and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- ETH
    and timestamp < current_date
    and timestamp >= CAST('{{start_date}}' AS date)
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


SELECT
  block_date AS day,
  "to" AS address,
  ROW(
    SUM((gas_used / POWER(10,18) * CAST(gas_price AS DOUBLE)) * pol_per_eth),              -- gas_fees_eth
    ROUND(
      SUM((gas_used / POWER(10,18) * price_pol * CAST(gas_price AS DOUBLE))),
      4
    ),                                                                                                -- gas_fees_usd
    COUNT(*),                                                                                         -- txcount
    COUNT(DISTINCT "from"),                                                                           -- daa
    SUM(CASE WHEN success THEN 1 ELSE 0 END) * 1.0 / COUNT(*),                                        -- success_rate
    approx_percentile(
      (gas_used / POWER(10,18) * CAST(gas_price AS DOUBLE)) * pol_per_eth,
      0.5
    ),                                                                                                -- median_tx_fee
    SUM(gas_used)                                                                                     -- gas_used
  ) AS metrics
FROM polygon.transactions t
left join prices p on t.block_date = p.day 
WHERE block_date < (SELECT least(current_date, CAST(date_trunc('day', max(block_time)) AS date)) FROM polygon.transactions WHERE block_date >= current_date - interval '2' day)
  AND block_date >= CAST('{{start_date}}' AS date)
  AND "to" IS NOT NULL
  AND "data" IS NOT NULL
  AND gas_price > 0
GROUP BY 1,2
Having count(*) > 3