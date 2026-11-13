-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121761 (https://dune.com/queries/9121761), generated from 6683678.
-- Column order here is important! Will be split up later
WITH
assets AS (
  SELECT *
  FROM (
    VALUES
      -- single-address assets
      ('eth',  0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2),

      -- multi-address asset: POL (POL + MATIC)
      ('pol',  0x455e53cbb86018ac2b8092fdcd39d8444affc3f6),
      ('pol',  0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0)
  ) AS t(asset, contract_address)
),

hist AS (
  SELECT
    a.asset,
    CAST(date_trunc('day', d.timestamp) AS date) AS day,
    max(d.price) AS price
  FROM prices.day d
  JOIN assets a
    ON d.contract_address = a.contract_address
  WHERE d.blockchain = 'ethereum'
    AND d.timestamp < date_trunc('hour', now())
    AND d.timestamp >= from_iso8601_timestamp('{{start_time}}') - interval '24' hour
  GROUP BY 1, 2
),

latest_asset AS (
  -- prices.latest has 1 row per contract; collapse to 1 row per asset
  SELECT
    a.asset,
    max_by(l.price, l.timestamp) AS price
  FROM prices.latest l
  JOIN assets a
    ON l.contract_address = a.contract_address
  WHERE l.blockchain = 'ethereum'
  GROUP BY 1
),

series AS (
  SELECT * FROM hist

  UNION ALL

  SELECT
    la.asset,
    CAST(date_trunc('day', now()) AS date) AS day,
    la.price
  FROM latest_asset la
  WHERE NOT EXISTS (
    SELECT 1
    FROM hist h
    WHERE h.asset = la.asset
      AND h.day = CAST(date_trunc('day', now()) AS date)
  )
),

prices as (
    SELECT
      day,
      max(CASE WHEN asset = 'eth'  THEN price END) AS price_eth,
      max(CASE WHEN asset = 'pol'  THEN price END) AS price_pol,
      max(CASE WHEN asset = 'pol' THEN price END)
      / max(CASE WHEN asset = 'eth'  THEN price END) AS pol_per_eth
    FROM series
    GROUP BY 1
)

SELECT
  date_trunc('hour', block_time) as hour,
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
WHERE t.block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM polygon.transactions WHERE block_date >= current_date - interval '1' day)
  and t.block_time >= from_iso8601_timestamp('{{start_time}}')
    AND t.block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)
  AND "to" IS NOT NULL
  AND "data" IS NOT NULL
  AND gas_price > 0
GROUP BY 1,2
Having count(*) > 3