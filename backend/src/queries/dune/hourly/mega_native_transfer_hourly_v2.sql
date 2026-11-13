-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121765 (https://dune.com/queries/9121765), generated from 8944401.
-- Hourly native transfers for the 'eth' app on growthepie, mapped to placeholder address 0x000000000000000000000000000000000000EeEe
-- Same native transfer definition as mega_category_level (6364531): "to" IS NOT NULL AND gas_used = 60000
-- Column order matches mega_contract_level_hourly (6683663) so it can be loaded with prepare_df_contract_level_hourly. Column order here is important! Will be split up later

WITH
assets AS (
  SELECT *
  FROM (
    VALUES
      ('eth',  0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2)
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

series AS (
  -- historical
  SELECT * FROM hist

  UNION ALL

  -- add "today" from prices.latest only if missing in hist
  SELECT
    a.asset,
    CAST(date_trunc('day', now()) AS date) AS day,
    l.price
  FROM prices.latest l
  JOIN assets a
    ON l.contract_address = a.contract_address
  WHERE l.blockchain = 'ethereum'
    AND NOT EXISTS (
      SELECT 1
      FROM hist h
      WHERE h.asset = a.asset
        AND h.day = CAST(date_trunc('day', now()) AS date)
    )
),

prices AS (
    SELECT
      day,
      max(CASE WHEN asset = 'eth'  THEN price END) AS price_eth
    FROM series
    GROUP BY 1
)

SELECT
  date_trunc('hour', block_time) as hour,
  0x000000000000000000000000000000000000eeee AS address,
  ROW(
    SUM((gas_used / POWER(10,18) * CAST(gas_price AS DOUBLE)) + l1_fee / POWER(10,18)),                                                                                   -- gas_fees_eth
    ROUND(SUM((gas_used / POWER(10,18) * price_eth * CAST(gas_price AS DOUBLE)) + l1_fee / POWER(10,18) * price_eth), 4),                                                                         -- gas_fees_usd
    COUNT(*),                                                                                         -- txcount
    COUNT(DISTINCT "from"),                                                                           -- daa
    SUM(CASE WHEN success THEN 1 ELSE 0 END) * 1.0 / COUNT(*),                                        -- success_rate
    approx_percentile((gas_used / POWER(10,18) * CAST(gas_price AS DOUBLE)) + l1_fee / POWER(10,18), 0.5),                                                                -- median_tx_fee
    SUM(gas_used)                                                                                   -- gas_used
  ) AS metrics
FROM megaeth.transactions t
left join prices p on t.block_date = p.day
WHERE block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM megaeth.transactions WHERE block_date >= current_date - interval '1' day)
  and block_time >= from_iso8601_timestamp('{{start_time}}')
    AND block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)
  AND "to" IS NOT NULL
  AND gas_used = 60000
GROUP BY 1, 2
