-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121757 (https://dune.com/queries/9121757), generated from 6683568.
WITH
assets AS (
  SELECT *
  FROM (
    VALUES
      ('strk', 0xca14007eff0db1f8135f4c25b34de49ab0d42766),
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
      max(CASE WHEN asset = 'strk' THEN price END) AS price_strk,
      max(CASE WHEN asset = 'eth'  THEN price END) AS price_eth,
      max(CASE WHEN asset = 'strk' THEN price END)
      / max(CASE WHEN asset = 'eth'  THEN price END) AS strk_per_eth
    FROM series
    GROUP BY 1
),

calls_enriched as (
   SELECT
       date_trunc('hour', c.block_time) as hour,
        c.contract_address,
        c.transaction_hash,
        t.sender_address,
        t.execution_status,
        CASE
          WHEN t.actual_fee_unit = 'WEI' THEN t.actual_fee_amount / 1e18
          WHEN t.actual_fee_unit = 'FRI' THEN (t.actual_fee_amount / 1e18) * p.strk_per_eth
          ELSE 0
        END AS tx_fee_eth,
        CASE
          WHEN t.actual_fee_unit = 'WEI' THEN (t.actual_fee_amount / 1e18) * p.price_eth
          WHEN t.actual_fee_unit = 'FRI' THEN (t.actual_fee_amount / 1e18) * p.strk_per_eth * p.price_eth
          ELSE 0
        END AS tx_fee_usd,
        COUNT(*) OVER (PARTITION BY c.transaction_hash) AS calls_in_tx,
        COUNT(distinct c.contract_address) OVER (PARTITION BY c.transaction_hash) AS contracts_called_in_tx
    FROM starknet.calls c
    LEFT JOIN starknet.transactions t
        ON c.transaction_hash = t.hash
    LEFT JOIN prices p
        ON t.block_date = p.day 
        WHERE c.block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM starknet.calls WHERE block_date >= current_date - interval '1' day)
        and c.block_time >= from_iso8601_timestamp('{{start_time}}')
    AND c.block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)
        and t.block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM starknet.calls WHERE block_date >= current_date - interval '1' day)
        and t.block_time >= from_iso8601_timestamp('{{start_time}}')
    AND t.block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)

        and c.contract_address is not null
        and c.transaction_type = 'INVOKE' --exclude DECLARE and DEPLOY_ACCOUNT
        and c.call_type = 'CALL' -- exclude DELEGATE and internal cairo helpers
        and c.context = 'EXECUTE' -- exclude fee transfer, signature checks, etc
        --and entry_point_type = 'EXTERNAL' -- excludes constructore, l1 handler
)

SELECT
    hour,
    contract_address as address,

    Row(
        -- allocate tx fee equally across calls
        SUM(
            CAST(tx_fee_eth AS DOUBLE) / NULLIF(CAST(calls_in_tx AS DOUBLE), 0.0)
        ) , -- gas_ees_eth
        ROUND(
            SUM(
                CAST(tx_fee_usd AS DOUBLE) / NULLIF(CAST(calls_in_tx AS DOUBLE), 0.0)
            )
            ,4
        ), --gas_ees_usd
        
        COUNT(DISTINCT transaction_hash), -- txcount, but in Starknets case double-counted -> one tx can be assigned to multiple contracts
        COUNT(DISTINCT sender_address), -- daa, but in Starknets case double-counted -> one tx assigned to multiple contracts
        -- COUNT(*) AS contract_calls, -- contract_calls, this would be a new metric -> on EVM level this would be count(*) from traces
        SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), -- success_rate
        0, -- median tx_fee
        0 -- gas_used
    ) as metrics
FROM calls_enriched
GROUP BY 1,2
HAVING COUNT(DISTINCT transaction_hash) > 3
