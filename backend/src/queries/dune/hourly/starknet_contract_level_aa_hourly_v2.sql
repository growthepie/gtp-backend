-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121758 (https://dune.com/queries/9121758), generated from 6905476.
WITH base AS (
    SELECT
        date_trunc('hour', c.block_time) AS hour,
        c.contract_address AS address,
        c.caller_address,
        t.sender_address
    FROM starknet.calls c
    LEFT JOIN starknet.transactions t
        ON c.transaction_hash = t.hash
    WHERE c.block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM starknet.calls WHERE block_date >= current_date - interval '1' day)
        AND c.block_time >= from_iso8601_timestamp('{{start_time}}')
    AND c.block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)
        AND t.block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM starknet.calls WHERE block_date >= current_date - interval '1' day)
        AND t.block_time >= from_iso8601_timestamp('{{start_time}}')
    AND t.block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)
),
unioned AS (
    SELECT hour, address, caller_address AS from_address
    FROM base
    UNION
    SELECT hour, address, sender_address AS from_address
    FROM base
),

numbered AS (
  SELECT
    b.hour,
    b.address,
    b.from_address,
    row_number() OVER (
      PARTITION BY b.hour, b.address
      ORDER BY b.from_address
    ) AS rn
  FROM unioned b
)
SELECT
  hour,
  address,
  CAST(floor((rn - 1) / 500000) AS bigint) AS chunk_id,
  array_agg(from_address ORDER BY from_address) AS from_addresses
FROM numbered
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;