-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121764 (https://dune.com/queries/9121764), generated from 7922169.
WITH base AS (
  SELECT
    date_trunc('hour', block_time) AS hour,
    "to" AS address,
    "from" AS from_address
  FROM robinhood.transactions
    WHERE block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM robinhood.transactions WHERE block_date >= current_date - interval '1' day)
    AND block_time >= from_iso8601_timestamp('{{start_time}}')
    AND block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)
  GROUP BY 1, 2, 3
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
  FROM base b
)
SELECT
  hour,
  address,
  CAST(floor((rn - 1) / 500000) AS bigint) AS chunk_id,
  array_agg(from_address ORDER BY from_address) AS from_addresses
FROM numbered
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;