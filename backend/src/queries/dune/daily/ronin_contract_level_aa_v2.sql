-- v2 (incremental): returns complete days >= start_date (YYYY-MM-DD) up to the last day Dune has fully ingested
-- (block_date < date_trunc('day', max(block_time))). Airflow passes start_date = last day loaded in our DB + 1.
-- Dune query 9134316 (https://dune.com/queries/9134316), generated from 6776362.
WITH base AS (
  SELECT
    block_date AS day,
    "to" AS address,
    "from" AS from_address
  FROM ronin.transactions
  WHERE block_date < (SELECT least(current_date, CAST(date_trunc('day', max(block_time)) AS date)) FROM ronin.transactions WHERE block_date >= current_date - interval '2' day)
    AND block_date >= CAST('{{start_date}}' AS date)
  GROUP BY 1, 2, 3
),

numbered AS (
  SELECT
    b.day,
    b.address,
    b.from_address,
    row_number() OVER (
      PARTITION BY b.day, b.address
      ORDER BY b.from_address
    ) AS rn
  FROM base b
)
SELECT
  day,
  address,
  CAST(floor((rn - 1) / 500000) AS bigint) AS chunk_id,
  array_agg(from_address ORDER BY from_address) AS from_addresses
FROM numbered
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;