-- v2 (incremental): returns complete hours >= start_time (ISO8601 UTC, e.g. 2026-11-13T07:00:00Z) up to the last hour
-- Dune has fully ingested (hour < date_trunc('hour', max(block_time))). Airflow passes start_time = last hour loaded in our DB + 1h.
-- Dune query 9121768 (https://dune.com/queries/9121768), generated from 8944400.
-- Hourly native transfer senders for the 'eth' app on growthepie, mapped to placeholder address 0x000000000000000000000000000000000000EeEe
-- Same native transfer definition as robinhood_category_level (7922174): "to" IS NOT NULL AND gas_used = 21000
-- Output format matches robinhood_contract_level_aa_hourly (7922169) so it can be loaded with prepare_df_contract_level_aa_hourly
WITH base AS (
  SELECT
    date_trunc('hour', block_time) AS hour,
    "from" AS from_address
  FROM robinhood.transactions
  WHERE block_time < (SELECT least(date_trunc('hour', now()), date_trunc('hour', max(block_time))) FROM robinhood.transactions WHERE block_date >= current_date - interval '1' day)
    AND block_time >= from_iso8601_timestamp('{{start_time}}')
    AND block_date >= CAST(date_trunc('day', from_iso8601_timestamp('{{start_time}}')) AS date)
    AND "to" IS NOT NULL
    AND gas_used = 21000
  GROUP BY 1, 2
),

numbered AS (
  SELECT
    hour,
    from_address,
    row_number() OVER (PARTITION BY hour ORDER BY from_address) AS rn
  FROM base
)
SELECT
  hour,
  0x000000000000000000000000000000000000eeee AS address,
  CAST(floor((rn - 1) / 500000) AS bigint) AS chunk_id,
  array_agg(from_address ORDER BY from_address) AS from_addresses
FROM numbered
GROUP BY 1, 3
ORDER BY 1, 3;
