-- Dune query 9121927 (https://dune.com/queries/9121927). Used by other_qb_robinhood.pull_data_from_dune.
-- v2 of Robinhood_stock_daily_values (5435746): reads arbitrum.logs directly with a block_date partition filter
-- instead of query_5435466 (all TransferShares logs since launch joined with the full-history stock list query).
-- Not limited to the stock contracts: other contracts can emit TransferShares too. The DAG keeps only contracts in robinhood_stock_list.
SELECT
    block_date AS date,
    contract_address,
    SUM(CASE WHEN varbinary_substring(topic1, 13, 20) = 0x0000000000000000000000000000000000000000 THEN bytearray_to_uint256("data") / POWER(10, 18) ELSE 0 END) AS total_minted,
    SUM(CASE WHEN varbinary_substring(topic1, 13, 20) <> 0x0000000000000000000000000000000000000000
              AND varbinary_substring(topic2, 13, 20) = 0x0000000000000000000000000000000000000000 THEN bytearray_to_uint256("data") / POWER(10, 18) ELSE 0 END) AS total_burned,
    SUM(CASE WHEN varbinary_substring(topic1, 13, 20) <> 0x0000000000000000000000000000000000000000
              AND varbinary_substring(topic2, 13, 20) <> 0x0000000000000000000000000000000000000000 THEN bytearray_to_uint256("data") / POWER(10, 18) ELSE 0 END) AS total_transferred
FROM arbitrum.logs
WHERE block_date < current_date
  AND block_date >= current_date - interval '{{days}}' day
  AND block_number > 339773154 -- first transfer (mint) on 339773155
  AND topic0 = 0x9d9c909296d9c674451c0c24f02cb64981eb3b727f99865939192f880a755dcb -- "TransferShares"
GROUP BY 1, 2
ORDER BY 1 DESC
