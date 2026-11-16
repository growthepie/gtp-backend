-- v2 (incremental): returns complete days >= start_date (YYYY-MM-DD) up to the last day Dune has fully ingested
-- (block_date < date_trunc('day', max(block_time))). Airflow passes start_date = last day loaded in our DB + 1.
-- Dune query 9134314 (https://dune.com/queries/9134314), generated from 6587405.
with price_strk as (
    select 
        date_trunc('day', timestamp) as day,
        max(price) as price
    from prices.day
    where blockchain = 'ethereum' and contract_address in (0xca14007eff0db1f8135f4c25b34de49ab0d42766) -- STRK
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
        strk.day,
        strk.price as price_strk,
        eth.price as price_eth,
        strk.price/eth.price as strk_per_eth
    from price_strk strk
    inner join price_eth eth on strk.day = eth.day
),

calls_enriched as (
   SELECT
       c.block_date,
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
    WHERE c.block_date < (SELECT least(current_date, CAST(date_trunc('day', max(block_time)) AS date)) FROM starknet.calls WHERE block_date >= current_date - interval '2' day)
        and c.block_date >= CAST('{{start_date}}' AS date)
        and t.block_date < (SELECT least(current_date, CAST(date_trunc('day', max(block_time)) AS date)) FROM starknet.calls WHERE block_date >= current_date - interval '2' day)
        and t.block_date >= CAST('{{start_date}}' AS date)

        and c.contract_address is not null
        and c.transaction_type = 'INVOKE' --exclude DECLARE and DEPLOY_ACCOUNT
        and c.call_type = 'CALL' -- exclude DELEGATE and internal cairo helpers
        and c.context = 'EXECUTE' -- exclude fee transfer, signature checks, etc
        --and entry_point_type = 'EXTERNAL' -- excludes constructore, l1 handler
)

SELECT
    block_date as day,
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
