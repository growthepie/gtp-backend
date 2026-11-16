-- Dune query 9121926 (https://dune.com/queries/9121926). Used by other_qb_robinhood.pull_data_from_dune.
-- v2 of Robinhood_stock_list (5429585): the {{days}} window is applied inside the arbitrum.transactions / logs / creation_traces scans
-- instead of only at the end (the original scanned full history on every run). Same output.
-- There are two ways Robinhood stock contracts are initialized:
-- 1. 
    -- Through the setMetadata(string,string) function calls (4 bytes: 0x51335b50)
    -- Tracked via arbitrum.transactions calldata. As it can be called multiple times, we only get the latest transaction per contract.
-- 2.
    -- At initialization, they emit a log with topic0 = 0xd9b0c6a1c0de228715ad0fa09f3259686ee84f8cc675e03ef7e47a9cdafa76d6
    -- Tracked via arbitrum.logs

-- because sometimes stock names and tickers are updated, we group once more by contract address at the end


-- 1. --
WITH tx_hex_data AS (
    SELECT 
        *,
        -- Convert data to hex string for easier parsing
        to_hex(data) AS hex_string,
        -- Rank transactions by block_number descending for each contract
        ROW_NUMBER() OVER (PARTITION BY "to" ORDER BY block_number DESC) as rn
    FROM arbitrum.transactions
    WHERE 
        bytearray_substring("data", 1, 4) = 0x51335b50 -- setMetadata function
        AND block_number > 352875388
        AND block_date >= CAST(current_date - interval '{{days}}' day AS date) -- v2: window applied in the scan (latest tx in window = latest tx overall if it falls in the window)
        AND success = True
        --AND "from" = 0x1ac52464C695d00856a6628227A343Eaa329Be39 -- admin account
),
latest_transactions AS (
    SELECT *
    FROM tx_hex_data
    WHERE rn = 1
),
parsed_offsets AS (
    SELECT 
        *,
        -- Extract parameter offsets from hex string
        -- Skip function signature (8 chars) and extract next 64 chars for first offset
        from_base(substr(hex_string, 9, 64), 16) AS param1_offset,
        -- Extract second offset (next 64 chars)
        from_base(substr(hex_string, 73, 64), 16) AS param2_offset
    FROM latest_transactions
),
string_info AS (
    SELECT 
        *,
        -- Calculate positions in hex string (multiply by 2 for hex chars, add 8 for function sig)
        (param1_offset * 2) + 8 + 1 AS param1_hex_pos,
        (param2_offset * 2) + 8 + 1 AS param2_hex_pos
    FROM parsed_offsets
),
string_lengths AS (
    SELECT 
        *,
        -- Extract string lengths (32 bytes = 64 hex chars at each offset)
        from_base(substr(hex_string, param1_hex_pos, 64), 16) AS param1_length,
        from_base(substr(hex_string, param2_hex_pos, 64), 16) AS param2_length,
    
        -- Calculate start positions of actual string data (after 32-byte length field)
        param1_hex_pos + 64 AS param1_data_pos,
        param2_hex_pos + 64 AS param2_data_pos
    FROM string_info
),
stock_list_1 AS(
    SELECT 
        block_time,
        --block_date,
        block_number,
        --"from",
        "to" AS contract_address,
        --hash,
      
        -- Decode first string parameter (metadata key)
        CASE 
            WHEN param1_length > 0 AND param1_length <= 1000 THEN
                from_utf8(from_hex(substr(hex_string, param1_data_pos, param1_length * 2)))
            ELSE NULL
        END AS name,
      
        -- Decode second string parameter (metadata value)
        CASE 
            WHEN param2_length > 0 AND param2_length <= 1000 THEN
                from_utf8(from_hex(substr(hex_string, param2_data_pos, param2_length * 2)))
            ELSE NULL
        END AS ticker--,
        
        -- Include hex string for reference/debugging
        --hex_string,
        
        -- Include intermediate values for debugging
        --param1_offset,
        --param2_offset,
        --param1_length,
        --param2_length
    FROM string_lengths
    WHERE 
        block_time >= current_date - interval '{{days}}' day
    ORDER BY block_number
),

-- 2. --
logs_hex_data AS (
    SELECT
        block_time,
        block_number,
        tx_hash,
        contract_address,
        TO_HEX(data) as hex_string,
        LENGTH(TO_HEX(data)) as data_length
    FROM arbitrum.logs
    WHERE
        contract_address = 0xd29AA2B0584856C2B3b097595E9D4B1Cd1242e05
        AND block_number > 355335707
        AND block_date >= CAST(current_date - interval '{{days}}' day AS date) -- v2: window applied in the scan
        AND topic0 = 0xd9b0c6a1c0de228715ad0fa09f3259686ee84f8cc675e03ef7e47a9cdafa76d6
        AND LENGTH(TO_HEX(data)) > 384
),
deployment_traces AS(
    SELECT
        address AS contract_address,
        tx_hash
    FROM arbitrum.creation_traces
    WHERE
        "from" = 0xd29AA2B0584856C2B3b097595E9D4B1Cd1242e05
        AND block_time >= current_date - interval '{{days}}' day -- v2: deployment is in the same tx as the joined log
),
stock_list_2 AS(
    SELECT
        h.block_time,
        h.block_number,
        --h.tx_hash,
        d.contract_address,
        --h.hex_string,
        --h.data_length,
        CASE
            WHEN h.data_length = 448 THEN 
                TRIM(TRAILING CHR(0) FROM FROM_UTF8(FROM_HEX(SUBSTR(h.hex_string, 257, 64))))
            WHEN h.data_length = 512 THEN 
                TRIM(TRAILING CHR(0) FROM FROM_UTF8(FROM_HEX(SUBSTR(h.hex_string, 257, 128))))
            WHEN h.data_length = 576 THEN 
                TRIM(TRAILING CHR(0) FROM FROM_UTF8(FROM_HEX(SUBSTR(h.hex_string, 257, 192))))
        END AS name,
        CASE
            WHEN h.data_length = 448 THEN 
                TRIM(TRAILING CHR(0) FROM FROM_UTF8(FROM_HEX(SUBSTR(h.hex_string, 385, 64))))
            WHEN h.data_length = 512 THEN 
                TRIM(TRAILING CHR(0) FROM FROM_UTF8(FROM_HEX(SUBSTR(h.hex_string, 449, 64))))
            WHEN h.data_length = 576 THEN 
                TRIM(TRAILING CHR(0) FROM FROM_UTF8(FROM_HEX(SUBSTR(h.hex_string, 513, 64))))
        END AS ticker
    FROM logs_hex_data h
    LEFT JOIN deployment_traces d ON h.tx_hash = d.tx_hash
    WHERE 
        h.block_time >= current_date - interval '{{days}}' day
),

-- Union --
combined_stocks AS (
    SELECT * FROM stock_list_1
    UNION ALL
    SELECT * FROM stock_list_2
),

-- Group by contract_address and keep most recent row based on block_time (e.g. 0x56a022ba00e93121086f93262fc007baa6b1edd5 which changed ticker)
ranked_stocks AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY block_time DESC) as rn
    FROM combined_stocks
)

SELECT 
    block_time,
    block_number,
    contract_address,
    name,
    ticker
FROM ranked_stocks
WHERE rn = 1
ORDER BY block_time DESC
