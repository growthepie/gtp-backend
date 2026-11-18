-- Raw Robinhood Chain transactions, loaded by airflow/dags/raw/raw_robinhood.py (chain config 'arbitrum_nitro', Orbit chain).
-- Same layout and indexes as arbitrum_tx. Fees in ETH. ~5.5M tx/day (~2.4 GB/day incl. indexes): runs_archive_raw_tx / runs_delete_archived_raw_tx are on.
CREATE TABLE IF NOT EXISTS public.robinhood_tx (
    block_number     bigint,
    block_timestamp  timestamp without time zone,
    tx_hash          bytea NOT NULL,
    from_address     bytea,
    to_address       bytea,
    tx_fee           double precision,
    status           integer,
    value            double precision,
    gas_limit        bigint,
    gas_used         bigint,
    gas_price        double precision,
    empty_input      boolean,
    l1_gas_used      bigint,
    block_date       date,
    tx_type          text,
    nonce            integer
);
CREATE UNIQUE INDEX IF NOT EXISTS robinhood_tx_pkey ON public.robinhood_tx USING btree (tx_hash);
CREATE INDEX IF NOT EXISTS robinhood_tx_block_number_idx ON public.robinhood_tx USING btree (block_number);
CREATE INDEX IF NOT EXISTS robinhood_tx_block_timestamp_idx ON public.robinhood_tx USING btree (block_timestamp);
CREATE INDEX IF NOT EXISTS robinhood_tx_block_date_idx ON public.robinhood_tx USING btree (block_date);
CREATE INDEX IF NOT EXISTS robinhood_tx_to_address_idx ON public.robinhood_tx USING btree (to_address);
CREATE INDEX IF NOT EXISTS robinhood_tx_empty_input_idx ON public.robinhood_tx USING btree (empty_input);
CREATE INDEX IF NOT EXISTS robinhood_tx_tx_type_idx ON public.robinhood_tx USING btree (tx_type);
