-- Raw Polygon PoS transactions, loaded by airflow/dags/raw/raw_polygon_pos.py (chain config 'polygon_pos' in src/adapters/rpc_funcs/chain_configs.py).
-- Same layout as ethereum_tx without input_data / max_fee_per_blob_gas (Polygon PoS has no blobs; only empty_input is kept to save space).
-- tx_fee, value and gas prices are in POL.
CREATE TABLE IF NOT EXISTS public.polygon_pos_tx (
    block_number              bigint,
    block_timestamp           timestamp without time zone,
    tx_hash                   bytea NOT NULL,
    nonce                     integer,
    position                  integer,
    from_address              bytea,
    to_address                bytea,
    tx_fee                    double precision,
    status                    integer,
    value                     double precision,
    gas_limit                 bigint,
    gas_used                  bigint,
    gas_price                 double precision,
    empty_input               boolean,
    max_fee_per_gas           double precision,
    max_priority_fee_per_gas  double precision,
    priority_fee_per_gas      double precision,
    tx_type                   character varying,
    block_date                date
);
CREATE UNIQUE INDEX IF NOT EXISTS polygon_pos_tx_pkey ON public.polygon_pos_tx USING btree (tx_hash);
CREATE INDEX IF NOT EXISTS polygon_pos_tx_block_number_idx ON public.polygon_pos_tx USING btree (block_number);
CREATE INDEX IF NOT EXISTS polygon_pos_tx_block_timestamp_idx ON public.polygon_pos_tx USING btree (block_timestamp);
CREATE INDEX IF NOT EXISTS polygon_pos_tx_block_date_idx ON public.polygon_pos_tx USING btree (block_date);
CREATE INDEX IF NOT EXISTS polygon_pos_tx_to_address_idx ON public.polygon_pos_tx USING btree (to_address);
CREATE INDEX IF NOT EXISTS polygon_pos_tx_tx_type_idx ON public.polygon_pos_tx USING btree (tx_type);
