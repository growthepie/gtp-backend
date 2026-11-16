-- One row per Dune execution run through AdapterDune (credit cost tracking), written by AdapterDune.log_execution_cost.
-- Billed credits per execution = compute_credits + result_bytes / 1e6 * export rate (Enterprise ~0.5, Plus 2 credits per MB).
CREATE TABLE IF NOT EXISTS public.dune_execution_log (
    execution_id          varchar PRIMARY KEY,
    query_id              bigint NOT NULL,
    query_name            varchar,
    params                varchar,
    performance           varchar,
    state                 varchar,
    submitted_at          timestamptz,
    execution_started_at  timestamptz,
    execution_ended_at    timestamptz,
    pending_ms            bigint,
    execution_ms          bigint,
    compute_credits       double precision,
    result_rows           bigint,
    result_bytes          bigint,
    datapoints            bigint,
    dag_id                varchar,
    task_id               varchar,
    logged_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dune_execution_log_submitted_at_idx ON public.dune_execution_log (submitted_at);
CREATE INDEX IF NOT EXISTS dune_execution_log_query_id_idx ON public.dune_execution_log (query_id);
