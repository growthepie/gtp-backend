-- Dune query 9121925 (https://dune.com/queries/9121925), v2 of 5302614. Used by metrics_dune.run_fact_kpis.
/*──────────────────────────────────────────────────────────────
  1. ─ INITIATED PAYMENTS
     • Two-step flow : PaymentAuthorized
     • One-step flow : PaymentCharged (doubles as init + settle)

  TOKEN GATE: amount < 1e15 raw (= $1B USDC) excludes 18-decimal
  non-USDC tokens (e.g. GITLAWB, BNKR) whose raw amounts start at
  1e18 and would inflate volume by ~1e12 when divided by 1e6.
──────────────────────────────────────────────────────────────*/

WITH authorized AS (           -- two-step init
    SELECT
        paymentInfoHash,
        evt_block_time                                 AS authorized_at,
        amount / 1e6                                   AS auth_amt_usdc,
        JSON_EXTRACT_SCALAR(paymentInfo,'$.payer')     AS payer,
        JSON_EXTRACT_SCALAR(paymentInfo,'$.receiver')  AS receiver,
        JSON_EXTRACT_SCALAR(paymentInfo,'$.operator')  AS operator
    FROM commerce_payments_protocol_base.authcaptureescrow_evt_paymentauthorized
    WHERE amount < UINT256 '1000000000000000'
),

charged_init AS (              -- one-step init (same ts as settle)
    SELECT
        paymentInfoHash,
        evt_block_time                                 AS authorized_at,
        amount / 1e6                                   AS auth_amt_usdc,
        JSON_EXTRACT_SCALAR(paymentInfo,'$.payer')     AS payer,
        JSON_EXTRACT_SCALAR(paymentInfo,'$.receiver')  AS receiver,
        JSON_EXTRACT_SCALAR(paymentInfo,'$.operator')  AS operator
    FROM commerce_payments_protocol_base.authcaptureescrow_evt_paymentcharged
    WHERE amount < UINT256 '1000000000000000'
),

initiated AS (                 -- union = every checkout
    SELECT * FROM authorized
    UNION ALL
    SELECT * FROM charged_init
)

-- Get distinct payers per day
, distinct_payers_by_day AS (
  SELECT
    DATE_TRUNC('day', authorized_at) AS day,
    payer
  FROM initiated
  GROUP BY 1, 2
)

-- Count how many unique payers existed up to each day
, running_unique_payers AS (   -- cumulative count of first-seen payers (replaces a d2.day <= d1.day self-join)
  SELECT
    d.day,
    SUM(COALESCE(n.new_payers, 0)) OVER (ORDER BY d.day) AS total_unique_payers
  FROM (SELECT DISTINCT day FROM distinct_payers_by_day) d
  LEFT JOIN (
    SELECT first_day AS day, COUNT(payer) AS new_payers
    FROM (SELECT payer, MIN(day) AS first_day FROM distinct_payers_by_day GROUP BY 1)
    GROUP BY 1
  ) n ON n.day = d.day
)

, distinct_receivers_by_day AS (
  SELECT
    DATE_TRUNC('day', authorized_at) AS day,
    receiver
  FROM initiated
  GROUP BY 1, 2
)

-- Count how many unique receivers existed up to each day
, running_unique_receivers AS (   -- cumulative count of first-seen receivers (replaces a d2.day <= d1.day self-join)
  SELECT
    d.day,
    SUM(COALESCE(n.new_receivers, 0)) OVER (ORDER BY d.day) AS total_unique_receivers
  FROM (SELECT DISTINCT day FROM distinct_receivers_by_day) d
  LEFT JOIN (
    SELECT first_day AS day, COUNT(receiver) AS new_receivers
    FROM (SELECT receiver, MIN(day) AS first_day FROM distinct_receivers_by_day GROUP BY 1)
    GROUP BY 1
  ) n ON n.day = d.day
)

/* ─  NEW / RETURNING FLAG  ─ */
, payer_first_seen AS (
    SELECT
        payer,
        MIN(authorized_at) AS first_seen_at
    FROM initiated
    GROUP BY 1
)

, receiver_first_seen AS (
    SELECT
        receiver,
        MIN(authorized_at) AS first_seen_at
    FROM initiated
    GROUP BY 1
)

, initiated_new AS ( -- add new vs returning cohort
    SELECT
        i.*,
        CASE
            WHEN DATE_TRUNC('day',fs.first_seen_at) = DATE_TRUNC('day', i.authorized_at)
                 THEN 'new'
            ELSE 'returning'
        END AS payer_cohort,
        CASE
            WHEN DATE_TRUNC('day',fr.first_seen_at) = DATE_TRUNC('day', i.authorized_at)
                 THEN 'new'
            ELSE 'returning'
        END AS receiver_cohort
    FROM initiated            i
    JOIN payer_first_seen     fs ON fs.payer          = i.payer
    JOIN receiver_first_seen  fr ON fr.receiver = i.receiver
)

/*──────────────────────────────────────────────────────────────
  2. ─ SETTLED PAYMENTS
     • capture() = 2-step settle
     • charge()  = 1-step settle
──────────────────────────────────────────────────────────────*/
, captured AS (                  -- two-step settle
    SELECT
        paymentInfoHash,
        evt_block_time                           AS settled_at,
        amount / 1e6                             AS settled_usdc,
        feeBps,
        amount / 1e6 * feeBps / 1e4              AS fee_usdc
    FROM commerce_payments_protocol_base.authcaptureescrow_evt_paymentcaptured
    WHERE amount < UINT256 '1000000000000000'
),

charged_final AS (             -- one-step settle
    SELECT
        paymentInfoHash,
        evt_block_time                           AS settled_at,
        amount / 1e6                             AS settled_usdc,
        feeBps,
        amount / 1e6 * feeBps / 1e4              AS fee_usdc
    FROM commerce_payments_protocol_base.authcaptureescrow_evt_paymentcharged
    WHERE amount < UINT256 '1000000000000000'
),

finalised AS (
    SELECT * FROM captured
    UNION ALL
    SELECT * FROM charged_final
),

/*──────────────────────────────────────────────────────────────
  3. ─ REFUNDS  (includes partial or full refund amounts)
──────────────────────────────────────────────────────────────*/
refunded AS (
    SELECT
        paymentInfoHash,
        evt_block_time                           AS refunded_at,
        amount / 1e6                             AS refunded_usdc
    FROM commerce_payments_protocol_base.authcaptureescrow_evt_paymentrefunded
    WHERE amount < UINT256 '1000000000000000'
)

/*──────────────────────────────────────────────────────────────
  4. ─ RECLAIMS  (from expired authorizations)
──────────────────────────────────────────────────────────────*/
, reclaimed AS (
    SELECT
        paymentInfoHash,
        evt_block_time                           AS reclaimed_at,
        amount / 1e6                             AS reclaimed_usdc
    FROM commerce_payments_protocol_base.authcaptureescrow_evt_paymentreclaimed
    WHERE amount < UINT256 '1000000000000000'
)

/*──────────────────────────────────────────────────────────────
  5. ─ VOIDED  (from canceled authorizations)
──────────────────────────────────────────────────────────────*/
, voided AS (
    SELECT
        paymentInfoHash,
        evt_block_time                           AS voided_at,
        amount / 1e6                             AS voided_usdc
    FROM commerce_payments_protocol_base.authcaptureescrow_evt_paymentvoided
    WHERE amount < UINT256 '1000000000000000'
)

/*──────────────────────────────────────────────────────────────
  6. ─ DAILY KPI ROLL-UP
──────────────────────────────────────────────────────────────*/

, daily_vals as (
    SELECT
      -- time grain
      DATE_TRUNC('day', i.authorized_at)                                        AS day,
      
      /* ───── Funnel counts ───── */
      --COUNT(DISTINCT i.paymentInfoHash)                                         AS payments_initiated,
      --COUNT(DISTINCT a.paymentInfoHash)                                         AS payments_authorized,
      COUNT(DISTINCT f.paymentInfoHash)                                         AS payments_settled,
      --COUNT(DISTINCT r.paymentInfoHash)                                         AS payments_refuned,
      --COUNT(DISTINCT c.paymentInfoHash)                                         AS payments_reclaimed,
      --COUNT(DISTINCT v.paymentInfoHash)                                         AS payments_voided,
      /*COUNT(DISTINCT a.paymentInfoHash) 
      - COUNT(DISTINCT v.paymentInfoHash) 
      - COUNT(DISTINCT c.paymentInfoHash)                                       AS payments_not_voided_reclaim,*/
    
      /* ───── New vs returning ───── */
      COUNT(DISTINCT CASE WHEN i_n.payer_cohort = 'new' THEN i.payer END)           AS new_payers,
      COUNT(DISTINCT CASE WHEN i_n.payer_cohort = 'returning' THEN i.payer END)     AS returning_payers,
      COUNT(DISTINCT CASE WHEN i_n.receiver_cohort = 'new' THEN i.receiver END)           AS new_merchants,
      COUNT(DISTINCT CASE WHEN i_n.receiver_cohort = 'returning' THEN i.receiver END)     AS returning_merchants,
      
      /* ───── Initiated footprint ───── */
      COUNT(DISTINCT i.payer)                                                   AS daily_unique_payers,
      COUNT(DISTINCT i.receiver)                                                AS daily_unique_merchants,
      AVG(up.total_unique_payers) AS total_unique_payers,
      AVG(ur.total_unique_receivers) AS total_unique_merchants,
      --COUNT(DISTINCT i.operator)                                                AS init_unique_operators,
      
      /* ───── Settled footprint ───── */
      --COUNT(DISTINCT CASE WHEN f.paymentInfoHash IS NOT NULL THEN i.payer END)  AS settled_unique_payers,
      --COUNT(DISTINCT CASE WHEN f.paymentInfoHash IS NOT NULL THEN i.receiver END) AS settled_unique_merchants,
      --COUNT(DISTINCT CASE WHEN f.paymentInfoHash IS NOT NULL THEN i.operator END) AS settled_unique_operators,
      
      /* ───── Volumes (settled only) ───── */
      SUM(f.settled_usdc)                                                       AS gross_volume_usdc,
      --SUM(r.refunded_usdc)                                                      AS refunds_usdc,
      --SUM(c.reclaimed_usdc)                                                     AS reclaims_usdc,
      --SUM(v.voided_usdc)                                                        AS voids_usdc,
      SUM(f.fee_usdc)                                                           AS protocol_fees_usdc,
    
      SUM(f.settled_usdc) /
      COUNT(DISTINCT f.paymentInfoHash)                                         AS vol_per_payment,
      SUM(f.settled_usdc) /
      COUNT(DISTINCT CASE WHEN f.paymentInfoHash IS NOT NULL THEN i.payer END)  AS vol_per_payer
      
    FROM   initiated i
    LEFT JOIN authorized a ON i.paymentInfoHash = a.paymentInfoHash
    LEFT JOIN finalised f ON i.paymentInfoHash = f.paymentInfoHash
    LEFT JOIN initiated_new i_n  ON i.paymentInfoHash = i_n.paymentInfoHash
    LEFT JOIN refunded  r  ON i.paymentInfoHash = r.paymentInfoHash
    LEFT JOIN reclaimed c  ON i.paymentInfoHash = c.paymentInfoHash
    LEFT JOIN voided  v  ON i.paymentInfoHash = v.paymentInfoHash
    LEFT JOIN running_unique_receivers ur ON DATE_TRUNC('day', i.authorized_at) = ur.day
    LEFT JOIN running_unique_payers up ON DATE_TRUNC('day', i.authorized_at) = up.day
    WHERE  i.authorized_at >= current_date - interval '{{days}}' day AND i.authorized_at < Current_Date
    GROUP  BY 1
    ORDER  BY 1 DESC
)

SELECT
  day,
  'shopify_usdc' as origin_key,
  metric_key,
  value
FROM daily_vals
CROSS JOIN UNNEST(
    ARRAY['payments_settled', 'daily_unique_payers', 'total_unique_payers', 'daily_unique_merchants', 'total_unique_merchants', 'gross_volume_usdc', 'protocol_fees_usdc', 'vol_per_payment', 'vol_per_payer', 'new_payers', 'returning_payers', 'new_merchants', 'returning_merchants'],
    ARRAY[payments_settled, daily_unique_payers, total_unique_payers, daily_unique_merchants, total_unique_merchants, gross_volume_usdc, protocol_fees_usdc, vol_per_payment, vol_per_payer, new_payers, returning_payers, new_merchants, returning_merchants]
) AS t(metric_key, value)
