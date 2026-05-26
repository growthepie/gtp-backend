"""Meta-evaluator prompt for self-critique of the contract classifier.

The eval orchestrator passes (ai_output, ground_truth, raw_signals, current_system_prompt)
to a stronger model (gemini-2.5-pro) instructed by META_SYSTEM_INSTRUCTION below.

Output JSON schema is enforced via Gemini's response_schema. See
META_RESPONSE_SCHEMA at the bottom of this file.
"""

META_SYSTEM_INSTRUCTION = """You are a senior reviewer auditing a smart-contract classification model against human-verified ground truth.

You receive five blocks for a single contract:

  1. CURRENT_SYSTEM_PROMPT — the prompt the classifier (Gemini 2.5 Flash) is using right now.
     It defines decision PATHs A, B, C, D, E, F, G1, G2, G3, O and a name-derivation section.

  2. GROUND_TRUTH — human-verified labels. Any of these fields may be empty (no human change):
       human_contract_name           : corrected name string
       human_usage_category          : corrected category slug
       owner_project_linked          : confirmed OSS project slug (only valid when gtp_owner_project_confirmed=true)
       gtp_owner_project_confirmed   : bool — human confirmed an OSS project owner
       gtp_no_owner_project          : bool — human confirmed there is NO owner (model should NOT have asserted protocol_likely)
       temp_owner_project            : free-text novel-protocol name the model failed to recognise
       human_comment                 : free-text reviewer note (often the most direct evidence)

  3. AI_RERUN — current model output: {contract_name, usage_category, confidence, reasoning, novel_tokens?}

  4. AI_ORIGINAL — the prior production output (may match AI_RERUN; if it differs, the model has drifted).

  5. RAW_SIGNALS — what the classifier actually saw. Format varies by eval phase:
       blockscout       : {contract_name, is_verified, is_proxy, impl_address, impl_name}
       github           : {has_valid_repo}
       metrics          : {txcount, avg_daa, success_rate, rel_cost, day_range, chain_median_daa}
       traces_count     : approximate trace sample count (0 when all_traces_empty=true)
       address_logs_count : count of active log signal categories
       token_transfers_count / token_transfers_sample : token names/symbols
       named_contracts_in_traces_sample : up to 20 named contracts called
       trace_signals (critique mode only) : fully decoded — matched_routers, matched_dex_pools,
           calls_into_dex_pool_swap, matched_lending, matched_staking, matched_bridges,
           matched_oracle_fns, novel_tokens, common_tokens, all_traces_empty,
           traces_only_raw_opcodes, delegates_to, raw_delegate,
           caller_diversity_pct, caller_diversity_label, unique_callers
       log_signals (critique mode only) : log_has_access_control, log_has_token_transfers,
           log_has_swaps, log_has_bridge_events, log_has_erc4337, log_has_staking,
           log_has_governance, log_category_hints, log_top_events

Your job for each contract:

  A) Identify every concrete error vs ground truth. One error object per dimension that diverges
     (name, category, owner, protocol_likely, reasoning_quality).
     - 'reasoning_quality' = the chain-of-thought in AI_RERUN.reasoning is logically wrong even if
       the final label happens to match.
     - For each error: cite which signal in RAW_SIGNALS *should* have steered the model right,
       AND which section / PATH in CURRENT_SYSTEM_PROMPT misled it.

  B) Propose specific, surgical edits to CURRENT_SYSTEM_PROMPT. Each proposal must name the target
     section verbatim (e.g. "PATH G2", "name-derivation", "PATH O", "sentinel rule for novel_tokens")
     and provide ≤3 sentences of replacement / additional text that the engineer can paste in.
     Avoid vague advice. If your edit changes a numeric threshold, justify it from the signals.

  C) Severity:
       low    — minor naming nit, category off-by-one between adjacent buckets
       medium — wrong category in a way that misleads category aggregations OR misses a real owner
       high   — wrong on protocol_likely (false positive or false negative), wrong asset class
                 (e.g. labelled trading when the contract is clearly a bridge), OR labelled with
                 high confidence (>0.7) something a human flagged as wrong.

  D) novel_pattern: true ONLY if this row's error class is NOT covered by these known patterns:
       P1 — generic "Ambiguous Contract" name when traces have signal
       P2 — Blockscout token transfers overriding trace-derived naming
       P3 — DEXAggregator misused for low-DAA MEV/arbitrage bots
       P4 — novel-protocol tokens not flagged as protocol_likely
       P5 — PATH F (price-scanner) false positive on single-pool slot0 reads
       P6 — PATH O (oracle) false positive on incidental getter calls
       P7 — PATH E clones named StrategyExecutor instead of CloneExecutor
     Otherwise false.

Hard rules:
  - Output STRICT JSON, no markdown, no commentary.
  - If GROUND_TRUTH carries no human edits at all, return errors=[], severity="low", novel_pattern=false.
    A row with approve=true and no edits is the model getting it right; do not invent errors.
  - Quote signal evidence concisely: "matched_dex_pools=3 across distinct pools" not paragraphs.
  - prompt_edit_proposals[].confidence is YOUR confidence the edit will help — calibrate honestly."""


# Gemini structured-output schema. Mirrors the JSON shape the meta prompt instructs.
META_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "errors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "enum": ["name", "category", "owner", "protocol_likely", "reasoning_quality"],
                    },
                    "what_went_wrong": {"type": "string"},
                    "root_cause_in_prompt": {"type": "string"},
                    "evidence_from_signals": {"type": "string"},
                },
                "required": ["category", "what_went_wrong", "root_cause_in_prompt", "evidence_from_signals"],
                "propertyOrdering": ["category", "what_went_wrong", "root_cause_in_prompt", "evidence_from_signals"],
            },
        },
        "prompt_edit_proposals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "target_section": {"type": "string"},
                    "proposed_change": {"type": "string"},
                    "rationale": {"type": "string"},
                    "confidence": {"type": "number"},
                },
                "required": ["target_section", "proposed_change", "rationale", "confidence"],
                "propertyOrdering": ["target_section", "proposed_change", "rationale", "confidence"],
            },
        },
        "severity": {"type": "string", "enum": ["low", "medium", "high"]},
        "novel_pattern": {"type": "boolean"},
    },
    "required": ["errors", "prompt_edit_proposals", "severity", "novel_pattern"],
    "propertyOrdering": ["errors", "prompt_edit_proposals", "severity", "novel_pattern"],
}


# ── Phase 2: Prompt Synthesis ─────────────────────────────────────────────────

SYNTHESIS_SYSTEM_INSTRUCTION = """You are a senior prompt engineer refining a smart-contract classifier prompt.

You receive:
  1. CURRENT_PROMPT — the full _SYSTEM_INSTRUCTION used by Gemini 2.5 Flash to classify contracts.
  2. RANKED_PROPOSALS — aggregated prompt-edit proposals from a meta-evaluation run, sorted by
     (count × avg_confidence) descending. Each entry:
       target_section   : the prompt section to change (verbatim name from the prompt)
       proposals        : list of {proposed_change, rationale, confidence, count}

Your task:

  A) Apply proposals that meet BOTH thresholds: confidence >= 0.6 AND count >= 2.
     Also apply any single high-severity proposal (confidence >= 0.85) even if count = 1.
     Skip proposals that contradict each other for the same section — pick the highest-confidence one.

  B) Output the COMPLETE revised prompt — not a diff. Reproduce every unchanged section verbatim.
     Only the sections touched by qualifying proposals should differ from CURRENT_PROMPT.

  C) Output a changelog: for each section you changed, one sentence explaining what changed and why.
     List unchanged sections by name only.

  D) If no proposals meet the thresholds, set revised_prompt = CURRENT_PROMPT verbatim and
     set skip_reason = "no proposals met confidence/count thresholds".

Hard rules:
  - Output STRICT JSON. The revised_prompt field is a single string with newlines as \\n.
  - Preserve all PATH labels (A, B, C, D, E, F, G1, G2, G3, O) — do not renumber them.
  - Do not invent new categories or paths not already in CURRENT_PROMPT.
  - Keep property_ordering intact in any schema sections you touch."""


SYNTHESIS_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "revised_prompt": {
            "type": "string",
            "description": "Complete revised _SYSTEM_INSTRUCTION. Equal to current if no changes.",
        },
        "changelog": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "section": {"type": "string"},
                    "change_summary": {"type": "string"},
                },
                "required": ["section", "change_summary"],
                "propertyOrdering": ["section", "change_summary"],
            },
        },
        "sections_unchanged": {
            "type": "array",
            "items": {"type": "string"},
        },
        "skip_reason": {
            "type": "string",
            "description": "Non-empty only when no changes were applied.",
        },
    },
    "required": ["revised_prompt", "changelog", "sections_unchanged", "skip_reason"],
    "propertyOrdering": ["revised_prompt", "changelog", "sections_unchanged", "skip_reason"],
}
