import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

# this is fully vibe coded

import pandas as pd
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from src.adapters.abstract_adapters import AbstractAdapter


class AdapterFirstBlockOfDay(AbstractAdapter):
    """
    adapter_params:
        - none

    extract_params:
        - origin_keys: list[str]
    """

    METRIC_KEY = "first_block_of_day"
    DEFAULT_AVG_BLOCK_TIME = 12.0

    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("First block of day", adapter_params, db_connector)
        self._block_ts_cache: Dict[Tuple[str, int], int] = {}
        self._rpc_get_block_calls = 0
        self._rpc_candidates_by_chain: Dict[str, List[str]] = {}

    def extract(self, extract_params: dict) -> pd.DataFrame:
        origin_keys = extract_params.get("origin_keys")
        if origin_keys is None:
            raise ValueError("extract_params must include `origin_keys`.")
        if not isinstance(origin_keys, list):
            raise ValueError("`origin_keys` must be a list.")

        if origin_keys == ["*"]:
            origin_keys = self._get_all_chains_with_rpcs()

        origin_keys = [ok for ok in dict.fromkeys(origin_keys) if ok]
        if not origin_keys:
            return pd.DataFrame(columns=["metric_key", "origin_key", "date", "value"])

        progress_df = self._get_chain_progress(origin_keys)
        progress_map: Dict[str, dict] = {}
        if not progress_df.empty:
            progress_df["date"] = pd.to_datetime(progress_df["date"]).dt.date
            progress_map = progress_df.set_index("origin_key").to_dict(orient="index")

        rows: List[dict] = []
        for origin_key in origin_keys:
            try:
                # exception here for starknet
                if origin_key == 'starknet':
                    rows.extend(self._extract_starknet(progress_map.get(origin_key)))
                else:
                    rows.extend(self._extract_chain(origin_key, progress_map.get(origin_key)))
            except Exception as exc:
                print(f"Failed to extract first_block_of_day for {origin_key}: {exc}")

        if not rows:
            return pd.DataFrame(columns=["metric_key", "origin_key", "date", "value"])

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["value"])
        df["value"] = df["value"].astype(int)
        df = df[df["value"] != 0]
        df = df.drop_duplicates(subset=["metric_key", "origin_key", "date"], keep="last")
        df = df.sort_values(by=["origin_key", "date"]).reset_index(drop=True)
        return df[["metric_key", "origin_key", "date", "value"]]

    def load(self, df: pd.DataFrame):
        if df is None or df.empty:
            print("No first_block_of_day rows to load.")
            return 0

        df_upload = df.copy()
        df_upload["date"] = pd.to_datetime(df_upload["date"]).dt.date
        df_upload["value"] = pd.to_numeric(df_upload["value"], errors="coerce").astype("Int64")
        df_upload = df_upload.dropna(subset=["value"])
        df_upload["value"] = df_upload["value"].astype(int)
        df_upload = df_upload[df_upload["value"] != 0]
        df_upload = df_upload.drop_duplicates(subset=["metric_key", "origin_key", "date"], keep="last")
        df_upload = df_upload.set_index(["metric_key", "origin_key", "date"])

        print(f"[DB] UPSERT fact_kpis: {len(df_upload)} row(s)")
        upserted = self.db_connector.upsert_table("fact_kpis", df_upload)
        print(f"Loaded {upserted} first_block_of_day records into fact_kpis.")
        return upserted

    def _extract_chain(self, origin_key: str, progress_row: Optional[dict]) -> List[dict]:
        w3 = self._connect_web3(origin_key)
        latest_block = int(w3.eth.block_number)
        latest_block, latest_ts = self._get_latest_fetchable_block_and_ts(origin_key, w3, latest_block)
        block0_ts = self._get_block_timestamp(origin_key, w3, 0)

        # Pull up to current UTC day when it has started from a UTC perspective.
        # (first block of day is final once UTC midnight has passed)
        max_target_date = min(
            datetime.fromtimestamp(latest_ts, tz=timezone.utc).date(),
            datetime.now(timezone.utc).date(),
        )

        chain_start_block, chain_start_ts = self._get_chain_start_block_and_ts(origin_key, w3, latest_block)
        chain_start_date = datetime.fromtimestamp(chain_start_ts, tz=timezone.utc).date()

        if max_target_date < chain_start_date:
            print(f"{origin_key}: no completed day to backfill yet.")
            return []

        resume_from = chain_start_date
        if progress_row:
            latest_db_date = progress_row.get("date")
            if latest_db_date is not None and pd.notna(latest_db_date):
                resume_from = max(chain_start_date, latest_db_date + timedelta(days=1))

        if resume_from > max_target_date:
            print(f"{origin_key}: no missing first_block_of_day data.")
            return []

        missing_dates = pd.date_range(start=resume_from, end=max_target_date, freq="D")[::-1].date
        print(
            f"{origin_key}: filling {len(missing_dates)} missing day(s), "
            f"from {missing_dates[-1]} to {missing_dates[0]}."
        )

        rows: List[dict] = []
        search_high = latest_block
        avg_block_time = self._resolve_avg_block_time(progress_row)
        learned_blocks_per_day = max(1.0, 86400.0 / max(avg_block_time, 0.1))
        learned_blocks_per_day_std = max(128.0, learned_blocks_per_day * 0.15)
        prev_found_date: Optional[date] = None
        prev_found_block: Optional[int] = None
        prev_found_ts: Optional[int] = None
        for target_date in missing_dates:
            calls_before = self._rpc_get_block_calls
            target_ts = self._start_of_day_ts(target_date)
            target_ts_next_day = target_ts + 86400

            block_number = None
            if prev_found_date is not None and prev_found_block is not None:
                day_delta = max(1, (prev_found_date - target_date).days)
                expected_delta_blocks = max(1, int(round(learned_blocks_per_day * day_delta)))
                center = max(chain_start_block, int(prev_found_block) - expected_delta_blocks)
                uncertainty = max(
                    128,
                    int((learned_blocks_per_day_std * day_delta * 3.0) + (expected_delta_blocks * 0.03)),
                )
                narrow_low = max(chain_start_block, center - uncertainty)
                narrow_high = min(search_high, center + uncertainty)

                if narrow_low < narrow_high:
                    block_number = self._find_first_block_by_timestamp(
                        origin_key=origin_key,
                        w3=w3,
                        target_ts=target_ts,
                        low=narrow_low,
                        high=narrow_high,
                        latest_block=latest_block,
                        floor_block=chain_start_block,
                    )

            if block_number is None:
                block_number = self._find_first_block_by_timestamp(
                    origin_key=origin_key,
                    w3=w3,
                    target_ts=target_ts,
                    low=chain_start_block,
                    high=search_high,
                    latest_block=latest_block,
                    floor_block=chain_start_block,
                )
            if block_number is None:
                calls_used = self._rpc_get_block_calls - calls_before
                print(f"{origin_key} {target_date}: no block found, rpc get_block calls={calls_used}")
                continue

            try:
                block_ts = self._get_block_timestamp(origin_key, w3, block_number)
            except Exception as exc:
                calls_used = self._rpc_get_block_calls - calls_before
                print(
                    f"{origin_key} {target_date}: failed to validate block {block_number}: {exc}, "
                    f"rpc get_block calls={calls_used}"
                )
                continue
            if block_ts < target_ts or block_ts >= target_ts_next_day:
                calls_used = self._rpc_get_block_calls - calls_before
                print(
                    f"{origin_key} {target_date}: block {block_number} outside UTC day window, "
                    f"rpc get_block calls={calls_used}"
                )
                continue

            # Strict UTC-first validation:
            # previous block must be before UTC midnight and current block must be inside target day.
            needs_fallback = False
            if int(block_number) > int(chain_start_block):
                try:
                    prev_ts = self._get_block_timestamp(origin_key, w3, int(block_number) - 1)
                except Exception as exc:
                    calls_used = self._rpc_get_block_calls - calls_before
                    print(
                        f"{origin_key} {target_date}: failed to validate previous block "
                        f"{int(block_number) - 1}: {exc}, rpc get_block calls={calls_used}"
                    )
                    continue
                if prev_ts >= target_ts:
                    needs_fallback = True

            if needs_fallback:
                block_number = self._find_first_block_by_timestamp(
                    origin_key=origin_key,
                    w3=w3,
                    target_ts=target_ts,
                    low=chain_start_block,
                    high=search_high,
                    latest_block=latest_block,
                    floor_block=chain_start_block,
                )
                if block_number is None:
                    calls_used = self._rpc_get_block_calls - calls_before
                    print(f"{origin_key} {target_date}: fallback search found no block, rpc get_block calls={calls_used}")
                    continue

                try:
                    block_ts = self._get_block_timestamp(origin_key, w3, block_number)
                except Exception as exc:
                    calls_used = self._rpc_get_block_calls - calls_before
                    print(
                        f"{origin_key} {target_date}: failed to validate fallback block "
                        f"{block_number}: {exc}, rpc get_block calls={calls_used}"
                    )
                    continue
                if block_ts < target_ts or block_ts >= target_ts_next_day:
                    calls_used = self._rpc_get_block_calls - calls_before
                    print(
                        f"{origin_key} {target_date}: fallback block {block_number} outside UTC day window, "
                        f"rpc get_block calls={calls_used}"
                    )
                    continue

                if int(block_number) > int(chain_start_block):
                    try:
                        prev_ts = self._get_block_timestamp(origin_key, w3, int(block_number) - 1)
                    except Exception as exc:
                        calls_used = self._rpc_get_block_calls - calls_before
                        print(
                            f"{origin_key} {target_date}: failed to validate previous fallback block "
                            f"{int(block_number) - 1}: {exc}, rpc get_block calls={calls_used}"
                        )
                        continue
                    if prev_ts >= target_ts:
                        calls_used = self._rpc_get_block_calls - calls_before
                        print(
                            f"{origin_key} {target_date}: unable to validate UTC-first block after fallback "
                            f"(candidate={block_number}), rpc get_block calls={calls_used}"
                        )
                        continue

            # If RPC has synthetic block0 timestamp 0, keep chain-start day as block 0 for semantics.
            if (
                target_date == chain_start_date
                and int(block_number) == int(chain_start_block)
                and int(chain_start_block) > 0
                and int(block0_ts) <= 0
            ):
                calls_used = self._rpc_get_block_calls - calls_before
                print(
                    f"{origin_key} {target_date}: normalizing first_block_of_day from "
                    f"{block_number} to 0 (rpc returns block0 timestamp={block0_ts}), "
                    f"rpc get_block calls={calls_used}"
                )
                block_number = 0

            calls_used = self._rpc_get_block_calls - calls_before
            print(
                f"{origin_key} {target_date}: first_block_of_day={block_number}, "
                f"rpc get_block calls={calls_used}"
            )

            rows.append(
                {
                    "metric_key": self.METRIC_KEY,
                    "origin_key": origin_key,
                    "date": target_date,
                    "value": int(block_number),
                }
            )

            # We iterate newest -> oldest, so the next search can only be at or below this block.
            if int(block_number) > 0:
                search_high = max(chain_start_block, int(block_number))

            # Adapt per-chain block-time estimate as we go to keep future windows tight.
            if (
                prev_found_date is not None
                and prev_found_block is not None
                and prev_found_ts is not None
                and int(prev_found_block) > int(block_number)
                and prev_found_date != target_date
            ):
                dt_days = max(1, (prev_found_date - target_date).days)
                dt_seconds = int(prev_found_ts) - int(block_ts)
                db_blocks = int(prev_found_block) - int(block_number)
                if dt_seconds > 0 and db_blocks > 0:
                    observed_blocks_per_day = db_blocks / dt_days
                    observed = dt_seconds / db_blocks
                    # Learn chain-specific production rate (blocks/day) while pulling more data.
                    alpha = 0.25
                    prev_bpd = learned_blocks_per_day
                    learned_blocks_per_day = ((1.0 - alpha) * learned_blocks_per_day) + (alpha * observed_blocks_per_day)
                    learned_blocks_per_day_std = ((1.0 - alpha) * learned_blocks_per_day_std) + (
                        alpha * max(1.0, abs(observed_blocks_per_day - prev_bpd))
                    )
                    avg_block_time = (0.8 * avg_block_time) + (0.2 * observed)

            prev_found_date = target_date
            prev_found_block = int(block_number)
            prev_found_ts = int(block_ts)

            if int(block_number) == 0:
                break

        return rows

    def _extract_starknet(self, progress_row: Optional[dict]) -> List[dict]:
        from src.adapters.adapter_dune import AdapterDune
        import os

        today_utc = datetime.now(timezone.utc).date()
        latest_db_date: Optional[date] = None

        if progress_row:
            candidate = progress_row.get("date")
            if candidate is not None and pd.notna(candidate):
                latest_db_date = pd.to_datetime(candidate).date()

        # Starknet mainnet launched in Nov 2021; use day-before to include launch day on first backfill.
        if latest_db_date is None:
            latest_db_date = date(2021, 11, 15)

        days = int((today_utc - latest_db_date).days)
        if days <= 0:
            print("starknet: no missing first_block_of_day data.")
            return []

        api_key = os.getenv("DUNE_API")
        if not api_key:
            raise ValueError("DUNE_API environment variable is not set.")

        adapter_params = {"api_key": api_key}
        print("Initializing AdapterDune...")
        ad = AdapterDune(adapter_params, self.db_connector)
        load_params = {
            "queries": [
                {
                    "name": "starknet_first_block_of_day",
                    "query_id": 6722519,
                    "params": {"days": days},
                }
            ]
        }
        df = ad.extract(load_params)
        if df.empty:
            print(f"starknet: Dune returned no rows for the last {days} day(s).")
            return []

        expected_cols = {"date", "first_block_of_day"}
        missing_cols = expected_cols.difference(df.columns)
        if missing_cols:
            raise ValueError(
                "starknet_first_block_of_day query result missing required column(s): "
                f"{sorted(missing_cols)}"
            )

        df = df.rename(columns={"first_block_of_day": "value"})

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["value"])
        df["value"] = df["value"].astype(int)
        df = df[df["value"] != 0]
        df = df[df["date"] > latest_db_date]
        df = df.drop_duplicates(subset=["date"], keep="last").sort_values(by="date")

        rows = [
            {
                "metric_key": self.METRIC_KEY,
                "origin_key": "starknet",
                "date": row["date"],
                "value": int(row["value"]),
            }
            for _, row in df.iterrows()
        ]
        print(f"starknet: pulled {len(rows)} first_block_of_day row(s) from Dune (days={days}).")
        return rows

    def _find_first_block_by_timestamp(
        self,
        origin_key: str,
        w3: Web3,
        target_ts: int,
        low: int,
        high: int,
        latest_block: int,
        floor_block: int,
    ) -> Optional[int]:
        low = max(int(floor_block), int(low))
        high = min(int(high), int(latest_block))
        if low > high:
            return None

        try:
            high_ts = self._get_block_timestamp(origin_key, w3, high)
        except Exception as exc:
            print(f"{origin_key}: failed to fetch search high block {high}: {exc}")
            return None

        if high_ts < target_ts:
            return None

        while low < high:
            mid = (low + high) // 2
            try:
                mid_ts = self._get_block_timestamp(origin_key, w3, mid)
            except Exception as exc:
                print(f"{origin_key}: failed to fetch search mid block {mid}: {exc}")
                return None
            if mid_ts < target_ts:
                low = mid + 1
            else:
                high = mid
        return int(low)

    def _get_all_chains_with_rpcs(self) -> List[str]:
        print("[DB] SELECT chain universe from sys_main_conf (L1/L2, PROD/DEV/ZIRCUIT)")
        chains = self.db_connector.get_all_L1_L2_origin_keys_prod(['PROD', 'DEV', 'ZIRCUIT'])
        print(f"[DB] -> returned {len(chains)} chain row(s)")
        ### remove certain chains
        to_be_removed = ['loopring']
        chains = list(set(chains) - set(to_be_removed))
        print(f"[DB] -> removed {to_be_removed} from chains, {len(chains)} chain(s) left")
        if not chains:
            return []
        return chains

    def _get_chain_progress(self, origin_keys: List[str]) -> pd.DataFrame:
        in_clause = ", ".join(f"'{self._escape_literal(ok)}'" for ok in origin_keys)
        query = f"""
            SELECT DISTINCT ON (origin_key)
                origin_key,
                "date",
                value,
                CASE
                    WHEN LAG(value, 7) OVER (PARTITION BY origin_key ORDER BY "date") IS NOT NULL
                        AND (value - LAG(value, 7) OVER (PARTITION BY origin_key ORDER BY "date")) > 0
                    THEN 604800.0 / (value - LAG(value, 7) OVER (PARTITION BY origin_key ORDER BY "date"))
                    ELSE NULL
                END AS avg_block_time_7d
            FROM public.fact_kpis
            WHERE metric_key = '{self.METRIC_KEY}'
              AND origin_key IN ({in_clause})
            ORDER BY origin_key, "date" DESC;
        """
        print(f"[DB] SELECT latest {self.METRIC_KEY} progress for {len(origin_keys)} chain(s)")
        df = self.db_connector.execute_query(query, load_df=True)
        print(f"[DB] -> returned {len(df)} progress row(s)")
        return df

    def _get_rpc_candidates(self, origin_key: str) -> List[str]:
        if origin_key in self._rpc_candidates_by_chain:
            return self._rpc_candidates_by_chain[origin_key]

        print(f"[DB] SELECT special RPC for {origin_key}")
        primary_rpc = self.db_connector.get_special_use_rpc(origin_key)
        print(f"[DB] SELECT all RPCs for {origin_key}")
        fallback_rpcs = self.db_connector.get_all_rpcs_for_chain(origin_key)
        rpc_candidates = [rpc for rpc in dict.fromkeys([primary_rpc] + fallback_rpcs) if rpc]
        if len(rpc_candidates) == 0:
            raise ValueError(f"No RPC found for chain {origin_key}.")
        self._rpc_candidates_by_chain[origin_key] = rpc_candidates
        return rpc_candidates

    def _build_web3(self, rpc_url: str) -> Web3:
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
        try:
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        except Exception:
            pass
        return w3

    def _connect_web3(self, origin_key: str) -> Web3:
        rpc_candidates = self._get_rpc_candidates(origin_key)

        last_error = None
        for rpc_url in rpc_candidates:
            try:
                w3 = self._build_web3(rpc_url)
                _ = w3.eth.block_number
                return w3
            except Exception as exc:
                last_error = exc
                print(f"{origin_key}: RPC connection failed ({rpc_url}): {exc}")
        raise ConnectionError(f"Could not connect to any RPC for {origin_key}: {last_error}")

    def _get_block_timestamp(self, origin_key: str, w3: Web3, block_number: int) -> int:
        block_number = int(block_number)
        cache_key = (origin_key, block_number)
        if cache_key in self._block_ts_cache:
            return self._block_ts_cache[cache_key]

        retries = 5
        rpc_candidates = self._rpc_candidates_by_chain.get(origin_key, [])
        if not rpc_candidates:
            rpc_candidates = [None]

        last_error = None
        for attempt in range(retries):
            rpc_url = rpc_candidates[attempt % len(rpc_candidates)]
            call_w3 = w3 if rpc_url is None or attempt == 0 else self._build_web3(rpc_url)
            try:
                self._rpc_get_block_calls += 1
                block = call_w3.eth.get_block(block_number)
                ts = int(block["timestamp"])
                self._block_ts_cache[cache_key] = ts
                return ts
            except Exception as exc:
                last_error = exc
                if attempt == retries - 1:
                    raise last_error
                sleep_seconds = min(2 ** attempt, 8)
                print(
                    f"{origin_key}: failed to fetch block {block_number} "
                    f"(attempt {attempt + 1}/{retries}): {exc}; retrying in {sleep_seconds}s"
                )
                time.sleep(sleep_seconds)
        raise RuntimeError("Unreachable block timestamp retry path")

    def _get_latest_fetchable_block_and_ts(self, origin_key: str, w3: Web3, latest_block: int) -> Tuple[int, int]:
        """
        Some RPCs briefly report a head block before eth_getBlockByNumber can serve it.
        Anchor searches on the newest nearby block that is actually fetchable.
        """
        latest_block = int(latest_block)
        last_error = None
        for offset in [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]:
            candidate = max(0, latest_block - offset)
            try:
                ts = self._get_block_timestamp(origin_key, w3, candidate)
                if candidate != latest_block:
                    print(
                        f"{origin_key}: using fetchable block {candidate} instead of reported head "
                        f"{latest_block} for first_block_of_day search."
                    )
                return candidate, ts
            except Exception as exc:
                last_error = exc
                if candidate == 0:
                    break

        raise last_error

    def _get_chain_start_block_and_ts(self, origin_key: str, w3: Web3, latest_block: int) -> Tuple[int, int]:
        """
        Some RPCs return block 0 with timestamp 0 (1970-01-01).
        Use the first block with timestamp > 0 as the chain start for backfill bounds.
        """
        ts0 = self._get_block_timestamp(origin_key, w3, 0)
        if ts0 > 0:
            return 0, ts0

        low, high = 0, int(latest_block)
        while low < high:
            mid = (low + high) // 2
            if self._get_block_timestamp(origin_key, w3, mid) > 0:
                high = mid
            else:
                low = mid + 1

        start_block = int(low)
        start_ts = self._get_block_timestamp(origin_key, w3, start_block)
        if start_ts <= 0:
            raise ValueError(f"{origin_key}: could not find chain start block with timestamp > 0.")

        print(
            f"{origin_key}: detected chain start at block {start_block} "
            f"({datetime.fromtimestamp(start_ts, tz=timezone.utc).date()})"
        )
        return start_block, int(start_ts)

    @staticmethod
    def _start_of_day_ts(day: date) -> int:
        return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())

    @staticmethod
    def _escape_literal(value: str) -> str:
        return str(value).replace("'", "''")

    def _resolve_avg_block_time(self, progress_row: Optional[dict]) -> float:
        if not progress_row:
            return self.DEFAULT_AVG_BLOCK_TIME

        avg_7d = progress_row.get("avg_block_time_7d")
        if avg_7d is not None and pd.notna(avg_7d) and float(avg_7d) > 0:
            return float(avg_7d)
        return self.DEFAULT_AVG_BLOCK_TIME
