"""Batch-mode loader for the telco churn Accounts — the fast path.

This is a drop-in alternative to ``scripts/load_accounts.py``. It reads the same
CSV, applies the same column mapping, idempotency check and two-pass statecode
pattern, but pushes the bulk insert through the Dataverse ``$batch`` endpoint
(``DataverseClient.create_records_batch`` / ``update_records_batch``) instead of
one HTTP call per row. That collapses ~7000 round-trips into ~8 batches.

Sequential vs batch — pick the right tool:

  +-------------------+--------------------------------+-----------------------------+
  | Aspect            | Sequential (load_accounts.py)  | Batch (this file)           |
  +-------------------+--------------------------------+-----------------------------+
  | Throughput        | one row per HTTP call          | up to 1000 rows per call    |
  | 7043 rows         | ~15 min                        | ~30 sec (~30x faster)       |
  | Failure unit      | per row (skip & continue)      | per batch (all-or-nothing   |
  |                   |                                | rollback within a changeset)|
  | Debuggability     | trivial — one row, one error   | must isolate the bad row in |
  |                   |                                | a rolled-back batch         |
  | Reads like        | a tutorial                     | a migration job             |
  +-------------------+--------------------------------+-----------------------------+

  Use sequential for: first-time learning, complex per-row logic, when you need
    each row to succeed or fail independently.
  Use batch for: bulk migrations, ETL pipelines, re-loads — anywhere raw speed
    matters and you can re-run a failed batch.

The 5-row test/verification phase is reused verbatim from the sequential loader
(it is cheap and pedagogical, and it still decides the statecode strategy). Only
the bulk insert and the churned-deactivation pass are batched.

Run from project root: `python -m scripts.load_accounts_batch`
"""

from __future__ import annotations

import sys
import time
from collections import Counter
from typing import Any

import pandas as pd
from tqdm import tqdm

from scripts.load_accounts import (
    CSV_PATH,
    ENTITY_SET,
    FAILURES_PATH,
    TEST_BATCH_SIZE,
    _error_text,
    aggregate_count,
    build_payload,
    count_with_telco_id,
    existing_telco_ids,
    run_test_phase,
)
from src.dataverse_client import DataverseClient

# Operations per change set. Dataverse caps this at 1000; 7043 rows -> 8 batches.
BATCH_SIZE = 1000

# Sequential baseline used only to extrapolate the speed-up in the final report:
# the sequential loader takes ~15 min for the 7043-row reference load.
SEQUENTIAL_SECONDS_PER_ROW = (15 * 60) / 7043


# ----------------------------------------------------------------------
# Batched bulk load
# ----------------------------------------------------------------------

class BatchTiming:
    """Accumulates per-batch wall-clock timings for the final report."""

    def __init__(self) -> None:
        self.per_batch: list[float] = []

    def record(self, seconds: float) -> None:
        self.per_batch.append(seconds)

    @property
    def total(self) -> float:
        return sum(self.per_batch)

    @property
    def count(self) -> int:
        return len(self.per_batch)

    @property
    def mean(self) -> float:
        return self.total / self.count if self.count else 0.0


def load_rows_batch(
    client: DataverseClient, df: pd.DataFrame, include_state: bool, timing: BatchTiming
) -> tuple[list[tuple[str, str]], list[dict[str, Any]]]:
    """Insert all rows via $batch; return (churned [(telco_id, record_id)], failures).

    Rows are split into BATCH_SIZE change sets. Because a change set is
    transactional, a single bad row fails the whole batch — that batch's rows
    are recorded as failures and the load continues with the next batch.

    When include_state is False the churned rows are paired with their returned
    GUIDs so a second batch PATCH pass can set them Inactive.
    """
    churned_inserted: list[tuple[str, str]] = []
    failures: list[dict[str, Any]] = []
    total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE

    for start in tqdm(
        range(0, len(df), BATCH_SIZE), total=total_batches, desc="Inserting", unit="batch"
    ):
        chunk = df.iloc[start : start + BATCH_SIZE]
        payloads = [build_payload(row, include_state) for _, row in chunk.iterrows()]
        began = time.perf_counter()
        try:
            ids = client.create_records_batch(ENTITY_SET, payloads, max_per_batch=BATCH_SIZE)
        except Exception as exc:  # noqa: BLE001 - whole batch rolled back; record and continue
            timing.record(time.perf_counter() - began)
            error = _error_text(exc)
            for offset, (idx, row) in enumerate(chunk.iterrows()):
                failures.append({
                    "row_index": idx,
                    "telco_customer_id": row["telco_customer_id"],
                    "phase": "insert",
                    "error": f"batch starting at {start} rolled back: {error}"
                    if offset == 0 else "rolled back with batch",
                })
            continue
        timing.record(time.perf_counter() - began)

        if not include_state:
            for (_idx, row), record_id in zip(chunk.iterrows(), ids, strict=True):
                if int(row["statecode"]) == 1:
                    churned_inserted.append((row["telco_customer_id"], record_id))

    return churned_inserted, failures


def patch_churned_batch(
    client: DataverseClient, churned: list[tuple[str, str]], timing: BatchTiming
) -> tuple[int, list[dict[str, Any]]]:
    """Second pass: batch-PATCH inserted churned rows to Inactive.

    Returns (patched_count, failures). Like the insert pass, a failed change set
    rolls back its whole batch, which is recorded and skipped.
    """
    patched = 0
    failures: list[dict[str, Any]] = []
    total_batches = (len(churned) + BATCH_SIZE - 1) // BATCH_SIZE

    for start in tqdm(
        range(0, len(churned), BATCH_SIZE),
        total=total_batches,
        desc="Deactivating churned",
        unit="batch",
    ):
        chunk = churned[start : start + BATCH_SIZE]
        updates = [(record_id, {"statecode": 1, "statuscode": 2}) for _tid, record_id in chunk]
        began = time.perf_counter()
        try:
            client.update_records_batch(ENTITY_SET, updates)
            patched += len(chunk)
        except Exception as exc:  # noqa: BLE001
            error = _error_text(exc)
            for tid, _record_id in chunk:
                failures.append({
                    "row_index": "",
                    "telco_customer_id": tid,
                    "phase": "patch",
                    "error": f"batch starting at {start} rolled back: {error}",
                })
        timing.record(time.perf_counter() - began)

    return patched, failures


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main() -> int:
    if not CSV_PATH.exists():
        print(f"CSV not found: {CSV_PATH}")
        return 1

    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows from {CSV_PATH}")

    client = DataverseClient()

    # --- Idempotency guard (identical to the sequential loader) ---
    already = count_with_telco_id(client)
    skip_ids: set[str] = set()
    if already > 0:
        print(f"\n⚠️  {already} Account record(s) already have rm_telcocustomerid set.")
        try:
            answer = input("This may create duplicates. Proceed? Rows already present "
                           "will be skipped. [y/N]: ").strip().lower()
        except EOFError:
            answer = "n"
        if answer != "y":
            print("Aborted by user. Nothing was loaded.")
            return 1
        print("Fetching existing telco ids to skip already-loaded rows...")
        skip_ids = existing_telco_ids(client)
        print(f"Will skip {len(skip_ids)} row(s) that are already in Dataverse.")

    to_load = df[~df["telco_customer_id"].isin(skip_ids)].reset_index(drop=True)
    skipped = len(df) - len(to_load)
    if to_load.empty:
        print("Every row is already loaded — nothing to do.")
        return 0

    # --- Test batch (sequential, reused), then the batched bulk load ---
    test_df = to_load.iloc[:TEST_BATCH_SIZE]
    rest_df = to_load.iloc[TEST_BATCH_SIZE:]

    test_result = run_test_phase(client, test_df)
    inserted_count = len(test_result.inserted)
    failures: list[dict[str, Any]] = []
    timing = BatchTiming()

    print(f"\n=== Batch bulk load: {len(rest_df)} remaining rows "
          f"in batches of {BATCH_SIZE} ===")
    churned, insert_failures = load_rows_batch(
        client, rest_df, test_result.include_state, timing
    )
    inserted_count += len(rest_df) - len(insert_failures)
    failures.extend(insert_failures)

    # --- Two-pass state fix, batched ---
    patched = 0
    if not test_result.include_state and churned:
        print(f"\n=== Second pass: batch-deactivating {len(churned)} churned accounts ===")
        patched, patch_failures = patch_churned_batch(client, churned, timing)
        failures.extend(patch_failures)

    # --- Persist failures ---
    if failures:
        pd.DataFrame(failures, columns=["row_index", "telco_customer_id", "phase", "error"]).to_csv(
            FAILURES_PATH, index=False
        )

    # --- Final report (with batch timing + extrapolated comparison) ---
    final_total = aggregate_count(client)
    final_with_telco = count_with_telco_id(client)
    seq_estimate = len(rest_df) * SEQUENTIAL_SECONDS_PER_ROW
    speedup = seq_estimate / timing.total if timing.total > 0 else float("nan")

    print("\n" + "=" * 60)
    print("BATCH LOAD REPORT")
    print("=" * 60)
    print(f"  Source rows ...................... {len(df)}")
    print(f"  Inserted ......................... {inserted_count}")
    print(f"  Skipped (already present) ........ {skipped}")
    print(f"  Failed ........................... {len(failures)}")
    if not test_result.include_state:
        print(f"  Churned deactivated (PATCH) ...... {patched}")
    if failures:
        print(f"  Failures written to .............. {FAILURES_PATH}")
        print("  Error breakdown:")
        breakdown = Counter(f["error"].split(":")[0] for f in failures)
        for key, count in breakdown.most_common():
            print(f"      {count:>5}  {key}")
    print("-" * 60)
    print("  Batch performance:")
    print(f"      Total batches ................ {timing.count}")
    print(f"      Time per batch (avg) ......... {timing.mean:.2f}s")
    print(f"      Total batch time ............. {timing.total:.2f}s")
    print(f"      Sequential estimate .......... {seq_estimate:.0f}s "
          f"(~{seq_estimate / 60:.1f} min)")
    print(f"      Speed-up vs sequential ....... {speedup:.1f}x")
    print("-" * 60)
    print(f"  Accounts in Dataverse (total) .... {final_total}")
    print(f"  Accounts with rm_telcocustomerid . {final_with_telco}")
    print("=" * 60)

    return 0 if not failures else 2


if __name__ == "__main__":
    sys.exit(main())
