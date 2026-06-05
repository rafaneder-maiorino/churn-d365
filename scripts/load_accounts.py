"""Load the prepared telco churn data into the Dataverse Account table.

Reads data/processed/accounts_for_upload.csv (one row per customer) and creates
one Account record per row, mapping the CSV columns to their Dataverse logical
names.

Flow:
  1. Idempotency guard  - count accounts that already carry an rm_telcocustomerid;
                          if any exist, list the count and ask before continuing.
                          Rows whose telco id already exists are skipped.
  2. Test batch (5 rows) - insert, read back via $filter, and verify every field
                          against the source CSV. Abort on any mismatch.
  3. Bulk load           - insert the remaining rows with a tqdm progress bar,
                          recording per-row failures rather than crashing.
  4. State handling      - statecode/statuscode are attempted on create; if
                          Dataverse rejects them, fall back to a two-pass load
                          (insert Active, then PATCH the churned rows Inactive).
  5. Report              - inserted / skipped / failed counts, an error breakdown,
                          and the final Account total in Dataverse.

Run from project root: `python -m scripts.load_accounts`
"""

from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm

from src.dataverse_client import DataverseClient

logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ENTITY_SET = "accounts"
CSV_PATH = Path("data/processed/accounts_for_upload.csv")
FAILURES_PATH = Path("data/processed/load_failures.csv")
TEST_BATCH_SIZE = 5

# CSV column -> Dataverse logical name. Split into state fields (handled
# separately because Dataverse may reject them on create) and the rest.
FIELD_MAP = {
    "name": "name",
    "revenue": "revenue",
    "overriddencreatedon": "overriddencreatedon",
    "telco_customer_id": "rm_telcocustomerid",
    "tenure_months": "rm_tenuremonths",
    "total_charges": "rm_totalcharges",
    "contract_type": "rm_contracttype",
    "internet_service": "rm_internetservice",
    "tech_support": "rm_techsupport",
    "online_security": "rm_onlinesecurity",
    "payment_method": "rm_paymentmethod",
}
STATE_MAP = {"statecode": "statecode", "statuscode": "statuscode"}
ALL_LOGICAL = list(FIELD_MAP.values()) + list(STATE_MAP.values())

# Per-field converters so numpy scalars become JSON-serialisable Python types.
INT_FIELDS = {"tenure_months", "statecode", "statuscode"}
FLOAT_FIELDS = {"revenue", "total_charges"}
BOOL_FIELDS = {"tech_support", "online_security"}


# ----------------------------------------------------------------------
# Payload + comparison helpers
# ----------------------------------------------------------------------

def _convert(csv_col: str, value: Any) -> Any:
    """Coerce a pandas/numpy cell into a native Python type for JSON."""
    if csv_col in INT_FIELDS:
        return int(value)
    if csv_col in FLOAT_FIELDS:
        return float(value)
    if csv_col in BOOL_FIELDS:
        return bool(value)
    return str(value)


def build_payload(row: pd.Series, include_state: bool) -> dict[str, Any]:
    """Map one CSV row to a Dataverse attribute payload."""
    payload = {logical: _convert(csv_col, row[csv_col]) for csv_col, logical in FIELD_MAP.items()}
    if include_state:
        for csv_col, logical in STATE_MAP.items():
            payload[logical] = _convert(csv_col, row[csv_col])
    return payload


def _norm_dt(value: Any) -> str:
    """Normalise a datetime to second precision for round-trip comparison.

    Source has microseconds (2026-05-06T15:40:18.982450); Dataverse returns
    second precision with a Z suffix (2026-05-06T15:40:18Z). Compare on
    YYYY-MM-DDTHH:MM:SS.
    """
    text = str(value).replace("Z", "").replace(" ", "T")
    if "." in text:
        text = text.split(".", 1)[0]
    return text[:19]


def compare_row(row: pd.Series, record: dict[str, Any]) -> list[tuple[str, Any, Any]]:
    """Return a list of (logical_name, source_value, dataverse_value) mismatches.

    Note on overriddencreatedon: writing this field tells Dataverse to backdate
    the record, so the value lands in the read-only ``createdon`` column while
    ``overriddencreatedon`` itself records the actual write time. We therefore
    verify the source date against ``createdon``.
    """
    mismatches: list[tuple[str, Any, Any]] = []
    for csv_col, logical in {**FIELD_MAP, **STATE_MAP}.items():
        src = row[csv_col]
        if logical == "overriddencreatedon":
            got = record.get("createdon")
            ok = _norm_dt(src) == _norm_dt(got)
            if not ok:
                mismatches.append(("createdon", _norm_dt(src), got))
            continue
        got = record.get(logical)
        if csv_col in FLOAT_FIELDS:
            ok = got is not None and abs(float(src) - float(got)) < 0.01
        elif csv_col in INT_FIELDS:
            ok = got is not None and int(src) == int(got)
        elif csv_col in BOOL_FIELDS:
            ok = bool(src) == bool(got)
        else:
            ok = str(src) == ("" if got is None else str(got))
        if not ok:
            mismatches.append((logical, src, got))
    return mismatches


def _error_text(exc: Exception) -> str:
    """Short, log-friendly description of a request failure."""
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        body = exc.response.text or ""
        return f"HTTP {exc.response.status_code}: {body[:300]}"
    return f"{type(exc).__name__}: {exc}"


def _is_state_rejection(exc: Exception) -> bool:
    """Heuristic: did this failure come from sending statecode/statuscode?"""
    if not (isinstance(exc, requests.HTTPError) and exc.response is not None):
        return False
    text = (exc.response.text or "").lower()
    return exc.response.status_code == 400 and ("statecode" in text or "statuscode" in text or "status" in text)


# ----------------------------------------------------------------------
# Dataverse queries
# ----------------------------------------------------------------------

def aggregate_count(client: DataverseClient, filter_expr: str | None = None) -> int:
    """Return an exact record count via OData aggregation.

    Both $count=true and the /$count endpoint are capped at 5000 in Dataverse,
    so $apply=aggregate is used instead to get the true total.
    """
    apply = "aggregate($count as cnt)"
    if filter_expr:
        apply = f"filter({filter_expr})/{apply}"
    response = requests.get(
        f"{client._base_url}/{ENTITY_SET}",
        headers=client._headers(),
        params={"$apply": apply},
        timeout=120,
    )
    response.raise_for_status()
    value = response.json().get("value", [])
    return int(value[0]["cnt"]) if value else 0


def count_with_telco_id(client: DataverseClient) -> int:
    """Count accounts that already have rm_telcocustomerid set (non-null)."""
    return aggregate_count(client, "rm_telcocustomerid ne null")


def existing_telco_ids(client: DataverseClient) -> set[str]:
    """Fetch the set of rm_telcocustomerid values already in Dataverse."""
    records = client.get_records(
        ENTITY_SET, select=["rm_telcocustomerid"], filter_expr="rm_telcocustomerid ne null"
    )
    return {r["rm_telcocustomerid"] for r in records if r.get("rm_telcocustomerid")}


def fetch_by_telco_ids(client: DataverseClient, telco_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Read records back, keyed by rm_telcocustomerid, for the given ids."""
    clause = " or ".join(f"rm_telcocustomerid eq '{tid}'" for tid in telco_ids)
    # createdon is read back too: it holds the backdated value we sent as overriddencreatedon.
    records = client.get_records(ENTITY_SET, select=ALL_LOGICAL + ["createdon"], filter_expr=clause)
    return {r["rm_telcocustomerid"]: r for r in records}


def patch_to_inactive(client: DataverseClient, record_id: str) -> None:
    """Set a churned account to Inactive (statecode=1, statuscode=2)."""
    client.update_record(ENTITY_SET, record_id, {"statecode": 1, "statuscode": 2})


# ----------------------------------------------------------------------
# Test / verification phase
# ----------------------------------------------------------------------

class TestPhaseResult:
    def __init__(self, include_state: bool, inserted: dict[str, str]) -> None:
        self.include_state = include_state  # strategy to use for the remaining rows
        self.inserted = inserted            # telco_id -> record_id for the test rows


def run_test_phase(client: DataverseClient, test_df: pd.DataFrame) -> TestPhaseResult:
    """Insert the test rows, read them back, and verify every field.

    Determines whether statecode/statuscode can be set on create. Returns the
    strategy (include_state) to use for the bulk load. Raises SystemExit on a
    verification mismatch.
    """
    print(f"\n=== Test phase: inserting {len(test_df)} rows and verifying round-trip ===")
    inserted: dict[str, str] = {}
    include_state = True

    # Pass 1: try inserting with statecode/statuscode included.
    for idx, row in test_df.iterrows():
        tid = row["telco_customer_id"]
        try:
            inserted[tid] = client.create_record(ENTITY_SET, build_payload(row, include_state=True))
        except requests.HTTPError as exc:
            if _is_state_rejection(exc):
                print("  Dataverse rejected statecode/statuscode on create — "
                      "switching to two-pass (insert Active, then PATCH churned).")
                include_state = False
                break
            print(f"  ABORT: test row {idx} ({tid}) failed to insert: {_error_text(exc)}")
            raise SystemExit(1) from exc

    # If state was rejected, finish inserting the test rows as Active, then PATCH churned ones.
    if not include_state:
        for _idx, row in test_df.iterrows():
            tid = row["telco_customer_id"]
            if tid not in inserted:
                inserted[tid] = client.create_record(ENTITY_SET, build_payload(row, include_state=False))
        for _idx, row in test_df.iterrows():
            if int(row["statecode"]) == 1:
                patch_to_inactive(client, inserted[row["telco_customer_id"]])

    # Read back and verify.
    fetched = fetch_by_telco_ids(client, list(inserted.keys()))
    failed = False
    for idx, row in test_df.iterrows():
        tid = row["telco_customer_id"]
        record = fetched.get(tid)
        if record is None:
            print(f"  MISMATCH: row {idx} ({tid}) was not found when reading back.")
            failed = True
            continue
        mismatches = compare_row(row, record)

        # Single-pass but only statecode/statuscode differ on a churned row ->
        # Dataverse silently ignored state on create. Fix via PATCH and re-check.
        if include_state and mismatches and all(m[0] in STATE_MAP.values() for m in mismatches):
            print(f"  Row {idx} ({tid}): state ignored on create — applying two-pass PATCH.")
            include_state = False
            patch_to_inactive(client, inserted[tid])
            record = fetch_by_telco_ids(client, [tid])[tid]
            mismatches = compare_row(row, record)

        if mismatches:
            failed = True
            print(f"  MISMATCH on row {idx} ({tid}):")
            for logical, src, got in mismatches:
                print(f"      {logical}: source={src!r}  dataverse={got!r}")
        else:
            print(f"  OK  row {idx} ({tid}) — all {len(FIELD_MAP) + len(STATE_MAP)} fields match")

    if failed:
        print("\nABORT: test verification failed. No further rows were loaded.")
        raise SystemExit(1)

    strategy = "single-pass (state on create)" if include_state else "two-pass (PATCH churned)"
    print(f"Test phase passed. State strategy for remaining rows: {strategy}")
    return TestPhaseResult(include_state, inserted)


# ----------------------------------------------------------------------
# Bulk load
# ----------------------------------------------------------------------

def load_rows(
    client: DataverseClient, df: pd.DataFrame, include_state: bool
) -> tuple[list[tuple[str, str]], list[dict[str, Any]]]:
    """Insert each row; return (inserted churned [(telco_id, record_id)], failures).

    Continues past individual failures. When include_state is False the churned
    rows are collected so a second PATCH pass can set them Inactive.
    """
    churned_inserted: list[tuple[str, str]] = []
    failures: list[dict[str, Any]] = []

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Inserting", unit="row"):
        tid = row["telco_customer_id"]
        try:
            record_id = client.create_record(ENTITY_SET, build_payload(row, include_state))
            if not include_state and int(row["statecode"]) == 1:
                churned_inserted.append((tid, record_id))
        except Exception as exc:  # noqa: BLE001 - record and continue
            failures.append({"row_index": idx, "telco_customer_id": tid,
                             "phase": "insert", "error": _error_text(exc)})
    return churned_inserted, failures


def patch_churned_pass(
    client: DataverseClient, churned: list[tuple[str, str]]
) -> tuple[int, list[dict[str, Any]]]:
    """Second pass: PATCH inserted churned rows to Inactive. Returns (patched, failures)."""
    patched = 0
    failures: list[dict[str, Any]] = []
    for tid, record_id in tqdm(churned, total=len(churned), desc="Deactivating churned", unit="row"):
        try:
            patch_to_inactive(client, record_id)
            patched += 1
        except Exception as exc:  # noqa: BLE001
            failures.append({"row_index": "", "telco_customer_id": tid,
                             "phase": "patch", "error": _error_text(exc)})
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

    # --- Requirement 4: idempotency guard ---
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

    # --- Requirement 3: test batch, then the rest ---
    test_df = to_load.iloc[:TEST_BATCH_SIZE]
    rest_df = to_load.iloc[TEST_BATCH_SIZE:]

    test_result = run_test_phase(client, test_df)
    inserted_count = len(test_result.inserted)
    failures: list[dict[str, Any]] = []

    print(f"\n=== Bulk load: {len(rest_df)} remaining rows ===")
    churned, insert_failures = load_rows(client, rest_df, test_result.include_state)
    inserted_count += len(rest_df) - len(insert_failures)
    failures.extend(insert_failures)

    # --- Requirement 6: two-pass state fix for the bulk rows ---
    patched = 0
    if not test_result.include_state and churned:
        print(f"\n=== Second pass: deactivating {len(churned)} churned accounts ===")
        patched, patch_failures = patch_churned_pass(client, churned)
        failures.extend(patch_failures)

    # --- Persist failures ---
    if failures:
        pd.DataFrame(failures, columns=["row_index", "telco_customer_id", "phase", "error"]).to_csv(
            FAILURES_PATH, index=False
        )

    # --- Requirement 5: final report ---
    final_total = aggregate_count(client)
    final_with_telco = count_with_telco_id(client)

    print("\n" + "=" * 60)
    print("LOAD REPORT")
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
    print(f"  Accounts in Dataverse (total) .... {final_total}")
    print(f"  Accounts with rm_telcocustomerid . {final_with_telco}")
    print("=" * 60)

    return 0 if not failures else 2


if __name__ == "__main__":
    sys.exit(main())
