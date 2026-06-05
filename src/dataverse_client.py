"""Client for Microsoft Dataverse (Dynamics 365) Web API.

Handles OAuth 2.0 client credentials authentication via MSAL and provides
high-level methods for reading data from Dataverse tables.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv
from msal import ConfidentialClientApplication

logger = logging.getLogger(__name__)


@dataclass
class DataverseConfig:
    """Configuration for connecting to a Dataverse environment."""

    dataverse_url: str
    tenant_id: str
    client_id: str
    client_secret: str

    @classmethod
    def from_env(cls, env_path: str | None = None) -> "DataverseConfig":
        """Load configuration from a .env file.

        If env_path is None, searches upward from the current working
        directory for a .env file. This makes the client work from
        notebooks, scripts, or REPLs anywhere within the project tree.
        """
        if env_path is None:
            env_path = find_dotenv(usecwd=True)
            if not env_path:
                raise FileNotFoundError(
                    ".env file not found. Searched from CWD upward. "
                    "Pass env_path explicitly to from_env()."
                )
        load_dotenv(env_path)
        return cls(
            dataverse_url=os.environ["DATAVERSE_URL"],
            tenant_id=os.environ["TENANT_ID"],
            client_id=os.environ["CLIENT_ID"],
            client_secret=os.environ["CLIENT_SECRET"],
        )


class DataverseClient:
    """Client for the Dataverse Web API using OAuth client credentials flow.

    The access token is cached in memory and refreshed automatically when
    it gets within 60s of expiry. Pagination is handled transparently in
    methods that return lists or DataFrames.
    """

    API_VERSION = "v9.2"
    PAGE_SIZE = 5000

    def __init__(self, config: DataverseConfig | None = None) -> None:
        self.config = config or DataverseConfig.from_env()
        self._msal_app = ConfidentialClientApplication(
            client_id=self.config.client_id,
            client_credential=self.config.client_secret,
            authority=f"https://login.microsoftonline.com/{self.config.tenant_id}",
        )
        self._token: str | None = None
        self._token_expires_at: datetime | None = None
        self._base_url = f"{self.config.dataverse_url}/api/data/{self.API_VERSION}"
        # Reused for write requests so a bulk load shares one TCP/TLS connection.
        self._session = requests.Session()

    def _get_token(self) -> str:
        """Return a valid access token, refreshing if it's near expiry."""
        now = datetime.now(timezone.utc)
        if (
            self._token is None
            or self._token_expires_at is None
            or now >= self._token_expires_at - timedelta(seconds=60)
        ):
            logger.info("Acquiring new Dataverse access token")
            result = self._msal_app.acquire_token_for_client(
                scopes=[f"{self.config.dataverse_url}/.default"]
            )
            if "access_token" not in result:
                raise RuntimeError(
                    f"Failed to acquire token: {result.get('error')} - "
                    f"{result.get('error_description')}"
                )
            self._token = result["access_token"]
            self._token_expires_at = now + timedelta(seconds=result["expires_in"])
        return self._token

    def _headers(self, accept: str = "application/json") -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Accept": accept,
            "Prefer": f"odata.maxpagesize={self.PAGE_SIZE}",
        }

    def list_tables(self) -> list[dict[str, Any]]:
        """List all entity (table) definitions in the environment.

        Returns LogicalName, DisplayName, and EntitySetName for each table.
        EntitySetName is what you use in API URLs (e.g. 'accounts').
        """
        url = f"{self._base_url}/EntityDefinitions"
        params = {"$select": "LogicalName,DisplayName,EntitySetName,IsCustomEntity"}
        return self._paginate(url, params)

    def count_records(self, entity_set: str) -> int:
        """Count records in a table using the $count endpoint."""
        url = f"{self._base_url}/{entity_set}/$count"
        response = requests.get(url, headers=self._headers(accept="text/plain"), timeout=30)
        response.raise_for_status()
        return int(response.content.decode("utf-8-sig").strip())

    def get_records(
        self,
        entity_set: str,
        select: list[str] | None = None,
        filter_expr: str | None = None,
        top: int | None = None,
        orderby: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch records from a table using OData query options.

        Args:
            entity_set: Plural API name of the table (e.g. 'accounts').
            select: List of column logical names to return.
            filter_expr: OData $filter expression (e.g. "statecode eq 0").
            top: Maximum records to return. If None, returns all (paginated).
            orderby: OData $orderby expression (e.g. "createdon desc").
        """
        url = f"{self._base_url}/{entity_set}"
        params: dict[str, str] = {}
        if select:
            params["$select"] = ",".join(select)
        if filter_expr:
            params["$filter"] = filter_expr
        if orderby:
            params["$orderby"] = orderby
        if top:
            params["$top"] = str(top)

        # If top is small enough to fit in a single page, single request
        if top and top <= self.PAGE_SIZE:
            response = requests.get(url, headers=self._headers(), params=params, timeout=30)
            response.raise_for_status()
            return response.json().get("value", [])

        return self._paginate(url, params)

    def to_dataframe(
        self,
        entity_set: str,
        select: list[str] | None = None,
        filter_expr: str | None = None,
        top: int | None = None,
    ) -> pd.DataFrame:
        """Convenience wrapper that returns a Pandas DataFrame."""
        records = self.get_records(
            entity_set, select=select, filter_expr=filter_expr, top=top
        )
        return pd.DataFrame(records)

    def _paginate(
        self, url: str, params: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        """Iterate through all pages of an OData response via @odata.nextLink."""
        all_records: list[dict[str, Any]] = []
        next_url: str | None = url
        next_params: dict[str, str] | None = params

        while next_url:
            response = requests.get(
                next_url, headers=self._headers(), params=next_params, timeout=30
            )
            response.raise_for_status()
            data = response.json()
            all_records.extend(data.get("value", []))
            next_url = data.get("@odata.nextLink")
            # nextLink already encodes the original query params
            next_params = None
            logger.debug("Page fetched, running total: %d records", len(all_records))

        return all_records

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    MAX_THROTTLE_RETRIES = 5
    DEFAULT_RETRY_AFTER = 5

    def _send_with_retry(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        data: bytes | str | None = None,
        content_type: str = "application/json",
        extra_headers: dict[str, str] | None = None,
        timeout: int = 60,
    ) -> requests.Response:
        """Send a write request, retrying on HTTP 429 (throttling).

        Pass either ``json`` (a dict, serialised by requests) for a normal
        JSON write, or ``data`` (raw bytes/str) plus a ``content_type`` for a
        pre-built body such as a multipart/mixed $batch payload.

        On a 429 the Retry-After header is honoured (falling back to
        DEFAULT_RETRY_AFTER seconds) and the request is retried up to
        MAX_THROTTLE_RETRIES times. Any other 4xx/5xx raises immediately
        via raise_for_status(); a 429 that survives all retries also raises.
        """
        headers = {**self._headers(), "Content-Type": content_type}
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(self.MAX_THROTTLE_RETRIES + 1):
            response = self._session.request(
                method, url, headers=headers, json=json, data=data, timeout=timeout
            )
            if response.status_code == 429 and attempt < self.MAX_THROTTLE_RETRIES:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = int(retry_after) if retry_after is not None else self.DEFAULT_RETRY_AFTER
                except ValueError:
                    delay = self.DEFAULT_RETRY_AFTER
                logger.warning(
                    "Throttled (429) on %s; sleeping %ds then retrying (attempt %d/%d)",
                    url, delay, attempt + 1, self.MAX_THROTTLE_RETRIES,
                )
                time.sleep(delay)
                # A new token may be needed if the sleep was long; _headers() refreshes it.
                headers["Authorization"] = self._headers()["Authorization"]
                continue
            response.raise_for_status()
            return response

        # All retries exhausted on 429 — surface the throttling error.
        response.raise_for_status()
        return response

    def create_record(self, entity_set_name: str, data: dict[str, Any]) -> str:
        """Create a record and return its primary-key GUID.

        Args:
            entity_set_name: Plural API name of the table (e.g. 'accounts').
            data: Column logical names mapped to values.

        Returns:
            The new record's GUID, parsed from the OData-EntityId header.
        """
        url = f"{self._base_url}/{entity_set_name}"
        response = self._send_with_retry("POST", url, json=data)
        entity_id = response.headers.get("OData-EntityId", "")
        match = re.search(r"\(([0-9a-fA-F-]{36})\)", entity_id)
        if not match:
            raise RuntimeError(
                f"Record created but could not parse id from OData-EntityId: {entity_id!r}"
            )
        return match.group(1)

    def update_record(
        self, entity_set_name: str, record_id: str, data: dict[str, Any]
    ) -> None:
        """Update an existing record via PATCH (with the same 429 retry policy).

        If-Match: * is sent so the call only ever updates an existing record
        and never upserts a new one.
        """
        url = f"{self._base_url}/{entity_set_name}({record_id})"
        self._send_with_retry("PATCH", url, json=data, extra_headers={"If-Match": "*"})

    # ------------------------------------------------------------------
    # Batch ($batch) operations
    # ------------------------------------------------------------------
    #
    # Dataverse $batch limits (enforced by the service):
    #   * Max 1000 operations per change set.
    #   * Max 16 MB total payload per batch request.
    #   * A change set is transactional: all operations in it commit together
    #     or the whole set rolls back (all-or-nothing). One bad row fails the
    #     entire batch, so callers must be prepared to retry/repair a batch.

    MAX_BATCH_OPERATIONS = 1000
    MAX_BATCH_PAYLOAD_BYTES = 16 * 1024 * 1024
    BATCH_TIMEOUT = 600

    def create_records_batch(
        self,
        entity_set_name: str,
        records: list[dict[str, Any]],
        max_per_batch: int = 1000,
    ) -> list[str]:
        """Create many records via the Dataverse $batch endpoint.

        Records are split into change sets of at most ``max_per_batch`` and each
        change set is POSTed to ``/api/data/v9.2/$batch`` as a single
        multipart/mixed request. This collapses N HTTP round-trips into
        ceil(N / max_per_batch), which is the source of the ~30x speed-up over
        per-record ``create_record`` calls.

        Dataverse $batch limits (enforced by the service):
          * Max 1000 operations per change set (hence the ``max_per_batch`` cap).
          * Max 16 MB total payload per batch request.
          * A change set is transactional — all-or-nothing. If any single
            operation fails, the *entire* change set is rolled back and this
            method raises; none of that batch's records are created.

        Args:
            entity_set_name: Plural API name of the table (e.g. 'accounts').
            records: List of attribute payloads (column logical name -> value).
            max_per_batch: Operations per change set (1..1000).

        Returns:
            The created record GUIDs, in the same order as ``records``.
        """
        if not 1 <= max_per_batch <= self.MAX_BATCH_OPERATIONS:
            raise ValueError(
                f"max_per_batch must be between 1 and {self.MAX_BATCH_OPERATIONS}"
            )
        url = f"{self._base_url}/{entity_set_name}"
        ids: list[str] = []
        for start in range(0, len(records), max_per_batch):
            chunk = records[start : start + max_per_batch]
            operations = [{"method": "POST", "url": url, "payload": rec} for rec in chunk]
            ops = self._execute_changeset(operations, expected=len(chunk))
            for i, op in enumerate(ops):
                if op["entity_id"] is None:
                    raise RuntimeError(
                        f"Batch create succeeded (status {op['status']}) but no "
                        f"OData-EntityId/Location was returned for operation {i}"
                    )
                ids.append(op["entity_id"])
        return ids

    def update_records_batch(
        self,
        entity_set_name: str,
        updates: list[tuple[str, dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        """Update many records via $batch (PATCH per operation).

        Used for the two-pass statecode pass: after a fast batch insert of
        Active rows, the churned rows are flipped to Inactive in one or more
        update batches. Each PATCH carries ``If-Match: *`` so it only ever
        updates an existing record (never upserts).

        Dataverse $batch limits (enforced by the service):
          * Max 1000 operations per change set; ``updates`` is chunked
            automatically at this limit.
          * Max 16 MB total payload per batch request.
          * Change sets are transactional — all-or-nothing rollback within a
            set. A failing PATCH rolls back every other update in its batch and
            raises.

        Args:
            entity_set_name: Plural API name of the table (e.g. 'accounts').
            updates: List of ``(record_id, data)`` pairs.

        Returns:
            One parsed operation result dict per update, in input order. Each
            has keys ``status``, ``entity_id``, ``ok`` and ``body``.
        """
        results: list[dict[str, Any]] = []
        for start in range(0, len(updates), self.MAX_BATCH_OPERATIONS):
            chunk = updates[start : start + self.MAX_BATCH_OPERATIONS]
            operations = [
                {
                    "method": "PATCH",
                    "url": f"{self._base_url}/{entity_set_name}({record_id})",
                    "payload": data,
                    "headers": {"If-Match": "*"},
                }
                for record_id, data in chunk
            ]
            results.extend(self._execute_changeset(operations, expected=len(chunk)))
        return results

    def _execute_changeset(
        self, operations: list[dict[str, Any]], expected: int
    ) -> list[dict[str, Any]]:
        """Build one change set, POST it to $batch, and return parsed results.

        Raises RuntimeError if any operation failed (the whole change set then
        rolled back) or if the number of responses doesn't match ``expected``.
        """
        batch_id = f"batch_{uuid.uuid4().hex}"
        changeset_id = f"changeset_{uuid.uuid4().hex}"
        body = self._build_changeset_body(batch_id, changeset_id, operations).encode("utf-8")
        if len(body) > self.MAX_BATCH_PAYLOAD_BYTES:
            raise ValueError(
                f"Batch payload is {len(body)} bytes, over the Dataverse limit "
                f"of {self.MAX_BATCH_PAYLOAD_BYTES} bytes; lower max_per_batch."
            )
        response = self._send_with_retry(
            "POST",
            f"{self._base_url}/$batch",
            data=body,
            content_type=f"multipart/mixed; boundary={batch_id}",
            timeout=self.BATCH_TIMEOUT,
        )
        ops = self._parse_batch_response(response)
        failed = [op for op in ops if not op["ok"]]
        if failed or len(ops) != expected:
            if failed:
                detail = f"HTTP {failed[0]['status']}: {failed[0]['body'][:300]}"
            else:
                detail = f"expected {expected} responses but parsed {len(ops)}"
            raise RuntimeError(
                "Batch change set failed and was rolled back (transactional). "
                f"First error: {detail}"
            )
        return ops

    @staticmethod
    def _build_changeset_body(
        batch_id: str, changeset_id: str, operations: list[dict[str, Any]]
    ) -> str:
        """Render a multipart/mixed body with a single change set.

        Each operation is a dict with ``method``, ``url``, ``payload`` and an
        optional ``headers`` dict. Content-IDs are assigned 1..N. CRLF line
        endings are used as required by the MIME multipart spec.
        """
        lines: list[str] = [
            f"--{batch_id}",
            f"Content-Type: multipart/mixed; boundary={changeset_id}",
            "",
        ]
        for content_id, op in enumerate(operations, start=1):
            lines += [
                f"--{changeset_id}",
                "Content-Type: application/http",
                "Content-Transfer-Encoding: binary",
                f"Content-ID: {content_id}",
                "",
                f"{op['method']} {op['url']} HTTP/1.1",
                "Content-Type: application/json; type=entry",
            ]
            for header_key, header_val in op.get("headers", {}).items():
                lines.append(f"{header_key}: {header_val}")
            lines += ["", json.dumps(op["payload"]), ""]
        lines += [f"--{changeset_id}--", f"--{batch_id}--", ""]
        return "\r\n".join(lines)

    def _parse_batch_response(self, response: requests.Response) -> list[dict[str, Any]]:
        """Extract one result dict per operation from a $batch response.

        The response is multipart/mixed; a change set's responses are nested in
        a further multipart/mixed part, so parsing recurses. Each leaf is an
        ``application/http`` part wrapping a raw HTTP response.

        Returns dicts with keys ``status`` (int), ``entity_id`` (created GUID
        from the OData-EntityId/Location header, or None), ``ok`` (2xx) and
        ``body`` (the raw inner HTTP response text).
        """
        results: list[dict[str, Any]] = []
        boundary = self._extract_boundary(response.headers.get("Content-Type", ""))
        if boundary:
            self._collect_operations(response.text, boundary, results)
        return results

    def _collect_operations(
        self, body: str, boundary: str, results: list[dict[str, Any]]
    ) -> None:
        """Walk multipart parts, recursing into nested change-set responses."""
        for part in self._split_multipart(body, boundary):
            part = part.lstrip("\r\n")
            if "\r\n\r\n" in part:
                raw_headers, part_body = part.split("\r\n\r\n", 1)
            elif "\n\n" in part:
                raw_headers, part_body = part.split("\n\n", 1)
            else:
                continue
            lowered = raw_headers.lower()
            if "multipart/mixed" in lowered:
                nested = self._extract_boundary(raw_headers)
                if nested:
                    self._collect_operations(part_body, nested, results)
            elif "application/http" in lowered:
                results.append(self._parse_http_part(part_body))

    @staticmethod
    def _split_multipart(body: str, boundary: str) -> list[str]:
        """Split a multipart body into its parts, dropping the closing epilogue."""
        parts: list[str] = []
        for segment in body.split(f"--{boundary}")[1:]:
            if segment.lstrip().startswith("--"):  # closing delimiter: --boundary--
                break
            parts.append(segment)
        return parts

    @staticmethod
    def _parse_http_part(http_text: str) -> dict[str, Any]:
        """Parse one inner HTTP response (status line + headers) from a part."""
        text = http_text.lstrip("\r\n")
        lines = text.splitlines()
        status = 0
        entity_id: str | None = None
        if lines:
            status_match = re.match(r"HTTP/\d\.\d\s+(\d+)", lines[0].strip())
            if status_match:
                status = int(status_match.group(1))
        for line in lines[1:]:
            if not line.strip():
                break  # blank line ends the header block
            key, _, value = line.partition(":")
            if key.strip().lower() in ("odata-entityid", "location"):
                guid = re.search(r"\(([0-9a-fA-F-]{36})\)", value)
                entity_id = guid.group(1) if guid else value.strip()
        return {
            "status": status,
            "entity_id": entity_id,
            "ok": 200 <= status < 300,
            "body": text,
        }

    @staticmethod
    def _extract_boundary(content_type: str) -> str | None:
        """Pull the boundary token out of a multipart Content-Type header."""
        match = re.search(r'boundary="?([^";\s]+)"?', content_type)
        return match.group(1) if match else None
