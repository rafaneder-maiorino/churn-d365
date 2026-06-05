"""Client for Microsoft Dataverse (Dynamics 365) Web API.

Handles OAuth 2.0 client credentials authentication via MSAL and provides
high-level methods for reading data from Dataverse tables.
"""

from __future__ import annotations

import logging
import os
import re
import time
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
        self, method: str, url: str, *, json: dict[str, Any], extra_headers: dict[str, str] | None = None
    ) -> requests.Response:
        """Send a write request, retrying on HTTP 429 (throttling).

        On a 429 the Retry-After header is honoured (falling back to
        DEFAULT_RETRY_AFTER seconds) and the request is retried up to
        MAX_THROTTLE_RETRIES times. Any other 4xx/5xx raises immediately
        via raise_for_status(); a 429 that survives all retries also raises.
        """
        headers = {**self._headers(), "Content-Type": "application/json"}
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(self.MAX_THROTTLE_RETRIES + 1):
            response = self._session.request(method, url, headers=headers, json=json, timeout=60)
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
