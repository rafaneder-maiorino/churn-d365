"""Client for Microsoft Dataverse (Dynamics 365) Web API.

Handles OAuth 2.0 client credentials authentication via MSAL and provides
high-level methods for reading data from Dataverse tables.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv
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
    def from_env(cls, env_path: str = ".env") -> "DataverseConfig":
        """Load configuration from a .env file with explicit path."""
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
        return int(response.text)

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
