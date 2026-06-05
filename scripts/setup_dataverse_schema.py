"""Set up the Dataverse schema for the churn prediction project.

Creates:
  1. A custom Publisher 'Rafael Maiorino' with prefix 'rm' (if not present)
  2. 8 custom columns on the Account table:
     - rm_telcocustomerid (text)
     - rm_tenuremonths (whole number)
     - rm_totalcharges (decimal)
     - rm_contracttype (text)
     - rm_internetservice (text)
     - rm_techsupport (yes/no)
     - rm_onlinesecurity (yes/no)
     - rm_paymentmethod (text)

Idempotent: safe to run multiple times. Existing publishers/columns are skipped.
Run from project root: `python -m scripts.setup_dataverse_schema`
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import requests

from src.dataverse_client import DataverseClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

PUBLISHER_PREFIX = "rm"
PUBLISHER_UNIQUE_NAME = "RafaelMaiorino"
PUBLISHER_FRIENDLY_NAME = "Rafael Maiorino"
PUBLISHER_OPTIONVALUE_PREFIX = 10000

COLUMNS: list[dict[str, Any]] = [
    {"schema_name": "rm_TelcoCustomerId", "display_name": "Telco Customer ID",
     "description": "Original customer identifier from the IBM Telco dataset",
     "type": "string", "max_length": 50},
    {"schema_name": "rm_TenureMonths", "display_name": "Tenure (months)",
     "description": "Number of months as a customer",
     "type": "integer", "min_value": 0, "max_value": 120},
    {"schema_name": "rm_TotalCharges", "display_name": "Total Charges",
     "description": "Cumulative amount billed to the customer",
     "type": "decimal", "precision": 2, "min_value": 0, "max_value": 100000},
    {"schema_name": "rm_ContractType", "display_name": "Contract Type",
     "description": "Type of contract (Month-to-month, One year, Two year)",
     "type": "string", "max_length": 30},
    {"schema_name": "rm_InternetService", "display_name": "Internet Service",
     "description": "Internet service type (DSL, Fiber optic, No)",
     "type": "string", "max_length": 30},
    {"schema_name": "rm_TechSupport", "display_name": "Tech Support",
     "description": "Whether the customer has tech support service",
     "type": "boolean"},
    {"schema_name": "rm_OnlineSecurity", "display_name": "Online Security",
     "description": "Whether the customer has online security service",
     "type": "boolean"},
    {"schema_name": "rm_PaymentMethod", "display_name": "Payment Method",
     "description": "Customer's payment method",
     "type": "string", "max_length": 50},
]


def _localized(text: str) -> dict[str, Any]:
    return {"LocalizedLabels": [{"Label": text, "LanguageCode": 1033}]}


def ensure_publisher(client: DataverseClient) -> str:
    base = client._base_url
    headers = client._headers()

    response = requests.get(
        f"{base}/publishers",
        headers=headers,
        params={"$filter": f"uniquename eq '{PUBLISHER_UNIQUE_NAME}'",
                "$select": "publisherid,uniquename,customizationprefix"},
        timeout=30,
    )
    response.raise_for_status()
    existing = response.json().get("value", [])
    if existing:
        pub = existing[0]
        logger.info("Publisher already exists: %s (prefix=%s)",
                    pub["uniquename"], pub["customizationprefix"])
        return pub["publisherid"]

    logger.info("Creating publisher %s (prefix=%s)...", PUBLISHER_UNIQUE_NAME, PUBLISHER_PREFIX)
    response = requests.post(
        f"{base}/publishers",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "uniquename": PUBLISHER_UNIQUE_NAME,
            "friendlyname": PUBLISHER_FRIENDLY_NAME,
            "customizationprefix": PUBLISHER_PREFIX,
            "customizationoptionvalueprefix": PUBLISHER_OPTIONVALUE_PREFIX,
            "description": "Publisher for the churn prediction project",
        },
        timeout=30,
    )
    if not response.ok:
        logger.error("Failed to create publisher: %s", response.text)
        response.raise_for_status()

    response = requests.get(
        f"{base}/publishers",
        headers=headers,
        params={"$filter": f"uniquename eq '{PUBLISHER_UNIQUE_NAME}'", "$select": "publisherid"},
        timeout=30,
    )
    pid = response.json()["value"][0]["publisherid"]
    logger.info("Publisher created: %s", pid)
    return pid


def column_exists(client: DataverseClient, logical_name: str) -> bool:
    base = client._base_url
    headers = client._headers()
    response = requests.get(
        f"{base}/EntityDefinitions(LogicalName='account')"
        f"/Attributes(LogicalName='{logical_name.lower()}')",
        headers=headers,
        params={"$select": "LogicalName"},
        timeout=30,
    )
    return response.status_code == 200


def build_metadata(spec: dict[str, Any]) -> dict[str, Any]:
    base = {
        "SchemaName": spec["schema_name"],
        "DisplayName": _localized(spec["display_name"]),
        "Description": _localized(spec.get("description", "")),
        "RequiredLevel": {"Value": "None"},
    }
    t = spec["type"]
    if t == "string":
        return {**base,
                "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                "AttributeType": "String",
                "AttributeTypeName": {"Value": "StringType"},
                "FormatName": {"Value": "Text"},
                "MaxLength": spec.get("max_length", 100)}
    if t == "integer":
        return {**base,
                "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
                "AttributeType": "Integer",
                "AttributeTypeName": {"Value": "IntegerType"},
                "Format": "None",
                "MinValue": spec.get("min_value", -2147483648),
                "MaxValue": spec.get("max_value", 2147483647)}
    if t == "decimal":
        return {**base,
                "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
                "AttributeType": "Decimal",
                "AttributeTypeName": {"Value": "DecimalType"},
                "Precision": spec.get("precision", 2),
                "MinValue": spec.get("min_value", 0),
                "MaxValue": spec.get("max_value", 100000000)}
    if t == "boolean":
        return {**base,
                "@odata.type": "Microsoft.Dynamics.CRM.BooleanAttributeMetadata",
                "AttributeType": "Boolean",
                "AttributeTypeName": {"Value": "BooleanType"},
                "OptionSet": {
                    "@odata.type": "Microsoft.Dynamics.CRM.BooleanOptionSetMetadata",
                    "TrueOption": {"Value": 1, "Label": _localized("Yes")},
                    "FalseOption": {"Value": 0, "Label": _localized("No")},
                },
                "DefaultValue": False}
    raise ValueError(f"Unknown column type: {t}")


def create_column(client: DataverseClient, spec: dict[str, Any]) -> None:
    logical_name = spec["schema_name"].lower()
    if column_exists(client, logical_name):
        logger.info("Column already exists: %s — skipping", logical_name)
        return

    base = client._base_url
    headers = {**client._headers(), "Content-Type": "application/json"}
    metadata = build_metadata(spec)

    logger.info("Creating %s (%s)...", spec["schema_name"], spec["type"])
    response = requests.post(
        f"{base}/EntityDefinitions(LogicalName='account')/Attributes",
        headers=headers,
        json=metadata,
        timeout=60,
    )
    if not response.ok:
        logger.error("Failed: %s", response.text)
        response.raise_for_status()
    logger.info("Created: %s", logical_name)


def main() -> int:
    client = DataverseClient()
    logger.info("Step 1/2: Ensure publisher exists")
    ensure_publisher(client)
    logger.info("Step 2/2: Creating %d custom columns on Account", len(COLUMNS))
    for spec in COLUMNS:
        create_column(client, spec)
    logger.info("Schema setup complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
