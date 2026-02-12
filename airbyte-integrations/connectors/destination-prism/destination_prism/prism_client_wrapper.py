#
# Copyright (c) 2025 ClearTax. All rights reserved.
#
"""
PrismClient wrapper for Airbyte destination connector.

Translates Airbyte configuration and stream schemas into PrismClient calls.
"""

import logging
from typing import Any, Dict, List, Mapping, Optional

import pyarrow as pa

from .prism_client import PrismClient
from .schema_mapper import extract_primary_key, map_airbyte_schema_to_prism

logger = logging.getLogger("airbyte")


class PrismClientWrapper:
    """
    Thin adapter between Airbyte config/catalog and the PrismClient Flight SQL API.
    """

    def __init__(self, config: Mapping[str, Any]):
        """Initialize from Airbyte config."""
        self.config = config
        self.batch_size: int = int(config.get("batch_size", 10_000))

        self.client = PrismClient(
            grpc_url=config["grpc_url"],
            http_url=config["http_url"],
            token=config["token"],
            tenant_id=config["tenant_id"],
            timeout=int(config.get("timeout", 300)),
        )

    # ------------------------------------------------------------------
    # Connection health
    # ------------------------------------------------------------------

    def check_connection(self) -> bool:
        """Test connection by executing ``SELECT 1``."""
        try:
            result = self.client.query("SELECT 1 AS test")
            return result.row_count == 1
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False

    # ------------------------------------------------------------------
    # DDL helpers
    # ------------------------------------------------------------------

    def create_table_from_airbyte_schema(
        self,
        table_name: str,
        json_schema: Dict[str, Any],
        primary_key: Optional[List[List[str]]] = None,
    ) -> None:
        """Create a Prism table from an Airbyte stream's JSON Schema."""
        from .schema_mapper import AIRBYTE_EMITTED_AT_COLUMN
        prism_schema = map_airbyte_schema_to_prism(json_schema)
        pk = extract_primary_key(primary_key)
        
        # Determine tracking column: if source has 'created_at', use that, otherwise use our system column
        tracking_col = "created_at" if "created_at" in prism_schema else AIRBYTE_EMITTED_AT_COLUMN

        self.client.create_table(
            table_name=table_name,
            schema=prism_schema,
            primary_key=pk,
            created_at_column=tracking_col,
        )

    def truncate_table(self, table_name: str) -> None:
        """Truncate table rows."""
        self.client.execute_ddl(f'TRUNCATE TABLE "{table_name}"')

    def drop_table(self, table_name: str) -> None:
        """Drop table if exists (used for overwriting with schema change)."""
        self.client.execute_ddl(f'DROP TABLE IF EXISTS "{table_name}"')

    # ------------------------------------------------------------------
    # Data ingestion
    # ------------------------------------------------------------------

    def insert_batch(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        schema: Optional[pa.Schema] = None,
    ) -> int:
        """
        Insert a batch of records into the given table.
        Uses explicit schema if provided to ensure correct Arrow types.
        """
        if not records:
            return 0

        if schema:
            try:
                # Construct Arrow Table with explicit schema
                return self.client.insert(table_name, pa.Table.from_pylist(records, schema=schema))
            except Exception as e:
                logger.error(f"!!! ARROW TABLE ERROR IN {table_name} !!!")
                logger.error(f"Error: {e}")
                logger.error(f"Schema: {schema}")
                # Log types of fields in the first record to find the offender
                if records:
                    first_record = records[0]
                    for k, v in first_record.items():
                        logger.error(f"Field '{k}': value={v}, type={type(v)}")
                raise e

        return self.client.insert(table_name, records)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Release underlying Flight SQL connection."""
        if hasattr(self.client, "close"):
            self.client.close()
