#
# Copyright (c) 2025 ClearTax. All rights reserved.
#
"""
Airbyte destination connector for Prism Engine.

Ingests data from Airbyte sources into Prism via Flight SQL protocol
using the PrismClient library.
"""

import json
import logging
import time
from collections import defaultdict
import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

import dateutil.parser
import pyarrow as pa
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    Type,
)

from .prism_client_wrapper import PrismClientWrapper
from .schema_mapper import (
    AIRBYTE_EMITTED_AT_COLUMN,
    map_airbyte_schema_to_arrow,
)

logger = logging.getLogger("airbyte")


class DestinationPrism(Destination):
    """Airbyte destination connector for Prism Engine via Flight SQL."""

    # ------------------------------------------------------------------
    # check
    # ------------------------------------------------------------------

    def check(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> AirbyteConnectionStatus:
        """
        Test connection to Prism Engine.

        Called when the user clicks "Test" in the Airbyte UI.
        Executes ``SELECT 1`` over Flight SQL to validate connectivity, auth, and tenant.
        """
        logger.info(">>> PRISM DESTINATION STARTING (Varcharization + Deep Cleaner V4) <<<")
        try:
            client = PrismClientWrapper(config)

            if client.check_connection():
                logger.info("Prism connection test succeeded")
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)

            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message="Connection test failed: SELECT 1 did not return expected result",
            )

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Connection failed: {e!s}",
            )

    # ------------------------------------------------------------------
    # write
    # ------------------------------------------------------------------

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """
        Main write loop. Buffers records and flushes them to Prism.
        """
        logger.info(">>> PRISM DESTINATION STARTING (Varcharization + Deep Cleaner V4) <<<")
        client = PrismClientWrapper(config) # Changed from DestinationPrism._get_client(config) to PrismClientWrapper(config) to match existing pattern

        # helper for recursive cleaning
        def clean_record(obj):
            """Recursively convert datetime/date objects to ISO strings."""
            if isinstance(obj, dict):
                return {k: clean_record(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_record(v) for v in obj]
            elif isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            return obj

        # Lookup: stream_name → ConfiguredAirbyteStream
        catalog_map: Dict[str, ConfiguredAirbyteStream] = {
            s.stream.name: s for s in configured_catalog.streams
        }

        # Cache Arrow Schemas and Timestamp field names per stream
        stream_schemas: Dict[str, pa.Schema] = {}
        timestamp_fields: Dict[str, Set[str]] = {}

        # Per-stream record buffer
        buffer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Track which tables have been initialised
        tables_created: Set[str] = set()

        try:
            for message in input_messages:
                if message.type == Type.RECORD:
                    stream_name = message.record.stream

                    if stream_name not in catalog_map:
                        logger.debug(
                            f"Stream {stream_name} not in configured catalog, skipping"
                        )
                        continue

                    # Lazy table creation and schema caching
                    if stream_name not in tables_created:
                        stream = catalog_map[stream_name]
                        self._ensure_table_exists(client, stream_name, stream)
                        tables_created.add(stream_name)

                        # Cache schema for typed ingestion
                        schema = map_airbyte_schema_to_arrow(stream.stream.json_schema)
                        stream_schemas[stream_name] = schema
                        timestamp_fields[stream_name] = {
                            f.name for f in schema if isinstance(f.type, pa.TimestampType)
                        }

                    # Preprocess record data: serialize JSON, convert timestamps
                    raw_data = message.record.data
                    schema = stream_schemas[stream_name]
                    ts_cols = timestamp_fields[stream_name]

                    # Initial recursive clean
                    raw_data = clean_record(raw_data)

                    record_data = {}

                    record_data = {}
                    for k, v in raw_data.items():
                        if v is None:
                            record_data[k] = None
                            continue

                        # Serialize complex types
                        if isinstance(v, (dict, list)):
                            record_data[k] = json.dumps(v)
                        # Explicit timestamp conversion for Arrow
                        elif k in ts_cols:
                            try:
                                if isinstance(v, (int, float)):
                                    # Assume epoch ms
                                    record_data[k] = datetime.datetime.fromtimestamp(v / 1000.0)
                                else:
                                    import dateutil.parser
                                    record_data[k] = dateutil.parser.parse(str(v))
                            except Exception:
                                record_data[k] = v
                        else:
                            record_data[k] = v


                        # Generic Type Coercion based on Arrow Schema
                        # This handles "new schema" fields dynamically without hardcoding names.
                        field_index = schema.get_field_index(k)
                        if field_index != -1:
                            target_type = schema.field(field_index).type
                            val = record_data[k]
                            
                            # If target is string, force string conversion for EVERYTHING
                            if pa.types.is_string(target_type) or pa.types.is_large_string(target_type):
                                if val is not None and not isinstance(val, str):
                                    if isinstance(val, datetime.datetime):
                                        record_data[k] = val.isoformat()
                                    else:
                                        record_data[k] = str(val)
                            
                            # If target is boolean, ensure bool
                            elif pa.types.is_boolean(target_type):
                                if val is not None:
                                    record_data[k] = bool(val)

                    # Handle CDC metadata fields explicitly if they are not in the schema
                    # (e.g. if schema mapping missed them but they are in the record)
                    for cdc_field in ["_ab_cdc_cursor", "_ab_cdc_deleted_at", "_ab_cdc_updated_at"]:
                        if cdc_field in record_data and schema.get_field_index(cdc_field) == -1:
                             val = record_data[cdc_field]
                             if isinstance(val, datetime.datetime):
                                 record_data[cdc_field] = val.isoformat()
                             elif val is not None and not isinstance(val, str):
                                 record_data[cdc_field] = str(val)


                    # Handle CDC metadata fields explicitly
                    # MongoDB source might emit these as datetime objects, but we map them to strings
                    for cdc_field in ["_ab_cdc_cursor", "_ab_cdc_deleted_at", "_ab_cdc_updated_at"]:
                        if cdc_field in record_data:
                            val = record_data[cdc_field]
                            if isinstance(val, datetime.datetime):
                                record_data[cdc_field] = val.isoformat()
                            elif val is not None and not isinstance(val, str):
                                record_data[cdc_field] = str(val)


                    # Add system timestamp
                    # ALWAYS use ISO string to avoid ArrowTypeError
                    emitted_at = datetime.datetime.now().isoformat()
                    record_data[AIRBYTE_EMITTED_AT_COLUMN] = emitted_at

                    # FINAL SAFEGUARD: Deep clean the ENTIRE record_data before buffering
                    # This catches emitted_at, CDC fields, and anything else added during processing
                    record_data = clean_record(record_data)

                    buffer[stream_name].append(record_data)

                    # Flush on batch_size
                    if len(buffer[stream_name]) >= client.batch_size:
                        self._flush_stream(
                            client, stream_name, buffer[stream_name], schema=schema
                        )
                        buffer[stream_name].clear()

                    # Flush buffers before checkpointing
                    self._flush_all(client, buffer, stream_schemas)
                    
                    # FORCE VALID STATE MESSAGE WITH ID
                    # The modern protocol requires 'id' for all state messages.
                    # We reconstruct the message to ensure it's valid and has an ID.
                    import uuid
                    
                    original_state = message.state
                    state_id = getattr(original_state, 'id', None) or str(uuid.uuid4())
                    
                    # If using newer CDK models, we might need to be careful about strictness.
                    # We'll try to use the official model first, but ensure ID is set.
                    if original_state:
                         original_state.id = state_id
                         yield message
                    else:
                         # Fallback if state is somehow None but type is STATE (shouldn't happen)
                         pass


                elif message.type == Type.LOG:
                    yield message

                else:
                    logger.info(
                        f"Message type {message.type} not handled, skipping"
                    )

            # Final flush for remaining buffered records
            self._flush_all(client, buffer, stream_schemas)

        finally:
            client.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure_table_exists(
        client: PrismClientWrapper,
        table_name: str,
        stream: ConfiguredAirbyteStream,
    ) -> None:
        """
        Create or prepare the target table for a stream.

        - **overwrite**: drop existing table, then create fresh
        - **append / append_dedup**: create if not exists
        """
        # Handle overwrite sync mode: drop and recreate to ensure schema match
        if stream.destination_sync_mode == DestinationSyncMode.overwrite:
            try:
                client.drop_table(table_name)
                logger.info(f"Dropped table {table_name} (overwrite mode)")
            except Exception:
                logger.debug(f"Could not drop {table_name} (may not exist yet)")

        # Create table (idempotent for append mode)
        try:
            client.create_table_from_airbyte_schema(
                table_name=table_name,
                json_schema=stream.stream.json_schema,
                primary_key=stream.stream.source_defined_primary_key,
            )
            logger.info(f"Ensured table {table_name} exists")
        except Exception:
            logger.debug(f"Table {table_name} creation noted, continuing")

    @staticmethod
    def _flush_stream(
        client: PrismClientWrapper,
        stream_name: str,
        records: List[Dict[str, Any]],
        schema: Optional[pa.Schema] = None,
    ) -> None:
        """Flush buffered records for a single stream using explicit schema."""
        if not records:
            return

        try:
            if schema:
                # logger.info(f"Using explicit schema for {stream_name}: {schema}")
                pass

            rows = client.insert_batch(stream_name, records, schema=schema)
            logger.info(f"Flushed {rows} records to {stream_name}")
        except Exception as e:
            logger.error(
                f"Failed to flush {len(records)} records to {stream_name}: {e}"
            )
            raise

    @staticmethod
    def _flush_all(
        client: PrismClientWrapper,
        buffer: Dict[str, List[Dict[str, Any]]],
        stream_schemas: Dict[str, pa.Schema],
    ) -> None:
        """Flush all per-stream buffers."""
        for stream_name, records in buffer.items():
            schema = stream_schemas.get(stream_name)
            DestinationPrism._flush_stream(client, stream_name, records, schema=schema)
        buffer.clear()
