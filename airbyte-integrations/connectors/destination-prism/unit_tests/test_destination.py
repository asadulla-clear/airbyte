#
# Copyright (c) 2025 ClearTax. All rights reserved.
#
"""Unit tests for DestinationPrism."""

import logging
from unittest.mock import MagicMock, patch

import pytest
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)

from destination_prism.destination import DestinationPrism


@pytest.fixture
def config():
    return {
        "grpc_url": "grpc://localhost:8815",
        "http_url": "http://localhost:9815",
        "token": "test-token",
        "tenant_id": "test-tenant",
    }


@pytest.fixture
def configured_catalog():
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream={
                    "name": "users",
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"},
                        },
                    },
                    "supported_sync_modes": ["full_refresh"],
                },
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            )
        ]
    )


class TestCheck:
    def test_check_success(self, config):
        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            MockClient.return_value.check_connection.return_value = True
            destination = DestinationPrism()
            status = destination.check(logging.getLogger("test"), config)
            assert status.status.value == "SUCCEEDED"

    def test_check_failure(self, config):
        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            MockClient.return_value.check_connection.return_value = False
            destination = DestinationPrism()
            status = destination.check(logging.getLogger("test"), config)
            assert status.status.value == "FAILED"

    def test_check_exception(self, config):
        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            MockClient.return_value.check_connection.side_effect = Exception("error")
            destination = DestinationPrism()
            status = destination.check(logging.getLogger("test"), config)
            assert status.status.value == "FAILED"
            assert "error" in status.message


class TestWrite:
    def test_write_records_flushed_on_state(self, config, configured_catalog):
        messages = [
            AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream="users", data={"id": 1, "name": "Alice"}, emitted_at=123
                ),
            ),
            AirbyteMessage(
                type=Type.STATE,
                state=AirbyteStateMessage(data={"checkpoint": 1}),
            ),
        ]

        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            destination = DestinationPrism()
            list(destination.write(config, configured_catalog, messages))

            # Should be called once during state flush
            assert MockClient.return_value.insert_batch.call_count == 1

    def test_write_batch_flush_on_size(self, config, configured_catalog):
        # Set batch size to 1 for test
        config["batch_size"] = 1
        messages = [
            AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream="users", data={"id": 1, "name": "Alice"}, emitted_at=123
                ),
            ),
            AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream="users", data={"id": 2, "name": "Bob"}, emitted_at=124
                ),
            ),
        ]

        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 1
            destination = DestinationPrism()
            list(destination.write(config, configured_catalog, messages))

            # Flushed twice (one per record)
            assert MockClient.return_value.insert_batch.call_count == 2

    def test_write_overwrite_mode_drops(self, config, configured_catalog):
        configured_catalog.streams[0].destination_sync_mode = DestinationSyncMode.overwrite
        messages = [
            AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream="users", data={"id": 1, "name": "Alice"}, emitted_at=123
                ),
            )
        ]

        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            destination = DestinationPrism()
            list(destination.write(config, configured_catalog, messages))

            # Drop should be called for overwrite
            MockClient.return_value.drop_table.assert_called_once_with("users")

    def test_write_creates_table(self, config, configured_catalog):
        messages = [
            AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream="users", data={"id": 1, "name": "Alice"}, emitted_at=123
                ),
            )
        ]

        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            destination = DestinationPrism()
            list(destination.write(config, configured_catalog, messages))

            MockClient.return_value.create_table_from_airbyte_schema.assert_called_once()

    def test_write_closes_client(self, config, configured_catalog):
        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            destination = DestinationPrism()
            list(destination.write(config, configured_catalog, []))
            MockClient.return_value.close.assert_called_once()

    def test_write_skips_unknown_stream(self, config, configured_catalog):
        messages = [
            AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream="unknown", data={"id": 1}, emitted_at=123
                ),
            )
        ]

        with patch("destination_prism.destination.PrismClientWrapper") as MockClient:
            MockClient.return_value.batch_size = 10000
            destination = DestinationPrism()
            list(destination.write(config, configured_catalog, messages))

            # No table created or records inserted
            assert MockClient.return_value.create_table_from_airbyte_schema.call_count == 0
            assert MockClient.return_value.insert_batch.call_count == 0
