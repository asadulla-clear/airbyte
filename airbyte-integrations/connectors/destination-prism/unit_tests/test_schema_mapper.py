#
# Copyright (c) 2025 ClearTax. All rights reserved.
#
"""Unit tests for schema_mapper module."""

import pyarrow as pa
import pytest

from destination_prism.schema_mapper import (
    AIRBYTE_EMITTED_AT_COLUMN,
    extract_primary_key,
    map_airbyte_schema_to_arrow,
    map_airbyte_schema_to_prism,
    map_airbyte_type_to_sql,
)


# ------------------------------------------------------------------
# map_airbyte_type_to_sql
# ------------------------------------------------------------------


class TestMapAirbyteTypeToSql:
    """Tests for individual type mapping."""

    def test_string(self):
        assert map_airbyte_type_to_sql("string") == "VARCHAR"

    def test_integer(self):
        assert map_airbyte_type_to_sql("integer") == "VARCHAR"

    def test_number(self):
        assert map_airbyte_type_to_sql("number") == "VARCHAR"

    def test_boolean(self):
        assert map_airbyte_type_to_sql("boolean") == "BOOLEAN"

    def test_array(self):
        assert map_airbyte_type_to_sql("array") == "VARCHAR"

    def test_object(self):
        assert map_airbyte_type_to_sql("object") == "VARCHAR"

    def test_null(self):
        assert map_airbyte_type_to_sql("null") == "VARCHAR"

    def test_unknown_type_defaults_to_varchar(self):
        assert map_airbyte_type_to_sql("binary") == "VARCHAR"

    def test_format_date(self):
        assert map_airbyte_type_to_sql("string", format_hint="date") == "DATE"

    def test_format_datetime(self):
        assert map_airbyte_type_to_sql("string", format_hint="date-time") == "VARCHAR"

    def test_format_time(self):
        assert map_airbyte_type_to_sql("string", format_hint="time") == "TIME"

    def test_format_overrides_base_type(self):
        # Even if type is "number", a date-time format should win
        assert map_airbyte_type_to_sql("number", format_hint="date-time") == "VARCHAR"


# ------------------------------------------------------------------
# map_airbyte_schema_to_prism
# ------------------------------------------------------------------


class TestMapAirbyteSchemaToSql:
    """Tests for full schema conversion."""

    def test_simple_schema(self):
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "amount": {"type": "number"},
            },
        }
        result = map_airbyte_schema_to_prism(schema)

        assert result["id"] == "VARCHAR"
        assert result["name"] == "VARCHAR"
        assert result["amount"] == "VARCHAR"
        assert result[AIRBYTE_EMITTED_AT_COLUMN] == "VARCHAR"
        assert AIRBYTE_EMITTED_AT_COLUMN == "created_at"

    def test_nullable_fields(self):
        schema = {
            "properties": {
                "email": {"type": ["null", "string"]},
                "age": {"type": ["null", "integer"]},
            }
        }
        result = map_airbyte_schema_to_prism(schema)
        assert result["email"] == "VARCHAR"
        assert result["age"] == "VARCHAR"
        assert "created_at" in result

    def test_format_hints(self):
        schema = {
            "properties": {
                "created_at_source": {"type": "string", "format": "date-time"},
                "birthday": {"type": "string", "format": "date"},
            }
        }
        result = map_airbyte_schema_to_prism(schema)
        assert result["created_at_source"] == "VARCHAR"
        assert result["birthday"] == "DATE"

    def test_complex_types(self):
        schema = {
            "properties": {
                "tags": {"type": "array", "items": {"type": "string"}},
                "metadata": {"type": "object"},
            }
        }
        result = map_airbyte_schema_to_prism(schema)
        assert result["tags"] == "VARCHAR"
        assert result["metadata"] == "VARCHAR"

    def test_empty_schema(self):
        result = map_airbyte_schema_to_prism({})
        # Should only have the system column
        assert len(result) == 1
        assert AIRBYTE_EMITTED_AT_COLUMN in result
        assert AIRBYTE_EMITTED_AT_COLUMN == "created_at"

    def test_airbyte_emitted_at_always_appended(self):
        schema = {"properties": {"id": {"type": "integer"}}}
        result = map_airbyte_schema_to_prism(schema)
        assert AIRBYTE_EMITTED_AT_COLUMN in result
        assert AIRBYTE_EMITTED_AT_COLUMN == "created_at"


class TestMapAirbyteSchemaToArrow:
    """Tests for PyArrow schema conversion."""

    def test_simple_arrow_schema(self):
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "is_active": {"type": "boolean"},
                "amount": {"type": "number"},
                "created_at_source": {"type": "string", "format": "date-time"},
            }
        }
        arrow_schema = map_airbyte_schema_to_arrow(schema)

        assert arrow_schema.field("id").type == pa.string()
        assert arrow_schema.field("name").type == pa.string()
        assert arrow_schema.field("is_active").type == pa.bool_()
        assert arrow_schema.field("amount").type == pa.string()
        assert arrow_schema.field("created_at_source").type == pa.string()
        assert arrow_schema.field(AIRBYTE_EMITTED_AT_COLUMN).type == pa.string()
        assert AIRBYTE_EMITTED_AT_COLUMN == "created_at"

    def test_complex_types_to_arrow(self):
        schema = {
            "properties": {
                "tags": {"type": "array"},
                "metadata": {"type": "object"},
            }
        }
        arrow_schema = map_airbyte_schema_to_arrow(schema)
        assert arrow_schema.field("tags").type == pa.string()
        assert arrow_schema.field("metadata").type == pa.string()


# ------------------------------------------------------------------
# extract_primary_key
# ------------------------------------------------------------------


class TestExtractPrimaryKey:
    """Tests for PK extraction."""

    def test_single_pk(self):
        assert extract_primary_key([["id"]]) == "id"

    def test_composite_pk(self):
        assert extract_primary_key([["org_id"], ["user_id"]]) == "org_id, user_id"

    def test_none_pk(self):
        assert extract_primary_key(None) is None

    def test_empty_pk(self):
        assert extract_primary_key([]) is None

    def test_empty_inner_pk(self):
        assert extract_primary_key([[]]) is None
