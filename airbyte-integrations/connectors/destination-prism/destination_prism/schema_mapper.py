#
# Copyright (c) 2025 ClearTax. All rights reserved.
#
"""
Airbyte JSON Schema to Prism SQL type mapper.

Converts Airbyte catalog JSON Schema property definitions into
Prism-compatible SQL column types for CREATE TABLE statements and
PyArrow schemas for typed ingestion.
"""

from typing import Any, Dict, List, Optional

import pyarrow as pa

# Airbyte JSON Schema type → Prism SQL type
# Airbyte JSON Schema type → Prism SQL type
# Airbyte JSON Schema type → Prism SQL type
# Airbyte JSON Schema type → Prism SQL type
_BASE_TYPE_MAP = {
    "string": "VARCHAR",
    "number": "VARCHAR",
    "integer": "VARCHAR",
    "boolean": "BOOLEAN",
    "array": "VARCHAR",
    "object": "VARCHAR",
    "null": "VARCHAR",
}

# Format hint overrides (applied before base type map)
_FORMAT_MAP = {
    "date": "DATE",
    "date-time": "VARCHAR",
    "time": "TIME",
}

# Airbyte system column - using a namespaced name to avoid collisions with source data
AIRBYTE_EMITTED_AT_COLUMN = "_airbyte_emitted_at"

# Mapping from Airbyte types to PyArrow types for typed ingestion
_ARROW_TYPE_MAP = {
    "string": pa.string(),
    "number": pa.string(),
    "integer": pa.string(),
    "boolean": pa.bool_(),
    "array": pa.string(),
    "object": pa.string(),
}

_ARROW_FORMAT_MAP = {
    "date": pa.date32(),
    "date-time": pa.string(),
    "time": pa.time64("us"),
}


def map_airbyte_type_to_sql(airbyte_type: str, format_hint: Optional[str] = None) -> str:
    """Map a single Airbyte type [+ format] to a SQL type string."""
    if format_hint and format_hint in _FORMAT_MAP:
        return _FORMAT_MAP[format_hint]
    return _BASE_TYPE_MAP.get(airbyte_type, "VARCHAR")


def map_airbyte_schema_to_prism(json_schema: Dict[str, Any]) -> Dict[str, str]:
    """
    Map an Airbyte JSON Schema to a dictionary of {column_name: sql_type}.
    """
    result = {}
    properties = json_schema.get("properties", {})

    for col_name, col_def in properties.items():
        # Airbyte types can be a single string or a list like ['null', 'string']
        field_type = col_def.get("type", "string")
        if isinstance(field_type, list):
            non_null_types = [t for t in field_type if t != "null"]
            primary_type = non_null_types[0] if non_null_types else "string"
        else:
            primary_type = field_type

        format_hint = col_def.get("format")
        result[col_name] = map_airbyte_type_to_sql(primary_type, format_hint)

    # Always append Airbyte metadata column, if not already present in the source schema
    if AIRBYTE_EMITTED_AT_COLUMN not in result:
        result[AIRBYTE_EMITTED_AT_COLUMN] = "VARCHAR"

    return result


def map_airbyte_schema_to_arrow(json_schema: Dict[str, Any]) -> pa.Schema:
    """
    Map an Airbyte JSON Schema to a PyArrow Schema for typed ingestion.
    """
    fields = []
    properties = json_schema.get("properties", {})

    for col_name, col_def in properties.items():
        field_type = col_def.get("type", "string")
        if isinstance(field_type, list):
            non_null_types = [t for t in field_type if t != "null"]
            primary_type = non_null_types[0] if non_null_types else "string"
        else:
            primary_type = field_type

        format_hint = col_def.get("format")
        if format_hint and format_hint in _ARROW_FORMAT_MAP:
            arrow_type = _ARROW_FORMAT_MAP[format_hint]
        else:
            arrow_type = _ARROW_TYPE_MAP.get(primary_type, pa.string())

        fields.append(pa.field(col_name, arrow_type))

    # Always append Airbyte metadata column, if not already present in the source schema
    # (using a namespaced name reduces collision risk significantly)
    if AIRBYTE_EMITTED_AT_COLUMN not in properties:
        fields.append(pa.field(AIRBYTE_EMITTED_AT_COLUMN, pa.string()))

    return pa.schema(fields)


def extract_primary_key(source_defined_primary_key: Optional[List[List[str]]]) -> Optional[str]:
    """
    Extract a comma-separated primary key string from Airbyte's nested PK format.
    """
    if not source_defined_primary_key:
        return None

    pk_columns = [pk[0] for pk in source_defined_primary_key if pk]
    return ", ".join(pk_columns) if pk_columns else None
