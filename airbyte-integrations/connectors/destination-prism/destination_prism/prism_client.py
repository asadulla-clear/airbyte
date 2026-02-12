"""
Prism Client v2 - Improved Flight SQL Client for Prism Engine

Key improvements based on integration test analysis:
- X-Tenant-ID header support for multi-tenancy
- Per-request header authentication (not middleware)
- Simplified query execution via direct Ticket API
- DDL operation support (CREATE/DROP TABLE)
- Multi-tenant context switching
"""

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import logging
from dataclasses import dataclass
from datetime import datetime
from contextlib import contextmanager
from enum import Enum
import os
import requests

pd = None

logger = logging.getLogger(__name__)


class ExecutionEngine(str, Enum):
    """
    Execution engine for query routing (ADR-021).

    Prism supports two execution engines:
    - TRINO: Distributed query engine (default) - handles DDL/DML and large datasets
    - DUCKDB: Fast local analytics engine - 10-100x faster for SELECT queries

    Both engines read from the same Iceberg tables on S3.
    """
    TRINO = "trino"
    DUCKDB = "duckdb"

    @classmethod
    def default(cls) -> 'ExecutionEngine':
        """Get default execution engine (Trino)."""
        return cls.TRINO

    @classmethod
    def validate(cls, value: Optional[str]) -> Optional['ExecutionEngine']:
        """
        Validate and convert string to ExecutionEngine enum.

        Args:
            value: String value ("trino", "duckdb", or None)

        Returns:
            ExecutionEngine enum or None

        Raises:
            ValueError: If value is not a valid engine
        """
        if value is None:
            return None

        value_lower = value.lower()
        try:
            return cls(value_lower)
        except ValueError:
            valid_engines = [e.value for e in cls]
            raise ValueError(
                f"Invalid execution_engine: '{value}'. "
                f"Must be one of: {valid_engines}"
            )


@dataclass
class QueryResult:
    """Query result container."""
    data: pa.Table
    row_count: int
    column_names: List[str]
    execution_time_ms: float

    def to_pandas(self):
        """
        Convert to pandas DataFrame.

        Note: pandas has been removed from prism_client.
        Use PyArrow operations instead:
        - result.to_arrow() for Arrow Table
        - result.to_dict() for list of dicts
        """
        raise NotImplementedError(
            "pandas has been removed from prism_client. "
            "Use PyArrow operations instead:\n"
            "  - result.to_arrow() for Arrow Table\n"
            "  - result.to_dict() for list of dicts\n"
            "  - Or install pandas separately and use: table.to_pandas()"
        )

    def to_dict(self) -> List[Dict[str, Any]]:
        """Convert to list of dictionaries."""
        return self.data.to_pylist()

    def to_arrow(self) -> pa.Table:
        """Get Arrow Table."""
        return self.data


class PrismClient:
    """
    PyArrow-based Flight SQL Client for Prism Engine.

    Key features:
    - Token + Tenant ID authentication
    - Multi-tenant support with context switching
    - Simplified query execution
    - DDL operation support
    - PyArrow-only (zero-copy, efficient operations)
    - Efficient Parquet file ingestion

    Requirements:
    - pyarrow (required)

    Example:
        ```python
        # Initialize with separate URLs
        client = PrismClient(
            grpc_url="grpc://prism.cleartax.co:8815",
            http_url="http://prism.cleartax.co:9815",
            token="prism_cs_test_caller_abc123",
            tenant_id="company-a"
        )

        # Execute query (returns Arrow Table)
        result = client.query("SELECT * FROM sales_register LIMIT 10")
        print(f"Rows: {result.row_count}")
        rows = result.to_dict()

        # Insert from Parquet file (most efficient)
        rows_inserted = client.insert_parquet("users", "data.parquet")

        # Insert from Arrow Table (zero-copy)
        import pyarrow as pa
        table = pa.table({'id': [1, 2], 'name': ['Alice', 'Bob']})
        client.insert("users", table)

        # Insert from list of dicts
        data = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        client.insert("users", data)

        # Switch tenant
        client.set_tenant("company-b")
        result = client.query("SELECT * FROM sales_register")  # Different data

        # DDL operation
        client.execute_ddl("CREATE TABLE users (...)")

        # Context manager for tenant switching
        with client.as_tenant("company-c"):
            result = client.query("SELECT * FROM users")
        ```
    """

    def __init__(
        self,
        grpc_url: str,
        http_url: str,
        token: Optional[str] = None,
        tenant_id: Optional[str] = None,
        timeout: int = 300
    ):
        """
        Initialize Prism Flight SQL client.

        Args:
            grpc_url: Complete gRPC URL (e.g., "grpc://prism.cleartax.co:8815")
            http_url: Complete HTTP URL (e.g., "http://prism.cleartax.co:9815")
            token: Bearer token for authentication
            tenant_id: Tenant ID for multi-tenant isolation (required for queries)
            timeout: Query timeout in seconds
        """
        self.grpc_url = grpc_url
        self.http_url = http_url
        self.token = token
        self.tenant_id = tenant_id
        self.timeout = timeout
        self.client = None

        # Connect (no auth in constructor - headers sent per request)
        self._connect()

    def _connect(self):
        """Establish Flight SQL connection (no authentication yet)."""
        try:
            logger.info(f"Connecting to Prism at {self.grpc_url}")

            # Create Flight client WITHOUT middleware auth
            # Authentication happens per-request via headers
            self.client = flight.FlightClient(self.grpc_url)

            logger.info("Connected to Prism Flight SQL server")

        except Exception as e:
            logger.error(f"Failed to connect to Prism: {e}")
            raise

    def _get_call_options(
        self,
        tenant_id: Optional[str] = None,
        execution_engine: Optional[Union[ExecutionEngine, str]] = None,
        limit: Optional[int] = None
    ) -> flight.FlightCallOptions:
        """
        Build FlightCallOptions with authentication headers.

        Args:
            tenant_id: Override tenant ID for this request
            execution_engine: Execution engine for query routing ("trino" or "duckdb")
            limit: Optional row limit to apply server-side

        Returns:
            FlightCallOptions with Authorization, X-Tenant-ID, X-Execution-Engine, and X-Limit headers
        """
        headers = []

        # Add Bearer token if available
        if self.token:
            headers.append((b"authorization", f"Bearer {self.token}".encode()))

        # Add tenant ID (use override or default)
        effective_tenant_id = tenant_id or self.tenant_id
        if effective_tenant_id:
            headers.append((b"x-tenant-id", effective_tenant_id.encode()))
        else:
            logger.warning("No tenant_id specified - query may fail")

        # Add execution engine (ADR-021: Multi-engine routing)
        if execution_engine:
            # Validate and convert to enum if string
            if isinstance(execution_engine, str):
                engine_enum = ExecutionEngine.validate(execution_engine)
            else:
                engine_enum = execution_engine

            if engine_enum:
                headers.append((b"x-execution-engine", engine_enum.value.encode()))
                logger.debug(f"Using execution engine: {engine_enum.value}")

        # Add limit header for server-side limit application
        if limit is not None and limit > 0:
            headers.append((b"x-limit", str(limit).encode()))
            logger.debug(f"Using server-side limit: {limit}")

        return flight.FlightCallOptions(headers=headers)

    def set_tenant(self, tenant_id: str):
        """
        Set default tenant ID for subsequent queries.

        Args:
            tenant_id: Tenant ID to use

        Example:
            ```python
            client.set_tenant("company-a")
            result = client.query("SELECT * FROM sales")  # Uses company-a
            ```
        """
        self.tenant_id = tenant_id
        logger.info(f"Switched to tenant: {tenant_id}")

    @contextmanager
    def as_tenant(self, tenant_id: str):
        """
        Context manager for temporary tenant switching.

        Args:
            tenant_id: Temporary tenant ID

        Example:
            ```python
            client.set_tenant("company-a")

            with client.as_tenant("company-b"):
                result = client.query("SELECT * FROM sales")  # Uses company-b

            # Back to company-a
            result = client.query("SELECT * FROM sales")  # Uses company-a
            ```
        """
        original_tenant_id = self.tenant_id
        self.tenant_id = tenant_id
        logger.info(f"Entering tenant context: {tenant_id}")
        try:
            yield self
        finally:
            self.tenant_id = original_tenant_id
            logger.info(f"Exiting tenant context, restored: {original_tenant_id}")

    def query(
        self,
        sql: str,
        limit: Optional[int] = None,
        tenant_id: Optional[str] = None,
        return_type: str = "arrow",
        execution_engine: Optional[Union[ExecutionEngine, str]] = None
    ) -> Union[QueryResult, List[Dict[str, Any]]]:
        """
        Execute SQL query.

        Args:
            sql: SQL query to execute
            limit: Optional row limit
            tenant_id: Override tenant ID for this query
            return_type: 'arrow' (QueryResult) or 'dict'
            execution_engine: Execution engine ("trino", "duckdb", or ExecutionEngine enum)
                             Defaults to "trino" if not specified.
                             Use "duckdb" for 10-100x faster analytics queries.

        Returns:
            QueryResult or list of dicts based on return_type

        Example:
            ```python
            # Return as Arrow Table wrapped in QueryResult (default)
            result = client.query("SELECT * FROM sales_register LIMIT 10")
            print(f"Rows: {result.row_count}")
            print(f"Columns: {result.column_names}")

            # Access as Arrow Table
            arrow_table = result.to_arrow()

            # Return as list of dicts
            rows = client.query("SELECT * FROM sales_register", return_type="dict")
            for row in rows:
                print(row)

            # Override tenant for this query
            result = client.query("SELECT * FROM sales", tenant_id="company-b")

            # Route query to DuckDB for fast analytics (ADR-021)
            result = client.query(
                "SELECT SUM(amount) FROM orders",
                execution_engine="duckdb"  # 10-100x faster
            )

            # Or use enum
            from prism_client import ExecutionEngine
            result = client.query(
                "SELECT * FROM large_dataset",
                execution_engine=ExecutionEngine.DUCKDB
            )
            ```
        """
        try:
            import time
            start_time = time.time()

            # Query is sent to server as-is; limit is applied server-side via X-Limit header
            query = sql.strip()

            logger.info(f"Executing query: {query[:100]}...")

            # Create Ticket with query (simplified API)
            ticket = flight.Ticket(query.encode('utf-8'))

            # Execute with authentication headers (including execution_engine and limit)
            options = self._get_call_options(
                tenant_id=tenant_id,
                execution_engine=execution_engine,
                limit=limit
            )
            reader = self.client.do_get(ticket, options)

            # Read all batches into Arrow Table
            arrow_table = reader.read_all()

            execution_time_ms = (time.time() - start_time) * 1000

            logger.info(
                f"Query executed: {arrow_table.num_rows} rows, "
                f"{arrow_table.num_columns} columns in {execution_time_ms:.2f}ms"
            )

            # Create QueryResult
            result = QueryResult(
                data=arrow_table,
                row_count=arrow_table.num_rows,
                column_names=arrow_table.column_names,
                execution_time_ms=execution_time_ms
            )

            # Return in requested format
            if return_type == "dict":
                return result.to_dict()
            else:
                return result

        except flight.FlightUnauthenticatedError as e:
            logger.error(f"Authentication failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def execute(self, sql: str, tenant_id: Optional[str] = None) -> QueryResult:
        """
        Execute SQL query (alias for query).

        Args:
            sql: SQL query to execute
            tenant_id: Override tenant ID for this query

        Returns:
            QueryResult
        """
        return self.query(sql, tenant_id=tenant_id, return_type="arrow")

    def execute_ddl(
        self,
        sql: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """
        Execute DDL statement (CREATE TABLE, DROP TABLE, etc.).

        DDL commands don't return result sets, so we just execute and return success.

        Args:
            sql: DDL statement to execute
            tenant_id: Override tenant ID for this statement

        Returns:
            True if successful

        Example:
            ```python
            client.execute_ddl('''
                CREATE TABLE users (
                    id INTEGER,
                    name VARCHAR,
                    created_at TIMESTAMP
                ) WITH (
                    primary_key = 'id',
                    created_at_column = 'created_at'
                )
            ''')
            ```
        """
        try:
            logger.info(f"Executing DDL: {sql[:100]}...")

            # Create Ticket with DDL statement
            ticket = flight.Ticket(sql.encode('utf-8'))

            # Execute with authentication headers
            options = self._get_call_options(tenant_id=tenant_id)
            reader = self.client.do_get(ticket, options)

            # Consume the reader (DDL may return empty result or metadata)
            list(reader)

            logger.info("DDL executed successfully")
            return True

        except Exception as e:
            logger.error(f"DDL execution failed: {e}")
            raise

    def list_tables(
        self,
        schema: Optional[str] = None,
        include_system: bool = False,
        tenant_id: Optional[str] = None
    ) -> List[str]:
        """
        List all tables.

        Args:
            schema: Optional schema/namespace filter
            include_system: Include system tables (prism_* tables)
            tenant_id: Override tenant ID for this query

        Returns:
            List of table names

        Example:
            ```python
            tables = client.list_tables()
            print(f"Available tables: {tables}")
            ```
        """
        try:
            # Query SHOW TABLES
            query = "SHOW TABLES"
            if schema:
                query = f"SHOW TABLES IN {schema}"

            result = self.query(query, tenant_id=tenant_id, return_type="dict")

            # Extract table names from result
            tables = []
            for row in result:
                # Try different column names (depends on backend)
                table_name = row.get('table_name') or row.get('Table') or row.get('name')
                if table_name:
                    # Filter system tables
                    if not include_system and table_name.startswith('prism_'):
                        continue
                    tables.append(table_name)

            logger.info(f"Found {len(tables)} tables")
            return sorted(tables)

        except Exception as e:
            logger.error(f"List tables failed: {e}")
            raise

    def get_schema(self, table_name: str, tenant_id: Optional[str] = None) -> pa.Schema:
        """
        Get Arrow schema for a table.

        Args:
            table_name: Table name
            tenant_id: Override tenant ID for this query

        Returns:
            pyarrow.Schema

        Example:
            ```python
            schema = client.get_schema("sales_register")
            for field in schema:
                print(f"{field.name}: {field.type}")
            ```
        """
        try:
            # Query DESCRIBE to get schema
            result = self.query(f"DESCRIBE {table_name}", tenant_id=tenant_id, return_type="arrow")

            # Build schema from DESCRIBE result
            fields = []
            for row in result.to_dict():
                col_name = row.get('column_name') or row.get('col_name') or row.get('name')
                col_type = row.get('data_type') or row.get('type')

                # Simple type mapping
                arrow_type = self._map_sql_type_to_arrow(col_type)
                fields.append(pa.field(col_name, arrow_type))

            return pa.schema(fields)

        except Exception as e:
            logger.error(f"Get schema failed for {table_name}: {e}")
            raise

    def describe(self, table_name: str, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get table schema as list of dicts.

        Args:
            table_name: Table name
            tenant_id: Override tenant ID for this query

        Returns:
            List of dicts with schema information

        Example:
            ```python
            schema_rows = client.describe("sales_register")
            for row in schema_rows:
                print(f"{row['column_name']}: {row['data_type']}")
            ```
        """
        result = self.query(f"DESCRIBE {table_name}", tenant_id=tenant_id, return_type="dict")
        return result

    def insert(
        self,
        table_name: str,
        data: Union[pa.Table, List[Dict[str, Any]]],
        tenant_id: Optional[str] = None
    ) -> int:
        """
        Insert data into table (converted to MERGE/UPSERT by Prism).

        Args:
            table_name: Target table name
            data: Data as Arrow Table or list of dicts
            tenant_id: Override tenant ID for this operation

        Returns:
            Number of rows inserted

        Example:
            ```python
            # Using Arrow Table (zero-copy, efficient)
            import pyarrow as pa
            table = pa.table({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie']
            })
            rows_inserted = client.insert("users", table)

            # Using list of dicts
            data = [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
                {'id': 3, 'name': 'Charlie'}
            ]
            rows_inserted = client.insert("users", data)
            ```
        """
        try:
            # Convert to Arrow Table
            if isinstance(data, pa.Table):
                arrow_table = data
            elif isinstance(data, list):
                arrow_table = pa.Table.from_pylist(data)
            else:
                raise ValueError(f"Unsupported data type: {type(data)}. Use pa.Table or list of dicts.")

            logger.info(f"Inserting {arrow_table.num_rows} rows into {table_name}")

            # Use Flight SQL DoPut
            descriptor = flight.FlightDescriptor.for_path(table_name.encode('utf-8'))
            options = self._get_call_options(tenant_id=tenant_id)

            # Create upload descriptor
            upload_descriptor = self.client.do_put(
                descriptor,
                arrow_table.schema,
                options=options
            )

            # Write table
            writer, _ = upload_descriptor
            writer.write_table(arrow_table)
            writer.close()

            logger.info(f"Inserted {arrow_table.num_rows} rows successfully")

            return arrow_table.num_rows

        except Exception as e:
            logger.error(f"Insert failed: {e}")
            raise

    def insert_parquet(
        self,
        table_name: str,
        parquet_file: str,
        tenant_id: Optional[str] = None,
        s3_client: Optional[Any] = None
    ) -> int:
        """
        Insert data from Parquet file into table via Prism Upload API.

        Simple interface that handles all complexity internally:
        - Small files (< 100MB): Direct PUT upload via presigned URL
        - Large files (>= 100MB): Automatic multipart upload
        - No AWS credentials needed - uses presigned URLs
        - Automatic INSERT → MERGE if table has primary_key

        Args:
            table_name: Target table name (e.g., "users" or "input_raw::users")
            parquet_file: Path to local Parquet file
            tenant_id: Override tenant ID for this operation
            s3_client: Deprecated - no longer used (kept for backward compatibility)

        Returns:
            Number of rows inserted

        Example:
            ```python
            # Simple upload - just like S3!
            rows = client.insert_parquet("users", "users_data.parquet")
            print(f"Inserted {rows} rows")

            # With namespace in table name
            rows = client.insert_parquet("input_raw::leads", "data.parquet")

            # Large files work automatically (multipart behind the scenes)
            rows = client.insert_parquet("transactions", "big_data.parquet")
            ```
        """
        result = self._upload_via_presigned_url(table_name, parquet_file, None, tenant_id)
        return result["rows_inserted"]

    def upload_parquet(
        self,
        table_name: str,
        parquet_file: str,
        namespace: Optional[str] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upload a Parquet file via presigned URL and execute INSERT via Flight SQL.

        This is the recommended method for bulk data ingestion (ADR-025).
        Returns detailed result including s3_path and upload method used.

        Args:
            table_name: Target table name
            parquet_file: Path to local Parquet file
            namespace: Optional namespace (e.g., "input_raw")
            tenant_id: Override tenant ID for this operation

        Returns:
            Dict with upload result:
            - success: bool
            - s3_path: S3 location of uploaded file
            - rows_inserted: Number of rows inserted
            - method: "PUT" or "MULTIPART"
            - upload_id: Unique upload identifier

        Example:
            ```python
            result = client.upload_parquet("users", "users_data.parquet")
            print(f"Uploaded to: {result['s3_path']}")
            print(f"Rows inserted: {result['rows_inserted']}")
            print(f"Method: {result['method']}")

            # With namespace
            result = client.upload_parquet(
                "leads",
                "zoho_leads.parquet",
                namespace="input_raw"
            )
            ```
        """
        return self._upload_via_presigned_url(table_name, parquet_file, namespace, tenant_id)

    def _upload_via_presigned_url(
        self,
        table_name: str,
        parquet_file: str,
        namespace: Optional[str] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Internal method: Upload parquet via presigned URL and execute INSERT.

        Handles all complexity:
        - Gets presigned URL from /v1/upload/initiate
        - Uploads to S3 (simple PUT or multipart based on file size)
        - Executes INSERT via Flight SQL

        Args:
            table_name: Target table name
            parquet_file: Path to Parquet file
            namespace: Optional namespace
            tenant_id: Override tenant ID

        Returns:
            Dict with success, s3_path, rows_inserted, method
        """
        file_path = Path(parquet_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_file}")

        file_size = file_path.stat().st_size
        logger.info(f"Uploading parquet: {parquet_file} ({file_size:,} bytes)")

        # Parse table name for namespace (e.g., "input_raw::users")
        if "::" in table_name:
            namespace, table_name = table_name.split("::", 1)

        # Step 1: Initiate upload to get presigned URL
        init_response = self._initiate_upload(
            table_name=table_name,
            namespace=namespace,
            file_size=file_size,
            tenant_id=tenant_id
        )

        upload_method = init_response["method"]
        s3_path = init_response["s3_path"]

        # Step 2: Upload to S3 (method is transparent to caller)
        if upload_method == "PUT":
            self._upload_simple_put(parquet_file, init_response)
        elif upload_method == "MULTIPART":
            self._upload_multipart(parquet_file, init_response, tenant_id)
        else:
            raise ValueError(f"Unknown upload method: {upload_method}")

        logger.info(f"File uploaded to S3: {s3_path}")

        # Step 3: Execute INSERT via Flight SQL
        full_table_name = f"{namespace}::{table_name}" if namespace else table_name
        insert_sql = f'INSERT INTO "{full_table_name}" SELECT * FROM read_parquet(\'{s3_path}\')'

        logger.info(f"Executing INSERT...")
        self.query(insert_sql, tenant_id=tenant_id)

        # Get row count from parquet metadata
        row_count = self._get_parquet_row_count(parquet_file)
        logger.info(f"Successfully inserted {row_count:,} rows")

        return {
            "success": True,
            "s3_path": s3_path,
            "rows_inserted": row_count,
            "method": upload_method,
            "upload_id": init_response["upload_id"]
        }

    def _initiate_upload(
        self,
        table_name: str,
        namespace: Optional[str],
        file_size: int,
        tenant_id: Optional[str]
    ) -> Dict[str, Any]:
        """Call /v1/upload/initiate to get presigned URL(s)."""
        api_url = f"{self.http_url.rstrip('/')}/v1/upload/initiate"
        headers = self._get_http_headers(tenant_id)

        request_body = {
            "table": table_name,
            "file_size_bytes": file_size
        }
        if namespace:
            request_body["namespace"] = namespace

        response = requests.post(api_url, json=request_body, headers=headers, timeout=30)
        response.raise_for_status()

        init_data = response.json()
        logger.debug(
            f"Upload initiated: method={init_data['method']}, "
            f"upload_id={init_data['upload_id']}"
        )

        return init_data

    def _upload_simple_put(self, file_path: str, upload_info: Dict[str, Any]) -> None:
        """Upload file using simple PUT request to presigned URL."""
        upload_url = upload_info["upload_url"]

        with open(file_path, "rb") as f:
            response = requests.put(
                upload_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=300
            )
            response.raise_for_status()

    def _upload_multipart(
        self,
        file_path: str,
        upload_info: Dict[str, Any],
        tenant_id: Optional[str]
    ) -> None:
        """Upload file using multipart upload with presigned chunk URLs."""
        import concurrent.futures

        chunk_urls = upload_info["chunk_urls"]
        total_chunks = upload_info["total_chunks"]
        upload_s3_id = upload_info["upload_s3_id"]
        s3_key = upload_info["s3_key"]

        logger.info(f"Multipart upload: {total_chunks} chunks")

        parts = []

        def upload_chunk(chunk_info: Dict[str, Any]) -> Dict[str, Any]:
            """Upload a single chunk and return part info."""
            part_number = chunk_info["part_number"]
            chunk_url = chunk_info["upload_url"]
            start_byte = chunk_info["start_byte"]
            chunk_size = chunk_info["size"]

            with open(file_path, "rb") as f:
                f.seek(start_byte)
                chunk_data = f.read(chunk_size)

            response = requests.put(
                chunk_url,
                data=chunk_data,
                headers={"Content-Type": "application/octet-stream"},
                timeout=300
            )
            response.raise_for_status()

            etag = response.headers.get("ETag", "").strip('"')
            return {
                "PartNumber": part_number,
                "ETag": f'"{etag}"' if not etag.startswith('"') else etag
            }

        # Upload chunks in parallel
        max_workers = min(10, total_chunks)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {
                executor.submit(upload_chunk, chunk): chunk
                for chunk in chunk_urls
            }

            for future in concurrent.futures.as_completed(future_to_chunk):
                part_result = future.result()
                parts.append(part_result)

        # Sort and complete
        parts.sort(key=lambda x: x["PartNumber"])

        # Complete multipart upload via Prism API
        api_url = f"{self.http_url.rstrip('/')}/v1/upload/complete"
        headers = self._get_http_headers(tenant_id)

        request_body = {
            "upload_id": upload_info["upload_id"],
            "upload_s3_id": upload_s3_id,
            "s3_key": s3_key,
            "parts": parts
        }

        response = requests.post(api_url, json=request_body, headers=headers, timeout=60)
        response.raise_for_status()

    def _get_parquet_row_count(self, parquet_file: str) -> int:
        """Get row count from parquet metadata without loading data."""
        try:
            import pyarrow.parquet as pq
            parquet_file_obj = pq.ParquetFile(parquet_file)
            return parquet_file_obj.metadata.num_rows
        except Exception as e:
            logger.warning(f"Could not get parquet row count: {e}")
            return 0


 
    def create_table(
        self,
        table_name: str,
        schema: Union[pa.Schema, Dict[str, str]],
        primary_key: Optional[str] = None,
        partitioned_by: Optional[str] = None,
        created_at_column: Optional[str] = None,
        tenant_id: Optional[str] = None
    ) -> bool:
        """
        Create a new table.

        Args:
            table_name: Table name
            schema: Arrow schema or dict of {column_name: type_str}
            primary_key: Primary key column
            partitioned_by: Partition specification
            tenant_id: Override tenant ID for this operation

        Returns:
            True if successful

        Example:
            ```python
            schema = {
                'id': 'BIGINT',
                'name': 'VARCHAR',
                'created_at': 'TIMESTAMP'
            }

            client.create_table(
                "users",
                schema,
                primary_key="id",
                partitioned_by="month(created_at)"
            )
            ```
        """
        try:
            # Build CREATE TABLE SQL
            if isinstance(schema, pa.Schema):
                columns_sql = ", ".join([
                    f"{field.name} {self._arrow_type_to_sql(field.type)}"
                    for field in schema
                ])
            elif isinstance(schema, dict):
                columns_sql = ", ".join([
                    f"{col_name} {col_type}"
                    for col_name, col_type in schema.items()
                ])
            else:
                raise ValueError(f"Unsupported schema type: {type(schema)}")

            # Build WITH clause
            with_clauses = []
            if primary_key:
                with_clauses.append(f"primary_key = '{primary_key}'")
            if partitioned_by:
                with_clauses.append(f"partitioned_by = '{partitioned_by}'")

            # Add created_at_column
            if created_at_column:
                with_clauses.append(f"created_at_column = '{created_at_column}'")
            else:
                # Auto-detect fallback
                col_names = []
                if isinstance(schema, pa.Schema):
                    col_names = [f.name for f in schema]
                elif isinstance(schema, dict):
                    col_names = list(schema.keys())
                
                if 'created_at' in col_names:
                    with_clauses.append("created_at_column = 'created_at'")
                elif '_airbyte_emitted_at' in col_names:
                    with_clauses.append("created_at_column = '_airbyte_emitted_at'")

            with_sql = f"WITH ({', '.join(with_clauses)})" if with_clauses else ""

            sql = f"CREATE TABLE {table_name} ({columns_sql}) {with_sql}"

            logger.info(f"Creating table: {sql}")

            # Execute CREATE TABLE
            return self.execute_ddl(sql, tenant_id=tenant_id)

        except Exception as e:
            logger.error(f"Create table failed: {e}")
            raise

    def drop_table(self, table_name: str, tenant_id: Optional[str] = None) -> bool:
        """
        Drop a table.

        Args:
            table_name: Table name
            tenant_id: Override tenant ID for this operation

        Returns:
            True if successful
        """
        try:
            sql = f"DROP TABLE {table_name}"
            return self.execute_ddl(sql, tenant_id=tenant_id)
        except Exception as e:
            logger.error(f"Drop table failed: {e}")
            raise

    def get_stats(self, table_name: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get table statistics.

        Args:
            table_name: Table name
            tenant_id: Override tenant ID for this query

        Returns:
            Dictionary with row_count, size_bytes, etc.

        Example:
            ```python
            stats = client.get_stats("sales_register")
            print(f"Row count: {stats['row_count']:,}")
            print(f"Size: {stats['size_bytes'] / 1024 / 1024:.2f} MB")
            ```
        """
        try:
            # Query prism_tables metadata
            sql = f"""
            SELECT
                row_count,
                size_bytes,
                last_updated,
                primary_key,
                composite_key,
                partitioned_by
            FROM prism_tables
            WHERE table_name = '{table_name}'
            """

            result = self.query(sql, tenant_id=tenant_id, return_type="dict")

            if not result:
                # Fallback: count rows
                count_result = self.query(f"SELECT COUNT(*) as cnt FROM {table_name}", tenant_id=tenant_id, return_type="dict")
                return {
                    'row_count': count_result[0]['cnt'],
                    'size_bytes': 0,
                    'last_updated': None
                }

            return result[0]

        except Exception as e:
            logger.error(f"Get stats failed: {e}")
            raise

    def _map_sql_type_to_arrow(self, sql_type: str) -> pa.DataType:
        """Map SQL type string to Arrow type."""
        sql_type_upper = sql_type.upper()

        if 'BIGINT' in sql_type_upper or 'INT64' in sql_type_upper:
            return pa.int64()
        elif 'INT' in sql_type_upper:
            return pa.int32()
        elif 'VARCHAR' in sql_type_upper or 'STRING' in sql_type_upper or 'TEXT' in sql_type_upper:
            return pa.string()
        elif 'DOUBLE' in sql_type_upper or 'FLOAT8' in sql_type_upper:
            return pa.float64()
        elif 'FLOAT' in sql_type_upper:
            return pa.float32()
        elif 'DECIMAL' in sql_type_upper:
            return pa.decimal128(38, 9)  # Default precision
        elif 'BOOLEAN' in sql_type_upper or 'BOOL' in sql_type_upper:
            return pa.bool_()
        elif 'TIMESTAMP' in sql_type_upper:
            return pa.timestamp('us')
        elif 'DATE' in sql_type_upper:
            return pa.date32()
        else:
            return pa.string()  # Fallback

    def _arrow_type_to_sql(self, arrow_type: pa.DataType) -> str:
        """Map Arrow type to SQL type string."""
        if pa.types.is_int64(arrow_type):
            return "BIGINT"
        elif pa.types.is_int32(arrow_type):
            return "INTEGER"
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "VARCHAR"
        elif pa.types.is_float64(arrow_type):
            return "DOUBLE"
        elif pa.types.is_float32(arrow_type):
            return "FLOAT"
        elif pa.types.is_decimal(arrow_type):
            return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        elif pa.types.is_date(arrow_type):
            return "DATE"
        else:
            return "VARCHAR"

    def describe_table(self, table_name: str, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get table metadata and maintenance stats via DESCRIBE TABLE action.

        Args:
            table_name: Name of the table to describe
            tenant_id: Override tenant ID for this request

        Returns:
            Dictionary with table schema, columns, and maintenance statistics

        Example:
            ```python
            info = client.describe_table("users")
            print(f"Table: {info['table_name']}")
            print(f"Columns: {info['columns']}")
            print(f"Update count: {info['maintenance']['update_count']}")
            print(f"Last optimized: {info['maintenance']['last_optimized_at']}")
            ```
        """
        try:
            import json

            logger.info(f"Describing table: {table_name}")

            # Create DESCRIBE TABLE action
            action = flight.Action("DescribeTable", table_name.encode('utf-8'))

            # Execute with authentication headers
            options = self._get_call_options(tenant_id=tenant_id)
            results = list(self.client.do_action(action, options))

            if not results:
                raise ValueError(f"No response from DESCRIBE TABLE for {table_name}")

            # Parse JSON response
            response = json.loads(results[0].body.to_pybytes().decode('utf-8'))

            logger.info(f"Table described: {table_name} ({len(response.get('columns', []))} columns)")

            return response

        except flight.FlightServerError as e:
            logger.error(f"DESCRIBE TABLE failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to describe table: {e}")
            raise

    def get_maintenance_stats(
        self,
        table_name: str,
        limit: int = 10,
        offset: int = 0,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed maintenance statistics and history for a table.

        Args:
            table_name: Name of the table
            limit: Maximum number of history records to return (default: 10)
            offset: Number of history records to skip for pagination (default: 0)
            tenant_id: Override tenant ID for this request

        Returns:
            Dictionary with:
            - current_stats: Current maintenance stats (update_count, workflow_id, timestamps)
            - history: List of recent maintenance operations
            - pagination: Pagination info (limit, offset, returned, total)

        Example:
            ```python
            # Get recent maintenance stats
            stats = client.get_maintenance_stats("users", limit=5)
            print(f"Update count: {stats['current_stats']['update_count']}")
            print(f"Workflow ID: {stats['current_stats']['last_maintenance_workflow_id']}")

            # Paginate through history
            for record in stats['history']:
                print(f"  {record['status']}: {record['workflow_id']}")

            # Get next page
            if stats['pagination']['returned'] < stats['pagination']['total']:
                next_page = client.get_maintenance_stats("users", limit=5, offset=5)
            ```
        """
        try:
            import json

            logger.info(f"Getting maintenance stats: {table_name} (limit={limit}, offset={offset})")

            # Create action request
            request = {
                "table_name": table_name,
                "limit": limit,
                "offset": offset
            }
            action = flight.Action("GetMaintenanceStats", json.dumps(request).encode())

            # Execute with authentication headers
            options = self._get_call_options(tenant_id=tenant_id)
            results = list(self.client.do_action(action, options))

            if not results:
                raise ValueError(f"No response from GetMaintenanceStats for {table_name}")

            # Parse JSON response
            response = json.loads(results[0].body.to_pybytes().decode('utf-8'))

            logger.info(
                f"Maintenance stats retrieved: {table_name} "
                f"({response['pagination']['returned']}/{response['pagination']['total']} history records)"
            )

            return response

        except flight.FlightServerError as e:
            logger.error(f"GetMaintenanceStats failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to get maintenance stats: {e}")
            raise

    # =========================================================================
    # Table Metadata Management (REST API)
    # =========================================================================

    def _get_http_headers(self, tenant_id: Optional[str] = None) -> Dict[str, str]:
        """
        Build HTTP headers for REST API calls.

        Args:
            tenant_id: Override tenant ID for this request

        Returns:
            Dict of headers with Authorization and X-Tenant-ID
        """
        headers = {"Content-Type": "application/json"}

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        effective_tenant_id = tenant_id or self.tenant_id
        if effective_tenant_id:
            headers["X-Tenant-ID"] = effective_tenant_id

        return headers

    def update_table(
        self,
        table_name: str,
        description: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        tags_remove: Optional[List[str]] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update table metadata (description and tags only).

        Uses the REST API PATCH endpoint to update table metadata.
        Only description and tags can be updated after table creation.

        Args:
            table_name: Name of the table to update
            description: New table description (None to leave unchanged)
            tags: Dict of tags to merge with existing tags (adds new keys, updates existing)
            tags_remove: List of tag keys to remove from existing tags
            tenant_id: Override tenant ID for this request

        Returns:
            Dict containing updated table metadata

        Example:
            ```python
            # Add/update description
            result = client.update_table(
                "users",
                description="User accounts table"
            )

            # Add tags
            result = client.update_table(
                "users",
                tags={"owner": "data-team", "pii": "true", "domain": "identity"}
            )

            # Update specific tags while keeping others
            result = client.update_table(
                "users",
                tags={"owner": "platform-team"}  # Updates only 'owner' tag
            )

            # Remove specific tags
            result = client.update_table(
                "users",
                tags_remove=["deprecated", "temp"]
            )

            # Combined: update description, add tags, remove tags
            result = client.update_table(
                "users",
                description="Updated user accounts table",
                tags={"version": "2.0"},
                tags_remove=["old_tag"]
            )
            ```
        """
        try:
            # Build request body
            request_body = {}
            if description is not None:
                request_body["description"] = description
            if tags is not None:
                request_body["tags"] = tags
            if tags_remove is not None:
                request_body["tags_remove"] = tags_remove

            if not request_body:
                raise ValueError("At least one of description, tags, or tags_remove must be provided")

            # Make PATCH request
            api_url = f"{self.http_url.rstrip('/')}/api/v1/tables/{table_name}"
            headers = self._get_http_headers(tenant_id)

            logger.info(f"Updating table metadata: {table_name}")
            response = requests.patch(api_url, json=request_body, headers=headers)
            response.raise_for_status()

            result = response.json()
            logger.info(f"Table metadata updated: {table_name}")

            return result

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise ValueError(f"Table '{table_name}' not found")
            elif e.response.status_code == 401:
                raise PermissionError(f"Authentication failed: {e.response.text}")
            else:
                raise RuntimeError(f"Failed to update table: {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to update table {table_name}: {e}")
            raise

    def close(self):
        """Close Flight SQL connection."""
        if self.client:
            self.client.close()
            logger.info("Prism connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
