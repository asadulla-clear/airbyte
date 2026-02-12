# Prism Destination Connector for Airbyte

The Prism Destination Connector enables high-performance data ingestion from any Airbyte source into the [Prism Engine](https://github.com/cleartax/prism-cdp). It leverages the **Flight SQL** protocol for efficient, zero-copy data transfer using PyArrow.

## Features

- **High Performance**: Uses Apache Arrow and Flight SQL for low-latency, high-throughput ingestion.
- **Varcharization Strategy**: Automatically maps complex JSON structures and large numbers to VARCHAR to ensure compatibility with Trino/Iceberg backends.
- **Deep Recursive Cleaning**: Automatically sanitizes nested data structures, converting all datetime/date objects to ISO-8601 strings.
- **Multi-Tenant Isolation**: Built-in support for `tenant_id` to ensure strict data segregation at the storage layer.
- **Auto-Schema Evolution**: Automatically creates and updates tables in Prism based on the incoming Airbyte stream schema.
- **CDC Support**: Correctly handles Change Data Capture (CDC) metadata fields.

## Architecture & Interfaces

The connector is built using the **Airbyte Python CDK** and implements the following core interfaces:

- **`Destination`**: Primary interface defining the `check` (connection test) and `write` (data ingestion) methods.
- **`AirbyteConnectionStatus`**: Used to communicate the health of the Flight SQL connection back to Airbyte.
- **`AirbyteMessage`**: The standard message format for records and state checkpoints processed by the connector.
- **`ConfiguredAirbyteCatalog`**: Used to map incoming streams to their corresponding Prism table schemas.

## The Role of Prism Client

The **`PrismClient`** is the core communication engine of the connector. Its primary responsibilities include:

1. **Flight SQL Implementation**: Acts as the low-level driver for the Arrow Flight SQL protocol.
2. **Technical Upserts**: Automatically converts `INSERT` operations into `MERGE` statements in the backend when primary keys are provided.
3. **Multi-Tenant Routing**: Dynamically sets the `x-tenant-id` header on every gRPC request to ensure data isolation.
4. **Arrow Table Serialization**: Handles the efficient serialization of Python dicts into PyArrow Tables for high-performance, zero-copy ingestion.
5. **Schema Discovery**: Provides the `DESCRIBE` and `SHOW TABLES` functionality used to validate table existence and schema alignment.

## Configuration

The following parameters are required to configure the Prism destination:

| Field        | Type    | Required | Default | Description                                          |
| ------------ | ------- | -------- | ------- | ---------------------------------------------------- |
| `grpc_url`   | string  | ✅       | —       | Prism Flight SQL URL (e.g., `grpc://localhost:8815`) |
| `http_url`   | string  | ✅       | —       | Prism REST API URL (e.g., `http://localhost:9815`)   |
| `token`      | string  | ✅       | —       | Prism Authentication Token (`prism_cs_...`)          |
| `tenant_id`  | string  | ✅       | —       | Unique identifier for multi-tenant data isolation    |
| `batch_size` | integer | ❌       | 10000   | Number of records to buffer before flushing          |
| `timeout`    | integer | ❌       | 300     | Maximum time (seconds) to wait for query execution   |

## Data Type Mapping

To ensure maximum compatibility and avoid "ICEBERG_COMMIT_ERROR" type mismatches, the connector uses a **Varcharization** strategy for most non-primitive types.

| Airbyte Type                   | Prism (Arrow) Type | SQL Type  |
| ------------------------------ | ------------------ | --------- |
| `string`                       | `pa.string()`      | `VARCHAR` |
| `number`                       | `pa.string()`      | `VARCHAR` |
| `integer`                      | `pa.string()`      | `VARCHAR` |
| `boolean`                      | `pa.bool_()`       | `BOOLEAN` |
| `array` (nested)               | `pa.string()`      | `VARCHAR` |
| `object` (nested)              | `pa.string()`      | `VARCHAR` |
| `string` (format: `date`)      | `pa.date32()`      | `DATE`    |
| `string` (format: `date-time`) | `pa.string()`      | `VARCHAR` |

### Metadata Columns

The connector automatically adds the following column to every table:

- `_airbyte_emitted_at`: ISO-8601 timestamp indicating when the record was processed by the destination.

## Sync Modes

The connector supports the following Airbyte sync modes:

- **Overwrite**: Drops the existing table and creates a fresh one before syncing.
- **Append**: Adds new records to the existing table. Prism automatically handles technical upserts if a primary key is defined.
- **Append Dedup**: Appends records while ensuring uniqueness based on the primary key defined in the source.

## Development & Deployment

### Local Development

1. **Install Dependencies**:

   ```bash
   pip install .
   ```

2. **Run Unit Tests**:

   ```bash
   pytest unit_tests/
   ```

3. **Check Connection**:
   ```bash
   python main.py check --config integration_tests/config.json
   ```

### Docker Implementation (for Kind/Kubernetes)

To deploy the connector to an Airbyte instance running in Kind:

1. **Build the Image**:

   ```bash
   docker build -t airbyte/destination-prism:dev .
   ```

2. **Load into Kind**:

   ```bash
   kind load docker-image airbyte/destination-prism:dev --name airbyte-abctl
   ```

3. **Restart Pods**:
   ```bash
   kubectl delete pods -n airbyte-abctl --all
   ```

## Support

For issues or questions, please refer to the [Prism Documentation](https://github.com/cleartax/prism-cdp) or contact the platform team.
