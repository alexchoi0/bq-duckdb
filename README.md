# bq-duckdb

A BigQuery emulator powered by DuckDB. Run BigQuery-compatible SQL locally for development and testing.

## Features

- **BigQuery SQL compatibility**: Transforms BigQuery SQL syntax to DuckDB
- **Session isolation**: Each session gets its own schema, fully isolated
- **DAG execution**: Register tables as a DAG and execute with parallel processing
- **WebSocket RPC**: JSON-RPC 2.0 over WebSocket

## Quick Start

```bash
# Build
cargo build --release

# Run server
./target/release/bq-duckdb
```

Server starts on `ws://localhost:3000/ws`

## RPC Methods

| Method | Description |
|--------|-------------|
| `bq.ping` | Health check |
| `bq.createSession` | Create isolated session |
| `bq.destroySession` | Drop session and all its tables |
| `bq.query` | Execute SQL query |
| `bq.createTable` | Create table with schema |
| `bq.insert` | Insert rows into table |
| `bq.registerDag` | Register DAG of source/derived tables |
| `bq.runDag` | Execute DAG with parallel processing |
| `bq.getDag` | Get registered DAG tables |
| `bq.clearDag` | Clear DAG registry |

## Supported BigQuery Functions

Automatically transformed to DuckDB equivalents:

- `SAFE_DIVIDE` → division with NULLIF
- `TIMESTAMP_DIFF/ADD/SUB` → DATE_DIFF with interval
- `FORMAT_TIMESTAMP` → strftime
- `PARSE_TIMESTAMP` → strptime
- `SAFE_CAST` → TRY_CAST
- `ANY_VALUE` → ARBITRARY
- `IFNULL` → COALESCE
- `ARRAY_LENGTH` → LEN
- `DATE()`, `DATETIME()` → CAST
- `REGEXP_CONTAINS` → regexp_matches
- `GENERATE_UUID` → uuid

## Client Adapters

- [Clojure](adaptors/clojure/) - Full-featured Clojure client

## Development

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=info ./target/release/bq-duckdb
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Copyright 2025 Alex Choi
