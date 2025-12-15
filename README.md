# bq-runner

A BigQuery runner with three execution modes: mock (DuckDB emulation), interactive (real BigQuery with high priority), and batch (real BigQuery with queued execution).

## Features

- **BigQuery SQL compatibility**: Transforms BigQuery SQL syntax to DuckDB (mock mode)
- **Session isolation**: Each session gets its own schema, fully isolated
- **DAG execution**: Register tables as a DAG and execute with parallel processing
- **WebSocket RPC**: JSON-RPC 2.0 over WebSocket
- **Multiple execution modes**: Mock, Interactive, and Batch

## Quick Start

```bash
# Build
cargo build --release

# Run in mock mode (default) - uses DuckDB
./target/release/bq-runner

# Run in interactive mode - real BigQuery, high priority
./target/release/bq-runner --mode interactive --project my-gcp-project

# Run in batch mode - real BigQuery, queued execution
./target/release/bq-runner --mode batch --project my-gcp-project
```

Server starts on `ws://localhost:3000/ws`

## Execution Modes

| Mode | Backend | Priority | Use Case |
|------|---------|----------|----------|
| `mock` | DuckDB | N/A | Local development, testing |
| `interactive` | BigQuery | High | Production queries needing immediate results |
| `batch` | BigQuery | Low | Large analytics jobs, cost optimization |

### BigQuery Authentication

For `interactive` and `batch` modes, set up authentication:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

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

Automatically transformed to DuckDB equivalents (mock mode):

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
RUST_LOG=info ./target/release/bq-runner
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Copyright 2025 Alex Choi
