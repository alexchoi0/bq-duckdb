# bq-runner

A BigQuery runner with two execution backends: mock (BigQuery emulation via YachtSQL) and bigquery (real BigQuery).

## Features

- **Native BigQuery SQL support**: YachtSQL provides native BigQuery SQL dialect support
- **Session isolation**: Each session gets its own database, fully isolated
- **DAG execution**: Register tables as a DAG and execute in dependency order
- **WebSocket & stdio transports**: JSON-RPC 2.0 over WebSocket or stdio
- **Multiple backends**: Mock (local) and BigQuery (real)

## Quick Start

```bash
# Build
cargo build --release

# Run with mock backend (default) on port 3000
./target/release/bq-runner

# Run on a different port
./target/release/bq-runner --transport ws://localhost:8080

# Run with bigquery backend
./target/release/bq-runner --backend bigquery

# Run with stdio transport (for process-based IPC)
./target/release/bq-runner --transport stdio
```

## Backends

| Backend | Engine | Use Case |
|---------|--------|----------|
| `mock` | YachtSQL | Local development, testing |
| `bigquery` | BigQuery | Production queries against real BigQuery |

### BigQuery Authentication

For `bigquery` backend, set up authentication:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

The project ID is automatically read from the credentials file.

## CLI Options

```
Options:
      --transport <TRANSPORT>  Transport: stdio or ws://localhost:<port> [default: ws://localhost:3000]
      --backend <BACKEND>      Execution backend: mock (YachtSQL) or bigquery (real BigQuery) [default: mock]
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
| `bq.runDag` | Execute DAG in dependency order |
| `bq.getDag` | Get registered DAG tables |
| `bq.clearDag` | Clear DAG registry |

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
