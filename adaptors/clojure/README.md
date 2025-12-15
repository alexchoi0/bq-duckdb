# bq-duckdb Clojure Client

Clojure client for bq-duckdb, a BigQuery emulator powered by DuckDB.

## Installation

Add to `deps.edn`:

```clojure
{:deps {bq-duckdb/bq-duckdb {:local/root "/path/to/bq-duckdb/adaptors/clojure"}}}
```

## Quick Start

```clojure
(require '[bq-duckdb.api :as bq])

;; Connect and create session
(bq/with-connection [conn "ws://localhost:3000/ws"]
  (bq/with-session [s conn]

    ;; Simple query
    (bq/query s "SELECT 1 + 1 AS result")
    ;; => [{:result 2}]

    ;; Create table and insert data
    (bq/create-table! s :users {:id :int64 :name :string})
    (bq/insert! s :users [[1 "Alice"] [2 "Bob"]])

    ;; Query with BigQuery syntax
    (bq/query s "SELECT * FROM users WHERE id = 1")))
```

## DAG Execution

Register a DAG of tables and execute with parallel processing:

```clojure
(bq/with-connection [conn "ws://localhost:3000/ws"]
  (bq/with-session [s conn]

    ;; Register DAG with source and derived tables
    (bq/register-dag! s
      [{:name :events
        :schema {:user_id :int64 :event :string :value :float64}
        :rows [[1 "click" 1.0] [1 "purchase" 100.0] [2 "click" 1.0]]}

       {:name :user_totals
        :sql "SELECT user_id, SUM(value) as total FROM events GROUP BY user_id"}

       {:name :top_users
        :sql "SELECT * FROM user_totals ORDER BY total DESC LIMIT 10"}])

    ;; Execute entire DAG (parallel where possible)
    (bq/run-dag! s)

    ;; Or execute specific targets (runs only required dependencies)
    (bq/run-dag! s :top_users)

    ;; Query results
    (bq/query s "SELECT * FROM top_users")))
```

## API Reference

### Connection

```clojure
(bq/connect url)           ; Connect to server
(bq/close conn-or-session) ; Close connection (destroys session if given)
(bq/ping conn)             ; Health check, returns true if ok
```

### Session

```clojure
(bq/create-session conn)   ; Create new session
(bq/destroy-session s)     ; Destroy session and drop all tables
```

### Queries

```clojure
(bq/query s sql)           ; Execute SQL, return vector of maps
(bq/query-raw s sql)       ; Execute SQL, return BigQuery-format response
(bq/execute! s sql)        ; Execute statement, return nil
```

### Tables

```clojure
;; Create with map schema (keyword types)
(bq/create-table! s :users {:id :int64 :name :string :active :bool})

;; Create with vector schema (explicit types)
(bq/create-table! s "users" [{:name "id" :type "INT64"}
                              {:name "name" :type "STRING"}])

;; Insert rows as vectors
(bq/insert! s :users [[1 "Alice"] [2 "Bob"]])

;; Insert rows as maps
(bq/insert! s :users [{:id 1 :name "Alice"} {:id 2 :name "Bob"}])
```

### DAG

```clojure
(bq/register-dag! s tables)      ; Register tables (accumulates on same session)
(bq/run-dag! s)                  ; Run all tables
(bq/run-dag! s :target)          ; Run specific target and dependencies
(bq/run-dag! s :t1 :t2)          ; Run multiple targets
(bq/get-dag s)                   ; Get registered tables
(bq/clear-dag! s)                ; Clear registry
```

### Macros

```clojure
(bq/with-connection [conn url] body...)  ; Auto-close connection
(bq/with-session [s conn] body...)       ; Auto-destroy session
```

## Type Mappings

| Keyword | BigQuery Type |
|---------|---------------|
| `:string` | STRING |
| `:int64` | INT64 |
| `:float64` | FLOAT64 |
| `:bool` | BOOL |
| `:bytes` | BYTES |
| `:date` | DATE |
| `:datetime` | DATETIME |
| `:time` | TIME |
| `:timestamp` | TIMESTAMP |
| `:numeric` | NUMERIC |
| `:json` | JSON |

## Testing

```bash
# Start server first
cd ../.. && cargo build --release && ./target/release/bq-duckdb &

# Run tests
clojure -M:test
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](../../LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Copyright 2025 Alex Choi
