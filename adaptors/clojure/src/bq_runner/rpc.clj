(ns bq-runner.rpc
  "Low-level RPC methods for bq-runner"
  (:require [bq-runner.client :as client]))

(defn ping
  "Health check. Returns {:message \"pong\"}."
  [conn]
  (client/send-request conn "bq.ping" {}))

(defn create-session
  "Create a new session. Returns {:sessionId \"uuid\"}."
  [conn]
  (client/send-request conn "bq.createSession" {}))

(defn destroy-session
  "Destroy a session. Returns {:success true}."
  [conn session-id]
  (client/send-request conn "bq.destroySession" {:sessionId session-id}))

(defn query
  "Execute a SQL query. Returns BigQuery-format response."
  [conn session-id sql]
  (client/send-request conn "bq.query" {:sessionId session-id
                                        :sql sql}))

(defn create-table
  "Create a table with the given schema.
   Schema is a vector of {:name \"col\" :type \"STRING\"} maps.
   Returns {:success true}."
  [conn session-id table-name schema]
  (client/send-request conn "bq.createTable" {:sessionId session-id
                                               :tableName table-name
                                               :schema schema}))

(defn insert
  "Insert rows into a table.
   Rows can be vectors (positional) or maps (by column name).
   Returns {:insertedRows n}."
  [conn session-id table-name rows]
  (client/send-request conn "bq.insert" {:sessionId session-id
                                          :tableName table-name
                                          :rows rows}))

(defn register-dag
  "Register a DAG of tables.
   Tables is a vector of table definitions:
   - Source table: {:name \"tbl\" :schema [{:name \"col\" :type \"STRING\"}] :rows [[val1 val2]]}
   - Derived table: {:name \"tbl\" :sql \"SELECT * FROM other\"}
   Returns {:success true :tables [{:name \"tbl\" :dependencies [\"dep1\"]}]}"
  [conn session-id tables]
  (client/send-request conn "bq.registerDag" {:sessionId session-id
                                               :tables tables}))

(defn run-dag
  "Execute the DAG with maximum parallelism.
   Optional table-names (vector) to run only those tables and their dependencies.
   Returns {:success true :executedTables [\"tbl1\" \"tbl2\"]}"
  ([conn session-id]
   (client/send-request conn "bq.runDag" {:sessionId session-id}))
  ([conn session-id table-names]
   (client/send-request conn "bq.runDag" {:sessionId session-id
                                           :tableNames table-names})))

(defn get-dag
  "Get the registered DAG tables.
   Returns {:tables [{:name \"tbl\" :sql \"...\" :isSource false :dependencies [\"dep\"]}]}"
  [conn session-id]
  (client/send-request conn "bq.getDag" {:sessionId session-id}))

(defn clear-dag
  "Clear all registered DAG tables.
   Returns {:success true}"
  [conn session-id]
  (client/send-request conn "bq.clearDag" {:sessionId session-id}))
