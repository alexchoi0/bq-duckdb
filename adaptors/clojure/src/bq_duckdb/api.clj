(ns bq-duckdb.api
  "High-level API with macros for bq-duckdb"
  (:require [bq-duckdb.client :as client]
            [bq-duckdb.rpc :as rpc]))

(def ^:private type-mapping
  {:string "STRING"
   :int64 "INT64"
   :float64 "FLOAT64"
   :bool "BOOL"
   :boolean "BOOL"
   :bytes "BYTES"
   :date "DATE"
   :datetime "DATETIME"
   :time "TIME"
   :timestamp "TIMESTAMP"
   :numeric "NUMERIC"
   :bignumeric "BIGNUMERIC"
   :json "JSON"})

(defn- resolve-type [t]
  (if (keyword? t)
    (get type-mapping t (name t))
    (str t)))

(defn- schema->columns [schema]
  (cond
    (map? schema)
    (mapv (fn [[k v]]
            {:name (name k)
             :type (resolve-type v)})
          schema)

    (sequential? schema)
    (mapv (fn [col]
            (if (map? col)
              {:name (name (:name col))
               :type (resolve-type (:type col))}
              (throw (ex-info "Invalid column definition" {:col col}))))
          schema)

    :else
    (throw (ex-info "Invalid schema format" {:schema schema}))))

(defrecord Session [conn session-id])

(defn connect
  "Connect to bq-duckdb server."
  [url]
  (client/connect url))

(defn close
  "Close connection. Accepts a connection or a session.
   If given a session, destroys it first (dropping all tables/views)
   then closes the connection."
  [conn-or-session]
  (if (or (instance? Session conn-or-session)
          (:session-id conn-or-session))
    (let [conn (:conn conn-or-session)
          session-id (:session-id conn-or-session)]
      (try
        (rpc/destroy-session conn session-id)
        (catch Exception _))
      (client/close conn))
    (client/close conn-or-session)))

(defn ping
  "Health check. Returns true if server responds with pong."
  [conn]
  (= "pong" (:message (rpc/ping conn))))

(defn create-session
  "Create a new session. Returns a Session record."
  [conn]
  (let [result (rpc/create-session conn)]
    (->Session conn (:sessionId result))))

(defn destroy-session
  "Destroy a session."
  [session]
  (rpc/destroy-session (:conn session) (:session-id session)))

(defmacro with-connection
  "Execute body with a connection, ensuring it's closed afterward.

   (with-connection [conn \"ws://localhost:3000/ws\"]
     (do-stuff conn))"
  [[binding url] & body]
  `(let [~binding (connect ~url)]
     (try
       ~@body
       (finally
         (close ~binding)))))

(defmacro with-session
  "Execute body with a session, ensuring it's destroyed afterward.

   (with-session [s conn]
     (query s \"SELECT 1\"))"
  [[binding conn] & body]
  `(let [~binding (create-session ~conn)]
     (try
       ~@body
       (finally
         (destroy-session ~binding)))))

(defn- rows->maps
  "Convert BigQuery response rows to Clojure maps."
  [response]
  (let [columns (mapv (comp keyword :name) (-> response :schema :fields))]
    (mapv (fn [row]
            (zipmap columns (mapv :v (:f row))))
          (:rows response))))

(defn query
  "Execute SQL and return results as a vector of maps."
  [session sql]
  (let [result (rpc/query (:conn session) (:session-id session) sql)]
    (rows->maps result)))

(defn query-raw
  "Execute SQL and return raw BigQuery-format response."
  [session sql]
  (rpc/query (:conn session) (:session-id session) sql))

(defn create-table!
  "Create a table with the given schema.

   Schema can be:
   - A map: {:id :int64 :name :string}
   - A vector of maps: [{:name \"id\" :type \"INT64\"}]

   Type keywords: :string :int64 :float64 :bool :bytes :date
                  :datetime :time :timestamp :numeric :json"
  [session table-name schema]
  (let [table-str (if (keyword? table-name) (name table-name) (str table-name))
        cols (schema->columns schema)]
    (rpc/create-table (:conn session) (:session-id session) table-str cols)))

(defn insert!
  "Insert rows into a table.

   Rows can be:
   - Vectors: [[1 \"Alice\"] [2 \"Bob\"]]
   - Maps: [{:id 1 :name \"Alice\"} {:id 2 :name \"Bob\"}]"
  [session table-name rows]
  (let [table-str (if (keyword? table-name) (name table-name) (str table-name))
        rows-data (mapv (fn [row]
                          (cond
                            (vector? row) row
                            (map? row) (vec (vals row))
                            :else [row]))
                        rows)]
    (rpc/insert (:conn session) (:session-id session) table-str rows-data)))


(defn execute!
  "Execute a SQL statement (CREATE, INSERT, etc.) without returning results."
  [session sql]
  (query-raw session sql)
  nil)

(defn- normalize-dag-table
  "Normalize a DAG table definition."
  [table]
  (cond
    (:sql table)
    {:name (name (:name table))
     :sql (:sql table)}

    (:schema table)
    {:name (name (:name table))
     :schema (schema->columns (:schema table))
     :rows (or (:rows table) [])}

    :else
    (throw (ex-info "Invalid DAG table: must have either :sql or :schema" {:table table}))))

(defn register-dag!
  "Register a DAG of tables.

   Tables is a vector of table definitions:
   - Source: {:name :users :schema {:id :int64 :name :string} :rows [[1 \"Alice\"]]}
   - Derived: {:name :report :sql \"SELECT * FROM users\"}

   Multiple calls append to the existing DAG (use clear-dag! to reset).

   Returns {:success true :tables [{:name \"tbl\" :dependencies [\"dep\"]}]}"
  [session tables]
  (let [normalized (mapv normalize-dag-table tables)]
    (rpc/register-dag (:conn session) (:session-id session) normalized)))

(defn run-dag!
  "Execute the DAG with maximum parallelism.
   Tables with no dependencies run concurrently.
   As each table completes, dependent tables become eligible to run.

   Optional target-tables: only run those tables and their dependencies.
   Can pass multiple targets as separate args or as a vector.

   Returns {:success true :executedTables [\"tbl1\" \"tbl2\"]}"
  ([session]
   (rpc/run-dag (:conn session) (:session-id session)))
  ([session & targets]
   (let [targets (flatten targets)
         target-names (mapv name targets)]
     (rpc/run-dag (:conn session) (:session-id session) target-names))))

(defn get-dag
  "Get the registered DAG tables.
   Returns {:tables [{:name \"tbl\" :sql \"...\" :isSource false :dependencies [\"dep\"]}]}"
  [session]
  (rpc/get-dag (:conn session) (:session-id session)))

(defn clear-dag!
  "Clear all registered DAG tables."
  [session]
  (rpc/clear-dag (:conn session) (:session-id session)))
