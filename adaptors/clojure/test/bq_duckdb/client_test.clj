(ns bq-duckdb.client-test
  "Tests for bq-duckdb Clojure client.
   Server is automatically started on first test run."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [bq-duckdb.api :as bq]
            [bq-duckdb.rpc :as rpc]
            [bq-duckdb.client :as client]
            [bq-duckdb.test-server :as test-server]))

(def ^:dynamic *test-url* nil)

(defn server-fixture [f]
  (binding [*test-url* (test-server/ensure-server!)]
    (f)))

(use-fixtures :once server-fixture)

(deftest test-connection
  (testing "Can connect and disconnect"
    (let [conn (client/connect *test-url*)]
      (is (client/connected? conn))
      (client/close conn)
      (is (not (client/connected? conn))))))

(deftest test-ping
  (testing "Ping returns pong"
    (bq/with-connection [conn *test-url*]
      (is (true? (bq/ping conn)))
      (is (= "pong" (:message (rpc/ping conn)))))))

(deftest test-session-lifecycle
  (testing "Can create and destroy sessions"
    (bq/with-connection [conn *test-url*]
      (let [result (rpc/create-session conn)]
        (is (string? (:sessionId result)))
        (is (> (count (:sessionId result)) 0))
        (let [destroy-result (rpc/destroy-session conn (:sessionId result))]
          (is (true? (:success destroy-result))))))))

(deftest test-with-session-macro
  (testing "with-session macro works correctly"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [session conn]
        (is (some? (:session-id session)))
        (is (= conn (:conn session)))))))

(deftest test-simple-query
  (testing "Can execute simple queries"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query s "SELECT 1 AS num, 'hello' AS greeting")]
          (is (= 1 (count result)))
          (is (= 1 (:num (first result))))
          (is (= "hello" (:greeting (first result)))))))))

(deftest test-query-raw
  (testing "query-raw returns BigQuery format"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query-raw s "SELECT 42 AS answer")]
          (is (= "bigquery#queryResponse" (:kind result)))
          (is (some? (:schema result)))
          (is (some? (:rows result)))
          (is (= "1" (:totalRows result))))))))

(deftest test-create-table-with-map-schema
  (testing "Can create table with map schema"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/create-table! s :test_users
                                       {:id :int64
                                        :name :string
                                        :active :bool})]
          (is (true? (:success result))))))))

(deftest test-create-table-with-vector-schema
  (testing "Can create table with vector schema"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/create-table! s "products"
                                       [{:name "id" :type "INT64"}
                                        {:name "price" :type "FLOAT64"}])]
          (is (true? (:success result))))))))

(deftest test-insert-and-query
  (testing "Can insert and query data"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :employees {:id :int64 :name :string})
        (let [insert-result (bq/insert! s :employees
                                        [[1 "Alice"]
                                         [2 "Bob"]
                                         [3 "Charlie"]])]
          (is (= 3 (:insertedRows insert-result))))
        (let [query-result (bq/query s "SELECT * FROM employees ORDER BY id")]
          (is (= 3 (count query-result)))
          (is (= "Alice" (:name (first query-result))))
          (is (= "Charlie" (:name (last query-result)))))))))

(deftest test-aggregation-query
  (testing "Aggregation queries work"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :sales {:amount :float64})
        (bq/insert! s :sales [[10.0] [20.0] [30.0]])
        (let [result (bq/query s "SELECT SUM(amount) AS total, AVG(amount) AS avg FROM sales")]
          (is (= 1 (count result)))
          (is (= 60.0 (:total (first result)))))))))

(deftest test-error-handling
  (testing "SQL errors are properly propagated"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #".*"
                              (bq/query s "SELECT * FROM nonexistent_table")))))))

(deftest test-bigquery-syntax
  (testing "BigQuery-specific syntax works"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query s "SELECT * FROM UNNEST([1, 2, 3]) AS num")]
          (is (= 3 (count result))))))))

(deftest test-struct-query
  (testing "STRUCT queries work"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query-raw s "SELECT STRUCT(1 AS x, 2 AS y) AS point")]
          (is (= 1 (count (:rows result)))))))))

(deftest test-destroy-session-cleans-up-tables
  (testing "destroy-session drops all tables and views"
    (bq/with-connection [conn *test-url*]
      (let [session (bq/create-session conn)
            session-id (:session-id session)]
        (bq/create-table! session :cleanup_test {:id :int64 :name :string})
        (bq/insert! session :cleanup_test [[1 "test"]])
        (let [result (bq/query session "SELECT COUNT(*) as cnt FROM cleanup_test")]
          (is (= 1 (:cnt (first result)))))
        (bq/destroy-session session)
        (let [new-session (bq/create-session conn)]
          (is (thrown? Exception
                       (bq/query new-session "SELECT * FROM cleanup_test")))
          (bq/destroy-session new-session))))))

(deftest test-close-with-session-cleans-up
  (testing "close with session destroys session and closes connection"
    (let [conn (bq/connect *test-url*)
          session (bq/create-session conn)]
      (bq/create-table! session :close_test {:x :int64})
      (bq/insert! session :close_test [[42]])
      (is (= 42 (:x (first (bq/query session "SELECT x FROM close_test")))))
      (bq/close session)
      (is (not (client/connected? conn))))))

(comment
  (require '[clojure.test :refer [run-tests]])
  (run-tests 'bq-duckdb.client-test))
