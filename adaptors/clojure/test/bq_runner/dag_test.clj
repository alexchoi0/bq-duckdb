(ns bq-runner.dag-test
  "Tests for DAG executor functionality."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [bq-runner.api :as bq]
            [bq-runner.rpc :as rpc]
            [bq-runner.test-server :as test-server]))

(def ^:dynamic *test-url* nil)

(defn server-fixture [f]
  (binding [*test-url* (test-server/ensure-server!)]
    (f)))

(use-fixtures :once server-fixture)

(deftest test-register-dag-source-table
  (testing "Can register a source table"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/register-dag! s
                       [{:name :users
                         :schema {:id :int64 :name :string}
                         :rows [[1 "Alice"] [2 "Bob"]]}])]
          (is (true? (:success result)))
          (is (= 1 (count (:tables result))))
          (is (= "users" (-> result :tables first :name)))
          (is (= [] (-> result :tables first :dependencies))))))))

(deftest test-register-dag-derived-table
  (testing "Can register a derived table with inferred dependencies"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/register-dag! s
                       [{:name :users
                         :schema {:id :int64 :name :string}
                         :rows [[1 "Alice"]]}
                        {:name :report
                         :sql "SELECT * FROM users WHERE id > 0"}])]
          (is (true? (:success result)))
          (is (= 2 (count (:tables result))))
          (let [report-table (first (filter #(= "report" (:name %)) (:tables result)))]
            (is (= ["users"] (:dependencies report-table)))))))))

(deftest test-run-dag-simple
  (testing "Can run a simple DAG and query results"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :source_data
            :schema {:id :int64 :value :string}
            :rows [[1 "hello"] [2 "world"]]}
           {:name :transformed
            :sql "SELECT id, UPPER(value) as upper_value FROM source_data"}])
        (let [run-result (bq/run-dag! s)]
          (is (true? (:success run-result)))
          (is (= 2 (count (:executedTables run-result)))))
        (let [query-result (bq/query s "SELECT * FROM transformed ORDER BY id")]
          (is (= [{:id 1 :upper_value "HELLO"}
                  {:id 2 :upper_value "WORLD"}]
                 query-result)))))))

(deftest test-run-dag-with-target
  (testing "Can run DAG for specific target table"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :a :schema {:x :int64} :rows [[1]]}
           {:name :b :schema {:x :int64} :rows [[2]]}
           {:name :from_a :sql "SELECT x * 10 as x FROM a"}
           {:name :from_b :sql "SELECT x * 100 as x FROM b"}])
        (let [result (bq/run-dag! s :from_a)]
          (is (true? (:success result)))
          (is (some #{"a"} (:executedTables result)))
          (is (some #{"from_a"} (:executedTables result))))
        (let [query-result (bq/query s "SELECT * FROM from_a")]
          (is (= [{:x 10}] query-result)))))))

(deftest test-dag-chain
  (testing "DAG with chained dependencies executes in correct order"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :raw
            :schema {:value :int64}
            :rows [[10] [20] [30]]}
           {:name :step1
            :sql "SELECT value * 2 as value FROM raw"}
           {:name :step2
            :sql "SELECT value + 1 as value FROM step1"}
           {:name :final
            :sql "SELECT SUM(value) as total FROM step2"}])
        (bq/run-dag! s :final)
        (let [result (bq/query s "SELECT * FROM final")]
          (is (= [{:total 123}] result)))))))

(deftest test-dag-diamond
  (testing "Diamond-shaped DAG (A -> B,C -> D) works correctly"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :source
            :schema {:n :int64}
            :rows [[1] [2] [3]]}
           {:name :double_it
            :sql "SELECT n * 2 as doubled FROM source"}
           {:name :triple_it
            :sql "SELECT n * 3 as tripled FROM source"}
           {:name :combined
            :sql "SELECT d.doubled, t.tripled FROM double_it d, triple_it t WHERE d.doubled = t.tripled - 1"}])
        (bq/run-dag! s :combined)
        (let [result (bq/query s "SELECT * FROM combined")]
          (is (= [{:doubled 2 :tripled 3}] result)))))))

(deftest test-get-dag
  (testing "get-dag returns registered tables"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :src :schema {:x :int64} :rows [[1]]}
           {:name :derived :sql "SELECT x FROM src"}])
        (let [dag (bq/get-dag s)
              tables (:tables dag)]
          (is (= 2 (count tables)))
          (let [src-table (first (filter #(= "src" (:name %)) tables))
                derived-table (first (filter #(= "derived" (:name %)) tables))]
            (is (true? (:isSource src-table)))
            (is (false? (:isSource derived-table)))
            (is (= ["src"] (:dependencies derived-table)))))))))

(deftest test-clear-dag
  (testing "clear-dag removes all registered tables"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :a :schema {:x :int64} :rows [[1]]}
           {:name :b :sql "SELECT * FROM a"}])
        (is (= 2 (count (:tables (bq/get-dag s)))))
        (bq/clear-dag! s)
        (is (= 0 (count (:tables (bq/get-dag s)))))))))

(deftest test-dag-aggregation
  (testing "DAG with aggregation functions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :sales
            :schema {:region :string :amount :float64}
            :rows [["East" 100.0] ["East" 150.0] ["West" 200.0] ["West" 250.0]]}
           {:name :region_totals
            :sql "SELECT region, SUM(amount) as total FROM sales GROUP BY region"}
           {:name :summary
            :sql "SELECT COUNT(*) as num_regions, SUM(total) as grand_total FROM region_totals"}])
        (bq/run-dag! s)
        (let [result (bq/query s "SELECT * FROM summary")]
          (is (= [{:num_regions 2 :grand_total 700.0}] result)))))))

(deftest test-dag-join
  (testing "DAG with JOIN operations"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :customers
            :schema {:id :int64 :name :string}
            :rows [[1 "Alice"] [2 "Bob"]]}
           {:name :orders
            :schema {:customer_id :int64 :amount :float64}
            :rows [[1 100.0] [1 200.0] [2 150.0]]}
           {:name :customer_orders
            :sql "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name"}])
        (bq/run-dag! s)
        (let [result (bq/query s "SELECT * FROM customer_orders ORDER BY name")]
          (is (= [{:name "Alice" :total 300.0}
                  {:name "Bob" :total 150.0}]
                 result)))))))

(deftest test-dag-cte
  (testing "DAG with CTE in derived table"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :numbers
            :schema {:n :int64}
            :rows [[1] [2] [3] [4] [5]]}
           {:name :stats
            :sql "WITH evens AS (SELECT n FROM numbers WHERE n % 2 = 0)
                  SELECT COUNT(*) as even_count FROM evens"}])
        (bq/run-dag! s)
        (let [result (bq/query s "SELECT * FROM stats")]
          (is (= [{:even_count 2}] result)))))))

(deftest test-dag-window-functions
  (testing "DAG with window functions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :employees
            :schema {:dept :string :salary :float64}
            :rows [["Sales" 50000.0] ["Sales" 60000.0] ["IT" 70000.0] ["IT" 80000.0]]}
           {:name :ranked
            :sql "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank FROM employees"}])
        (bq/run-dag! s)
        (let [result (bq/query s "SELECT * FROM ranked WHERE rank = 1 ORDER BY dept")]
          (is (= [{:dept "IT" :salary 80000.0 :rank 1}
                  {:dept "Sales" :salary 60000.0 :rank 1}]
                 result)))))))

(deftest test-dag-multiple-source-tables
  (testing "DAG with multiple independent source tables"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :products
            :schema {:id :int64 :name :string :price :float64}
            :rows [[1 "Widget" 10.0] [2 "Gadget" 20.0]]}
           {:name :inventory
            :schema {:product_id :int64 :quantity :int64}
            :rows [[1 100] [2 50]]}
           {:name :product_value
            :sql "SELECT p.name, p.price * i.quantity as total_value FROM products p JOIN inventory i ON p.id = i.product_id"}])
        (bq/run-dag! s)
        (let [result (bq/query s "SELECT * FROM product_value ORDER BY name")]
          (is (= [{:name "Gadget" :total_value 1000.0}
                  {:name "Widget" :total_value 1000.0}]
                 result)))))))

(deftest test-dag-empty-source
  (testing "DAG with empty source table"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :empty_source
            :schema {:id :int64}
            :rows []}
           {:name :derived
            :sql "SELECT COUNT(*) as cnt FROM empty_source"}])
        (bq/run-dag! s)
        (let [result (bq/query s "SELECT * FROM derived")]
          (is (= [{:cnt 0}] result)))))))

(deftest test-dag-reregister
  (testing "Re-registering DAG accumulates rows; use clear-dag! to replace"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :data :schema {:x :int64} :rows [[1]]}])
        (bq/run-dag! s)
        (is (= [{:x 1}] (bq/query s "SELECT * FROM data")))

        (bq/register-dag! s
          [{:name :data :schema {:x :int64} :rows [[999]]}])
        (bq/run-dag! s)
        (is (= [{:x 1} {:x 999}] (bq/query s "SELECT * FROM data")))

        (bq/clear-dag! s)
        (bq/register-dag! s
          [{:name :data :schema {:x :int64} :rows [[42]]}])
        (bq/run-dag! s)
        (is (= [{:x 42}] (bq/query s "SELECT * FROM data")))))))

(deftest test-dag-complex-pipeline
  (testing "Complex DAG pipeline with multiple transformations"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/register-dag! s
          [{:name :events
            :schema {:event_type :string :user_id :int64 :value :float64}
            :rows [["click" 1 1.0] ["click" 1 1.0] ["click" 2 1.0]
                   ["purchase" 1 100.0] ["purchase" 2 50.0]]}
           {:name :event_counts
            :sql "SELECT event_type, COUNT(*) as cnt FROM events GROUP BY event_type"}
           {:name :user_stats
            :sql "SELECT user_id, SUM(value) as total_value FROM events GROUP BY user_id"}
           {:name :top_user
            :sql "SELECT user_id, total_value FROM user_stats ORDER BY total_value DESC LIMIT 1"}])
        (bq/run-dag! s)
        (let [counts (bq/query s "SELECT * FROM event_counts ORDER BY event_type")
              top (bq/query s "SELECT * FROM top_user")]
          (is (= [{:event_type "click" :cnt 3}
                  {:event_type "purchase" :cnt 2}]
                 counts))
          (is (= [{:user_id 1 :total_value 102.0}] top)))))))

(comment
  (require '[clojure.test :refer [run-tests]])
  (run-tests 'bq-runner.dag-test))
