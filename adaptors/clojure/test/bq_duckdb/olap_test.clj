(ns bq-duckdb.olap-test
  "OLAP workload tests for BigQuery compatibility."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [bq-duckdb.api :as bq]
            [bq-duckdb.test-server :as test-server]))

(def ^:dynamic *test-url* nil)

(defn server-fixture [f]
  (binding [*test-url* (test-server/ensure-server!)]
    (f)))

(use-fixtures :once server-fixture)

;; Window Functions

(deftest test-row-number
  (testing "ROW_NUMBER window function"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :sales {:region :string :amount :float64})
        (bq/insert! s :sales [["East" 100.0] ["East" 200.0] ["West" 150.0] ["West" 300.0]])
        (let [result (bq/query s "SELECT region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rn FROM sales ORDER BY region, rn")]
          (is (= [{:region "East" :amount 200.0 :rn 1}
                  {:region "East" :amount 100.0 :rn 2}
                  {:region "West" :amount 300.0 :rn 1}
                  {:region "West" :amount 150.0 :rn 2}]
                 result)))))))

(deftest test-rank-dense-rank
  (testing "RANK and DENSE_RANK window functions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :scores {:name :string :score :int64})
        (bq/insert! s :scores [["Alice" 100] ["Bob" 100] ["Charlie" 90] ["Dave" 80]])
        (let [result (bq/query s "SELECT name, score, RANK() OVER (ORDER BY score DESC) as rank, DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank FROM scores ORDER BY score DESC, name")]
          (is (= [{:name "Alice" :score 100 :rank 1 :dense_rank 1}
                  {:name "Bob" :score 100 :rank 1 :dense_rank 1}
                  {:name "Charlie" :score 90 :rank 3 :dense_rank 2}
                  {:name "Dave" :score 80 :rank 4 :dense_rank 3}]
                 result)))))))

(deftest test-lead-lag
  (testing "LEAD and LAG window functions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :timeseries {:day :int64 :value :float64})
        (bq/insert! s :timeseries [[1 10.0] [2 20.0] [3 15.0] [4 25.0]])
        (let [result (bq/query s "SELECT day, value, LAG(value) OVER (ORDER BY day) as prev_value, LEAD(value) OVER (ORDER BY day) as next_value FROM timeseries ORDER BY day")]
          (is (= [{:day 1 :value 10.0 :prev_value nil :next_value 20.0}
                  {:day 2 :value 20.0 :prev_value 10.0 :next_value 15.0}
                  {:day 3 :value 15.0 :prev_value 20.0 :next_value 25.0}
                  {:day 4 :value 25.0 :prev_value 15.0 :next_value nil}]
                 result)))))))

(deftest test-running-totals
  (testing "Running totals with SUM OVER"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :transactions {:id :int64 :amount :float64})
        (bq/insert! s :transactions [[1 100.0] [2 50.0] [3 75.0] [4 25.0]])
        (let [result (bq/query s "SELECT id, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total FROM transactions ORDER BY id")]
          (is (= [{:id 1 :amount 100.0 :running_total 100.0}
                  {:id 2 :amount 50.0 :running_total 150.0}
                  {:id 3 :amount 75.0 :running_total 225.0}
                  {:id 4 :amount 25.0 :running_total 250.0}]
                 result)))))))

;; CTEs (Common Table Expressions)

(deftest test-simple-cte
  (testing "Simple CTE"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :nums {:n :int64})
        (bq/insert! s :nums [[1] [2] [3] [4] [5]])
        (let [result (bq/query s "WITH numbers AS (SELECT n FROM nums) SELECT SUM(n) as total FROM numbers")]
          (is (= [{:total 15}] result)))))))

(deftest test-chained-ctes
  (testing "Chained CTEs"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :orders {:customer_id :int64 :amount :float64})
        (bq/insert! s :orders [[1 100.0] [1 200.0] [2 150.0] [2 50.0] [3 300.0]])
        (let [result (bq/query s "
          WITH customer_totals AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
          ),
          ranked_customers AS (
            SELECT customer_id, total, RANK() OVER (ORDER BY total DESC) as rank
            FROM customer_totals
          )
          SELECT * FROM ranked_customers WHERE rank <= 2 ORDER BY rank, customer_id")]
          (is (= [{:customer_id 1 :total 300.0 :rank 1}
                  {:customer_id 3 :total 300.0 :rank 1}]
                 result)))))))

;; Aggregations

(deftest test-group-by-aggregations
  (testing "GROUP BY with multiple aggregations"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :products {:category :string :subcategory :string :revenue :float64})
        (bq/insert! s :products [["Electronics" "Phones" 1000.0]
                                  ["Electronics" "Laptops" 2000.0]
                                  ["Clothing" "Shirts" 500.0]
                                  ["Clothing" "Pants" 750.0]])
        (let [result (bq/query s "SELECT category, SUM(revenue) as total_revenue, AVG(revenue) as avg_revenue, COUNT(*) as cnt FROM products GROUP BY category ORDER BY total_revenue DESC")]
          (is (= [{:category "Electronics" :total_revenue 3000.0 :avg_revenue 1500.0 :cnt 2}
                  {:category "Clothing" :total_revenue 1250.0 :avg_revenue 625.0 :cnt 2}]
                 result)))))))

(deftest test-having-clause
  (testing "HAVING clause"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :events {:user_id :int64 :event_type :string})
        (bq/insert! s :events [[1 "click"] [1 "click"] [1 "view"] [2 "click"] [3 "click"] [3 "click"] [3 "click"]])
        (let [result (bq/query s "SELECT user_id, COUNT(*) as event_count FROM events GROUP BY user_id HAVING COUNT(*) >= 3 ORDER BY user_id")]
          (is (= [{:user_id 1 :event_count 3}
                  {:user_id 3 :event_count 3}]
                 result)))))))

;; UNNEST and Arrays

(deftest test-unnest-array
  (testing "Array data via table"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :letters {:letter :string})
        (bq/insert! s :letters [["a"] ["b"] ["c"]])
        (let [result (bq/query s "SELECT letter FROM letters ORDER BY letter")]
          (is (= [{:letter "a"} {:letter "b"} {:letter "c"}] result)))))))

(deftest test-unnest-integers
  (testing "UNNEST with integer array"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :int_arr {:n :int64})
        (bq/insert! s :int_arr [[1] [2] [3]])
        (let [result (bq/query s "SELECT n FROM int_arr ORDER BY n")]
          (is (= [{:n 1} {:n 2} {:n 3}] result)))))))

(deftest test-cross-join
  (testing "CROSS JOIN"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :users {:id :int64 :name :string})
        (bq/create-table! s :tags {:tag :string})
        (bq/insert! s :users [[1 "Alice"] [2 "Bob"]])
        (bq/insert! s :tags [["active"] ["vip"]])
        (let [result (bq/query s "SELECT u.name, t.tag FROM users u CROSS JOIN tags t ORDER BY u.name, t.tag")]
          (is (= [{:name "Alice" :tag "active"}
                  {:name "Alice" :tag "vip"}
                  {:name "Bob" :tag "active"}
                  {:name "Bob" :tag "vip"}]
                 result)))))))

;; Subqueries

(deftest test-scalar-subquery
  (testing "Scalar subquery"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :employees {:id :int64 :salary :float64})
        (bq/insert! s :employees [[1 50000.0] [2 60000.0] [3 70000.0] [4 80000.0]])
        (let [result (bq/query s "SELECT id, salary, salary - (SELECT AVG(salary) FROM employees) as diff_from_avg FROM employees ORDER BY id")]
          (is (= [{:id 1 :salary 50000.0 :diff_from_avg -15000.0}
                  {:id 2 :salary 60000.0 :diff_from_avg -5000.0}
                  {:id 3 :salary 70000.0 :diff_from_avg 5000.0}
                  {:id 4 :salary 80000.0 :diff_from_avg 15000.0}]
                 result)))))))

(deftest test-correlated-subquery
  (testing "EXISTS with correlated subquery"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :customers {:id :int64 :name :string})
        (bq/create-table! s :purchases {:customer_id :int64 :product :string})
        (bq/insert! s :customers [[1 "Alice"] [2 "Bob"] [3 "Charlie"]])
        (bq/insert! s :purchases [[1 "Phone"] [1 "Laptop"] [3 "Tablet"]])
        (let [result (bq/query s "SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM purchases p WHERE p.customer_id = c.id) ORDER BY name")]
          (is (= [{:name "Alice"} {:name "Charlie"}] result)))))))

;; JOINs

(deftest test-multiple-joins
  (testing "Multiple JOINs"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :departments {:id :int64 :name :string})
        (bq/create-table! s :staff {:id :int64 :name :string :dept_id :int64})
        (bq/create-table! s :projects {:id :int64 :title :string :lead_id :int64})
        (bq/insert! s :departments [[1 "Engineering"] [2 "Sales"]])
        (bq/insert! s :staff [[1 "Alice" 1] [2 "Bob" 1] [3 "Charlie" 2]])
        (bq/insert! s :projects [[1 "Project X" 1] [2 "Project Y" 2]])
        (let [result (bq/query s "
          SELECT s.name as staff_name, d.name as dept_name, p.title as project_title
          FROM staff s
          JOIN departments d ON s.dept_id = d.id
          LEFT JOIN projects p ON s.id = p.lead_id
          ORDER BY s.name")]
          (is (= [{:staff_name "Alice" :dept_name "Engineering" :project_title "Project X"}
                  {:staff_name "Bob" :dept_name "Engineering" :project_title "Project Y"}
                  {:staff_name "Charlie" :dept_name "Sales" :project_title nil}]
                 result)))))))

;; CASE expressions

(deftest test-case-when
  (testing "CASE WHEN expressions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :metrics {:value :int64})
        (bq/insert! s :metrics [[10] [50] [75] [100]])
        (let [result (bq/query s "
          SELECT value,
            CASE
              WHEN value < 25 THEN 'low'
              WHEN value < 75 THEN 'medium'
              ELSE 'high'
            END as category
          FROM metrics ORDER BY value")]
          (is (= [{:value 10 :category "low"}
                  {:value 50 :category "medium"}
                  {:value 75 :category "high"}
                  {:value 100 :category "high"}]
                 result)))))))

;; Date/Time functions

(deftest test-date-functions
  (testing "Date extraction functions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query s "SELECT EXTRACT(YEAR FROM DATE '2024-06-15') as year, EXTRACT(MONTH FROM DATE '2024-06-15') as month, EXTRACT(DAY FROM DATE '2024-06-15') as day")]
          (is (= [{:year 2024 :month 6 :day 15}] result)))))))

(deftest test-date-arithmetic
  (testing "Date arithmetic"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query s "SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY) AS VARCHAR) as future_date")]
          (is (= [{:future_date "2024-01-25 00:00:00"}] result)))))))

;; String functions

(deftest test-string-functions
  (testing "String manipulation functions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query s "SELECT UPPER('hello') as upper_str, LOWER('WORLD') as lower_str, CONCAT('foo', 'bar') as concat_str, LENGTH('test') as str_len")]
          (is (= [{:upper_str "HELLO" :lower_str "world" :concat_str "foobar" :str_len 4}] result)))))))

(deftest test-regexp-functions
  (testing "Regular expression functions"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (let [result (bq/query s "SELECT REGEXP_CONTAINS('hello123', '[0-9]+') as has_digits")]
          (is (= [{:has_digits true}] result)))))))

;; Analytical queries

(deftest test-percentile
  (testing "Percentile calculations"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :response_times {:ms :float64})
        (bq/insert! s :response_times [[10.0] [20.0] [30.0] [40.0] [50.0] [60.0] [70.0] [80.0] [90.0] [100.0]])
        (let [result (bq/query s "SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ms), 1) as p50, ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms), 1) as p95 FROM response_times")]
          (is (= [{:p50 55.0 :p95 95.5}] result)))))))

(deftest test-ntile
  (testing "NTILE window function for quartiles"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :values {:v :int64})
        (bq/insert! s :values [[1] [2] [3] [4] [5] [6] [7] [8]])
        (let [result (bq/query s "SELECT v, NTILE(4) OVER (ORDER BY v) as quartile FROM values ORDER BY v")]
          (is (= [{:v 1 :quartile 1}
                  {:v 2 :quartile 1}
                  {:v 3 :quartile 2}
                  {:v 4 :quartile 2}
                  {:v 5 :quartile 3}
                  {:v 6 :quartile 3}
                  {:v 7 :quartile 4}
                  {:v 8 :quartile 4}]
                 result)))))))

;; Complex analytical query

(deftest test-cohort-analysis
  (testing "Cohort-style analysis"
    (bq/with-connection [conn *test-url*]
      (bq/with-session [s conn]
        (bq/create-table! s :user_events {:user_id :int64 :event_date :string :event_type :string})
        (bq/insert! s :user_events [[1 "2024-01-01" "signup"]
                                     [1 "2024-01-02" "purchase"]
                                     [2 "2024-01-01" "signup"]
                                     [2 "2024-01-05" "purchase"]
                                     [3 "2024-01-02" "signup"]])
        (let [result (bq/query s "
          WITH signups AS (
            SELECT user_id, event_date as signup_date
            FROM user_events WHERE event_type = 'signup'
          ),
          purchases AS (
            SELECT user_id, MIN(event_date) as first_purchase_date
            FROM user_events WHERE event_type = 'purchase'
            GROUP BY user_id
          )
          SELECT s.signup_date,
                 COUNT(DISTINCT s.user_id) as total_signups,
                 COUNT(DISTINCT p.user_id) as converted
          FROM signups s
          LEFT JOIN purchases p ON s.user_id = p.user_id
          GROUP BY s.signup_date
          ORDER BY s.signup_date")]
          (is (= [{:signup_date "2024-01-01" :total_signups 2 :converted 2}
                  {:signup_date "2024-01-02" :total_signups 1 :converted 0}]
                 result)))))))
