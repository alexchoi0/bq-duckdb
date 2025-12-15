use std::sync::Arc;

use serde_json::{json, Value};


use bq_duckdb::executor::DuckDbExecutor;
use bq_duckdb::rpc::RpcMethods;
use bq_duckdb::session::SessionManager;

fn create_rpc_methods() -> Arc<RpcMethods> {
    let executor = Arc::new(DuckDbExecutor::new().unwrap());
    let session_manager = Arc::new(SessionManager::new(executor));
    Arc::new(RpcMethods::new(session_manager))
}

#[tokio::test]
async fn test_ping() {
    let methods = create_rpc_methods();

    let result = methods.dispatch("bq.ping", json!({})).await.unwrap();

    assert_eq!(result["message"], "pong");
}

#[tokio::test]
async fn test_create_session() {
    let methods = create_rpc_methods();

    let result = methods.dispatch("bq.createSession", json!({})).await.unwrap();

    assert!(result.get("sessionId").is_some());
    let session_id = result["sessionId"].as_str().unwrap();
    assert!(!session_id.is_empty());
}

#[tokio::test]
async fn test_destroy_session() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    let destroy_result = methods
        .dispatch("bq.destroySession", json!({ "sessionId": session_id }))
        .await
        .unwrap();

    assert_eq!(destroy_result["success"], true);
}

#[tokio::test]
async fn test_destroy_nonexistent_session() {
    let methods = create_rpc_methods();

    let result = methods
        .dispatch(
            "bq.destroySession",
            json!({ "sessionId": "00000000-0000-0000-0000-000000000000" }),
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_simple_query() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT 1 + 1 AS result"
            }),
        )
        .await
        .unwrap();

    assert_eq!(query_result["kind"], "bigquery#queryResponse");
    assert_eq!(query_result["jobComplete"], true);
    assert_eq!(query_result["totalRows"], "1");

    let rows = query_result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    let first_row = &rows[0]["f"];
    let value = first_row[0]["v"].as_i64().unwrap();
    assert_eq!(value, 2);
}

#[tokio::test]
async fn test_create_table() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    let table_result = methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "users",
                "schema": [
                    { "name": "id", "type": "INT64" },
                    { "name": "name", "type": "STRING" },
                    { "name": "active", "type": "BOOL" }
                ]
            }),
        )
        .await
        .unwrap();

    assert_eq!(table_result["success"], true);

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT * FROM users"
            }),
        )
        .await
        .unwrap();

    let schema = &query_result["schema"]["fields"];
    assert_eq!(schema[0]["name"], "id");
    assert_eq!(schema[1]["name"], "name");
    assert_eq!(schema[2]["name"], "active");
}

#[tokio::test]
async fn test_insert_and_query() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "products",
                "schema": [
                    { "name": "id", "type": "INT64" },
                    { "name": "name", "type": "STRING" },
                    { "name": "price", "type": "FLOAT64" }
                ]
            }),
        )
        .await
        .unwrap();

    let insert_result = methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": "products",
                "rows": [
                    [1, "Apple", 1.50],
                    [2, "Banana", 0.75],
                    [3, "Orange", 2.00]
                ]
            }),
        )
        .await
        .unwrap();

    assert_eq!(insert_result["insertedRows"], 3);

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT COUNT(*) as cnt FROM products"
            }),
        )
        .await
        .unwrap();

    let count = query_result["rows"][0]["f"][0]["v"].as_i64().unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_bq_function_transformation() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "numbers",
                "schema": [
                    { "name": "a", "type": "FLOAT64" },
                    { "name": "b", "type": "FLOAT64" }
                ]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": "numbers",
                "rows": [
                    [10.0, 2.0],
                    [10.0, 0.0]
                ]
            }),
        )
        .await
        .unwrap();

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT SAFE_DIVIDE(a, b) as result FROM numbers ORDER BY b DESC"
            }),
        )
        .await
        .unwrap();

    let rows = query_result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let first_result = rows[0]["f"][0]["v"].as_f64().unwrap();
    assert_eq!(first_result, 5.0);

    let second_result = &rows[1]["f"][0]["v"];
    assert!(second_result.is_null());
}

#[tokio::test]
async fn test_ifnull_transformation() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT IFNULL(NULL, 'default') as result"
            }),
        )
        .await
        .unwrap();

    let result = query_result["rows"][0]["f"][0]["v"].as_str().unwrap();
    assert_eq!(result, "default");
}

#[tokio::test]
async fn test_session_isolation() {
    let methods = create_rpc_methods();

    let session1 = methods
        .dispatch("bq.createSession", json!({}))
        .await
        .unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    let session2 = methods
        .dispatch("bq.createSession", json!({}))
        .await
        .unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session1,
                "tableName": "items",
                "schema": [{ "name": "id", "type": "INT64" }]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session1,
                "tableName": "items",
                "rows": [[1], [2], [3]]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session2,
                "tableName": "items",
                "schema": [{ "name": "id", "type": "INT64" }]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session2,
                "tableName": "items",
                "rows": [[100]]
            }),
        )
        .await
        .unwrap();

    let result1 = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session1,
                "sql": "SELECT SUM(id) as total FROM items"
            }),
        )
        .await
        .unwrap();

    let result2 = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session2,
                "sql": "SELECT SUM(id) as total FROM items"
            }),
        )
        .await
        .unwrap();

    let val1 = &result1["rows"][0]["f"][0]["v"];
    let val2 = &result2["rows"][0]["f"][0]["v"];

    let total1 = parse_numeric(val1);
    let total2 = parse_numeric(val2);

    assert_eq!(total1, 6);
    assert_eq!(total2, 100);
}

fn parse_numeric(val: &Value) -> i64 {
    val.as_i64()
        .or_else(|| val.as_f64().map(|f| f as i64))
        .or_else(|| val.as_str().and_then(|s| s.parse::<i64>().ok()))
        .unwrap_or_else(|| panic!("Could not parse numeric: {:?}", val))
}

#[tokio::test]
async fn test_complex_query_with_joins() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "orders",
                "schema": [
                    { "name": "id", "type": "INT64" },
                    { "name": "customer_id", "type": "INT64" },
                    { "name": "amount", "type": "FLOAT64" }
                ]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "customers",
                "schema": [
                    { "name": "id", "type": "INT64" },
                    { "name": "name", "type": "STRING" }
                ]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": "customers",
                "rows": [
                    [1, "Alice"],
                    [2, "Bob"]
                ]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": "orders",
                "rows": [
                    [1, 1, 100.0],
                    [2, 1, 50.0],
                    [3, 2, 75.0]
                ]
            }),
        )
        .await
        .unwrap();

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC"
            }),
        )
        .await
        .unwrap();

    let rows = query_result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let first_name = rows[0]["f"][0]["v"].as_str().unwrap();
    let first_total = rows[0]["f"][1]["v"].as_f64().unwrap();

    assert_eq!(first_name, "Alice");
    assert_eq!(first_total, 150.0);
}

#[tokio::test]
async fn test_unknown_method() {
    let methods = create_rpc_methods();

    let result = methods.dispatch("bq.unknownMethod", json!({})).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_invalid_session_id() {
    let methods = create_rpc_methods();

    let result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": "not-a-valid-uuid",
                "sql": "SELECT 1"
            }),
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_empty_insert() {
    let methods = create_rpc_methods();

    let create_result = methods.dispatch("bq.createSession", json!({})).await.unwrap();
    let session_id = create_result["sessionId"].as_str().unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "empty_test",
                "schema": [{ "name": "id", "type": "INT64" }]
            }),
        )
        .await
        .unwrap();

    let insert_result = methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": "empty_test",
                "rows": []
            }),
        )
        .await
        .unwrap();

    assert_eq!(insert_result["insertedRows"], 0);
}

#[tokio::test]
async fn test_window_function_row_number() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "sales",
        vec![
            ("id", "INT64"),
            ("category", "STRING"),
            ("amount", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "sales",
        vec![
            json!([1, "A", 100.0]),
            json!([2, "A", 200.0]),
            json!([3, "B", 150.0]),
            json!([4, "A", 50.0]),
            json!([5, "B", 300.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                category,
                amount,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rank
            FROM sales
            ORDER BY category, rank
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);

    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "A");
    assert_eq!(parse_numeric(&rows[0]["f"][2]["v"]), 1);

    assert_eq!(rows[3]["f"][0]["v"].as_str().unwrap(), "B");
    assert_eq!(parse_numeric(&rows[3]["f"][2]["v"]), 1);
}

#[tokio::test]
async fn test_cte_with_aggregation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "transactions",
        vec![("user_id", "INT64"), ("amount", "FLOAT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "transactions",
        vec![
            json!([1, 100.0]),
            json!([1, 200.0]),
            json!([2, 50.0]),
            json!([2, 150.0]),
            json!([3, 300.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH user_totals AS (
                SELECT user_id, SUM(amount) AS total
                FROM transactions
                GROUP BY user_id
            )
            SELECT COUNT(*) as user_count, AVG(total) as avg_total
            FROM user_totals
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 3);
}

#[tokio::test]
async fn test_safe_divide_null_handling() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "ratios",
        vec![("numerator", "FLOAT64"), ("denominator", "FLOAT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "ratios",
        vec![
            json!([100.0, 10.0]),
            json!([50.0, 0.0]),
            json!([0.0, 5.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        "SELECT SAFE_DIVIDE(numerator, denominator) as ratio FROM ratios ORDER BY numerator DESC",
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0]["f"][0]["v"].as_f64().unwrap(), 10.0);
    assert!(rows[1]["f"][0]["v"].is_null());
    assert_eq!(rows[2]["f"][0]["v"].as_f64().unwrap(), 0.0);
}

#[tokio::test]
async fn test_case_when_expression() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "scores",
        vec![("student", "STRING"), ("score", "INT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "scores",
        vec![
            json!(["Alice", 95]),
            json!(["Bob", 82]),
            json!(["Charlie", 68]),
            json!(["Diana", 45]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                student,
                score,
                CASE
                    WHEN score >= 90 THEN 'A'
                    WHEN score >= 80 THEN 'B'
                    WHEN score >= 70 THEN 'C'
                    WHEN score >= 60 THEN 'D'
                    ELSE 'F'
                END as grade
            FROM scores
            ORDER BY score DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);

    assert_eq!(rows[0]["f"][2]["v"].as_str().unwrap(), "A");
    assert_eq!(rows[1]["f"][2]["v"].as_str().unwrap(), "B");
    assert_eq!(rows[2]["f"][2]["v"].as_str().unwrap(), "D");
    assert_eq!(rows[3]["f"][2]["v"].as_str().unwrap(), "F");
}

#[tokio::test]
async fn test_subquery_in_where() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "departments",
        vec![("id", "INT64"), ("name", "STRING")],
    )
    .await;

    setup_table(
        &methods,
        &session_id,
        "employees",
        vec![
            ("id", "INT64"),
            ("name", "STRING"),
            ("dept_id", "INT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "departments",
        vec![json!([1, "Engineering"]), json!([2, "Sales"]), json!([3, "HR"])],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "employees",
        vec![
            json!([1, "Alice", 1]),
            json!([2, "Bob", 1]),
            json!([3, "Charlie", 2]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT name FROM departments
            WHERE id IN (SELECT DISTINCT dept_id FROM employees)
            ORDER BY name
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "Engineering");
    assert_eq!(rows[1]["f"][0]["v"].as_str().unwrap(), "Sales");
}

#[tokio::test]
async fn test_left_join_with_nulls() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "users",
        vec![("id", "INT64"), ("name", "STRING")],
    )
    .await;

    setup_table(
        &methods,
        &session_id,
        "orders",
        vec![("id", "INT64"), ("user_id", "INT64"), ("total", "FLOAT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "users",
        vec![
            json!([1, "Alice"]),
            json!([2, "Bob"]),
            json!([3, "Charlie"]),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "orders",
        vec![json!([1, 1, 100.0]), json!([2, 1, 50.0]), json!([3, 2, 75.0])],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT u.name, IFNULL(SUM(o.total), 0) as total_spent
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.name
            ORDER BY total_spent DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "Alice");
    assert_eq!(rows[2]["f"][0]["v"].as_str().unwrap(), "Charlie");
    assert_eq!(parse_numeric(&rows[2]["f"][1]["v"]), 0);
}

#[tokio::test]
async fn test_union_all() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "table_a",
        vec![("id", "INT64"), ("name", "STRING")],
    )
    .await;

    setup_table(
        &methods,
        &session_id,
        "table_b",
        vec![("id", "INT64"), ("name", "STRING")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "table_a",
        vec![json!([1, "A1"]), json!([2, "A2"])],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "table_b",
        vec![json!([3, "B1"]), json!([4, "B2"])],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT id, name FROM table_a
            UNION ALL
            SELECT id, name FROM table_b
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_coalesce_function() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                COALESCE(NULL, NULL, 'default') as val1,
                COALESCE('first', 'second') as val2,
                COALESCE(NULL, 42) as val3
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "default");
    assert_eq!(rows[0]["f"][1]["v"].as_str().unwrap(), "first");
    assert_eq!(parse_numeric(&rows[0]["f"][2]["v"]), 42);
}

#[tokio::test]
async fn test_aggregate_with_having() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "products",
        vec![("category", "STRING"), ("price", "FLOAT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "products",
        vec![
            json!(["Electronics", 500.0]),
            json!(["Electronics", 300.0]),
            json!(["Electronics", 200.0]),
            json!(["Books", 20.0]),
            json!(["Books", 15.0]),
            json!(["Clothing", 50.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT category, COUNT(*) as count, SUM(price) as total
            FROM products
            GROUP BY category
            HAVING COUNT(*) >= 2
            ORDER BY total DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "Electronics");
    assert_eq!(rows[1]["f"][0]["v"].as_str().unwrap(), "Books");
}

#[tokio::test]
async fn test_distinct_values() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "logs",
        vec![("level", "STRING"), ("message", "STRING")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "logs",
        vec![
            json!(["INFO", "msg1"]),
            json!(["ERROR", "msg2"]),
            json!(["INFO", "msg3"]),
            json!(["WARN", "msg4"]),
            json!(["ERROR", "msg5"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        "SELECT DISTINCT level FROM logs ORDER BY level",
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "ERROR");
    assert_eq!(rows[1]["f"][0]["v"].as_str().unwrap(), "INFO");
    assert_eq!(rows[2]["f"][0]["v"].as_str().unwrap(), "WARN");
}

#[tokio::test]
async fn test_between_operator() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "numbers",
        vec![("val", "INT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "numbers",
        vec![
            json!([1]),
            json!([5]),
            json!([10]),
            json!([15]),
            json!([20]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        "SELECT val FROM numbers WHERE val BETWEEN 5 AND 15 ORDER BY val",
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 5);
    assert_eq!(parse_numeric(&rows[1]["f"][0]["v"]), 10);
    assert_eq!(parse_numeric(&rows[2]["f"][0]["v"]), 15);
}

#[tokio::test]
async fn test_like_pattern() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "names",
        vec![("name", "STRING")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "names",
        vec![
            json!(["John Smith"]),
            json!(["Jane Doe"]),
            json!(["Johnny Appleseed"]),
            json!(["Bob Johnson"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        "SELECT name FROM names WHERE name LIKE 'John%' ORDER BY name",
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "John Smith");
    assert_eq!(rows[1]["f"][0]["v"].as_str().unwrap(), "Johnny Appleseed");
}

#[tokio::test]
async fn test_order_by_multiple_columns() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "items",
        vec![("category", "STRING"), ("name", "STRING"), ("price", "FLOAT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "items",
        vec![
            json!(["A", "Item1", 10.0]),
            json!(["B", "Item2", 20.0]),
            json!(["A", "Item3", 5.0]),
            json!(["B", "Item4", 15.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        "SELECT category, name, price FROM items ORDER BY category ASC, price DESC",
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);

    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "A");
    assert_eq!(rows[0]["f"][2]["v"].as_f64().unwrap(), 10.0);

    assert_eq!(rows[1]["f"][0]["v"].as_str().unwrap(), "A");
    assert_eq!(rows[1]["f"][2]["v"].as_f64().unwrap(), 5.0);
}

async fn create_session(methods: &RpcMethods) -> String {
    methods
        .dispatch("bq.createSession", json!({}))
        .await
        .unwrap()["sessionId"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn setup_table(
    methods: &RpcMethods,
    session_id: &str,
    table_name: &str,
    schema: Vec<(&str, &str)>,
) {
    let schema_json: Vec<Value> = schema
        .into_iter()
        .map(|(name, typ)| json!({"name": name, "type": typ}))
        .collect();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": table_name,
                "schema": schema_json
            }),
        )
        .await
        .unwrap();
}

async fn insert_rows(methods: &RpcMethods, session_id: &str, table_name: &str, rows: Vec<Value>) {
    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": table_name,
                "rows": rows
            }),
        )
        .await
        .unwrap();
}

async fn query(methods: &RpcMethods, session_id: &str, sql: &str) -> Value {
    methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": sql
            }),
        )
        .await
        .unwrap()
}

// =============================================================================
// OLAP WORKLOAD TESTS - Real-world BigQuery analytics patterns
// =============================================================================

#[tokio::test]
async fn test_olap_ecommerce_sales_dashboard() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "orders",
        vec![
            ("order_id", "INT64"),
            ("customer_id", "INT64"),
            ("product_id", "INT64"),
            ("quantity", "INT64"),
            ("unit_price", "FLOAT64"),
            ("order_date", "STRING"),
            ("region", "STRING"),
            ("channel", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "orders",
        vec![
            json!([1, 101, 1, 2, 29.99, "2024-01-15", "US-West", "online"]),
            json!([2, 102, 2, 1, 49.99, "2024-01-15", "US-East", "store"]),
            json!([3, 101, 3, 3, 9.99, "2024-01-16", "US-West", "online"]),
            json!([4, 103, 1, 1, 29.99, "2024-01-16", "EU", "online"]),
            json!([5, 104, 2, 2, 49.99, "2024-01-17", "US-East", "online"]),
            json!([6, 102, 4, 1, 199.99, "2024-01-17", "US-East", "store"]),
            json!([7, 105, 1, 5, 29.99, "2024-01-18", "US-West", "online"]),
            json!([8, 101, 2, 1, 49.99, "2024-01-18", "US-West", "online"]),
            json!([9, 106, 3, 2, 9.99, "2024-01-19", "EU", "online"]),
            json!([10, 103, 4, 1, 199.99, "2024-01-19", "EU", "store"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                region,
                channel,
                COUNT(DISTINCT customer_id) AS unique_customers,
                COUNT(*) AS total_orders,
                SUM(quantity * unit_price) AS total_revenue,
                SAFE_DIVIDE(SUM(quantity * unit_price), COUNT(*)) AS avg_order_value
            FROM orders
            GROUP BY region, channel
            ORDER BY total_revenue DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert!(rows.len() >= 4);

    let first_row = &rows[0]["f"];
    let revenue = first_row[4]["v"].as_f64().unwrap();
    assert!(revenue > 0.0);
}

#[tokio::test]
async fn test_olap_time_series_daily_aggregation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "events",
        vec![
            ("event_id", "INT64"),
            ("user_id", "INT64"),
            ("event_type", "STRING"),
            ("event_date", "STRING"),
            ("revenue", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "events",
        vec![
            json!([1, 1, "purchase", "2024-01-01", 100.0]),
            json!([2, 2, "purchase", "2024-01-01", 50.0]),
            json!([3, 1, "view", "2024-01-01", 0.0]),
            json!([4, 3, "purchase", "2024-01-02", 75.0]),
            json!([5, 1, "purchase", "2024-01-02", 200.0]),
            json!([6, 4, "view", "2024-01-02", 0.0]),
            json!([7, 2, "purchase", "2024-01-03", 150.0]),
            json!([8, 5, "purchase", "2024-01-03", 80.0]),
            json!([9, 3, "view", "2024-01-03", 0.0]),
            json!([10, 1, "purchase", "2024-01-03", 120.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH daily_stats AS (
                SELECT
                    event_date,
                    COUNT(DISTINCT user_id) AS daily_active_users,
                    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
                    SUM(revenue) AS daily_revenue
                FROM events
                GROUP BY event_date
            )
            SELECT
                event_date,
                daily_active_users,
                purchases,
                daily_revenue,
                SUM(daily_revenue) OVER (ORDER BY event_date) AS cumulative_revenue
            FROM daily_stats
            ORDER BY event_date
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    let last_row = &rows[2]["f"];
    let cumulative = last_row[4]["v"].as_f64().unwrap();
    assert_eq!(cumulative, 775.0);
}

#[tokio::test]
async fn test_olap_funnel_analysis() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "user_events",
        vec![
            ("user_id", "INT64"),
            ("event_name", "STRING"),
            ("event_timestamp", "INT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "user_events",
        vec![
            json!([1, "page_view", 1000]),
            json!([1, "add_to_cart", 1100]),
            json!([1, "checkout", 1200]),
            json!([1, "purchase", 1300]),
            json!([2, "page_view", 2000]),
            json!([2, "add_to_cart", 2100]),
            json!([2, "checkout", 2200]),
            json!([3, "page_view", 3000]),
            json!([3, "add_to_cart", 3100]),
            json!([4, "page_view", 4000]),
            json!([5, "page_view", 5000]),
            json!([5, "add_to_cart", 5100]),
            json!([5, "checkout", 5200]),
            json!([5, "purchase", 5300]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH funnel AS (
                SELECT
                    COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN user_id END) AS step1_views,
                    COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_id END) AS step2_cart,
                    COUNT(DISTINCT CASE WHEN event_name = 'checkout' THEN user_id END) AS step3_checkout,
                    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) AS step4_purchase
                FROM user_events
            )
            SELECT
                step1_views,
                step2_cart,
                step3_checkout,
                step4_purchase,
                SAFE_DIVIDE(step2_cart * 100.0, step1_views) AS cart_rate,
                SAFE_DIVIDE(step4_purchase * 100.0, step1_views) AS conversion_rate
            FROM funnel
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    let row = &rows[0]["f"];
    assert_eq!(parse_numeric(&row[0]["v"]), 5); // 5 users viewed
    assert_eq!(parse_numeric(&row[1]["v"]), 4); // 4 added to cart
    assert_eq!(parse_numeric(&row[3]["v"]), 2); // 2 purchased
}

#[tokio::test]
async fn test_olap_cohort_retention_analysis() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "user_activity",
        vec![
            ("user_id", "INT64"),
            ("signup_month", "STRING"),
            ("activity_month", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "user_activity",
        vec![
            json!([1, "2024-01", "2024-01"]),
            json!([1, "2024-01", "2024-02"]),
            json!([1, "2024-01", "2024-03"]),
            json!([2, "2024-01", "2024-01"]),
            json!([2, "2024-01", "2024-02"]),
            json!([3, "2024-01", "2024-01"]),
            json!([4, "2024-02", "2024-02"]),
            json!([4, "2024-02", "2024-03"]),
            json!([5, "2024-02", "2024-02"]),
            json!([6, "2024-02", "2024-02"]),
            json!([6, "2024-02", "2024-03"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH cohort_size AS (
                SELECT signup_month, COUNT(DISTINCT user_id) AS cohort_users
                FROM user_activity
                WHERE signup_month = activity_month
                GROUP BY signup_month
            ),
            retention AS (
                SELECT
                    a.signup_month,
                    a.activity_month,
                    COUNT(DISTINCT a.user_id) AS active_users
                FROM user_activity a
                GROUP BY a.signup_month, a.activity_month
            )
            SELECT
                r.signup_month,
                r.activity_month,
                r.active_users,
                c.cohort_users,
                SAFE_DIVIDE(r.active_users * 100.0, c.cohort_users) AS retention_rate
            FROM retention r
            JOIN cohort_size c ON r.signup_month = c.signup_month
            ORDER BY r.signup_month, r.activity_month
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert!(rows.len() >= 4);

    let first_cohort_first_month = &rows[0]["f"];
    let retention = first_cohort_first_month[4]["v"].as_f64().unwrap();
    assert_eq!(retention, 100.0);
}

#[tokio::test]
async fn test_olap_running_totals_and_moving_averages() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "daily_sales",
        vec![
            ("sale_date", "STRING"),
            ("amount", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "daily_sales",
        vec![
            json!(["2024-01-01", 100.0]),
            json!(["2024-01-02", 150.0]),
            json!(["2024-01-03", 120.0]),
            json!(["2024-01-04", 180.0]),
            json!(["2024-01-05", 90.0]),
            json!(["2024-01-06", 200.0]),
            json!(["2024-01-07", 170.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                sale_date,
                amount,
                SUM(amount) OVER (ORDER BY sale_date) AS running_total,
                AVG(amount) OVER (ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3day,
                LAG(amount, 1) OVER (ORDER BY sale_date) AS prev_day_amount,
                amount - LAG(amount, 1) OVER (ORDER BY sale_date) AS day_over_day_change
            FROM daily_sales
            ORDER BY sale_date
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 7);

    let last_row = &rows[6]["f"];
    let running_total = last_row[2]["v"].as_f64().unwrap();
    assert_eq!(running_total, 1010.0);

    let first_row = &rows[0]["f"];
    assert!(first_row[4]["v"].is_null());
}

#[tokio::test]
async fn test_olap_percentile_and_ranking() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "employee_salaries",
        vec![
            ("employee_id", "INT64"),
            ("department", "STRING"),
            ("salary", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "employee_salaries",
        vec![
            json!([1, "Engineering", 120000.0]),
            json!([2, "Engineering", 95000.0]),
            json!([3, "Engineering", 150000.0]),
            json!([4, "Engineering", 85000.0]),
            json!([5, "Sales", 70000.0]),
            json!([6, "Sales", 90000.0]),
            json!([7, "Sales", 65000.0]),
            json!([8, "Marketing", 75000.0]),
            json!([9, "Marketing", 80000.0]),
            json!([10, "Marketing", 72000.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                employee_id,
                department,
                salary,
                RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank,
                DENSE_RANK() OVER (ORDER BY salary DESC) AS company_rank,
                NTILE(4) OVER (ORDER BY salary DESC) AS salary_quartile
            FROM employee_salaries
            ORDER BY salary DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 10);

    let top_earner = &rows[0]["f"];
    assert_eq!(parse_numeric(&top_earner[3]["v"]), 1);
    assert_eq!(parse_numeric(&top_earner[4]["v"]), 1);
    assert_eq!(parse_numeric(&top_earner[5]["v"]), 1);
}

#[tokio::test]
async fn test_olap_multi_level_aggregation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "sales_data",
        vec![
            ("year", "INT64"),
            ("quarter", "INT64"),
            ("region", "STRING"),
            ("product_category", "STRING"),
            ("sales_amount", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "sales_data",
        vec![
            json!([2024, 1, "North", "Electronics", 50000.0]),
            json!([2024, 1, "North", "Clothing", 30000.0]),
            json!([2024, 1, "South", "Electronics", 45000.0]),
            json!([2024, 1, "South", "Clothing", 25000.0]),
            json!([2024, 2, "North", "Electronics", 55000.0]),
            json!([2024, 2, "North", "Clothing", 35000.0]),
            json!([2024, 2, "South", "Electronics", 48000.0]),
            json!([2024, 2, "South", "Clothing", 28000.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH quarterly_totals AS (
                SELECT
                    year,
                    quarter,
                    region,
                    SUM(sales_amount) AS regional_sales
                FROM sales_data
                GROUP BY year, quarter, region
            ),
            regional_rankings AS (
                SELECT
                    year,
                    quarter,
                    region,
                    regional_sales,
                    RANK() OVER (PARTITION BY year, quarter ORDER BY regional_sales DESC) AS region_rank
                FROM quarterly_totals
            )
            SELECT
                year,
                quarter,
                region,
                regional_sales,
                region_rank,
                SUM(regional_sales) OVER (PARTITION BY year ORDER BY quarter, region_rank) AS ytd_sales
            FROM regional_rankings
            ORDER BY year, quarter, region_rank
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_olap_complex_nested_ctes() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "transactions",
        vec![
            ("txn_id", "INT64"),
            ("customer_id", "INT64"),
            ("merchant_id", "INT64"),
            ("amount", "FLOAT64"),
            ("txn_date", "STRING"),
        ],
    )
    .await;

    setup_table(
        &methods,
        &session_id,
        "merchants",
        vec![
            ("merchant_id", "INT64"),
            ("merchant_name", "STRING"),
            ("category", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "merchants",
        vec![
            json!([1, "Amazon", "Retail"]),
            json!([2, "Uber", "Transportation"]),
            json!([3, "Netflix", "Entertainment"]),
            json!([4, "Walmart", "Retail"]),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "transactions",
        vec![
            json!([1, 101, 1, 150.0, "2024-01-05"]),
            json!([2, 101, 2, 25.0, "2024-01-06"]),
            json!([3, 101, 3, 15.0, "2024-01-07"]),
            json!([4, 102, 1, 200.0, "2024-01-05"]),
            json!([5, 102, 4, 80.0, "2024-01-08"]),
            json!([6, 103, 2, 30.0, "2024-01-06"]),
            json!([7, 103, 3, 15.0, "2024-01-07"]),
            json!([8, 104, 1, 300.0, "2024-01-09"]),
            json!([9, 104, 2, 45.0, "2024-01-10"]),
            json!([10, 104, 3, 15.0, "2024-01-10"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH customer_spending AS (
                SELECT
                    t.customer_id,
                    m.category,
                    SUM(t.amount) AS category_spend,
                    COUNT(*) AS txn_count
                FROM transactions t
                JOIN merchants m ON t.merchant_id = m.merchant_id
                GROUP BY t.customer_id, m.category
            ),
            customer_totals AS (
                SELECT
                    customer_id,
                    SUM(category_spend) AS total_spend,
                    SUM(txn_count) AS total_txns
                FROM customer_spending
                GROUP BY customer_id
            ),
            category_percentages AS (
                SELECT
                    cs.customer_id,
                    cs.category,
                    cs.category_spend,
                    ct.total_spend,
                    SAFE_DIVIDE(cs.category_spend * 100.0, ct.total_spend) AS pct_of_total
                FROM customer_spending cs
                JOIN customer_totals ct ON cs.customer_id = ct.customer_id
            )
            SELECT
                customer_id,
                category,
                category_spend,
                pct_of_total
            FROM category_percentages
            WHERE pct_of_total > 10
            ORDER BY customer_id, pct_of_total DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert!(rows.len() >= 4);
}

#[tokio::test]
async fn test_olap_sessionization() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "clickstream",
        vec![
            ("user_id", "INT64"),
            ("event_timestamp", "INT64"),
            ("page_url", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "clickstream",
        vec![
            json!([1, 1000, "/home"]),
            json!([1, 1005, "/products"]),
            json!([1, 1010, "/cart"]),
            json!([1, 5000, "/home"]),
            json!([1, 5010, "/checkout"]),
            json!([2, 2000, "/home"]),
            json!([2, 2100, "/products"]),
            json!([2, 2200, "/product/123"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH event_gaps AS (
                SELECT
                    user_id,
                    event_timestamp,
                    page_url,
                    LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS prev_timestamp,
                    event_timestamp - LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS time_gap
                FROM clickstream
            ),
            session_markers AS (
                SELECT
                    user_id,
                    event_timestamp,
                    page_url,
                    CASE WHEN time_gap IS NULL OR time_gap > 1800 THEN 1 ELSE 0 END AS new_session
                FROM event_gaps
            ),
            sessions AS (
                SELECT
                    user_id,
                    event_timestamp,
                    page_url,
                    SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS session_id
                FROM session_markers
            )
            SELECT
                user_id,
                session_id,
                COUNT(*) AS pages_viewed,
                MIN(event_timestamp) AS session_start,
                MAX(event_timestamp) AS session_end
            FROM sessions
            GROUP BY user_id, session_id
            ORDER BY user_id, session_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    let user1_session1 = &rows[0]["f"];
    assert_eq!(parse_numeric(&user1_session1[0]["v"]), 1);
    assert_eq!(parse_numeric(&user1_session1[2]["v"]), 3);
}

#[tokio::test]
async fn test_olap_customer_lifetime_value() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "customer_orders",
        vec![
            ("customer_id", "INT64"),
            ("order_id", "INT64"),
            ("order_amount", "FLOAT64"),
            ("order_date", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "customer_orders",
        vec![
            json!([1, 1, 100.0, "2024-01-01"]),
            json!([1, 2, 150.0, "2024-02-01"]),
            json!([1, 3, 200.0, "2024-03-01"]),
            json!([2, 4, 50.0, "2024-01-15"]),
            json!([2, 5, 75.0, "2024-02-15"]),
            json!([3, 6, 500.0, "2024-01-01"]),
            json!([4, 7, 25.0, "2024-01-01"]),
            json!([4, 8, 30.0, "2024-01-15"]),
            json!([4, 9, 20.0, "2024-02-01"]),
            json!([4, 10, 25.0, "2024-02-15"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH customer_metrics AS (
                SELECT
                    customer_id,
                    COUNT(*) AS order_count,
                    SUM(order_amount) AS total_revenue,
                    MIN(order_date) AS first_order,
                    MAX(order_date) AS last_order,
                    AVG(order_amount) AS avg_order_value
                FROM customer_orders
                GROUP BY customer_id
            ),
            clv_segments AS (
                SELECT
                    customer_id,
                    order_count,
                    total_revenue,
                    avg_order_value,
                    CASE
                        WHEN total_revenue >= 400 THEN 'High Value'
                        WHEN total_revenue >= 100 THEN 'Medium Value'
                        ELSE 'Low Value'
                    END AS customer_segment,
                    NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile
                FROM customer_metrics
            )
            SELECT
                customer_segment,
                COUNT(*) AS customer_count,
                SUM(total_revenue) AS segment_revenue,
                AVG(order_count) AS avg_orders,
                AVG(avg_order_value) AS avg_aov
            FROM clv_segments
            GROUP BY customer_segment
            ORDER BY segment_revenue DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert!(rows.len() >= 2);

    let total_revenue: f64 = rows
        .iter()
        .map(|r| r["f"][2]["v"].as_f64().unwrap_or(0.0))
        .sum();
    assert_eq!(total_revenue, 1175.0);
}

#[tokio::test]
async fn test_olap_inventory_analysis() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "inventory_movements",
        vec![
            ("product_id", "INT64"),
            ("movement_type", "STRING"),
            ("quantity", "INT64"),
            ("movement_date", "STRING"),
            ("warehouse", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "inventory_movements",
        vec![
            json!([1, "IN", 100, "2024-01-01", "WH-A"]),
            json!([1, "OUT", 30, "2024-01-05", "WH-A"]),
            json!([1, "IN", 50, "2024-01-10", "WH-A"]),
            json!([1, "OUT", 40, "2024-01-15", "WH-A"]),
            json!([2, "IN", 200, "2024-01-01", "WH-A"]),
            json!([2, "OUT", 80, "2024-01-08", "WH-A"]),
            json!([2, "IN", 75, "2024-01-12", "WH-B"]),
            json!([1, "IN", 60, "2024-01-02", "WH-B"]),
            json!([1, "OUT", 25, "2024-01-07", "WH-B"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH inventory_balance AS (
                SELECT
                    product_id,
                    warehouse,
                    SUM(CASE WHEN movement_type = 'IN' THEN quantity ELSE 0 END) AS total_in,
                    SUM(CASE WHEN movement_type = 'OUT' THEN quantity ELSE 0 END) AS total_out,
                    SUM(CASE WHEN movement_type = 'IN' THEN quantity ELSE -quantity END) AS current_balance
                FROM inventory_movements
                GROUP BY product_id, warehouse
            )
            SELECT
                product_id,
                SUM(total_in) AS total_received,
                SUM(total_out) AS total_shipped,
                SUM(current_balance) AS on_hand,
                SAFE_DIVIDE(SUM(total_out) * 100.0, SUM(total_in)) AS turnover_rate
            FROM inventory_balance
            GROUP BY product_id
            ORDER BY product_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let product1 = &rows[0]["f"];
    let on_hand = parse_numeric(&product1[3]["v"]);
    assert_eq!(on_hand, 115);
}

#[tokio::test]
async fn test_olap_year_over_year_comparison() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "monthly_revenue",
        vec![
            ("year", "INT64"),
            ("month", "INT64"),
            ("revenue", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "monthly_revenue",
        vec![
            json!([2023, 1, 10000.0]),
            json!([2023, 2, 12000.0]),
            json!([2023, 3, 11000.0]),
            json!([2024, 1, 12000.0]),
            json!([2024, 2, 15000.0]),
            json!([2024, 3, 14000.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH revenue_with_prev_year AS (
                SELECT
                    year,
                    month,
                    revenue,
                    LAG(revenue) OVER (PARTITION BY month ORDER BY year) AS prev_year_revenue
                FROM monthly_revenue
            )
            SELECT
                year,
                month,
                revenue,
                prev_year_revenue,
                revenue - prev_year_revenue AS yoy_change,
                SAFE_DIVIDE((revenue - prev_year_revenue) * 100.0, prev_year_revenue) AS yoy_pct_change
            FROM revenue_with_prev_year
            WHERE prev_year_revenue IS NOT NULL
            ORDER BY year, month
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    let jan_2024 = &rows[0]["f"];
    let yoy_change = jan_2024[4]["v"].as_f64().unwrap();
    assert_eq!(yoy_change, 2000.0);

    let yoy_pct = jan_2024[5]["v"].as_f64().unwrap();
    assert_eq!(yoy_pct, 20.0);
}

// =============================================================================
// ADDITIONAL OLAP WORKLOAD TESTS - Extended BigQuery SQL coverage
// =============================================================================

#[tokio::test]
async fn test_olap_first_last_value_window() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "stock_prices",
        vec![
            ("symbol", "STRING"),
            ("trade_date", "STRING"),
            ("price", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "stock_prices",
        vec![
            json!(["AAPL", "2024-01-01", 150.0]),
            json!(["AAPL", "2024-01-02", 152.0]),
            json!(["AAPL", "2024-01-03", 148.0]),
            json!(["AAPL", "2024-01-04", 155.0]),
            json!(["GOOG", "2024-01-01", 140.0]),
            json!(["GOOG", "2024-01-02", 142.0]),
            json!(["GOOG", "2024-01-03", 145.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                symbol,
                trade_date,
                price,
                FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY trade_date) AS opening_price,
                LAST_VALUE(price) OVER (PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS closing_price
            FROM stock_prices
            ORDER BY symbol, trade_date
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 7);

    let aapl_first = &rows[0]["f"];
    assert_eq!(aapl_first[3]["v"].as_f64().unwrap(), 150.0);
    assert_eq!(aapl_first[4]["v"].as_f64().unwrap(), 155.0);
}

#[tokio::test]
async fn test_olap_lead_lag_analysis() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "user_logins",
        vec![
            ("user_id", "INT64"),
            ("login_date", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "user_logins",
        vec![
            json!([1, "2024-01-01"]),
            json!([1, "2024-01-03"]),
            json!([1, "2024-01-07"]),
            json!([1, "2024-01-08"]),
            json!([2, "2024-01-02"]),
            json!([2, "2024-01-05"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                user_id,
                login_date,
                LAG(login_date, 1) OVER (PARTITION BY user_id ORDER BY login_date) AS prev_login,
                LEAD(login_date, 1) OVER (PARTITION BY user_id ORDER BY login_date) AS next_login,
                LAG(login_date, 2) OVER (PARTITION BY user_id ORDER BY login_date) AS two_logins_ago
            FROM user_logins
            ORDER BY user_id, login_date
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);

    assert!(rows[0]["f"][2]["v"].is_null());
    assert_eq!(rows[1]["f"][2]["v"].as_str().unwrap(), "2024-01-01");
}

#[tokio::test]
async fn test_olap_cumulative_distribution() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "exam_scores",
        vec![
            ("student_id", "INT64"),
            ("score", "INT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "exam_scores",
        vec![
            json!([1, 85]),
            json!([2, 92]),
            json!([3, 78]),
            json!([4, 92]),
            json!([5, 88]),
            json!([6, 72]),
            json!([7, 95]),
            json!([8, 88]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                student_id,
                score,
                PERCENT_RANK() OVER (ORDER BY score) AS percentile,
                CUME_DIST() OVER (ORDER BY score) AS cumulative_dist
            FROM exam_scores
            ORDER BY score DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 8);
}

#[tokio::test]
async fn test_olap_multiple_aggregations_same_query() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "order_items",
        vec![
            ("order_id", "INT64"),
            ("product_id", "INT64"),
            ("quantity", "INT64"),
            ("unit_price", "FLOAT64"),
            ("discount", "FLOAT64"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "order_items",
        vec![
            json!([1, 101, 2, 25.0, 0.1]),
            json!([1, 102, 1, 50.0, 0.0]),
            json!([2, 101, 3, 25.0, 0.15]),
            json!([2, 103, 2, 30.0, 0.05]),
            json!([3, 102, 1, 50.0, 0.2]),
            json!([3, 104, 4, 15.0, 0.0]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                COUNT(*) AS total_items,
                COUNT(DISTINCT order_id) AS unique_orders,
                COUNT(DISTINCT product_id) AS unique_products,
                SUM(quantity) AS total_quantity,
                SUM(quantity * unit_price) AS gross_revenue,
                SUM(quantity * unit_price * (1 - discount)) AS net_revenue,
                AVG(discount) AS avg_discount,
                MAX(quantity * unit_price) AS largest_line_item,
                MIN(quantity * unit_price) AS smallest_line_item
            FROM order_items
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 6);
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 3);
    assert_eq!(parse_numeric(&rows[0]["f"][2]["v"]), 4);
}

#[tokio::test]
async fn test_olap_self_join_hierarchy() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "employees",
        vec![
            ("emp_id", "INT64"),
            ("name", "STRING"),
            ("manager_id", "INT64"),
            ("department", "STRING"),
        ],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "employees",
        vec![
            json!([1, "Alice", null, "Executive"]),
            json!([2, "Bob", 1, "Engineering"]),
            json!([3, "Carol", 1, "Sales"]),
            json!([4, "Dave", 2, "Engineering"]),
            json!([5, "Eve", 2, "Engineering"]),
            json!([6, "Frank", 3, "Sales"]),
        ],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                e.name AS employee,
                e.department,
                m.name AS manager,
                m.department AS manager_dept
            FROM employees e
            LEFT JOIN employees m ON e.manager_id = m.emp_id
            ORDER BY e.emp_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);

    assert!(rows[0]["f"][2]["v"].is_null());
    assert_eq!(rows[1]["f"][2]["v"].as_str().unwrap(), "Alice");
}

#[tokio::test]
async fn test_olap_multiple_joins_chain() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "customers", vec![("id", "INT64"), ("name", "STRING"), ("country_id", "INT64")]).await;
    setup_table(&methods, &session_id, "countries", vec![("id", "INT64"), ("name", "STRING"), ("region_id", "INT64")]).await;
    setup_table(&methods, &session_id, "regions", vec![("id", "INT64"), ("name", "STRING")]).await;
    setup_table(&methods, &session_id, "orders", vec![("id", "INT64"), ("customer_id", "INT64"), ("amount", "FLOAT64")]).await;

    insert_rows(&methods, &session_id, "regions", vec![json!([1, "North America"]), json!([2, "Europe"])]).await;
    insert_rows(&methods, &session_id, "countries", vec![json!([1, "USA", 1]), json!([2, "Canada", 1]), json!([3, "UK", 2])]).await;
    insert_rows(&methods, &session_id, "customers", vec![json!([1, "Acme Corp", 1]), json!([2, "Maple Inc", 2]), json!([3, "British Co", 3])]).await;
    insert_rows(&methods, &session_id, "orders", vec![json!([1, 1, 1000.0]), json!([2, 1, 500.0]), json!([3, 2, 750.0]), json!([4, 3, 300.0])]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                r.name AS region,
                co.name AS country,
                cu.name AS customer,
                SUM(o.amount) AS total_orders
            FROM orders o
            JOIN customers cu ON o.customer_id = cu.id
            JOIN countries co ON cu.country_id = co.id
            JOIN regions r ON co.region_id = r.id
            GROUP BY r.name, co.name, cu.name
            ORDER BY total_orders DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["f"][2]["v"].as_str().unwrap(), "Acme Corp");
}

#[tokio::test]
async fn test_olap_anti_join_pattern() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "all_users", vec![("id", "INT64"), ("email", "STRING")]).await;
    setup_table(&methods, &session_id, "active_users", vec![("user_id", "INT64"), ("last_active", "STRING")]).await;

    insert_rows(&methods, &session_id, "all_users", vec![
        json!([1, "user1@test.com"]),
        json!([2, "user2@test.com"]),
        json!([3, "user3@test.com"]),
        json!([4, "user4@test.com"]),
        json!([5, "user5@test.com"]),
    ]).await;

    insert_rows(&methods, &session_id, "active_users", vec![
        json!([1, "2024-01-15"]),
        json!([3, "2024-01-14"]),
        json!([5, "2024-01-13"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT u.id, u.email
            FROM all_users u
            LEFT JOIN active_users a ON u.id = a.user_id
            WHERE a.user_id IS NULL
            ORDER BY u.id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 2);
    assert_eq!(parse_numeric(&rows[1]["f"][0]["v"]), 4);
}

#[tokio::test]
async fn test_olap_exists_correlated_subquery() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "products", vec![("id", "INT64"), ("name", "STRING"), ("category", "STRING")]).await;
    setup_table(&methods, &session_id, "sales", vec![("id", "INT64"), ("product_id", "INT64"), ("quantity", "INT64")]).await;

    insert_rows(&methods, &session_id, "products", vec![
        json!([1, "Widget A", "Electronics"]),
        json!([2, "Widget B", "Electronics"]),
        json!([3, "Gadget C", "Home"]),
        json!([4, "Gadget D", "Home"]),
    ]).await;

    insert_rows(&methods, &session_id, "sales", vec![
        json!([1, 1, 10]),
        json!([2, 1, 5]),
        json!([3, 3, 20]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT p.id, p.name, p.category
            FROM products p
            WHERE EXISTS (
                SELECT 1 FROM sales s WHERE s.product_id = p.id AND s.quantity > 5
            )
            ORDER BY p.id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_olap_not_exists_subquery() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "categories", vec![("id", "INT64"), ("name", "STRING")]).await;
    setup_table(&methods, &session_id, "products", vec![("id", "INT64"), ("name", "STRING"), ("category_id", "INT64")]).await;

    insert_rows(&methods, &session_id, "categories", vec![
        json!([1, "Electronics"]),
        json!([2, "Clothing"]),
        json!([3, "Food"]),
        json!([4, "Books"]),
    ]).await;

    insert_rows(&methods, &session_id, "products", vec![
        json!([1, "Phone", 1]),
        json!([2, "Laptop", 1]),
        json!([3, "T-Shirt", 2]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT c.id, c.name
            FROM categories c
            WHERE NOT EXISTS (
                SELECT 1 FROM products p WHERE p.category_id = c.id
            )
            ORDER BY c.id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["f"][1]["v"].as_str().unwrap(), "Food");
    assert_eq!(rows[1]["f"][1]["v"].as_str().unwrap(), "Books");
}

#[tokio::test]
async fn test_olap_scalar_subquery() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "products", vec![("id", "INT64"), ("name", "STRING"), ("price", "FLOAT64")]).await;

    insert_rows(&methods, &session_id, "products", vec![
        json!([1, "Product A", 100.0]),
        json!([2, "Product B", 200.0]),
        json!([3, "Product C", 150.0]),
        json!([4, "Product D", 300.0]),
        json!([5, "Product E", 50.0]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                id,
                name,
                price,
                price - (SELECT AVG(price) FROM products) AS diff_from_avg,
                SAFE_DIVIDE(price * 100.0, (SELECT SUM(price) FROM products)) AS pct_of_total
            FROM products
            ORDER BY price DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_olap_string_aggregation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "team_members", vec![
        ("team_id", "INT64"),
        ("team_name", "STRING"),
        ("member_name", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "team_members", vec![
        json!([1, "Engineering", "Alice"]),
        json!([1, "Engineering", "Bob"]),
        json!([1, "Engineering", "Carol"]),
        json!([2, "Sales", "Dave"]),
        json!([2, "Sales", "Eve"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                team_name,
                COUNT(*) AS member_count,
                STRING_AGG(member_name, ', ' ORDER BY member_name) AS members
            FROM team_members
            GROUP BY team_name
            ORDER BY team_name
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "Engineering");
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 3);
}

#[tokio::test]
async fn test_olap_conditional_aggregation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "survey_responses", vec![
        ("response_id", "INT64"),
        ("question_id", "INT64"),
        ("answer", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "survey_responses", vec![
        json!([1, 1, "Yes"]),
        json!([2, 1, "No"]),
        json!([3, 1, "Yes"]),
        json!([4, 1, "Yes"]),
        json!([5, 1, "No"]),
        json!([6, 2, "Agree"]),
        json!([7, 2, "Disagree"]),
        json!([8, 2, "Neutral"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                question_id,
                COUNT(*) AS total_responses,
                SUM(CASE WHEN answer = 'Yes' THEN 1 ELSE 0 END) AS yes_count,
                SUM(CASE WHEN answer = 'No' THEN 1 ELSE 0 END) AS no_count,
                SUM(CASE WHEN answer = 'Agree' THEN 1 ELSE 0 END) AS agree_count,
                SUM(CASE WHEN answer = 'Disagree' THEN 1 ELSE 0 END) AS disagree_count,
                SUM(CASE WHEN answer = 'Neutral' THEN 1 ELSE 0 END) AS neutral_count
            FROM survey_responses
            GROUP BY question_id
            ORDER BY question_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(parse_numeric(&rows[0]["f"][2]["v"]), 3);
    assert_eq!(parse_numeric(&rows[0]["f"][3]["v"]), 2);
}

#[tokio::test]
async fn test_olap_pivot_simulation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "quarterly_sales", vec![
        ("year", "INT64"),
        ("quarter", "STRING"),
        ("revenue", "FLOAT64"),
    ]).await;

    insert_rows(&methods, &session_id, "quarterly_sales", vec![
        json!([2023, "Q1", 100000.0]),
        json!([2023, "Q2", 120000.0]),
        json!([2023, "Q3", 110000.0]),
        json!([2023, "Q4", 150000.0]),
        json!([2024, "Q1", 130000.0]),
        json!([2024, "Q2", 140000.0]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                year,
                SUM(CASE WHEN quarter = 'Q1' THEN revenue ELSE 0 END) AS q1_revenue,
                SUM(CASE WHEN quarter = 'Q2' THEN revenue ELSE 0 END) AS q2_revenue,
                SUM(CASE WHEN quarter = 'Q3' THEN revenue ELSE 0 END) AS q3_revenue,
                SUM(CASE WHEN quarter = 'Q4' THEN revenue ELSE 0 END) AS q4_revenue,
                SUM(revenue) AS total_revenue
            FROM quarterly_sales
            GROUP BY year
            ORDER BY year
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["f"][5]["v"].as_f64().unwrap(), 480000.0);
}

#[tokio::test]
async fn test_olap_date_bucketing() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "page_views", vec![
        ("view_id", "INT64"),
        ("page_url", "STRING"),
        ("view_hour", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "page_views", vec![
        json!([1, "/home", 0]),
        json!([2, "/home", 3]),
        json!([3, "/products", 9]),
        json!([4, "/home", 10]),
        json!([5, "/products", 14]),
        json!([6, "/home", 15]),
        json!([7, "/checkout", 20]),
        json!([8, "/home", 22]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                CASE
                    WHEN view_hour BETWEEN 0 AND 5 THEN 'Night'
                    WHEN view_hour BETWEEN 6 AND 11 THEN 'Morning'
                    WHEN view_hour BETWEEN 12 AND 17 THEN 'Afternoon'
                    ELSE 'Evening'
                END AS time_bucket,
                COUNT(*) AS view_count,
                COUNT(DISTINCT page_url) AS unique_pages
            FROM page_views
            GROUP BY
                CASE
                    WHEN view_hour BETWEEN 0 AND 5 THEN 'Night'
                    WHEN view_hour BETWEEN 6 AND 11 THEN 'Morning'
                    WHEN view_hour BETWEEN 12 AND 17 THEN 'Afternoon'
                    ELSE 'Evening'
                END
            ORDER BY view_count DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_olap_window_frame_rows() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "temperature_readings", vec![
        ("reading_date", "STRING"),
        ("temperature", "FLOAT64"),
    ]).await;

    insert_rows(&methods, &session_id, "temperature_readings", vec![
        json!(["2024-01-01", 32.0]),
        json!(["2024-01-02", 35.0]),
        json!(["2024-01-03", 28.0]),
        json!(["2024-01-04", 30.0]),
        json!(["2024-01-05", 33.0]),
        json!(["2024-01-06", 36.0]),
        json!(["2024-01-07", 34.0]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                reading_date,
                temperature,
                AVG(temperature) OVER (ORDER BY reading_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_last_3,
                MIN(temperature) OVER (ORDER BY reading_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS min_window,
                MAX(temperature) OVER (ORDER BY reading_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS max_window
            FROM temperature_readings
            ORDER BY reading_date
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 7);
}

#[tokio::test]
async fn test_olap_recursive_like_pattern() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "org_hierarchy", vec![
        ("id", "INT64"),
        ("parent_id", "INT64"),
        ("name", "STRING"),
        ("level", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "org_hierarchy", vec![
        json!([1, null, "CEO", 1]),
        json!([2, 1, "CTO", 2]),
        json!([3, 1, "CFO", 2]),
        json!([4, 2, "VP Engineering", 3]),
        json!([5, 2, "VP Product", 3]),
        json!([6, 4, "Director", 4]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH level_counts AS (
                SELECT level, COUNT(*) AS count_at_level
                FROM org_hierarchy
                GROUP BY level
            )
            SELECT
                o.level,
                o.name,
                p.name AS reports_to,
                lc.count_at_level AS peers_at_level
            FROM org_hierarchy o
            LEFT JOIN org_hierarchy p ON o.parent_id = p.id
            JOIN level_counts lc ON o.level = lc.level
            ORDER BY o.level, o.name
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
}

#[tokio::test]
async fn test_olap_gap_detection() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "sequence_ids", vec![("id", "INT64")]).await;

    insert_rows(&methods, &session_id, "sequence_ids", vec![
        json!([1]), json!([2]), json!([3]), json!([5]), json!([6]), json!([10]), json!([11]), json!([15]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH gaps AS (
                SELECT
                    id,
                    LEAD(id) OVER (ORDER BY id) AS next_id,
                    LEAD(id) OVER (ORDER BY id) - id AS gap_size
                FROM sequence_ids
            )
            SELECT id, next_id, gap_size
            FROM gaps
            WHERE gap_size > 1
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 3);
    assert_eq!(parse_numeric(&rows[0]["f"][2]["v"]), 2);
}

#[tokio::test]
async fn test_olap_islands_and_gaps() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "sensor_readings", vec![
        ("reading_time", "INT64"),
        ("sensor_id", "INT64"),
        ("status", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "sensor_readings", vec![
        json!([1, 1, "active"]),
        json!([2, 1, "active"]),
        json!([3, 1, "active"]),
        json!([4, 1, "inactive"]),
        json!([5, 1, "inactive"]),
        json!([6, 1, "active"]),
        json!([7, 1, "active"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH status_groups AS (
                SELECT
                    reading_time,
                    status,
                    reading_time - ROW_NUMBER() OVER (PARTITION BY status ORDER BY reading_time) AS grp
                FROM sensor_readings
            )
            SELECT
                status,
                MIN(reading_time) AS start_time,
                MAX(reading_time) AS end_time,
                COUNT(*) AS duration
            FROM status_groups
            GROUP BY status, grp
            ORDER BY start_time
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_olap_deduplication_keep_latest() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "user_profiles", vec![
        ("user_id", "INT64"),
        ("email", "STRING"),
        ("updated_at", "STRING"),
        ("name", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "user_profiles", vec![
        json!([1, "alice@test.com", "2024-01-01", "Alice V1"]),
        json!([1, "alice@test.com", "2024-01-15", "Alice V2"]),
        json!([1, "alice@test.com", "2024-01-10", "Alice V1.5"]),
        json!([2, "bob@test.com", "2024-01-05", "Bob V1"]),
        json!([2, "bob@test.com", "2024-01-20", "Bob V2"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH ranked AS (
                SELECT
                    user_id,
                    email,
                    updated_at,
                    name,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS rn
                FROM user_profiles
            )
            SELECT user_id, email, updated_at, name
            FROM ranked
            WHERE rn = 1
            ORDER BY user_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["f"][3]["v"].as_str().unwrap(), "Alice V2");
    assert_eq!(rows[1]["f"][3]["v"].as_str().unwrap(), "Bob V2");
}

#[tokio::test]
async fn test_olap_running_count_distinct() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "daily_visitors", vec![
        ("visit_date", "STRING"),
        ("visitor_id", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "daily_visitors", vec![
        json!(["2024-01-01", 1]),
        json!(["2024-01-01", 2]),
        json!(["2024-01-02", 1]),
        json!(["2024-01-02", 3]),
        json!(["2024-01-03", 2]),
        json!(["2024-01-03", 4]),
        json!(["2024-01-03", 1]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH daily_unique AS (
                SELECT visit_date, COUNT(DISTINCT visitor_id) AS unique_visitors
                FROM daily_visitors
                GROUP BY visit_date
            )
            SELECT
                visit_date,
                unique_visitors,
                SUM(unique_visitors) OVER (ORDER BY visit_date) AS cumulative_visits
            FROM daily_unique
            ORDER BY visit_date
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_olap_top_n_per_group() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "product_sales", vec![
        ("category", "STRING"),
        ("product_name", "STRING"),
        ("sales_amount", "FLOAT64"),
    ]).await;

    insert_rows(&methods, &session_id, "product_sales", vec![
        json!(["Electronics", "Phone", 5000.0]),
        json!(["Electronics", "Laptop", 8000.0]),
        json!(["Electronics", "Tablet", 3000.0]),
        json!(["Electronics", "Watch", 2000.0]),
        json!(["Clothing", "Shirt", 1000.0]),
        json!(["Clothing", "Pants", 1500.0]),
        json!(["Clothing", "Jacket", 2500.0]),
        json!(["Clothing", "Shoes", 2000.0]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH ranked_products AS (
                SELECT
                    category,
                    product_name,
                    sales_amount,
                    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales_amount DESC) AS rank
                FROM product_sales
            )
            SELECT category, product_name, sales_amount, rank
            FROM ranked_products
            WHERE rank <= 2
            ORDER BY category, rank
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_olap_median_approximation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "response_times", vec![
        ("request_id", "INT64"),
        ("latency_ms", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "response_times", vec![
        json!([1, 50]), json!([2, 75]), json!([3, 100]), json!([4, 120]),
        json!([5, 150]), json!([6, 200]), json!([7, 250]), json!([8, 500]),
        json!([9, 45]), json!([10, 80]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH percentiles AS (
                SELECT
                    latency_ms,
                    PERCENT_RANK() OVER (ORDER BY latency_ms) AS pct_rank
                FROM response_times
            )
            SELECT
                MIN(latency_ms) AS min_latency,
                MAX(latency_ms) AS max_latency,
                AVG(latency_ms) AS avg_latency,
                (SELECT MIN(latency_ms) FROM percentiles WHERE pct_rank >= 0.5) AS approx_median,
                (SELECT MIN(latency_ms) FROM percentiles WHERE pct_rank >= 0.95) AS p95_latency
            FROM response_times
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_olap_growth_rate_calculation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "monthly_users", vec![
        ("month", "STRING"),
        ("user_count", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "monthly_users", vec![
        json!(["2024-01", 1000]),
        json!(["2024-02", 1200]),
        json!(["2024-03", 1500]),
        json!(["2024-04", 1400]),
        json!(["2024-05", 1800]),
        json!(["2024-06", 2200]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                month,
                user_count,
                LAG(user_count) OVER (ORDER BY month) AS prev_month_users,
                user_count - LAG(user_count) OVER (ORDER BY month) AS absolute_growth,
                SAFE_DIVIDE((user_count - LAG(user_count) OVER (ORDER BY month)) * 100.0,
                    LAG(user_count) OVER (ORDER BY month)) AS growth_rate_pct
            FROM monthly_users
            ORDER BY month
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);

    assert!(rows[0]["f"][4]["v"].is_null());
    let feb_growth = rows[1]["f"][4]["v"].as_f64().unwrap();
    assert_eq!(feb_growth, 20.0);
}

#[tokio::test]
async fn test_olap_market_basket_analysis() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "order_products", vec![
        ("order_id", "INT64"),
        ("product_id", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "order_products", vec![
        json!([1, 101]), json!([1, 102]), json!([1, 103]),
        json!([2, 101]), json!([2, 102]),
        json!([3, 101]), json!([3, 103]),
        json!([4, 102]), json!([4, 103]),
        json!([5, 101]), json!([5, 102]), json!([5, 103]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH product_pairs AS (
                SELECT
                    a.product_id AS product_a,
                    b.product_id AS product_b,
                    COUNT(DISTINCT a.order_id) AS co_occurrence
                FROM order_products a
                JOIN order_products b ON a.order_id = b.order_id AND a.product_id < b.product_id
                GROUP BY a.product_id, b.product_id
            ),
            product_counts AS (
                SELECT product_id, COUNT(DISTINCT order_id) AS order_count
                FROM order_products
                GROUP BY product_id
            )
            SELECT
                pp.product_a,
                pp.product_b,
                pp.co_occurrence,
                SAFE_DIVIDE(pp.co_occurrence * 100.0, pc.order_count) AS support_pct
            FROM product_pairs pp
            JOIN product_counts pc ON pp.product_a = pc.product_id
            ORDER BY co_occurrence DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert!(rows.len() >= 3);
}

#[tokio::test]
async fn test_olap_null_handling_comprehensive() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "nullable_data", vec![
        ("id", "INT64"),
        ("value_a", "INT64"),
        ("value_b", "INT64"),
        ("category", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "nullable_data", vec![
        json!([1, 10, 20, "A"]),
        json!([2, null, 30, "A"]),
        json!([3, 15, null, "B"]),
        json!([4, null, null, "B"]),
        json!([5, 25, 35, null]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                id,
                COALESCE(value_a, 0) AS value_a_safe,
                COALESCE(value_b, 0) AS value_b_safe,
                COALESCE(value_a, value_b, -1) AS first_non_null,
                IFNULL(category, 'Unknown') AS category_safe,
                CASE WHEN value_a IS NULL THEN 'Missing A'
                     WHEN value_b IS NULL THEN 'Missing B'
                     ELSE 'Complete'
                END AS completeness
            FROM nullable_data
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(parse_numeric(&rows[1]["f"][1]["v"]), 0);
    assert_eq!(rows[3]["f"][3]["v"].as_i64().unwrap(), -1);
}

#[tokio::test]
async fn test_olap_complex_case_nesting() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "customer_data", vec![
        ("customer_id", "INT64"),
        ("age", "INT64"),
        ("income", "FLOAT64"),
        ("years_customer", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "customer_data", vec![
        json!([1, 25, 35000.0, 1]),
        json!([2, 35, 75000.0, 5]),
        json!([3, 45, 120000.0, 10]),
        json!([4, 55, 90000.0, 3]),
        json!([5, 28, 50000.0, 2]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                customer_id,
                CASE
                    WHEN age < 30 THEN
                        CASE
                            WHEN income < 40000 THEN 'Young-Low'
                            WHEN income < 60000 THEN 'Young-Mid'
                            ELSE 'Young-High'
                        END
                    WHEN age < 50 THEN
                        CASE
                            WHEN income < 60000 THEN 'Adult-Low'
                            WHEN income < 100000 THEN 'Adult-Mid'
                            ELSE 'Adult-High'
                        END
                    ELSE
                        CASE
                            WHEN years_customer < 5 THEN 'Senior-New'
                            ELSE 'Senior-Loyal'
                        END
                END AS customer_segment
            FROM customer_data
            ORDER BY customer_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0]["f"][1]["v"].as_str().unwrap(), "Young-Low");
    assert_eq!(rows[2]["f"][1]["v"].as_str().unwrap(), "Adult-High");
}

#[tokio::test]
async fn test_olap_arithmetic_expressions() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "financial_data", vec![
        ("id", "INT64"),
        ("principal", "FLOAT64"),
        ("rate", "FLOAT64"),
        ("years", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "financial_data", vec![
        json!([1, 10000.0, 0.05, 5]),
        json!([2, 25000.0, 0.03, 10]),
        json!([3, 50000.0, 0.07, 3]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                id,
                principal,
                rate,
                years,
                principal * rate * years AS simple_interest,
                principal * (1 + rate * years) AS simple_future_value,
                principal + (principal * rate * years) AS total_with_simple,
                SAFE_DIVIDE(principal * rate * years, years) AS annual_interest
            FROM financial_data
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    let simple_interest_1 = rows[0]["f"][4]["v"].as_f64().unwrap();
    assert_eq!(simple_interest_1, 2500.0);
}

#[tokio::test]
async fn test_olap_union_with_different_sources() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "online_sales", vec![
        ("sale_id", "INT64"),
        ("amount", "FLOAT64"),
        ("sale_date", "STRING"),
    ]).await;

    setup_table(&methods, &session_id, "store_sales", vec![
        ("sale_id", "INT64"),
        ("amount", "FLOAT64"),
        ("sale_date", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "online_sales", vec![
        json!([1, 100.0, "2024-01-01"]),
        json!([2, 200.0, "2024-01-02"]),
        json!([3, 150.0, "2024-01-03"]),
    ]).await;

    insert_rows(&methods, &session_id, "store_sales", vec![
        json!([101, 300.0, "2024-01-01"]),
        json!([102, 250.0, "2024-01-02"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH all_sales AS (
                SELECT sale_id, amount, sale_date, 'Online' AS channel FROM online_sales
                UNION ALL
                SELECT sale_id, amount, sale_date, 'Store' AS channel FROM store_sales
            )
            SELECT
                channel,
                COUNT(*) AS num_sales,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount
            FROM all_sales
            GROUP BY channel
            ORDER BY total_amount DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_olap_except_intersect() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "set_a", vec![("id", "INT64"), ("value", "STRING")]).await;
    setup_table(&methods, &session_id, "set_b", vec![("id", "INT64"), ("value", "STRING")]).await;

    insert_rows(&methods, &session_id, "set_a", vec![
        json!([1, "Apple"]), json!([2, "Banana"]), json!([3, "Cherry"]), json!([4, "Date"]),
    ]).await;

    insert_rows(&methods, &session_id, "set_b", vec![
        json!([2, "Banana"]), json!([4, "Date"]), json!([5, "Elderberry"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT id, value FROM set_a
            EXCEPT
            SELECT id, value FROM set_b
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
    assert_eq!(parse_numeric(&rows[1]["f"][0]["v"]), 3);
}

#[tokio::test]
async fn test_olap_intersect_operation() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "list_x", vec![("id", "INT64")]).await;
    setup_table(&methods, &session_id, "list_y", vec![("id", "INT64")]).await;

    insert_rows(&methods, &session_id, "list_x", vec![
        json!([1]), json!([2]), json!([3]), json!([4]), json!([5]),
    ]).await;

    insert_rows(&methods, &session_id, "list_y", vec![
        json!([2]), json!([4]), json!([6]), json!([8]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT id FROM list_x
            INTERSECT
            SELECT id FROM list_y
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 2);
    assert_eq!(parse_numeric(&rows[1]["f"][0]["v"]), 4);
}

#[tokio::test]
async fn test_olap_cross_join_cartesian() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "sizes", vec![("size", "STRING")]).await;
    setup_table(&methods, &session_id, "colors", vec![("color", "STRING")]).await;

    insert_rows(&methods, &session_id, "sizes", vec![
        json!(["S"]), json!(["M"]), json!(["L"]),
    ]).await;

    insert_rows(&methods, &session_id, "colors", vec![
        json!(["Red"]), json!(["Blue"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT s.size, c.color, CONCAT(s.size, '-', c.color) AS sku
            FROM sizes s
            CROSS JOIN colors c
            ORDER BY s.size, c.color
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
}

#[tokio::test]
async fn test_olap_window_with_filter() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "sales_reps", vec![
        ("rep_id", "INT64"),
        ("region", "STRING"),
        ("sales_amount", "FLOAT64"),
        ("is_active", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "sales_reps", vec![
        json!([1, "North", 50000.0, 1]),
        json!([2, "North", 75000.0, 1]),
        json!([3, "North", 30000.0, 0]),
        json!([4, "South", 60000.0, 1]),
        json!([5, "South", 45000.0, 1]),
        json!([6, "South", 80000.0, 0]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                rep_id,
                region,
                sales_amount,
                is_active,
                RANK() OVER (PARTITION BY region ORDER BY sales_amount DESC) AS overall_rank,
                SUM(sales_amount) OVER (PARTITION BY region) AS region_total
            FROM sales_reps
            ORDER BY region, sales_amount DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
}

#[tokio::test]
async fn test_olap_dense_rank_with_ties() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "competition_scores", vec![
        ("participant_id", "INT64"),
        ("score", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "competition_scores", vec![
        json!([1, 100]),
        json!([2, 95]),
        json!([3, 95]),
        json!([4, 90]),
        json!([5, 90]),
        json!([6, 90]),
        json!([7, 85]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                participant_id,
                score,
                RANK() OVER (ORDER BY score DESC) AS rank,
                DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank,
                ROW_NUMBER() OVER (ORDER BY score DESC) AS row_num
            FROM competition_scores
            ORDER BY score DESC, participant_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 7);

    assert_eq!(parse_numeric(&rows[1]["f"][2]["v"]), 2);
    assert_eq!(parse_numeric(&rows[2]["f"][2]["v"]), 2);
    assert_eq!(parse_numeric(&rows[3]["f"][2]["v"]), 4);

    assert_eq!(parse_numeric(&rows[1]["f"][3]["v"]), 2);
    assert_eq!(parse_numeric(&rows[2]["f"][3]["v"]), 2);
    assert_eq!(parse_numeric(&rows[3]["f"][3]["v"]), 3);
}

#[tokio::test]
async fn test_olap_ratio_to_report() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "department_budget", vec![
        ("dept_name", "STRING"),
        ("budget", "FLOAT64"),
    ]).await;

    insert_rows(&methods, &session_id, "department_budget", vec![
        json!(["Engineering", 500000.0]),
        json!(["Sales", 300000.0]),
        json!(["Marketing", 200000.0]),
        json!(["HR", 100000.0]),
        json!(["Operations", 400000.0]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                dept_name,
                budget,
                SUM(budget) OVER () AS total_budget,
                SAFE_DIVIDE(budget * 100.0, SUM(budget) OVER ()) AS budget_pct,
                SUM(budget) OVER (ORDER BY budget DESC) AS cumulative_budget,
                SAFE_DIVIDE(SUM(budget) OVER (ORDER BY budget DESC) * 100.0, SUM(budget) OVER ()) AS cumulative_pct
            FROM department_budget
            ORDER BY budget DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);

    let total = rows[0]["f"][2]["v"].as_f64().unwrap();
    assert_eq!(total, 1500000.0);
}

#[tokio::test]
async fn test_olap_distinct_count_over_window() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "user_actions", vec![
        ("action_date", "STRING"),
        ("user_id", "INT64"),
        ("action_type", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "user_actions", vec![
        json!(["2024-01-01", 1, "view"]),
        json!(["2024-01-01", 2, "view"]),
        json!(["2024-01-01", 1, "click"]),
        json!(["2024-01-02", 1, "view"]),
        json!(["2024-01-02", 3, "view"]),
        json!(["2024-01-02", 2, "click"]),
        json!(["2024-01-03", 4, "view"]),
        json!(["2024-01-03", 1, "purchase"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH daily_metrics AS (
                SELECT
                    action_date,
                    COUNT(*) AS total_actions,
                    COUNT(DISTINCT user_id) AS unique_users,
                    COUNT(DISTINCT action_type) AS action_types
                FROM user_actions
                GROUP BY action_date
            )
            SELECT
                action_date,
                total_actions,
                unique_users,
                action_types,
                SUM(total_actions) OVER (ORDER BY action_date) AS cumulative_actions,
                AVG(unique_users) OVER (ORDER BY action_date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_users_2day
            FROM daily_metrics
            ORDER BY action_date
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_olap_expression_in_group_by() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "transactions", vec![
        ("txn_id", "INT64"),
        ("amount", "FLOAT64"),
        ("txn_timestamp", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "transactions", vec![
        json!([1, 100.0, 1704067200]),
        json!([2, 200.0, 1704153600]),
        json!([3, 150.0, 1704240000]),
        json!([4, 300.0, 1704326400]),
        json!([5, 250.0, 1704412800]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                CASE
                    WHEN amount < 150 THEN 'Small'
                    WHEN amount < 250 THEN 'Medium'
                    ELSE 'Large'
                END AS txn_size,
                COUNT(*) AS txn_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount
            FROM transactions
            GROUP BY
                CASE
                    WHEN amount < 150 THEN 'Small'
                    WHEN amount < 250 THEN 'Medium'
                    ELSE 'Large'
                END
            ORDER BY total_amount DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_olap_nested_aggregation_with_cte() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "store_visits", vec![
        ("store_id", "INT64"),
        ("visit_date", "STRING"),
        ("visitor_count", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "store_visits", vec![
        json!([1, "2024-01-01", 100]),
        json!([1, "2024-01-02", 120]),
        json!([1, "2024-01-03", 90]),
        json!([2, "2024-01-01", 80]),
        json!([2, "2024-01-02", 95]),
        json!([2, "2024-01-03", 110]),
        json!([3, "2024-01-01", 150]),
        json!([3, "2024-01-02", 140]),
        json!([3, "2024-01-03", 160]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH store_totals AS (
                SELECT
                    store_id,
                    SUM(visitor_count) AS total_visitors,
                    AVG(visitor_count) AS avg_daily_visitors
                FROM store_visits
                GROUP BY store_id
            ),
            store_stats AS (
                SELECT
                    AVG(total_visitors) AS avg_store_total,
                    MAX(total_visitors) AS max_store_total,
                    MIN(total_visitors) AS min_store_total
                FROM store_totals
            )
            SELECT
                st.store_id,
                st.total_visitors,
                st.avg_daily_visitors,
                ss.avg_store_total,
                st.total_visitors - ss.avg_store_total AS diff_from_avg,
                CASE WHEN st.total_visitors > ss.avg_store_total THEN 'Above Average' ELSE 'Below Average' END AS performance
            FROM store_totals st
            CROSS JOIN store_stats ss
            ORDER BY st.total_visitors DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
}

// =============================================================================
// ARRAY OPERATIONS TESTS - UNNEST, ARRAY_AGG, OFFSET, ORDINAL
// =============================================================================

#[tokio::test]
async fn test_array_agg_basic() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "tags_data", vec![
        ("item_id", "INT64"),
        ("tag", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "tags_data", vec![
        json!([1, "red"]),
        json!([1, "large"]),
        json!([1, "sale"]),
        json!([2, "blue"]),
        json!([2, "small"]),
        json!([3, "green"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                item_id,
                ARRAY_AGG(tag ORDER BY tag) AS tags
            FROM tags_data
            GROUP BY item_id
            ORDER BY item_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
}

#[tokio::test]
async fn test_array_agg_with_distinct() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "repeated_tags", vec![
        ("item_id", "INT64"),
        ("tag", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "repeated_tags", vec![
        json!([1, "red"]),
        json!([1, "red"]),
        json!([1, "blue"]),
        json!([1, "red"]),
        json!([2, "green"]),
        json!([2, "green"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                item_id,
                ARRAY_AGG(DISTINCT tag ORDER BY tag) AS unique_tags
            FROM repeated_tags
            GROUP BY item_id
            ORDER BY item_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_unnest_basic() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT UNNEST([1, 2, 3, 4, 5]) AS num
            ORDER BY 1
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
    assert_eq!(parse_numeric(&rows[4]["f"][0]["v"]), 5);
}

#[tokio::test]
async fn test_unnest_with_ordinality() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT element, idx
            FROM UNNEST(['a', 'b', 'c', 'd']) WITH ORDINALITY AS t(element, idx)
            ORDER BY idx
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "a");
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 1);
    assert_eq!(rows[3]["f"][0]["v"].as_str().unwrap(), "d");
    assert_eq!(parse_numeric(&rows[3]["f"][1]["v"]), 4);
}

#[tokio::test]
async fn test_unnest_join_with_table() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "products_with_sizes", vec![
        ("product_id", "INT64"),
        ("product_name", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "products_with_sizes", vec![
        json!([1, "T-Shirt"]),
        json!([2, "Pants"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT p.product_name, s AS size
            FROM products_with_sizes p
            CROSS JOIN UNNEST(['S', 'M', 'L', 'XL']) AS s
            ORDER BY p.product_id, s
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 8);
}

#[tokio::test]
async fn test_array_literal_in_select() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                [1, 2, 3] AS numbers,
                ['a', 'b', 'c'] AS letters
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_array_subscript_basic() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                [10, 20, 30, 40][2] AS second_element,
                ['a', 'b', 'c'][1] AS first_element
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 20);
    assert_eq!(rows[0]["f"][1]["v"].as_str().unwrap(), "a");
}

#[tokio::test]
async fn test_array_length_function() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                ARRAY_LENGTH([1, 2, 3, 4, 5]) AS len5,
                ARRAY_LENGTH(['a', 'b']) AS len2,
                ARRAY_LENGTH(ARRAY[]::INTEGER[]) AS len0
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 5);
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 2);
    assert_eq!(parse_numeric(&rows[0]["f"][2]["v"]), 0);
}

#[tokio::test]
async fn test_unnest_aggregation_roundtrip() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "order_items_array", vec![
        ("order_id", "INT64"),
        ("item", "STRING"),
    ]).await;

    insert_rows(&methods, &session_id, "order_items_array", vec![
        json!([1, "apple"]),
        json!([1, "banana"]),
        json!([1, "cherry"]),
        json!([2, "dog"]),
        json!([2, "elephant"]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH aggregated AS (
                SELECT order_id, ARRAY_AGG(item ORDER BY item) AS items
                FROM order_items_array
                GROUP BY order_id
            ),
            expanded AS (
                SELECT order_id, item
                FROM aggregated, UNNEST(items) AS item
            )
            SELECT order_id, COUNT(*) AS item_count
            FROM expanded
            GROUP BY order_id
            ORDER BY order_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 3);
    assert_eq!(parse_numeric(&rows[1]["f"][1]["v"]), 2);
}

#[tokio::test]
async fn test_unnest_multiple_arrays() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT a, b
            FROM UNNEST([1, 2, 3]) AS a,
                 UNNEST(['x', 'y']) AS b
            ORDER BY a, b
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
}

#[tokio::test]
async fn test_array_contains_pattern() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "items_to_check", vec![
        ("id", "INT64"),
        ("value", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "items_to_check", vec![
        json!([1, 5]),
        json!([2, 10]),
        json!([3, 15]),
        json!([4, 20]),
        json!([5, 25]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT id, value
            FROM items_to_check
            WHERE value IN (SELECT UNNEST([10, 20, 30]))
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 10);
    assert_eq!(parse_numeric(&rows[1]["f"][1]["v"]), 20);
}

#[tokio::test]
async fn test_array_in_cte() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH sample_data AS (
                SELECT 1 AS id, [100, 200, 300] AS values
                UNION ALL
                SELECT 2 AS id, [400, 500] AS values
            )
            SELECT id, v
            FROM sample_data, UNNEST(values) AS v
            ORDER BY id, v
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_array_agg_with_ordering() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "scored_items", vec![
        ("category", "STRING"),
        ("item", "STRING"),
        ("score", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "scored_items", vec![
        json!(["A", "item1", 30]),
        json!(["A", "item2", 10]),
        json!(["A", "item3", 20]),
        json!(["B", "item4", 50]),
        json!(["B", "item5", 40]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                category,
                ARRAY_AGG(item ORDER BY score DESC) AS items_by_score
            FROM scored_items
            GROUP BY category
            ORDER BY category
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_nested_array_operations() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "user_purchases", vec![
        ("user_id", "INT64"),
        ("product", "STRING"),
        ("amount", "FLOAT64"),
    ]).await;

    insert_rows(&methods, &session_id, "user_purchases", vec![
        json!([1, "A", 100.0]),
        json!([1, "B", 200.0]),
        json!([1, "C", 150.0]),
        json!([2, "A", 50.0]),
        json!([2, "D", 300.0]),
        json!([3, "B", 75.0]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH user_products AS (
                SELECT
                    user_id,
                    ARRAY_AGG(product ORDER BY product) AS products,
                    ARRAY_AGG(amount ORDER BY product) AS amounts
                FROM user_purchases
                GROUP BY user_id
            )
            SELECT
                user_id,
                products,
                amounts,
                LIST_SUM(amounts) AS total_amount
            FROM user_products
            ORDER BY user_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
}

#[tokio::test]
async fn test_unnest_with_ordinality_strings() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT element, idx
            FROM UNNEST(['first', 'second', 'third']) WITH ORDINALITY AS t(element, idx)
            ORDER BY idx
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "first");
}

#[tokio::test]
async fn test_array_agg_filter_nulls() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "nullable_values", vec![
        ("group_id", "INT64"),
        ("value", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "nullable_values", vec![
        json!([1, 10]),
        json!([1, null]),
        json!([1, 20]),
        json!([1, null]),
        json!([1, 30]),
        json!([2, null]),
        json!([2, 40]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                group_id,
                ARRAY_AGG(value ORDER BY value) AS all_values
            FROM nullable_values
            WHERE value IS NOT NULL
            GROUP BY group_id
            ORDER BY group_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_array_transformation_pipeline() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "raw_events", vec![
        ("session_id", "INT64"),
        ("event_type", "STRING"),
        ("timestamp", "INT64"),
    ]).await;

    insert_rows(&methods, &session_id, "raw_events", vec![
        json!([1, "click", 100]),
        json!([1, "view", 50]),
        json!([1, "purchase", 200]),
        json!([2, "click", 150]),
        json!([2, "view", 100]),
    ]).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH session_events AS (
                SELECT
                    session_id,
                    ARRAY_AGG(event_type ORDER BY timestamp) AS event_sequence,
                    COUNT(*) AS event_count
                FROM raw_events
                GROUP BY session_id
            ),
            flattened AS (
                SELECT
                    session_id,
                    event_count,
                    event_type,
                    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_type) AS event_order
                FROM session_events, UNNEST(event_sequence) AS event_type
            )
            SELECT
                session_id,
                event_count,
                STRING_AGG(event_type, ' -> ' ORDER BY event_order) AS event_flow
            FROM flattened
            GROUP BY session_id, event_count
            ORDER BY session_id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

// ============================================================================
// User-Defined Function (UDF) Tests
// ============================================================================

#[tokio::test]
async fn test_udf_simple_arithmetic() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
            RETURNS FLOAT64
            AS ((x + 4) / y)
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                val,
                AddFourAndDivide(val, 2) AS result
            FROM (
                SELECT UNNEST([2, 3, 5, 8]) AS val
            )
            ORDER BY val
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);

    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 2);
    assert!((parse_float(&rows[0]["f"][1]["v"]) - 3.0).abs() < 0.001);

    assert_eq!(parse_numeric(&rows[1]["f"][0]["v"]), 3);
    assert!((parse_float(&rows[1]["f"][1]["v"]) - 3.5).abs() < 0.001);

    assert_eq!(parse_numeric(&rows[2]["f"][0]["v"]), 5);
    assert!((parse_float(&rows[2]["f"][1]["v"]) - 4.5).abs() < 0.001);

    assert_eq!(parse_numeric(&rows[3]["f"][0]["v"]), 8);
    assert!((parse_float(&rows[3]["f"][1]["v"]) - 6.0).abs() < 0.001);
}

#[tokio::test]
async fn test_udf_with_safe_divide() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION SafeRatio(numerator FLOAT64, denominator FLOAT64)
            RETURNS FLOAT64
            AS (SAFE_DIVIDE(numerator, denominator))
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                SafeRatio(10.0, 2.0) AS normal_result,
                SafeRatio(10.0, 0.0) AS div_by_zero
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    assert!((parse_float(&rows[0]["f"][0]["v"]) - 5.0).abs() < 0.001);
    assert!(rows[0]["f"][1]["v"].is_null());
}

#[tokio::test]
async fn test_udf_string_operations() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION FormatName(first_name STRING, last_name STRING)
            RETURNS STRING
            AS (CONCAT(UPPER(first_name), ' ', UPPER(last_name)))
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT FormatName('john', 'doe') AS formatted_name
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["f"][0]["v"].as_str().unwrap(), "JOHN DOE");
}

#[tokio::test]
async fn test_udf_conditional_logic() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION GetGrade(score INT64)
            RETURNS STRING
            AS (
                CASE
                    WHEN score >= 90 THEN 'A'
                    WHEN score >= 80 THEN 'B'
                    WHEN score >= 70 THEN 'C'
                    WHEN score >= 60 THEN 'D'
                    ELSE 'F'
                END
            )
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                score,
                GetGrade(score) AS grade
            FROM (
                SELECT UNNEST([95, 85, 75, 65, 55]) AS score
            )
            ORDER BY score DESC
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);

    assert_eq!(rows[0]["f"][1]["v"].as_str().unwrap(), "A");
    assert_eq!(rows[1]["f"][1]["v"].as_str().unwrap(), "B");
    assert_eq!(rows[2]["f"][1]["v"].as_str().unwrap(), "C");
    assert_eq!(rows[3]["f"][1]["v"].as_str().unwrap(), "D");
    assert_eq!(rows[4]["f"][1]["v"].as_str().unwrap(), "F");
}

#[tokio::test]
async fn test_udf_with_table_data() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(&methods, &session_id, "employees_udf", vec![
        ("id", "INT64"),
        ("base_salary", "FLOAT64"),
        ("bonus_pct", "FLOAT64"),
    ]).await;

    insert_rows(&methods, &session_id, "employees_udf", vec![
        json!([1, 50000.0, 0.10]),
        json!([2, 75000.0, 0.15]),
        json!([3, 100000.0, 0.20]),
    ]).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION CalculateTotalCompensation(salary FLOAT64, bonus_pct FLOAT64)
            RETURNS FLOAT64
            AS (salary * (1 + bonus_pct))
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                id,
                base_salary,
                bonus_pct,
                CalculateTotalCompensation(base_salary, bonus_pct) AS total_comp
            FROM employees_udf
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    assert!((parse_float(&rows[0]["f"][3]["v"]) - 55000.0).abs() < 0.01);
    assert!((parse_float(&rows[1]["f"][3]["v"]) - 86250.0).abs() < 0.01);
    assert!((parse_float(&rows[2]["f"][3]["v"]) - 120000.0).abs() < 0.01);
}

#[tokio::test]
async fn test_udf_nested_calls() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION Double(x FLOAT64)
            RETURNS FLOAT64
            AS (x * 2)
        "#,
    )
    .await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION AddOne(x FLOAT64)
            RETURNS FLOAT64
            AS (x + 1)
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                val,
                Double(AddOne(val)) AS doubled_plus_one,
                AddOne(Double(val)) AS plus_one_doubled
            FROM (SELECT 5.0 AS val)
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    assert!((parse_float(&rows[0]["f"][1]["v"]) - 12.0).abs() < 0.001);
    assert!((parse_float(&rows[0]["f"][2]["v"]) - 11.0).abs() < 0.001);
}

#[tokio::test]
async fn test_udf_in_cte() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION Discount(price FLOAT64, discount_rate FLOAT64)
            RETURNS FLOAT64
            AS (price * (1 - discount_rate))
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            WITH products AS (
                SELECT 'A' AS name, 100.0 AS price
                UNION ALL
                SELECT 'B' AS name, 200.0 AS price
            ),
            discounted AS (
                SELECT
                    name,
                    price,
                    Discount(price, 0.1) AS discounted_price
                FROM products
            )
            SELECT * FROM discounted ORDER BY name
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    assert!((parse_float(&rows[0]["f"][2]["v"]) - 90.0).abs() < 0.001);
    assert!((parse_float(&rows[1]["f"][2]["v"]) - 180.0).abs() < 0.001);
}

#[tokio::test]
async fn test_udf_or_replace() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION Multiplier(x INT64)
            RETURNS INT64
            AS (x * 2)
        "#,
    )
    .await;

    let result1 = query(
        &methods,
        &session_id,
        r#"SELECT Multiplier(5) AS result"#,
    )
    .await;

    let rows1 = result1["rows"].as_array().unwrap();
    assert_eq!(parse_numeric(&rows1[0]["f"][0]["v"]), 10);

    query(
        &methods,
        &session_id,
        r#"
            CREATE OR REPLACE TEMP FUNCTION Multiplier(x INT64)
            RETURNS INT64
            AS (x * 3)
        "#,
    )
    .await;

    let result2 = query(
        &methods,
        &session_id,
        r#"SELECT Multiplier(5) AS result"#,
    )
    .await;

    let rows2 = result2["rows"].as_array().unwrap();
    assert_eq!(parse_numeric(&rows2[0]["f"][0]["v"]), 15);
}

#[tokio::test]
async fn test_udf_mathematical_functions() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    query(
        &methods,
        &session_id,
        r#"
            CREATE TEMP FUNCTION CircleArea(radius FLOAT64)
            RETURNS FLOAT64
            AS (3.14159 * radius * radius)
        "#,
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                r AS radius,
                CircleArea(r) AS area
            FROM (SELECT UNNEST([1.0, 2.0, 3.0]) AS r)
            ORDER BY r
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    assert!((parse_float(&rows[0]["f"][1]["v"]) - 3.14159).abs() < 0.001);
    assert!((parse_float(&rows[1]["f"][1]["v"]) - 12.56636).abs() < 0.001);
    assert!((parse_float(&rows[2]["f"][1]["v"]) - 28.27431).abs() < 0.001);
}

fn parse_float(value: &serde_json::Value) -> f64 {
    match value {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

// ============================================================================
// ML.PREDICT Tests
// ============================================================================

#[tokio::test]
async fn test_ml_predict_basic() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT * FROM ML.PREDICT('my_model', (
                SELECT 1 AS feature1, 2 AS feature2
            ))
        "#,
    )
    .await;

    let schema = result["schema"]["fields"].as_array().unwrap();
    assert_eq!(schema.len(), 3);
    assert_eq!(schema[0]["name"].as_str().unwrap(), "feature1");
    assert_eq!(schema[1]["name"].as_str().unwrap(), "feature2");
    assert_eq!(schema[2]["name"].as_str().unwrap(), "predicted_label");

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 2);
    assert!(rows[0]["f"][2]["v"].is_null());
}

#[tokio::test]
async fn test_ml_predict_multiple_rows() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT * FROM ML.PREDICT('my_model', (
                SELECT id, id * 10 AS value FROM (SELECT UNNEST([1,2,3]) AS id)
            ))
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][1]["v"]), 10);
    assert!(rows[0]["f"][2]["v"].is_null());

    assert_eq!(parse_numeric(&rows[1]["f"][0]["v"]), 2);
    assert_eq!(parse_numeric(&rows[1]["f"][1]["v"]), 20);
    assert!(rows[1]["f"][2]["v"].is_null());

    assert_eq!(parse_numeric(&rows[2]["f"][0]["v"]), 3);
    assert_eq!(parse_numeric(&rows[2]["f"][1]["v"]), 30);
    assert!(rows[2]["f"][2]["v"].is_null());
}

#[tokio::test]
async fn test_ml_predict_with_table() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    setup_table(
        &methods,
        &session_id,
        "ml_features",
        vec![("id", "INT64"), ("value", "FLOAT64")],
    )
    .await;

    insert_rows(
        &methods,
        &session_id,
        "ml_features",
        vec![json!([1, 10.5]), json!([2, 20.3])],
    )
    .await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT * FROM ML.PREDICT('my_model', (
                SELECT id, value FROM ml_features
            ))
            ORDER BY id
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
    assert!((parse_float(&rows[0]["f"][1]["v"]) - 10.5).abs() < 0.001);
    assert!(rows[0]["f"][2]["v"].is_null());
}

#[tokio::test]
async fn test_ml_predict_with_alias() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT
                p.id,
                p.predicted_label,
                CASE WHEN p.predicted_label IS NULL THEN 'unknown' ELSE 'predicted' END AS status
            FROM ML.PREDICT('classifier', (
                SELECT 1 AS id, 'test' AS text
            )) AS p
        "#,
    )
    .await;

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 1);
    assert!(rows[0]["f"][1]["v"].is_null());
    assert_eq!(rows[0]["f"][2]["v"].as_str().unwrap(), "unknown");
}

#[tokio::test]
async fn test_ml_predict_subquery_only() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        r#"
            SELECT * FROM ML.PREDICT((SELECT 100 AS x))
        "#,
    )
    .await;

    let schema = result["schema"]["fields"].as_array().unwrap();
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0]["name"].as_str().unwrap(), "x");
    assert_eq!(schema[1]["name"].as_str().unwrap(), "predicted_label");

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(parse_numeric(&rows[0]["f"][0]["v"]), 100);
    assert!(rows[0]["f"][1]["v"].is_null());
}

#[tokio::test]
async fn test_struct_basic() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = query(
        &methods,
        &session_id,
        "SELECT STRUCT(1 AS x, 2 AS y) AS point",
    )
    .await;

    let schema = result["schema"]["fields"].as_array().unwrap();
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0]["name"].as_str().unwrap(), "point");

    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
}
