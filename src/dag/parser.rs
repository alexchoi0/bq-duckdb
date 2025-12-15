use std::collections::HashSet;

use sqlparser::ast::*;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;

use crate::error::Result;

pub fn extract_dependencies(sql: &str) -> Result<Vec<String>> {
    let dialect = BigQueryDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    let mut deps = HashSet::new();
    let mut cte_names = HashSet::new();

    for stmt in &statements {
        collect_cte_names(stmt, &mut cte_names);
        collect_table_refs(stmt, &cte_names, &mut deps);
    }

    Ok(deps.into_iter().collect())
}

fn collect_cte_names(stmt: &Statement, cte_names: &mut HashSet<String>) {
    if let Statement::Query(query) = stmt {
        collect_cte_names_from_query(query, cte_names);
    }
}

fn collect_cte_names_from_query(query: &Query, cte_names: &mut HashSet<String>) {
    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            cte_names.insert(cte.alias.name.value.clone());
            collect_cte_names_from_query(&cte.query, cte_names);
        }
    }
}

fn collect_table_refs(stmt: &Statement, cte_names: &HashSet<String>, deps: &mut HashSet<String>) {
    match stmt {
        Statement::Query(query) => collect_table_refs_from_query(query, cte_names, deps),
        Statement::Insert(insert) => {
            if let Some(ref source) = insert.source {
                collect_table_refs_from_query(source, cte_names, deps);
            }
        }
        Statement::CreateTable(create) => {
            if let Some(ref query) = create.query {
                collect_table_refs_from_query(query, cte_names, deps);
            }
        }
        _ => {}
    }
}

fn collect_table_refs_from_query(query: &Query, cte_names: &HashSet<String>, deps: &mut HashSet<String>) {
    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            collect_table_refs_from_query(&cte.query, cte_names, deps);
        }
    }
    collect_table_refs_from_set_expr(&query.body, cte_names, deps);
}

fn collect_table_refs_from_set_expr(expr: &SetExpr, cte_names: &HashSet<String>, deps: &mut HashSet<String>) {
    match expr {
        SetExpr::Select(select) => collect_table_refs_from_select(select, cte_names, deps),
        SetExpr::Query(query) => collect_table_refs_from_query(query, cte_names, deps),
        SetExpr::SetOperation { left, right, .. } => {
            collect_table_refs_from_set_expr(left, cte_names, deps);
            collect_table_refs_from_set_expr(right, cte_names, deps);
        }
        SetExpr::Values(_) => {}
        SetExpr::Insert(_) => {}
        SetExpr::Update(_) => {}
        SetExpr::Table(_) => {}
    }
}

fn collect_table_refs_from_select(select: &Select, cte_names: &HashSet<String>, deps: &mut HashSet<String>) {
    for table_with_joins in &select.from {
        collect_table_refs_from_table_factor(&table_with_joins.relation, cte_names, deps);
        for join in &table_with_joins.joins {
            collect_table_refs_from_table_factor(&join.relation, cte_names, deps);
        }
    }

    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            collect_table_refs_from_expr(expr, cte_names, deps);
        }
    }

    if let Some(ref selection) = select.selection {
        collect_table_refs_from_expr(selection, cte_names, deps);
    }

    if let Some(ref having) = select.having {
        collect_table_refs_from_expr(having, cte_names, deps);
    }
}

fn collect_table_refs_from_table_factor(factor: &TableFactor, cte_names: &HashSet<String>, deps: &mut HashSet<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            let table_name = extract_table_name(name);
            if !cte_names.contains(&table_name) {
                deps.insert(table_name);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_table_refs_from_query(subquery, cte_names, deps);
        }
        TableFactor::UNNEST { array_exprs, .. } => {
            for expr in array_exprs {
                collect_table_refs_from_expr(expr, cte_names, deps);
            }
        }
        TableFactor::TableFunction { expr, .. } => {
            collect_table_refs_from_expr(expr, cte_names, deps);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            collect_table_refs_from_table_factor(&table_with_joins.relation, cte_names, deps);
            for join in &table_with_joins.joins {
                collect_table_refs_from_table_factor(&join.relation, cte_names, deps);
            }
        }
        _ => {}
    }
}

fn collect_table_refs_from_expr(expr: &Expr, cte_names: &HashSet<String>, deps: &mut HashSet<String>) {
    match expr {
        Expr::Subquery(query) => collect_table_refs_from_query(query, cte_names, deps),
        Expr::InSubquery { expr, subquery, .. } => {
            collect_table_refs_from_expr(expr, cte_names, deps);
            collect_table_refs_from_query(subquery, cte_names, deps);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_table_refs_from_expr(left, cte_names, deps);
            collect_table_refs_from_expr(right, cte_names, deps);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_table_refs_from_expr(expr, cte_names, deps);
        }
        Expr::Nested(inner) => {
            collect_table_refs_from_expr(inner, cte_names, deps);
        }
        Expr::Function(func) => {
            if let FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } = arg
                    {
                        collect_table_refs_from_expr(e, cte_names, deps);
                    }
                }
            }
        }
        Expr::Case { operand, conditions, results, else_result } => {
            if let Some(op) = operand {
                collect_table_refs_from_expr(op, cte_names, deps);
            }
            for cond in conditions {
                collect_table_refs_from_expr(cond, cte_names, deps);
            }
            for res in results {
                collect_table_refs_from_expr(res, cte_names, deps);
            }
            if let Some(else_r) = else_result {
                collect_table_refs_from_expr(else_r, cte_names, deps);
            }
        }
        Expr::Cast { expr, .. } => {
            collect_table_refs_from_expr(expr, cte_names, deps);
        }
        Expr::InList { expr, list, .. } => {
            collect_table_refs_from_expr(expr, cte_names, deps);
            for item in list {
                collect_table_refs_from_expr(item, cte_names, deps);
            }
        }
        Expr::Between { expr, low, high, .. } => {
            collect_table_refs_from_expr(expr, cte_names, deps);
            collect_table_refs_from_expr(low, cte_names, deps);
            collect_table_refs_from_expr(high, cte_names, deps);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_table_refs_from_expr(inner, cte_names, deps);
        }
        _ => {}
    }
}

fn extract_table_name(name: &ObjectName) -> String {
    let parts: Vec<&str> = name.0.iter().map(|i| i.value.as_str()).collect();
    match parts.len() {
        1 => parts[0].trim_matches('`').to_string(),
        2 => parts[1].trim_matches('`').to_string(),
        3 => parts[2].trim_matches('`').to_string(),
        _ => parts.last().map(|s| s.trim_matches('`').to_string()).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deps(sql: &str) -> Vec<String> {
        let mut result = extract_dependencies(sql).unwrap();
        result.sort();
        result
    }

    #[test]
    fn test_simple_select() {
        assert_eq!(deps("SELECT * FROM users"), vec!["users"]);
    }

    #[test]
    fn test_multiple_tables() {
        let result = deps("SELECT * FROM users, orders");
        assert!(result.contains(&"users".to_string()));
        assert!(result.contains(&"orders".to_string()));
    }

    #[test]
    fn test_join() {
        let result = deps("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
        assert!(result.contains(&"users".to_string()));
        assert!(result.contains(&"orders".to_string()));
    }

    #[test]
    fn test_subquery() {
        let result = deps("SELECT * FROM (SELECT * FROM users) AS sub");
        assert_eq!(result, vec!["users"]);
    }

    #[test]
    fn test_cte_not_included() {
        let result = deps("WITH cte AS (SELECT * FROM users) SELECT * FROM cte");
        assert_eq!(result, vec!["users"]);
    }

    #[test]
    fn test_multiple_ctes() {
        let result = deps(r#"
            WITH
                cte1 AS (SELECT * FROM users),
                cte2 AS (SELECT * FROM orders)
            SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.user_id
        "#);
        assert!(result.contains(&"users".to_string()));
        assert!(result.contains(&"orders".to_string()));
        assert!(!result.contains(&"cte1".to_string()));
        assert!(!result.contains(&"cte2".to_string()));
    }

    #[test]
    fn test_three_part_name() {
        let result = deps("SELECT * FROM `project.dataset.table`");
        assert_eq!(result, vec!["table"]);
    }

    #[test]
    fn test_in_subquery() {
        let result = deps("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
        assert!(result.contains(&"users".to_string()));
        assert!(result.contains(&"orders".to_string()));
    }

    #[test]
    fn test_union() {
        let result = deps("SELECT * FROM users UNION ALL SELECT * FROM admins");
        assert!(result.contains(&"users".to_string()));
        assert!(result.contains(&"admins".to_string()));
    }

    #[test]
    fn test_no_dependencies() {
        let result = deps("SELECT 1 AS num, 'hello' AS greeting");
        assert!(result.is_empty());
    }
}
