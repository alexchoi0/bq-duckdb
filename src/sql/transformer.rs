use std::collections::HashSet;

use sqlparser::ast::*;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::dialect::{BigQueryDialect, DuckDbDialect};
use sqlparser::parser::Parser;

use crate::error::{Error, Result};

use super::functions::{FunctionTransform, FUNCTION_MAPPINGS};

pub fn transform_bq_to_duckdb(sql: &str, schema_name: &str) -> Result<String> {
    let dialect = BigQueryDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    let transformed: Vec<Statement> = statements
        .into_iter()
        .map(|stmt| transform_statement(stmt, schema_name, &HashSet::new()))
        .collect::<Result<Vec<_>>>()?;

    let _duck_dialect = DuckDbDialect {};
    let result = transformed
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    Ok(result)
}

fn transform_statement(stmt: Statement, schema_name: &str, cte_names: &HashSet<String>) -> Result<Statement> {
    match stmt {
        Statement::Query(query) => {
            let transformed = transform_query(*query, schema_name, cte_names)?;
            Ok(Statement::Query(Box::new(transformed)))
        }
        Statement::Insert(insert) => {
            let transformed = transform_insert(insert, schema_name, cte_names)?;
            Ok(Statement::Insert(transformed))
        }
        Statement::Update { .. } => Ok(stmt),
        Statement::Delete(_) => Ok(stmt),
        Statement::CreateTable(create) => {
            let transformed = transform_create_table(create, schema_name, cte_names)?;
            Ok(Statement::CreateTable(transformed))
        }
        Statement::CreateFunction(create_func) => {
            let transformed = transform_create_function(create_func, schema_name, cte_names)?;
            Ok(transformed)
        }
        _ => Ok(stmt),
    }
}

fn transform_query(query: Query, schema_name: &str, cte_names: &HashSet<String>) -> Result<Query> {
    let mut all_cte_names = cte_names.clone();

    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            all_cte_names.insert(cte.alias.name.value.clone());
        }
    }

    let transformed_with = query.with
        .map(|with| transform_with_clause(with, schema_name, &all_cte_names))
        .transpose()?;

    let body = transform_set_expr(*query.body, schema_name, &all_cte_names)?;

    Ok(Query {
        with: transformed_with,
        body: Box::new(body),
        ..query
    })
}

fn transform_with_clause(with: With, schema_name: &str, cte_names: &HashSet<String>) -> Result<With> {
    let cte_tables = with.cte_tables
        .into_iter()
        .map(|cte| transform_cte(cte, schema_name, cte_names))
        .collect::<Result<Vec<_>>>()?;

    Ok(With {
        cte_tables,
        ..with
    })
}

fn transform_cte(cte: Cte, schema_name: &str, cte_names: &HashSet<String>) -> Result<Cte> {
    let query = transform_query(*cte.query, schema_name, cte_names)?;

    Ok(Cte {
        query: Box::new(query),
        ..cte
    })
}

fn transform_set_expr(expr: SetExpr, schema_name: &str, cte_names: &HashSet<String>) -> Result<SetExpr> {
    match expr {
        SetExpr::Select(select) => {
            let transformed = transform_select(*select, schema_name, cte_names)?;
            Ok(SetExpr::Select(Box::new(transformed)))
        }
        SetExpr::Query(query) => {
            let transformed = transform_query(*query, schema_name, cte_names)?;
            Ok(SetExpr::Query(Box::new(transformed)))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left = transform_set_expr(*left, schema_name, cte_names)?;
            let right = transform_set_expr(*right, schema_name, cte_names)?;
            Ok(SetExpr::SetOperation {
                op,
                set_quantifier,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        other => Ok(other),
    }
}

fn transform_select(select: Select, schema_name: &str, cte_names: &HashSet<String>) -> Result<Select> {
    let projection = select
        .projection
        .into_iter()
        .map(|item| transform_select_item(item, schema_name, cte_names))
        .collect::<Result<Vec<_>>>()?;

    let from = select
        .from
        .into_iter()
        .map(|t| transform_table_with_joins(t, schema_name, cte_names))
        .collect::<Result<Vec<_>>>()?;

    let selection = select
        .selection
        .map(|e| transform_expr(e, schema_name, cte_names))
        .transpose()?;

    let group_by = transform_group_by(select.group_by, schema_name, cte_names)?;

    let having = select
        .having
        .map(|e| transform_expr(e, schema_name, cte_names))
        .transpose()?;

    Ok(Select {
        projection,
        from,
        selection,
        group_by,
        having,
        ..select
    })
}

fn transform_select_item(item: SelectItem, schema_name: &str, cte_names: &HashSet<String>) -> Result<SelectItem> {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            let transformed = transform_expr(expr, schema_name, cte_names)?;
            Ok(SelectItem::UnnamedExpr(transformed))
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            let transformed = transform_expr(expr, schema_name, cte_names)?;
            Ok(SelectItem::ExprWithAlias {
                expr: transformed,
                alias,
            })
        }
        other => Ok(other),
    }
}

fn transform_table_with_joins(twj: TableWithJoins, schema_name: &str, cte_names: &HashSet<String>) -> Result<TableWithJoins> {
    let relation = transform_table_factor(twj.relation, schema_name, cte_names)?;
    let joins = twj
        .joins
        .into_iter()
        .map(|j| transform_join(j, schema_name, cte_names))
        .collect::<Result<Vec<_>>>()?;

    Ok(TableWithJoins { relation, joins })
}

fn transform_table_factor(factor: TableFactor, schema_name: &str, cte_names: &HashSet<String>) -> Result<TableFactor> {
    match factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            partitions,
            with_ordinality,
            json_path,
        } => {
            if is_ml_predict(&name) {
                if let Some(TableFunctionArgs { args: func_args, .. }) = args {
                    return transform_ml_predict(func_args, alias, schema_name, cte_names);
                }
            }
            let transformed_name = transform_table_name(name, schema_name, cte_names);
            Ok(TableFactor::Table {
                name: transformed_name,
                alias,
                args,
                with_hints,
                version,
                partitions,
                with_ordinality,
                json_path,
            })
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            let transformed = transform_query(*subquery, schema_name, cte_names)?;
            Ok(TableFactor::Derived {
                lateral,
                subquery: Box::new(transformed),
                alias,
            })
        }
        TableFactor::UNNEST {
            alias,
            array_exprs,
            with_offset,
            with_offset_alias,
            with_ordinality,
        } => {
            let transformed_exprs = array_exprs
                .into_iter()
                .map(|e| transform_expr(e, schema_name, cte_names))
                .collect::<Result<Vec<_>>>()?;
            Ok(TableFactor::UNNEST {
                alias,
                array_exprs: transformed_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
            })
        }
        TableFactor::Function {
            lateral: _,
            name,
            args,
            alias,
        } => {
            if is_ml_predict(&name) {
                transform_ml_predict(args, alias, schema_name, cte_names)
            } else {
                Ok(TableFactor::Function {
                    lateral: false,
                    name,
                    args,
                    alias,
                })
            }
        }
        other => Ok(other),
    }
}

fn transform_join(join: Join, schema_name: &str, cte_names: &HashSet<String>) -> Result<Join> {
    let relation = transform_table_factor(join.relation, schema_name, cte_names)?;
    let join_operator = match join.join_operator {
        JoinOperator::Inner(constraint) => {
            JoinOperator::Inner(transform_join_constraint(constraint, schema_name, cte_names)?)
        }
        JoinOperator::LeftOuter(constraint) => {
            JoinOperator::LeftOuter(transform_join_constraint(constraint, schema_name, cte_names)?)
        }
        JoinOperator::RightOuter(constraint) => {
            JoinOperator::RightOuter(transform_join_constraint(constraint, schema_name, cte_names)?)
        }
        JoinOperator::FullOuter(constraint) => {
            JoinOperator::FullOuter(transform_join_constraint(constraint, schema_name, cte_names)?)
        }
        other => other,
    };

    Ok(Join {
        relation,
        join_operator,
        ..join
    })
}

fn transform_join_constraint(
    constraint: JoinConstraint,
    schema_name: &str,
    cte_names: &HashSet<String>,
) -> Result<JoinConstraint> {
    match constraint {
        JoinConstraint::On(expr) => {
            let transformed = transform_expr(expr, schema_name, cte_names)?;
            Ok(JoinConstraint::On(transformed))
        }
        other => Ok(other),
    }
}

fn transform_table_name(name: ObjectName, schema_name: &str, cte_names: &HashSet<String>) -> ObjectName {
    let parts: Vec<&str> = name.0.iter().map(|i| i.value.as_str()).collect();

    match parts.len() {
        1 => {
            let table = parts[0].trim_matches('`');
            if cte_names.contains(table) {
                return name;
            }
            ObjectName(vec![
                Ident::with_quote('"', schema_name),
                Ident::with_quote('"', table),
            ])
        }
        2 => {
            let table = parts[1].trim_matches('`');
            ObjectName(vec![
                Ident::with_quote('"', schema_name),
                Ident::with_quote('"', table),
            ])
        }
        3 => {
            let table = parts[2].trim_matches('`');
            ObjectName(vec![
                Ident::with_quote('"', schema_name),
                Ident::with_quote('"', table),
            ])
        }
        _ => name,
    }
}

fn transform_group_by(group_by: GroupByExpr, schema_name: &str, cte_names: &HashSet<String>) -> Result<GroupByExpr> {
    match group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            let transformed = exprs
                .into_iter()
                .map(|e| transform_expr(e, schema_name, cte_names))
                .collect::<Result<Vec<_>>>()?;
            Ok(GroupByExpr::Expressions(transformed, modifiers))
        }
        other => Ok(other),
    }
}

fn transform_expr(expr: Expr, schema_name: &str, cte_names: &HashSet<String>) -> Result<Expr> {
    match expr {
        Expr::Function(func) => transform_function(func, schema_name, cte_names),

        Expr::BinaryOp { left, op, right } => {
            let left = transform_expr(*left, schema_name, cte_names)?;
            let right = transform_expr(*right, schema_name, cte_names)?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }

        Expr::UnaryOp { op, expr } => {
            let transformed = transform_expr(*expr, schema_name, cte_names)?;
            Ok(Expr::UnaryOp {
                op,
                expr: Box::new(transformed),
            })
        }

        Expr::Nested(inner) => {
            let transformed = transform_expr(*inner, schema_name, cte_names)?;
            Ok(Expr::Nested(Box::new(transformed)))
        }

        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand = operand
                .map(|e| transform_expr(*e, schema_name, cte_names).map(Box::new))
                .transpose()?;
            let conditions = conditions
                .into_iter()
                .map(|c| transform_expr(c, schema_name, cte_names))
                .collect::<Result<Vec<_>>>()?;
            let results = results
                .into_iter()
                .map(|e| transform_expr(e, schema_name, cte_names))
                .collect::<Result<Vec<_>>>()?;
            let else_result = else_result
                .map(|e| transform_expr(*e, schema_name, cte_names).map(Box::new))
                .transpose()?;
            Ok(Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            })
        }

        Expr::Cast {
            expr,
            data_type,
            format,
            kind,
        } => {
            let transformed = transform_expr(*expr, schema_name, cte_names)?;
            let transformed_type = transform_data_type(data_type);
            let transformed_kind = match kind {
                CastKind::SafeCast => CastKind::TryCast,
                other => other,
            };
            Ok(Expr::Cast {
                expr: Box::new(transformed),
                data_type: transformed_type,
                format,
                kind: transformed_kind,
            })
        }

        Expr::Subquery(query) => {
            let transformed = transform_query(*query, schema_name, cte_names)?;
            Ok(Expr::Subquery(Box::new(transformed)))
        }

        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let transformed_expr = transform_expr(*expr, schema_name, cte_names)?;
            let transformed_query = transform_query(*subquery, schema_name, cte_names)?;
            Ok(Expr::InSubquery {
                expr: Box::new(transformed_expr),
                subquery: Box::new(transformed_query),
                negated,
            })
        }

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let transformed_expr = transform_expr(*expr, schema_name, cte_names)?;
            let transformed_list = list
                .into_iter()
                .map(|e| transform_expr(e, schema_name, cte_names))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(transformed_expr),
                list: transformed_list,
                negated,
            })
        }

        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let transformed_expr = transform_expr(*expr, schema_name, cte_names)?;
            let transformed_low = transform_expr(*low, schema_name, cte_names)?;
            let transformed_high = transform_expr(*high, schema_name, cte_names)?;
            Ok(Expr::Between {
                expr: Box::new(transformed_expr),
                negated,
                low: Box::new(transformed_low),
                high: Box::new(transformed_high),
            })
        }

        Expr::IsNull(inner) => {
            let transformed = transform_expr(*inner, schema_name, cte_names)?;
            Ok(Expr::IsNull(Box::new(transformed)))
        }

        Expr::IsNotNull(inner) => {
            let transformed = transform_expr(*inner, schema_name, cte_names)?;
            Ok(Expr::IsNotNull(Box::new(transformed)))
        }

        Expr::Struct { values, fields: _ } => {
            // Transform to DuckDB struct_pack function
            // STRUCT(1 AS x, 2 AS y) -> struct_pack(x := 1, y := 2)
            // The names are in values as Expr::Named { expr, name }
            let args: Vec<FunctionArg> = values
                .into_iter()
                .enumerate()
                .map(|(idx, val)| {
                    match val {
                        Expr::Named { expr, name } => {
                            let transformed = transform_expr(*expr, schema_name, cte_names)?;
                            Ok(FunctionArg::Named {
                                name,
                                arg: FunctionArgExpr::Expr(transformed),
                                operator: FunctionArgOperator::Assignment,
                            })
                        }
                        other => {
                            let transformed = transform_expr(other, schema_name, cte_names)?;
                            Ok(FunctionArg::Named {
                                name: Ident::new(format!("_{}", idx)),
                                arg: FunctionArgExpr::Expr(transformed),
                                operator: FunctionArgOperator::Assignment,
                            })
                        }
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let struct_pack = Function {
                name: ObjectName(vec![Ident::new("struct_pack")]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args,
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                over: None,
                within_group: vec![],
                filter: None,
                null_treatment: None,
                parameters: FunctionArguments::None,
                uses_odbc_syntax: false,
            };
            Ok(Expr::Function(struct_pack))
        }

        Expr::Array(arr) => {
            let transformed = transform_array(arr, schema_name, cte_names)?;
            Ok(Expr::Array(transformed))
        }

        Expr::Subscript { expr, subscript } => {
            let transformed_expr = transform_expr(*expr, schema_name, cte_names)?;
            let transformed_subscript = transform_subscript(*subscript, schema_name, cte_names)?;
            Ok(Expr::Subscript {
                expr: Box::new(transformed_expr),
                subscript: Box::new(transformed_subscript),
            })
        }

        Expr::MapAccess { column, keys } => {
            let transformed_column = transform_expr(*column, schema_name, cte_names)?;
            let transformed_keys = keys
                .into_iter()
                .map(|k| transform_map_access_key(k, schema_name, cte_names))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::MapAccess {
                column: Box::new(transformed_column),
                keys: transformed_keys,
            })
        }

        other => Ok(other),
    }
}

fn transform_subscript(subscript: Subscript, schema_name: &str, cte_names: &HashSet<String>) -> Result<Subscript> {
    match subscript {
        Subscript::Index { index } => {
            let transformed = transform_expr(index, schema_name, cte_names)?;
            let adjusted = transform_bq_array_index(transformed)?;
            Ok(Subscript::Index { index: adjusted })
        }
        Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        } => {
            let lower = lower_bound
                .map(|e| transform_expr(e, schema_name, cte_names))
                .transpose()?;
            let upper = upper_bound
                .map(|e| transform_expr(e, schema_name, cte_names))
                .transpose()?;
            let s = stride
                .map(|e| transform_expr(e, schema_name, cte_names))
                .transpose()?;
            Ok(Subscript::Slice {
                lower_bound: lower,
                upper_bound: upper,
                stride: s,
            })
        }
    }
}

fn transform_map_access_key(key: MapAccessKey, schema_name: &str, cte_names: &HashSet<String>) -> Result<MapAccessKey> {
    let transformed_key = transform_expr(key.key, schema_name, cte_names)?;
    Ok(MapAccessKey {
        key: transformed_key,
        syntax: key.syntax,
    })
}

fn transform_bq_array_index(expr: Expr) -> Result<Expr> {
    match &expr {
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_uppercase();
            match func_name.as_str() {
                "OFFSET" => {
                    let args = extract_args(func)?;
                    if args.len() != 1 {
                        return Err(Error::SqlTransform("OFFSET requires exactly 1 argument".to_string()));
                    }
                    Ok(args[0].clone())
                }
                "SAFE_OFFSET" => {
                    let args = extract_args(func)?;
                    if args.len() != 1 {
                        return Err(Error::SqlTransform("SAFE_OFFSET requires exactly 1 argument".to_string()));
                    }
                    Ok(args[0].clone())
                }
                "ORDINAL" => {
                    let args = extract_args(func)?;
                    if args.len() != 1 {
                        return Err(Error::SqlTransform("ORDINAL requires exactly 1 argument".to_string()));
                    }
                    Ok(Expr::BinaryOp {
                        left: Box::new(args[0].clone()),
                        op: BinaryOperator::Minus,
                        right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                    })
                }
                "SAFE_ORDINAL" => {
                    let args = extract_args(func)?;
                    if args.len() != 1 {
                        return Err(Error::SqlTransform("SAFE_ORDINAL requires exactly 1 argument".to_string()));
                    }
                    Ok(Expr::BinaryOp {
                        left: Box::new(args[0].clone()),
                        op: BinaryOperator::Minus,
                        right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                    })
                }
                _ => Ok(expr),
            }
        }
        _ => Ok(expr),
    }
}

fn transform_array(arr: Array, schema_name: &str, cte_names: &HashSet<String>) -> Result<Array> {
    let elem = arr
        .elem
        .into_iter()
        .map(|e| transform_expr(e, schema_name, cte_names))
        .collect::<Result<Vec<_>>>()?;
    Ok(Array {
        elem,
        named: arr.named,
    })
}

fn transform_function(func: Function, schema_name: &str, cte_names: &HashSet<String>) -> Result<Expr> {
    let func_name = func.name.to_string().to_uppercase();

    if let Some(mapping) = FUNCTION_MAPPINGS.get(func_name.as_str()) {
        return apply_function_transform(func, mapping.transform, mapping.duckdb_name, schema_name, cte_names);
    }

    let args = transform_function_args(func.args, schema_name, cte_names)?;

    Ok(Expr::Function(Function { args, ..func }))
}

fn transform_function_args(args: FunctionArguments, schema_name: &str, cte_names: &HashSet<String>) -> Result<FunctionArguments> {
    match args {
        FunctionArguments::List(arg_list) => {
            let transformed_args = arg_list
                .args
                .into_iter()
                .map(|arg| transform_function_arg(arg, schema_name, cte_names))
                .collect::<Result<Vec<_>>>()?;
            Ok(FunctionArguments::List(FunctionArgumentList {
                args: transformed_args,
                ..arg_list
            }))
        }
        other => Ok(other),
    }
}

fn transform_function_arg(arg: FunctionArg, schema_name: &str, cte_names: &HashSet<String>) -> Result<FunctionArg> {
    match arg {
        FunctionArg::Unnamed(arg_expr) => {
            let transformed = transform_function_arg_expr(arg_expr, schema_name, cte_names)?;
            Ok(FunctionArg::Unnamed(transformed))
        }
        FunctionArg::Named { name, arg, operator } => {
            let transformed = transform_function_arg_expr(arg, schema_name, cte_names)?;
            Ok(FunctionArg::Named {
                name,
                arg: transformed,
                operator,
            })
        }
        FunctionArg::ExprNamed { name, arg, operator } => {
            let transformed = transform_function_arg_expr(arg, schema_name, cte_names)?;
            Ok(FunctionArg::ExprNamed {
                name,
                arg: transformed,
                operator,
            })
        }
    }
}

fn transform_function_arg_expr(
    arg_expr: FunctionArgExpr,
    schema_name: &str,
    cte_names: &HashSet<String>,
) -> Result<FunctionArgExpr> {
    match arg_expr {
        FunctionArgExpr::Expr(expr) => {
            let transformed = transform_expr(expr, schema_name, cte_names)?;
            Ok(FunctionArgExpr::Expr(transformed))
        }
        other => Ok(other),
    }
}

fn apply_function_transform(
    func: Function,
    transform: FunctionTransform,
    duckdb_name: &str,
    schema_name: &str,
    cte_names: &HashSet<String>,
) -> Result<Expr> {
    let args = extract_args(&func)?;
    let transformed_args: Vec<Expr> = args
        .into_iter()
        .map(|e| transform_expr(e, schema_name, cte_names))
        .collect::<Result<Vec<_>>>()?;

    match transform {
        FunctionTransform::Rename => {
            let new_func = Function {
                name: ObjectName(vec![Ident::new(duckdb_name)]),
                args: build_function_args(transformed_args),
                ..func
            };
            Ok(Expr::Function(new_func))
        }

        FunctionTransform::SafeDivide => {
            if transformed_args.len() != 2 {
                return Err(Error::SqlTransform(
                    "SAFE_DIVIDE requires exactly 2 arguments".to_string(),
                ));
            }
            let a = transformed_args[0].clone();
            let b = transformed_args[1].clone();

            let nullif = Expr::Function(Function {
                name: ObjectName(vec![Ident::new("NULLIF")]),
                args: build_function_args(vec![b, Expr::Value(Value::Number("0".to_string(), false))]),
                over: None,
                within_group: vec![],
                filter: None,
                null_treatment: None,
                parameters: FunctionArguments::None,
                uses_odbc_syntax: false,
            });

            Ok(Expr::BinaryOp {
                left: Box::new(a),
                op: BinaryOperator::Divide,
                right: Box::new(nullif),
            })
        }

        FunctionTransform::TimestampDiff => {
            if transformed_args.len() != 3 {
                return Err(Error::SqlTransform(
                    "TIMESTAMP_DIFF requires exactly 3 arguments".to_string(),
                ));
            }
            let ts1 = transformed_args[0].clone();
            let ts2 = transformed_args[1].clone();
            let unit = extract_identifier(&transformed_args[2])?;

            let new_func = Function {
                name: ObjectName(vec![Ident::new("DATE_DIFF")]),
                args: build_function_args(vec![
                    Expr::Value(Value::SingleQuotedString(unit.to_lowercase())),
                    ts2,
                    ts1,
                ]),
                over: None,
                within_group: vec![],
                filter: None,
                null_treatment: None,
                parameters: FunctionArguments::None,
                uses_odbc_syntax: false,
            };
            Ok(Expr::Function(new_func))
        }

        FunctionTransform::TimestampAdd | FunctionTransform::TimestampSub => {
            if transformed_args.len() != 2 {
                return Err(Error::SqlTransform(
                    "TIMESTAMP_ADD/SUB requires exactly 2 arguments".to_string(),
                ));
            }
            let ts = transformed_args[0].clone();
            let interval = transformed_args[1].clone();

            let op = if matches!(transform, FunctionTransform::TimestampAdd) {
                BinaryOperator::Plus
            } else {
                BinaryOperator::Minus
            };

            Ok(Expr::BinaryOp {
                left: Box::new(ts),
                op,
                right: Box::new(interval),
            })
        }

        FunctionTransform::FormatTimestamp => {
            if transformed_args.len() != 2 {
                return Err(Error::SqlTransform(
                    "FORMAT_TIMESTAMP requires exactly 2 arguments".to_string(),
                ));
            }
            let format = transformed_args[0].clone();
            let ts = transformed_args[1].clone();

            let new_func = Function {
                name: ObjectName(vec![Ident::new("strftime")]),
                args: build_function_args(vec![ts, format]),
                over: None,
                within_group: vec![],
                filter: None,
                null_treatment: None,
                parameters: FunctionArguments::None,
                uses_odbc_syntax: false,
            };
            Ok(Expr::Function(new_func))
        }

        FunctionTransform::ParseTimestamp => {
            if transformed_args.len() != 2 {
                return Err(Error::SqlTransform(
                    "PARSE_TIMESTAMP requires exactly 2 arguments".to_string(),
                ));
            }
            let format = transformed_args[0].clone();
            let str_val = transformed_args[1].clone();

            let new_func = Function {
                name: ObjectName(vec![Ident::new("strptime")]),
                args: build_function_args(vec![str_val, format]),
                over: None,
                within_group: vec![],
                filter: None,
                null_treatment: None,
                parameters: FunctionArguments::None,
                uses_odbc_syntax: false,
            };
            Ok(Expr::Function(new_func))
        }

        FunctionTransform::SafeCast => {
            if transformed_args.len() != 1 {
                return Err(Error::SqlTransform(
                    "SAFE_CAST requires exactly 1 argument".to_string(),
                ));
            }
            let new_func = Function {
                name: ObjectName(vec![Ident::new("TRY_CAST")]),
                args: build_function_args(transformed_args),
                ..func
            };
            Ok(Expr::Function(new_func))
        }

        FunctionTransform::ArrayLength => {
            let new_func = Function {
                name: ObjectName(vec![Ident::new("LEN")]),
                args: build_function_args(transformed_args),
                over: None,
                within_group: vec![],
                filter: None,
                null_treatment: None,
                parameters: FunctionArguments::None,
                uses_odbc_syntax: false,
            };
            Ok(Expr::Function(new_func))
        }

        FunctionTransform::DatetimeCast => {
            if transformed_args.is_empty() {
                return Err(Error::SqlTransform(
                    "DATETIME requires at least 1 argument".to_string(),
                ));
            }
            let expr = transformed_args[0].clone();
            Ok(Expr::Cast {
                expr: Box::new(expr),
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                format: None,
                kind: CastKind::Cast,
            })
        }

        FunctionTransform::DateCast => {
            if transformed_args.is_empty() {
                return Err(Error::SqlTransform(
                    "DATE requires at least 1 argument".to_string(),
                ));
            }
            let expr = transformed_args[0].clone();
            Ok(Expr::Cast {
                expr: Box::new(expr),
                data_type: DataType::Date,
                format: None,
                kind: CastKind::Cast,
            })
        }

        FunctionTransform::Struct => {
            // STRUCT function is handled via Expr::Struct, not here
            // This is a fallback for any edge cases
            let new_func = Function {
                name: ObjectName(vec![Ident::new("struct_pack")]),
                args: func.args.clone(),
                over: func.over.clone(),
                within_group: func.within_group.clone(),
                filter: func.filter.clone(),
                null_treatment: func.null_treatment.clone(),
                parameters: func.parameters.clone(),
                uses_odbc_syntax: func.uses_odbc_syntax,
            };
            Ok(Expr::Function(new_func))
        }
    }
}

fn extract_args(func: &Function) -> Result<Vec<Expr>> {
    match &func.args {
        FunctionArguments::List(arg_list) => {
            arg_list
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
                    FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => Ok(e.clone()),
                    _ => Err(Error::SqlTransform("Unsupported function argument".to_string())),
                })
                .collect()
        }
        FunctionArguments::None => Ok(vec![]),
        FunctionArguments::Subquery(_) => {
            Err(Error::SqlTransform("Subquery arguments not supported".to_string()))
        }
    }
}

fn extract_identifier(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
        Expr::Value(Value::DoubleQuotedString(s)) => Ok(s.clone()),
        _ => Err(Error::SqlTransform(format!(
            "Expected identifier, got: {:?}",
            expr
        ))),
    }
}

fn build_function_args(exprs: Vec<Expr>) -> FunctionArguments {
    FunctionArguments::List(FunctionArgumentList {
        args: exprs
            .into_iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
            .collect(),
        duplicate_treatment: None,
        clauses: vec![],
    })
}

fn transform_data_type(dt: DataType) -> DataType {
    match dt {
        DataType::Int64 => DataType::BigInt(None),
        DataType::Float64 => DataType::Double,
        DataType::Bool => DataType::Boolean,
        DataType::String(_) => DataType::Varchar(None),
        DataType::Bytes(_) => DataType::Blob(None),
        other => other,
    }
}

fn transform_insert(insert: Insert, schema_name: &str, cte_names: &HashSet<String>) -> Result<Insert> {
    let table_name = transform_table_name(insert.table_name.clone(), schema_name, cte_names);
    let source = insert
        .source
        .map(|q| transform_query(*q, schema_name, cte_names).map(Box::new))
        .transpose()?;

    Ok(Insert {
        table_name,
        source,
        ..insert
    })
}

fn transform_create_table(create: CreateTable, schema_name: &str, cte_names: &HashSet<String>) -> Result<CreateTable> {
    let name = transform_table_name(create.name.clone(), schema_name, cte_names);

    Ok(CreateTable { name, ..create })
}

fn transform_create_function(
    create_func: CreateFunction,
    schema_name: &str,
    cte_names: &HashSet<String>,
) -> Result<Statement> {
    let func_body = create_func.function_body.ok_or_else(|| {
        Error::SqlTransform("CREATE FUNCTION requires a function body".to_string())
    })?;

    let body_expr = match func_body {
        CreateFunctionBody::AsBeforeOptions(expr) => expr,
        CreateFunctionBody::AsAfterOptions(expr) => expr,
        CreateFunctionBody::Return(expr) => expr,
    };

    let transformed_body = transform_expr(body_expr, schema_name, cte_names)?;

    let macro_name = transform_function_name(create_func.name, schema_name, create_func.temporary);

    let macro_args = create_func.args.map(|args| {
        args.into_iter()
            .map(|arg| MacroArg {
                name: arg.name.unwrap_or_else(|| Ident::new("arg")),
                default_expr: arg.default_expr,
            })
            .collect()
    });

    Ok(Statement::CreateMacro {
        or_replace: create_func.or_replace,
        temporary: create_func.temporary,
        name: macro_name,
        args: macro_args,
        definition: MacroDefinition::Expr(transformed_body),
    })
}

fn transform_function_name(name: ObjectName, schema_name: &str, is_temp: bool) -> ObjectName {
    let parts: Vec<&str> = name.0.iter().map(|i| i.value.as_str()).collect();

    match parts.len() {
        1 => {
            if is_temp {
                name
            } else {
                ObjectName(vec![
                    Ident::with_quote('"', schema_name),
                    Ident::new(parts[0]),
                ])
            }
        }
        2 => {
            ObjectName(vec![
                Ident::with_quote('"', schema_name),
                Ident::new(parts[1]),
            ])
        }
        _ => name,
    }
}

fn is_ml_predict(name: &ObjectName) -> bool {
    let parts: Vec<String> = name.0.iter().map(|i| i.value.to_uppercase()).collect();
    parts == vec!["ML", "PREDICT"]
}

fn transform_ml_predict(
    args: Vec<FunctionArg>,
    alias: Option<TableAlias>,
    schema_name: &str,
    cte_names: &HashSet<String>,
) -> Result<TableFactor> {
    let subquery = extract_ml_predict_subquery(&args)?;

    let transformed_subquery = transform_query(subquery, schema_name, cte_names)?;

    let inner_alias = TableAlias {
        name: Ident::new("__ml_input"),
        columns: vec![],
    };

    let inner_derived = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(transformed_subquery),
        alias: Some(inner_alias.clone()),
    };

    let wrapper_select = Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection: vec![
            SelectItem::QualifiedWildcard(
                ObjectName(vec![inner_alias.name.clone()]),
                WildcardAdditionalOptions::default(),
            ),
            SelectItem::ExprWithAlias {
                expr: Expr::Value(Value::Null),
                alias: Ident::new("predicted_label"),
            },
        ],
        into: None,
        from: vec![TableWithJoins {
            relation: inner_derived,
            joins: vec![],
        }],
        lateral_views: vec![],
        prewhere: None,
        selection: None,
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        window_before_qualify: false,
        qualify: None,
        value_table_mode: None,
        connect_by: None,
    };

    let wrapper_query = Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(wrapper_select))),
        order_by: None,
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    };

    Ok(TableFactor::Derived {
        lateral: false,
        subquery: Box::new(wrapper_query),
        alias,
    })
}

fn extract_ml_predict_subquery(args: &[FunctionArg]) -> Result<Query> {
    if args.is_empty() {
        return Err(Error::SqlTransform(
            "ML.PREDICT requires at least a subquery argument".to_string(),
        ));
    }

    fn extract_subquery_from_arg(arg: &FunctionArg) -> Option<Query> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(query))) => {
                Some((**query).clone())
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Nested(inner))) => {
                if let Expr::Subquery(query) = inner.as_ref() {
                    Some((**query).clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    for arg in args.iter().rev() {
        if let Some(query) = extract_subquery_from_arg(arg) {
            return Ok(query);
        }
    }

    Err(Error::SqlTransform(
        "ML.PREDICT requires a subquery argument".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transform(sql: &str) -> String {
        transform_bq_to_duckdb(sql, "test_schema").unwrap()
    }

    #[test]
    fn test_simple_select() {
        let result = transform("SELECT 1 + 1");
        assert_eq!(result, "SELECT 1 + 1");
    }

    #[test]
    fn test_select_from_table() {
        let result = transform("SELECT * FROM users");
        assert!(result.contains(r#""test_schema""#));
        assert!(result.contains(r#""users""#));
    }

    #[test]
    fn test_backtick_table_reference() {
        let result = transform("SELECT * FROM `my_table`");
        assert!(result.contains(r#""test_schema""#));
        assert!(result.contains(r#""my_table""#));
    }

    #[test]
    fn test_three_part_table_reference() {
        let result = transform("SELECT * FROM `project.dataset.table`");
        assert!(result.contains(r#""test_schema""#));
        assert!(result.contains(r#""table""#));
        assert!(!result.contains("project"));
        assert!(!result.contains("dataset"));
    }

    #[test]
    fn test_safe_divide() {
        let result = transform("SELECT SAFE_DIVIDE(a, b) FROM t");
        assert!(result.contains("NULLIF"));
        assert!(result.contains("0"));
        assert!(!result.contains("SAFE_DIVIDE"));
    }

    #[test]
    fn test_timestamp_diff() {
        let result = transform("SELECT TIMESTAMP_DIFF(t1, t2, DAY) FROM t");
        assert!(result.contains("DATE_DIFF"));
        assert!(result.contains("'day'"));
        assert!(!result.contains("TIMESTAMP_DIFF"));
    }

    #[test]
    fn test_any_value() {
        let result = transform("SELECT ANY_VALUE(x) FROM t GROUP BY y");
        assert!(result.contains("ARBITRARY"));
        assert!(!result.contains("ANY_VALUE"));
    }

    #[test]
    fn test_safe_cast() {
        let result = transform("SELECT SAFE_CAST(x AS INT64) FROM t");
        assert!(result.contains("TRY_CAST"));
        assert!(!result.contains("SAFE_CAST"));
    }

    #[test]
    fn test_ifnull() {
        let result = transform("SELECT IFNULL(a, b) FROM t");
        assert!(result.contains("COALESCE"));
        assert!(!result.contains("IFNULL"));
    }

    #[test]
    fn test_format_timestamp() {
        let result = transform("SELECT FORMAT_TIMESTAMP('%Y-%m-%d', ts) FROM t");
        assert!(result.contains("strftime"));
        assert!(!result.contains("FORMAT_TIMESTAMP"));
    }

    #[test]
    fn test_parse_timestamp() {
        let result = transform("SELECT PARSE_TIMESTAMP('%Y-%m-%d', s) FROM t");
        assert!(result.contains("strptime"));
        assert!(!result.contains("PARSE_TIMESTAMP"));
    }

    #[test]
    fn test_datetime_cast() {
        let result = transform("SELECT DATETIME(ts) FROM t");
        assert!(result.contains("CAST"));
        assert!(result.contains("TIMESTAMP"));
    }

    #[test]
    fn test_date_cast() {
        let result = transform("SELECT DATE(ts) FROM t");
        assert!(result.contains("CAST"));
        assert!(result.contains("DATE"));
    }

    #[test]
    fn test_array_length() {
        let result = transform("SELECT ARRAY_LENGTH(arr) FROM t");
        assert!(result.contains("LEN"));
        assert!(!result.contains("ARRAY_LENGTH"));
    }

    #[test]
    fn test_join() {
        let result = transform("SELECT * FROM a JOIN b ON a.id = b.id");
        assert!(result.contains("JOIN"));
        assert!(result.contains(r#""test_schema""#));
    }

    #[test]
    fn test_subquery() {
        let result = transform("SELECT * FROM (SELECT 1 AS x) AS sub");
        assert!(result.contains("SELECT 1 AS x"));
    }

    #[test]
    fn test_case_when() {
        let result = transform("SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t");
        assert!(result.contains("CASE"));
        assert!(result.contains("WHEN"));
        assert!(result.contains("THEN"));
        assert!(result.contains("ELSE"));
    }

    #[test]
    fn test_nested_function() {
        let result = transform("SELECT SAFE_DIVIDE(SAFE_DIVIDE(a, b), c) FROM t");
        assert!(!result.contains("SAFE_DIVIDE"));
        let nullif_count = result.matches("NULLIF").count();
        assert_eq!(nullif_count, 2);
    }

    #[test]
    fn test_type_conversion_int64() {
        let result = transform("SELECT CAST(x AS INT64) FROM t");
        assert!(result.contains("BIGINT"));
    }

    #[test]
    fn test_type_conversion_float64() {
        let result = transform("SELECT CAST(x AS FLOAT64) FROM t");
        assert!(result.contains("DOUBLE"));
    }

    #[test]
    fn test_type_conversion_string() {
        let result = transform("SELECT CAST(x AS STRING) FROM t");
        assert!(result.contains("VARCHAR"));
    }

    #[test]
    fn test_generate_uuid() {
        let result = transform("SELECT GENERATE_UUID()");
        assert!(result.contains("uuid"));
        assert!(!result.contains("GENERATE_UUID"));
    }

    #[test]
    fn test_complex_query() {
        let sql = r#"
            SELECT
                SAFE_DIVIDE(sum_val, count_val) AS avg_val,
                ANY_VALUE(name) AS sample_name,
                TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration
            FROM `project.dataset.metrics`
            WHERE DATE(created_at) = '2024-01-01'
            GROUP BY category
        "#;
        let result = transform(sql);

        assert!(!result.contains("SAFE_DIVIDE"));
        assert!(!result.contains("ANY_VALUE"));
        assert!(!result.contains("TIMESTAMP_DIFF"));
        assert!(result.contains("NULLIF"));
        assert!(result.contains("ARBITRARY"));
        assert!(result.contains("DATE_DIFF"));
        assert!(result.contains(r#""test_schema""#));
    }

    #[test]
    fn test_window_functions() {
        let result = transform(r#"
            SELECT
                user_id,
                amount,
                SUM(amount) OVER (PARTITION BY user_id ORDER BY created_at) AS running_total,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn,
                LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY created_at) AS prev_amount
            FROM orders
        "#);
        assert!(result.contains("OVER"));
        assert!(result.contains("PARTITION BY"));
        assert!(result.contains("ROW_NUMBER"));
        assert!(result.contains("LAG"));
    }

    #[test]
    fn test_cte_common_table_expression() {
        let result = transform(r#"
            WITH daily_totals AS (
                SELECT DATE(created_at) AS day, SUM(amount) AS total
                FROM orders
                GROUP BY DATE(created_at)
            ),
            weekly_avg AS (
                SELECT AVG(total) AS avg_daily
                FROM daily_totals
            )
            SELECT * FROM daily_totals, weekly_avg
        "#);
        assert!(result.contains("WITH"));
        assert!(result.contains("daily_totals"));
        assert!(result.contains("weekly_avg"));
    }

    #[test]
    fn test_union_all() {
        let result = transform(r#"
            SELECT id, name FROM customers
            UNION ALL
            SELECT id, name FROM suppliers
            UNION ALL
            SELECT id, name FROM partners
        "#);
        assert!(result.contains("UNION ALL"));
    }

    #[test]
    fn test_left_right_outer_join() {
        let result = transform(r#"
            SELECT o.id, c.name, p.product_name
            FROM orders o
            LEFT OUTER JOIN customers c ON o.customer_id = c.id
            RIGHT JOIN products p ON o.product_id = p.id
            FULL OUTER JOIN inventory i ON p.id = i.product_id
        "#);
        assert!(result.contains("LEFT"));
        assert!(result.contains("RIGHT"));
        assert!(result.contains("FULL"));
        assert!(result.contains("JOIN"));
    }

    #[test]
    fn test_exists_subquery() {
        let result = transform(r#"
            SELECT * FROM customers c
            WHERE EXISTS (
                SELECT 1 FROM orders o WHERE o.customer_id = c.id
            )
        "#);
        assert!(result.contains("EXISTS"));
    }

    #[test]
    fn test_not_exists_subquery() {
        let result = transform(r#"
            SELECT * FROM products p
            WHERE NOT EXISTS (
                SELECT 1 FROM order_items oi WHERE oi.product_id = p.id
            )
        "#);
        assert!(result.contains("NOT EXISTS"));
    }

    #[test]
    fn test_in_subquery() {
        let result = transform(r#"
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM active_sessions)
        "#);
        assert!(result.contains("IN"));
    }

    #[test]
    fn test_correlated_subquery() {
        let result = transform(r#"
            SELECT
                c.name,
                (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count
            FROM customers c
        "#);
        assert!(result.contains("SELECT COUNT"));
    }

    #[test]
    fn test_having_clause() {
        let result = transform(r#"
            SELECT category, COUNT(*) AS cnt, SUM(price) AS total
            FROM products
            GROUP BY category
            HAVING COUNT(*) > 10 AND SUM(price) > 1000
        "#);
        assert!(result.contains("HAVING"));
        assert!(result.contains("COUNT"));
    }

    #[test]
    fn test_order_by_with_nulls() {
        let result = transform(r#"
            SELECT * FROM items
            ORDER BY priority DESC NULLS LAST, name ASC NULLS FIRST
        "#);
        assert!(result.contains("ORDER BY"));
        assert!(result.contains("DESC"));
        assert!(result.contains("ASC"));
    }

    #[test]
    fn test_limit_offset() {
        let result = transform("SELECT * FROM users LIMIT 10 OFFSET 20");
        assert!(result.contains("LIMIT"));
        assert!(result.contains("OFFSET"));
    }

    #[test]
    fn test_distinct() {
        let result = transform("SELECT DISTINCT category, brand FROM products");
        assert!(result.contains("DISTINCT"));
    }

    #[test]
    fn test_distinct_on() {
        let result = transform(r#"
            SELECT DISTINCT user_id, MAX(created_at) AS last_login
            FROM logins
            GROUP BY user_id
        "#);
        assert!(result.contains("DISTINCT"));
        assert!(result.contains("MAX"));
    }

    #[test]
    fn test_between() {
        let result = transform(r#"
            SELECT * FROM events
            WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'
              AND amount BETWEEN 100 AND 500
        "#);
        assert!(result.contains("BETWEEN"));
    }

    #[test]
    fn test_like_patterns() {
        let result = transform(r#"
            SELECT * FROM users
            WHERE name LIKE 'John%'
              AND email NOT LIKE '%@spam.com'
        "#);
        assert!(result.contains("LIKE"));
        assert!(result.contains("NOT LIKE"));
    }

    #[test]
    fn test_is_null_is_not_null() {
        let result = transform(r#"
            SELECT * FROM contacts
            WHERE phone IS NOT NULL
              AND fax IS NULL
        "#);
        assert!(result.contains("IS NOT NULL"));
        assert!(result.contains("IS NULL"));
    }

    #[test]
    fn test_coalesce() {
        let result = transform(r#"
            SELECT COALESCE(nickname, first_name, 'Unknown') AS display_name
            FROM users
        "#);
        assert!(result.contains("COALESCE"));
    }

    #[test]
    fn test_nullif() {
        let result = transform(r#"
            SELECT NULLIF(status, 'inactive') AS active_status
            FROM users
        "#);
        assert!(result.contains("NULLIF"));
    }

    #[test]
    fn test_case_expression_searched() {
        let result = transform(r#"
            SELECT
                CASE
                    WHEN score >= 90 THEN 'A'
                    WHEN score >= 80 THEN 'B'
                    WHEN score >= 70 THEN 'C'
                    ELSE 'F'
                END AS grade
            FROM students
        "#);
        assert!(result.contains("CASE"));
        assert!(result.contains("WHEN"));
        assert!(result.contains("THEN"));
        assert!(result.contains("ELSE"));
        assert!(result.contains("END"));
    }

    #[test]
    fn test_case_expression_simple() {
        let result = transform(r#"
            SELECT
                CASE status
                    WHEN 'active' THEN 1
                    WHEN 'pending' THEN 2
                    WHEN 'inactive' THEN 0
                END AS status_code
            FROM users
        "#);
        assert!(result.contains("CASE"));
    }

    #[test]
    fn test_aggregate_functions() {
        let result = transform(r#"
            SELECT
                COUNT(*) AS total,
                COUNT(DISTINCT user_id) AS unique_users,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount,
                MIN(created_at) AS first_order,
                MAX(created_at) AS last_order
            FROM orders
        "#);
        assert!(result.contains("COUNT(*)"));
        assert!(result.contains("COUNT(DISTINCT"));
        assert!(result.contains("SUM"));
        assert!(result.contains("AVG"));
        assert!(result.contains("MIN"));
        assert!(result.contains("MAX"));
    }

    #[test]
    fn test_string_functions() {
        let result = transform(r#"
            SELECT
                UPPER(name) AS upper_name,
                LOWER(email) AS lower_email,
                LENGTH(description) AS desc_len,
                TRIM(notes) AS trimmed_notes,
                CONCAT(first_name, ' ', last_name) AS full_name,
                SUBSTR(phone, 1, 3) AS area_code
            FROM contacts
        "#);
        assert!(result.contains("UPPER"));
        assert!(result.contains("LOWER"));
        assert!(result.contains("LENGTH"));
        assert!(result.contains("TRIM"));
        assert!(result.contains("CONCAT"));
        assert!(result.contains("SUBSTR"));
    }

    #[test]
    fn test_regexp_contains() {
        let result = transform(r#"
            SELECT * FROM logs
            WHERE REGEXP_CONTAINS(message, r'error|warning|critical')
        "#);
        assert!(result.contains("regexp_matches"));
        assert!(!result.contains("REGEXP_CONTAINS"));
    }

    #[test]
    fn test_timestamp_functions() {
        let result = transform(r#"
            SELECT
                TIMESTAMP_ADD(created_at, INTERVAL 1 DAY) AS next_day,
                TIMESTAMP_SUB(expires_at, INTERVAL 1 HOUR) AS warning_time,
                TIMESTAMP_DIFF(end_time, start_time, MINUTE) AS duration_min
            FROM events
        "#);
        assert!(!result.contains("TIMESTAMP_ADD"));
        assert!(!result.contains("TIMESTAMP_SUB"));
        assert!(!result.contains("TIMESTAMP_DIFF"));
        assert!(result.contains("DATE_DIFF"));
    }

    #[test]
    fn test_date_functions() {
        let result = transform(r#"
            SELECT
                DATE(created_at) AS order_date,
                DATETIME(shipped_at) AS ship_datetime
            FROM orders
        "#);
        assert!(result.contains("CAST"));
        assert!(result.contains("DATE"));
        assert!(result.contains("TIMESTAMP"));
    }

    #[test]
    fn test_math_functions() {
        let result = transform(r#"
            SELECT
                ABS(balance) AS abs_balance,
                ROUND(price, 2) AS rounded_price,
                FLOOR(rating) AS floor_rating,
                CEIL(score) AS ceil_score,
                MOD(id, 10) AS bucket,
                POWER(base, exponent) AS result
            FROM calculations
        "#);
        assert!(result.contains("ABS"));
        assert!(result.contains("ROUND"));
        assert!(result.contains("FLOOR"));
        assert!(result.contains("CEIL"));
        assert!(result.contains("MOD"));
        assert!(result.contains("POWER"));
    }

    #[test]
    fn test_nested_safe_divide() {
        let result = transform(r#"
            SELECT
                SAFE_DIVIDE(
                    SAFE_DIVIDE(total_revenue, num_customers),
                    SAFE_DIVIDE(total_orders, num_days)
                ) AS revenue_per_customer_per_order_per_day
            FROM metrics
        "#);
        let nullif_count = result.matches("NULLIF").count();
        assert_eq!(nullif_count, 3);
        assert!(!result.contains("SAFE_DIVIDE"));
    }

    #[test]
    fn test_multiple_ctes_with_functions() {
        let result = transform(r#"
            WITH revenue AS (
                SELECT
                    customer_id,
                    SUM(amount) AS total_revenue
                FROM orders
                GROUP BY customer_id
            ),
            avg_revenue AS (
                SELECT AVG(total_revenue) AS avg_rev FROM revenue
            )
            SELECT
                r.customer_id,
                r.total_revenue,
                SAFE_DIVIDE(r.total_revenue, a.avg_rev) AS revenue_ratio
            FROM revenue r
            CROSS JOIN avg_revenue a
        "#);
        assert!(result.contains("WITH"));
        assert!(result.contains("NULLIF"));
        assert!(!result.contains("SAFE_DIVIDE"));
    }

    #[test]
    fn test_array_in_select() {
        let result = transform("SELECT [1, 2, 3] AS numbers");
        assert!(result.contains("[1, 2, 3]"));
    }

    #[test]
    fn test_cast_types() {
        let result = transform(r#"
            SELECT
                CAST(id AS STRING) AS id_str,
                CAST(price AS INT64) AS price_int,
                CAST(amount AS FLOAT64) AS amount_float,
                CAST(is_active AS BOOL) AS active
            FROM items
        "#);
        assert!(result.contains("VARCHAR"));
        assert!(result.contains("BIGINT"));
        assert!(result.contains("DOUBLE"));
        assert!(result.contains("BOOLEAN"));
    }

    #[test]
    fn test_safe_cast_types() {
        let result = transform(r#"
            SELECT
                SAFE_CAST(user_input AS INT64) AS parsed_int,
                SAFE_CAST(date_str AS DATE) AS parsed_date
            FROM raw_data
        "#);
        assert!(result.contains("TRY_CAST"));
        assert!(!result.contains("SAFE_CAST"));
    }

    #[test]
    fn test_timestamp_diff_various_units() {
        let result = transform(r#"
            SELECT
                TIMESTAMP_DIFF(t2, t1, MICROSECOND) AS diff_micro,
                TIMESTAMP_DIFF(t2, t1, MILLISECOND) AS diff_milli,
                TIMESTAMP_DIFF(t2, t1, SECOND) AS diff_sec,
                TIMESTAMP_DIFF(t2, t1, MINUTE) AS diff_min,
                TIMESTAMP_DIFF(t2, t1, HOUR) AS diff_hour,
                TIMESTAMP_DIFF(t2, t1, DAY) AS diff_day
            FROM timestamps
        "#);
        assert!(result.contains("'microsecond'"));
        assert!(result.contains("'millisecond'"));
        assert!(result.contains("'second'"));
        assert!(result.contains("'minute'"));
        assert!(result.contains("'hour'"));
        assert!(result.contains("'day'"));
    }

    #[test]
    fn test_complex_where_clause() {
        let result = transform(r#"
            SELECT * FROM orders
            WHERE (status = 'active' OR status = 'pending')
              AND amount > 100
              AND customer_id IN (SELECT id FROM premium_customers)
              AND created_at >= DATE('2024-01-01')
              AND NOT is_deleted
        "#);
        assert!(result.contains("AND"));
        assert!(result.contains("OR"));
        assert!(result.contains("IN"));
        assert!(result.contains("NOT"));
    }

    #[test]
    fn test_group_by_rollup() {
        let result = transform(r#"
            SELECT region, category, SUM(sales) AS total_sales
            FROM sales_data
            GROUP BY ROLLUP(region, category)
        "#);
        assert!(result.contains("ROLLUP"));
    }

    #[test]
    fn test_insert_select() {
        let result = transform(r#"
            INSERT INTO archive_orders
            SELECT * FROM orders WHERE created_at < '2023-01-01'
        "#);
        assert!(result.contains("INSERT INTO"));
        assert!(result.contains("SELECT"));
    }

    #[test]
    fn test_create_table_as_select() {
        let result = transform(r#"
            CREATE TABLE monthly_summary AS
            SELECT
                DATE_TRUNC('month', created_at) AS month,
                COUNT(*) AS order_count,
                SUM(amount) AS total_amount
            FROM orders
            GROUP BY 1
        "#);
        assert!(result.contains("CREATE TABLE"));
        assert!(result.contains("AS"));
        assert!(result.contains("SELECT"));
    }

    #[test]
    fn test_multiple_table_aliases() {
        let result = transform(r#"
            SELECT
                o.id AS order_id,
                c.name AS customer_name,
                p.name AS product_name,
                oi.quantity,
                oi.unit_price
            FROM orders o
            INNER JOIN customers c ON o.customer_id = c.id
            INNER JOIN order_items oi ON o.id = oi.order_id
            INNER JOIN products p ON oi.product_id = p.id
        "#);
        assert!(result.contains("JOIN"));
        assert!(result.contains("ON"));
    }

    #[test]
    fn test_cross_join() {
        let result = transform(r#"
            SELECT d.date, p.name
            FROM date_dimension d
            CROSS JOIN products p
        "#);
        assert!(result.contains("CROSS JOIN"));
    }

    #[test]
    fn test_self_join() {
        let result = transform(r#"
            SELECT e.name AS employee, m.name AS manager
            FROM employees e
            LEFT JOIN employees m ON e.manager_id = m.id
        "#);
        assert!(result.contains("LEFT JOIN"));
    }

    #[test]
    fn test_byte_length() {
        let result = transform("SELECT BYTE_LENGTH(data) AS size FROM blobs");
        assert!(result.contains("octet_length"));
        assert!(!result.contains("BYTE_LENGTH"));
    }

    #[test]
    fn test_char_length() {
        let result = transform("SELECT CHAR_LENGTH(name) AS name_len FROM users");
        assert!(result.contains("length"));
        assert!(!result.contains("CHAR_LENGTH"));
    }

    #[test]
    fn test_create_temp_function_simple() {
        let result = transform(r#"
            CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
            RETURNS FLOAT64
            AS ((x + 4) / y)
        "#);
        assert!(result.contains("CREATE TEMPORARY MACRO"));
        assert!(result.contains("AddFourAndDivide"));
        assert!(result.contains("(x + 4) / y"));
    }

    #[test]
    fn test_create_temp_function_with_safe_divide() {
        let result = transform(r#"
            CREATE TEMP FUNCTION SafeRatio(a FLOAT64, b FLOAT64)
            AS (SAFE_DIVIDE(a, b))
        "#);
        assert!(result.contains("CREATE TEMPORARY MACRO"));
        assert!(result.contains("SafeRatio"));
        assert!(result.contains("NULLIF"));
        assert!(!result.contains("SAFE_DIVIDE"));
    }

    #[test]
    fn test_create_function_in_schema() {
        let result = transform(r#"
            CREATE FUNCTION mydataset.MyFunc(x INT64)
            AS (x * 2)
        "#);
        assert!(result.contains("CREATE MACRO"));
        assert!(result.contains("test_schema"));
        assert!(result.contains("MyFunc"));
    }

    #[test]
    fn test_create_temp_function_no_types() {
        let result = transform(r#"
            CREATE TEMP FUNCTION addFourAndDivideAny(x FLOAT64, y FLOAT64)
            AS ((x + 4) / y)
        "#);
        assert!(result.contains("CREATE TEMPORARY MACRO"));
        assert!(result.contains("addFourAndDivideAny"));
    }

    #[test]
    fn test_create_or_replace_function() {
        let result = transform(r#"
            CREATE OR REPLACE TEMP FUNCTION MyFunc(x INT64)
            AS (x * 2)
        "#);
        assert!(result.contains("CREATE OR REPLACE TEMPORARY MACRO"));
        assert!(result.contains("MyFunc"));
    }

    #[test]
    fn test_ml_predict_string_model() {
        let result = transform(r#"
            SELECT * FROM ML.PREDICT('my_model', (
                SELECT 1 AS feature1, 2 AS feature2
            ))
        "#);
        assert!(result.contains("predicted_label"));
        assert!(result.contains("NULL"));
        assert!(!result.contains("ML.PREDICT"));
    }

    #[test]
    fn test_ml_predict_with_alias() {
        let result = transform(r#"
            SELECT p.feature1, p.predicted_label
            FROM ML.PREDICT('my_model', (
                SELECT 1 AS feature1
            )) AS p
        "#);
        assert!(result.contains("predicted_label"));
        assert!(result.contains("p.feature1"));
    }

    #[test]
    fn test_ml_predict_subquery_only() {
        let result = transform(r#"
            SELECT * FROM ML.PREDICT((SELECT 100 AS x))
        "#);
        assert!(result.contains("predicted_label"));
        assert!(result.contains("100"));
    }

    #[test]
    fn test_struct_basic() {
        let result = transform("SELECT STRUCT(1 AS x, 2 AS y) AS point");
        println!("STRUCT result: {}", result);
        assert!(result.contains("1"));
        assert!(result.contains("2"));
    }
}
