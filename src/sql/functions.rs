use std::collections::HashMap;
use std::sync::LazyLock;

#[derive(Clone)]
pub struct FunctionMapping {
    pub duckdb_name: &'static str,
    pub transform: FunctionTransform,
}

#[derive(Clone, Copy)]
pub enum FunctionTransform {
    Rename,
    SafeDivide,
    TimestampDiff,
    TimestampAdd,
    TimestampSub,
    FormatTimestamp,
    ParseTimestamp,
    SafeCast,
    ArrayLength,
    DatetimeCast,
    DateCast,
    Struct,
}

pub static FUNCTION_MAPPINGS: LazyLock<HashMap<&'static str, FunctionMapping>> = LazyLock::new(|| {
    let mut m = HashMap::new();

    m.insert(
        "SAFE_DIVIDE",
        FunctionMapping {
            duckdb_name: "",
            transform: FunctionTransform::SafeDivide,
        },
    );

    m.insert(
        "TIMESTAMP_DIFF",
        FunctionMapping {
            duckdb_name: "DATE_DIFF",
            transform: FunctionTransform::TimestampDiff,
        },
    );

    m.insert(
        "TIMESTAMP_ADD",
        FunctionMapping {
            duckdb_name: "",
            transform: FunctionTransform::TimestampAdd,
        },
    );

    m.insert(
        "TIMESTAMP_SUB",
        FunctionMapping {
            duckdb_name: "",
            transform: FunctionTransform::TimestampSub,
        },
    );

    m.insert(
        "FORMAT_TIMESTAMP",
        FunctionMapping {
            duckdb_name: "strftime",
            transform: FunctionTransform::FormatTimestamp,
        },
    );

    m.insert(
        "PARSE_TIMESTAMP",
        FunctionMapping {
            duckdb_name: "strptime",
            transform: FunctionTransform::ParseTimestamp,
        },
    );

    m.insert(
        "SAFE_CAST",
        FunctionMapping {
            duckdb_name: "TRY_CAST",
            transform: FunctionTransform::SafeCast,
        },
    );

    m.insert(
        "ANY_VALUE",
        FunctionMapping {
            duckdb_name: "ARBITRARY",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "ARRAY_LENGTH",
        FunctionMapping {
            duckdb_name: "LEN",
            transform: FunctionTransform::ArrayLength,
        },
    );

    m.insert(
        "DATETIME",
        FunctionMapping {
            duckdb_name: "",
            transform: FunctionTransform::DatetimeCast,
        },
    );

    m.insert(
        "DATE",
        FunctionMapping {
            duckdb_name: "",
            transform: FunctionTransform::DateCast,
        },
    );

    m.insert(
        "IFNULL",
        FunctionMapping {
            duckdb_name: "COALESCE",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "GENERATE_UUID",
        FunctionMapping {
            duckdb_name: "uuid",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "BYTE_LENGTH",
        FunctionMapping {
            duckdb_name: "octet_length",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "CHAR_LENGTH",
        FunctionMapping {
            duckdb_name: "length",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "REGEXP_CONTAINS",
        FunctionMapping {
            duckdb_name: "regexp_matches",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "REGEXP_EXTRACT",
        FunctionMapping {
            duckdb_name: "regexp_extract",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "REGEXP_REPLACE",
        FunctionMapping {
            duckdb_name: "regexp_replace",
            transform: FunctionTransform::Rename,
        },
    );

    m.insert(
        "STRUCT",
        FunctionMapping {
            duckdb_name: "struct_pack",
            transform: FunctionTransform::Struct,
        },
    );

    m
});
