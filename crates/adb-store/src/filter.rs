//! Payload predicates that run inside Lance (stage D3.6).
//!
//! Lance's `json_get_string` / `json_get_float` error on a row whose field has another
//! JSON type (an object, an array, a string where a number is asked for), which would
//! fail the whole scan. `adb_json_scalar` never errors: it returns a top-level scalar
//! field as text and NULL for anything else, and callers compare with that text or
//! `try_cast` it. Pushed-down filters are a superset of the backend's typed comparison
//! (the string "7" matches the number 7); the read path re-checks every row.

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, LargeBinaryArray, StringArray};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, ScalarUDF, TryCast, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::{col, create_udf, lit};

/// `adb_json_scalar(value, key)`: the top-level field `key` of the JSONB `value` as
/// text (strings unquoted, numbers and booleans in JSON form), or NULL when the field
/// is missing, null, an object or an array.
pub fn json_scalar_udf() -> ScalarUDF {
    create_udf(
        "adb_json_scalar",
        vec![DataType::LargeBinary, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(json_scalar),
    )
}

fn json_scalar(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let n = args.iter().find_map(|a| match a {
        ColumnarValue::Array(a) => Some(a.len()),
        _ => None,
    });
    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|a| a.clone().into_array(n.unwrap_or(1)))
        .collect::<Result<_>>()?;
    let [values, keys] = arrays.as_slice() else {
        return Err(DataFusionError::Execution("adb_json_scalar takes 2 arguments".into()));
    };
    let values = values
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| DataFusionError::Execution("adb_json_scalar: value must be LargeBinary".into()))?;
    let keys = keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("adb_json_scalar: key must be Utf8".into()))?;
    let mut out = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for i in 0..values.len() {
        let k = if keys.len() == 1 { 0 } else { i };
        if values.is_null(i) || keys.is_null(k) {
            out.append_null();
            continue;
        }
        match scalar_text(values.value(i), keys.value(k)) {
            Some(s) => out.append_value(s),
            None => out.append_null(),
        }
    }
    let result: ArrayRef = Arc::new(out.finish());
    Ok(ColumnarValue::Array(result))
}

fn scalar_text(bytes: &[u8], key: &str) -> Option<String> {
    let raw = jsonb::RawJsonb::new(bytes);
    if !raw.is_object().ok()? {
        return None;
    }
    let field = raw.get_by_name(key, false).ok()??;
    let field = field.as_raw();
    if field.is_string().ok()? {
        field.as_str().ok()?.map(|s| s.into_owned())
    } else if field.is_number().ok()? || field.is_boolean().ok()? {
        Some(field.to_string())
    } else {
        None
    }
}

/// The payload field `key` as text: `field_text("campaign").eq(lit("summer"))`.
pub fn field_text(key: &str) -> Expr {
    json_scalar_udf().call(vec![col("value"), lit(key.to_string())])
}

/// The payload field `key` as a number, NULL when it isn't one (or a numeric string).
pub fn field_num(key: &str) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(field_text(key)), DataType::Float64))
}

/// Integers whose f64 is exact and keeps order against every JSON number (|i| < 2^53).
const EXACT_INT: i64 = 1 << 53;

/// A Lance filter for `predicate` over `memory`'s rows, or `None` for "no pushdown".
/// The result matches a superset of what the backends' query code matches: conditions
/// it can't express become TRUE, combined left to right exactly as
/// `evaluate_conditions_on_record` does, and the backend still filters the rows it
/// returns. Record ids compare as row ids (`rows::row_id`): within one memory type
/// the prefix is common, so equality and order are the keys'.
pub fn predicate_filter(memory: adb_core::log::Memory, predicate: &adb_core::Predicate) -> Option<Expr> {
    use adb_core::Predicate;
    match predicate {
        Predicate::Where { conditions } => conditions_filter(memory, conditions),
        // The backends match KEY id / KEY key on the record id, other KEY fields on
        // the stringified payload value (no pushdown).
        Predicate::Key { field, value } if field == "id" || field == "key" => {
            key_text(value).map(|v| col("lineage_id").eq(lit(crate::row_id(memory, &v))))
        }
        _ => None,
    }
}

fn conditions_filter(memory: adb_core::log::Memory, conditions: &[adb_core::Condition]) -> Option<Expr> {
    use adb_core::LogicalOp;
    let mut it = conditions.iter();
    let mut acc = condition_filter(memory, it.next()?);
    for c in it {
        let next = condition_filter(memory, c);
        acc = match c.logical_op() {
            Some(LogicalOp::Or) => match (acc, next) {
                (Some(a), Some(b)) => Some(a.or(b)),
                _ => None,
            },
            Some(LogicalOp::And) | None => match (acc, next) {
                (Some(a), Some(b)) => Some(a.and(b)),
                (a, b) => a.or(b),
            },
        };
    }
    acc
}

/// `None` = TRUE (not expressible).
fn condition_filter(memory: adb_core::log::Memory, c: &adb_core::Condition) -> Option<Expr> {
    use adb_core::{Condition, Operator, Value};
    let (field, operator, value) = match c {
        Condition::Group { conditions, .. } => return conditions_filter(memory, conditions),
        Condition::Simple { field, operator, value, .. } => (field, *operator, value),
    };
    if field == "id" {
        // `resolve_field("id")` is the record id (its lineage's), a string.
        let Value::String(s) = value else { return None };
        return compare(col("lineage_id"), operator, lit(crate::row_id(memory, s)));
    }
    let key = payload_key(field)?;
    match (operator, value) {
        (Operator::In, Value::Array(items)) => {
            let mut any: Option<Expr> = None;
            for v in items {
                let e = condition_filter(memory, &Condition::new(field.clone(), Operator::Eq, v.clone()))?;
                any = Some(match any { Some(a) => a.or(e), None => e });
            }
            // An empty list matches nothing.
            Some(any.unwrap_or_else(|| lit(false)))
        }
        (_, Value::String(s)) => compare(field_text(key), operator, lit(s.clone())),
        (_, Value::Bool(b)) if operator == Operator::Eq => Some(field_text(key).eq(lit(b.to_string()))),
        (_, Value::Int(i)) if i.abs() < EXACT_INT => compare(field_num(key), operator, lit(*i as f64)),
        (_, Value::Float(f)) if f.is_finite() => compare(field_num(key), operator, lit(*f)),
        _ => None,
    }
}

fn compare(left: Expr, operator: adb_core::Operator, right: Expr) -> Option<Expr> {
    use adb_core::Operator;
    Some(match operator {
        Operator::Eq => left.eq(right),
        Operator::Gt => left.gt(right),
        Operator::Gte => left.gt_eq(right),
        Operator::Lt => left.lt(right),
        Operator::Lte => left.lt_eq(right),
        // Not pushed down: `!=` is true in the backends when the field has another
        // JSON type (an object, say), where `adb_json_scalar` gives NULL; CONTAINS,
        // STARTS_WITH and ENDS_WITH are left to the backends for now.
        _ => return None,
    })
}

/// The top-level payload key `resolve_field` reads for `field`, if there is one.
fn payload_key(field: &str) -> Option<&str> {
    match field.split_once('.') {
        None => Some(field),
        Some(("metadata", _)) => None,
        Some(("data", rest)) if !rest.contains('.') => Some(rest),
        Some(("data", _)) => None,
        // An unknown prefix is looked up as a literal key ("a.b").
        Some(_) => Some(field),
    }
}

/// The backends' `value_to_string` for KEY values.
fn key_text(value: &adb_core::Value) -> Option<String> {
    use adb_core::Value;
    Some(match value {
        Value::String(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => return None,
    })
}
