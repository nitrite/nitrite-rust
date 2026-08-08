//! The wire filter tree, turned into a Nitrite `Filter`.
//!
//! Every refusal is a `badRequest`, never a silently dropped clause: returning
//! an unfiltered page would show the developer rows they explicitly excluded.

use dbinspect_bridge::{BridgeError, BridgeResult};
use nitrite::common::Value;
use nitrite::filter::{and, field, or, Filter};
use serde_json::{Map, Value as Json};

/// The v1 filter operators this implementation actually supports.
///
/// **`exists` was absent until `nitrite` 0.6.0 and is now here**, exactly as in
/// the Dart and JVM adapters: `docs/PROTOCOL.md` §4.1 has always listed it among
/// the v1 operators, and it was reported unsupported for as long as the fluent
/// API had nothing that tested for a field's presence. `capabilities.filterOps`
/// stays authoritative either way — that is the mechanism that let the gap be
/// honest rather than mistranslated, and it is what makes closing it a one-line
/// change.
///
/// `between` and `elem_match` are the other direction — Rust has them and v1
/// does not, so they stay out until the protocol gains them.
pub const NITRITE_FILTER_OPS: [&str; 10] = [
    "eq", "ne", "gt", "gte", "lt", "lte", "in", "notIn", "exists", "text",
];

/// Reported in `filterOps` only when the developer set `allow_regex` (threat
/// model F10, criterion 9).
pub const NITRITE_REGEX_OP: &str = "regex";

/// Threat model F10 fix 3, and explicitly best-effort.
pub const MAX_REGEX_PATTERN_LENGTH: usize = 256;

/// A filter tree arrives from a paired but untrusted client, and each level is a
/// stack frame. Deep enough nesting is a crash inside the developer's
/// application, which is the same class of problem as an unbounded frame.
pub const MAX_FILTER_DEPTH: usize = 16;

pub fn parse_filter(tree: &Map<String, Json>, allow_regex: bool) -> BridgeResult<Filter> {
    parse(&Json::Object(tree.clone()), allow_regex, 0)
}

fn parse(node: &Json, allow_regex: bool, depth: usize) -> BridgeResult<Filter> {
    if depth > MAX_FILTER_DEPTH {
        return Err(BridgeError::bad_request("filter is nested too deeply"));
    }
    let Json::Object(node) = node else {
        return Err(BridgeError::bad_request("a filter node must be an object"));
    };

    if let Some(conjunction) = node.get("and") {
        return combine(conjunction, allow_regex, depth, and);
    }
    if let Some(disjunction) = node.get("or") {
        return combine(disjunction, allow_regex, depth, or);
    }
    if let Some(negation) = node.get("not") {
        return Ok(parse(negation, allow_regex, depth + 1)?.not());
    }
    leaf(node, allow_regex)
}

fn combine(
    raw: &Json,
    allow_regex: bool,
    depth: usize,
    join: fn(Vec<Filter>) -> Filter,
) -> BridgeResult<Filter> {
    let Json::Array(children) = raw else {
        return Err(BridgeError::bad_request(
            "and/or takes a non-empty list of filters",
        ));
    };
    if children.is_empty() {
        return Err(BridgeError::bad_request(
            "and/or takes a non-empty list of filters",
        ));
    }

    let mut parts = Vec::with_capacity(children.len());
    for child in children {
        parts.push(parse(child, allow_regex, depth + 1)?);
    }
    // The protocol's own `queryPage` example sends a one-element `and`, and one
    // clause is itself.
    Ok(if parts.len() == 1 {
        parts.remove(0)
    } else {
        join(parts)
    })
}

fn leaf(node: &Map<String, Json>, allow_regex: bool) -> BridgeResult<Filter> {
    let Some(Json::String(name)) = node.get("field") else {
        return Err(BridgeError::bad_request("a filter needs a field name"));
    };
    if name.is_empty() {
        return Err(BridgeError::bad_request("a filter needs a field name"));
    }
    let Some(Json::String(op)) = node.get("op") else {
        return Err(BridgeError::bad_request("a filter needs an operator"));
    };

    let value = node.get("value").unwrap_or(&Json::Null);
    let on = || field(name);

    match op.as_str() {
        "eq" => Ok(on().eq(any(value)?)),
        "ne" => Ok(on().ne(any(value)?)),
        "gt" => Ok(on().gt(ordered(value, op)?)),
        "gte" => Ok(on().gte(ordered(value, op)?)),
        "lt" => Ok(on().lt(ordered(value, op)?)),
        "lte" => Ok(on().lte(ordered(value, op)?)),
        "in" => Ok(on().in_array(ordered_list(value, op)?)),
        "notIn" => Ok(on().not_in_array(ordered_list(value, op)?)),
        // Presence only, and `value` is deliberately ignored: `exists: false` is
        // not "does not exist" in the protocol — that is `not`. Reading the
        // value would select the opposite rows for a client that sent one out of
        // habit.
        "exists" => Ok(on().exists()),
        "text" => Ok(on().text(text(value)?)),
        NITRITE_REGEX_OP => {
            if !allow_regex {
                // F10's load-bearing mitigation: off unless the developer opted
                // in, and absent from `filterOps` when off.
                return Err(BridgeError::bad_request(
                    "regex is not enabled on this adapter",
                ));
            }
            Ok(on().text_regex(&pattern(value)?))
        }
        other => Err(BridgeError::bad_request(format!(
            "this adapter does not support the \"{other}\" operator"
        ))),
    }
}

/// Equality takes anything JSON can carry; a list or an object compares against
/// an array or a sub-document, which is what a document store means by it.
fn any(value: &Json) -> BridgeResult<Value> {
    Ok(match value {
        Json::Null => Value::Null,
        Json::Bool(flag) => Value::Bool(*flag),
        Json::String(text) => Value::String(text.clone()),
        Json::Number(number) => number_value(number)?,
        Json::Array(values) => {
            let mut encoded = Vec::with_capacity(values.len());
            for element in values {
                encoded.push(any(element)?);
            }
            Value::Array(encoded)
        }
        Json::Object(_) => {
            return Err(BridgeError::bad_request(
                "a filter value cannot be an object",
            ))
        }
    })
}

fn number_value(number: &serde_json::Number) -> BridgeResult<Value> {
    if let Some(integer) = number.as_i64() {
        return Ok(Value::I64(integer));
    }
    if let Some(float) = number.as_f64() {
        return Ok(Value::F64(float));
    }
    Err(BridgeError::bad_request("a filter value is out of range"))
}

/// Ordering comparisons need something the store can order. A bool, a list or an
/// object would compare arbitrarily inside the engine; refusing here names the
/// problem instead of returning a page the client cannot explain.
fn ordered(value: &Json, op: &str) -> BridgeResult<Value> {
    match value {
        Json::Number(number) => number_value(number),
        Json::String(text) => Ok(Value::String(text.clone())),
        _ => Err(BridgeError::bad_request(format!(
            "\"{op}\" needs a number or a string to compare against"
        ))),
    }
}

fn ordered_list(value: &Json, op: &str) -> BridgeResult<Vec<Value>> {
    let Json::Array(values) = value else {
        return Err(BridgeError::bad_request(format!(
            "\"{op}\" needs a non-empty list of values"
        )));
    };
    if values.is_empty() {
        return Err(BridgeError::bad_request(format!(
            "\"{op}\" needs a non-empty list of values"
        )));
    }
    values.iter().map(|element| ordered(element, op)).collect()
}

fn text(value: &Json) -> BridgeResult<&str> {
    match value {
        Json::String(text) if !text.is_empty() => Ok(text),
        _ => Err(BridgeError::bad_request(
            "\"text\" needs a string to search for",
        )),
    }
}

/// F10 fix 3: a length cap and a nested-quantifier refusal.
///
/// **Unlike Dart and the JVM, this one is not the second line — it is a
/// convenience.** Rust's `regex` crate has no backtracking and matches in linear
/// time in the length of the input, so the exponential blowup criterion 9 is
/// about cannot happen here at all. The checks stay because a pattern that costs
/// exponential time on the *other two* implementations should be refused
/// identically on this one: the client greys the same operators everywhere, and
/// a filter that works on Rust and hangs on Dart is a worse outcome than one
/// that is refused on both.
fn pattern(value: &Json) -> BridgeResult<String> {
    let Json::String(pattern) = value else {
        return Err(BridgeError::bad_request("\"regex\" needs a pattern"));
    };
    if pattern.is_empty() {
        return Err(BridgeError::bad_request("\"regex\" needs a pattern"));
    }
    if pattern.len() > MAX_REGEX_PATTERN_LENGTH {
        return Err(BridgeError::bad_request(format!(
            "regex patterns are limited to {MAX_REGEX_PATTERN_LENGTH} characters"
        )));
    }
    // A group that ends in a quantifier and is itself quantified — `(a+)+`,
    // `(?:a*)*`, `(a{1,3})+`. This is the shape behind exponential backtracking,
    // and it is the pattern criterion 9 names.
    if has_nested_quantifier(pattern) {
        return Err(BridgeError::bad_request(
            "this regex has a nested quantifier and is refused: it is linear here \
             and exponential on the Dart and JVM bridges, and the operator must \
             mean the same thing on all three",
        ));
    }
    Ok(pattern.clone())
}

/// `[*+}])` followed by `[*+{]`, without pulling a regex engine in to find a
/// three-character shape.
fn has_nested_quantifier(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    bytes.windows(3).any(|window| {
        matches!(window[0], b'*' | b'+' | b'}')
            && window[1] == b')'
            && matches!(window[2], b'*' | b'+' | b'{')
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tree(value: serde_json::Value) -> Map<String, Json> {
        match value {
            Json::Object(object) => object,
            other => panic!("not an object: {other}"),
        }
    }

    fn parsed(value: serde_json::Value) -> BridgeResult<Filter> {
        parse_filter(&tree(value), false)
    }

    #[test]
    fn every_advertised_operator_parses() {
        for (op, value) in [
            ("eq", json!(1)),
            ("ne", json!("x")),
            ("gt", json!(1)),
            ("gte", json!(1.5)),
            ("lt", json!("m")),
            ("lte", json!(1)),
            ("in", json!([1, 2])),
            ("notIn", json!(["a"])),
            ("exists", json!(null)),
            ("text", json!("hello")),
        ] {
            assert!(NITRITE_FILTER_OPS.contains(&op));
            let filter = parsed(json!({"field": "age", "op": op, "value": value}));
            assert!(filter.is_ok(), "{op} was refused: {:?}", filter.err());
        }
    }

    #[test]
    fn an_operator_this_adapter_does_not_have_is_refused_rather_than_approximated() {
        // `between` and `elemMatch` are the other direction — Nitrite has them
        // and the protocol does not — and that is what `filterOps` is for.
        for op in ["between", "elemMatch", "nearby", ""] {
            assert!(parsed(json!({"field": "age", "op": op, "value": 1})).is_err());
        }
    }

    #[test]
    fn and_or_and_not_nest() {
        assert!(parsed(json!({"and": [
            {"field": "age", "op": "gt", "value": 30},
            {"or": [
                {"field": "name", "op": "eq", "value": "a"},
                {"not": {"field": "name", "op": "eq", "value": "b"}},
            ]},
        ]}))
        .is_ok());
    }

    #[test]
    fn a_one_clause_and_is_that_clause() {
        // The protocol's own `queryPage` example sends one.
        assert!(parsed(json!({"and": [{"field": "age", "op": "gt", "value": 30}]})).is_ok());
    }

    #[test]
    fn the_trees_a_bridge_must_refuse() {
        for bad in [
            json!({}),
            json!({"and": []}),
            json!({"and": "not a list"}),
            json!({"or": [{"field": "a", "op": "nope"}]}),
            json!({"not": 42}),
            json!({"op": "eq", "value": 1}),
            json!({"field": "", "op": "eq", "value": 1}),
            json!({"field": "a", "value": 1}),
            json!({"field": "a", "op": 12}),
            json!({"field": "a", "op": "gt", "value": true}),
            json!({"field": "a", "op": "gt", "value": [1]}),
            json!({"field": "a", "op": "in", "value": 1}),
            json!({"field": "a", "op": "in", "value": []}),
            json!({"field": "a", "op": "text", "value": 12}),
            json!({"field": "a", "op": "text", "value": ""}),
            json!({"field": "a", "op": "eq", "value": {"nested": 1}}),
        ] {
            assert!(parsed(bad.clone()).is_err(), "{bad} was accepted");
        }
    }

    #[test]
    fn a_tree_deep_enough_to_blow_the_stack_is_refused_first() {
        let mut deep = json!({"field": "a", "op": "eq", "value": 1});
        for _ in 0..(MAX_FILTER_DEPTH + 2) {
            deep = json!({"not": deep});
        }
        assert!(parsed(deep).is_err());
    }

    #[test]
    fn regex_is_refused_unless_the_developer_allowed_it() {
        let node = json!({"field": "name", "op": "regex", "value": "^a"});
        assert!(parse_filter(&tree(node.clone()), false).is_err());
        assert!(parse_filter(&tree(node), true).is_ok());
    }

    #[test]
    fn an_allowed_regex_is_still_bounded() {
        let allowed = |value: serde_json::Value| {
            parse_filter(
                &tree(json!({"field": "n", "op": "regex", "value": value})),
                true,
            )
        };
        assert!(allowed(json!("^abc$")).is_ok());
        assert!(allowed(json!("")).is_err());
        assert!(allowed(json!(12)).is_err());
        assert!(allowed(json!("a".repeat(MAX_REGEX_PATTERN_LENGTH + 1))).is_err());
        // Criterion 9's own pattern, plus the two other shapes of it.
        for catastrophic in ["(a+)+$", "(?:a*)*", "(a{1,3})+"] {
            assert!(allowed(json!(catastrophic)).is_err(), "{catastrophic}");
        }
        // …and a quantified group that is not nested is ordinary.
        assert!(allowed(json!("(abc)+")).is_ok());
    }
}
