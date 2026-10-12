//! 上游 429 配额拒绝中恢复时间的解析.
//!
//! Antigravity 的 429 错误体带有重置信息, 日志中的实例是
//! {"error":{"code":429,"message":"Individual quota reached. ... Resets in 1h14m5s.","details":[{"@type":
//! "type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED","metadata":{"model":
//! "gemini-3.8-flash-high","quotaResetDelay":"1h14m5.453370951s","quotaResetTimeStamp":
//! "2026-10-07T17:15:53Z"}},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":
//! "4445.453370951s"}]}}. 解析优先级: 绝对时间戳 quotaResetTimeStamp, 其次 Go 时长文本
//! quotaResetDelay 或 RetryInfo.retryDelay. 全部取不到时返回 None, 由调用方使用
//! aria-core 的有上限指数退避.
//!
//! Parsing the recovery time out of an upstream 429 quota rejection.
//!
//! An Antigravity 429 body carries reset information; the instance in the log is the body quoted
//! above. The parse order is the absolute quotaResetTimeStamp first, then the Go duration text in
//! quotaResetDelay or RetryInfo.retryDelay. When none can be taken the function returns None and
//! the caller uses the capped exponential backoff from aria-core.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// 从 429 错误体中解析恢复时长.
///
/// Parses the recovery delay from a 429 error body.
pub(crate) fn quota_reset(body: &[u8]) -> Option<Duration> {
    timestamp_reset(body).or_else(|| aria_core::exhaustion::reset_delay_from_json(body))
}

/// 查找 quotaResetTimeStamp 并换算为相对当前的剩余时长.
///
/// 时间戳已经过去, 格式不是 RFC3339 或解析失败时返回 None, 让调用方尝试下一项依据.
///
/// Looks up quotaResetTimeStamp and converts it into the remaining time from now.
///
/// A timestamp in the past, a value that is not RFC3339, or a failed parse yields None so the
/// caller can try the next source.
fn timestamp_reset(body: &[u8]) -> Option<Duration> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let text = string_field(&value, "quotaResetTimeStamp")?;
    let reset = DateTime::parse_from_rfc3339(text).ok()?;
    let remaining = reset.with_timezone(&Utc) - Utc::now();
    remaining.to_std().ok().filter(|delay| !delay.is_zero())
}

/// 按键名在 JSON 文档中深度优先查找字符串取值.
///
/// Looks up a string value by key name, walking the JSON document depth-first.
fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    match value {
        Value::Object(fields) => {
            if let Some(text) = fields.get(key).and_then(Value::as_str) {
                return Some(text);
            }
            fields.values().find_map(|item| string_field(item, key))
        }
        Value::Array(items) => items.iter().find_map(|item| string_field(item, key)),
        _ => None,
    }
}
