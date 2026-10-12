//! 额度与容量耗尽的共享规则: 有上限的指数退避, 以及从上游拒绝中解析恢复时长.
//!
//! lyra 的账号记录与 canto 的路由记录使用同一套退避策略与解析逻辑, 避免两条线路各自维护
//! 一份可能不一致的实现. 记录本身由各线路自己保存, 本模块只提供无状态的规则.
//!
//! 状态不在这里持久化: 与既有的账号冷却与模型不可用记录一致, 全部记录只保存在内存中,
//! 进程重启后清空, 由下一个请求重新建立.
//!
//! Shared rules for quota and capacity exhaustion: the capped exponential backoff and the
//! parsing of a recovery delay from an upstream rejection.
//!
//! lyra's account records and canto's route records use the same backoff policy and parsing
//! logic, so the two lines cannot drift apart. The records themselves live in each line; this
//! module only holds the stateless rules.
//!
//! Nothing here is persisted: like the existing account cooldown and unavailable model records,
//! every record lives in memory only and is cleared by a process restart, then rebuilt by the
//! next request.

use std::time::Duration;

use http::HeaderMap;

/// 没有解析值时的起始退避时长.
///
/// The starting backoff duration when no delay can be parsed.
pub const EXHAUSTION_BACKOFF_BASE: Duration = Duration::from_secs(60);

/// 退避时长的上限.
///
/// 连续耗尽的时间越接近上限, 说明上游越可能长时间无法恢复; 上限限制的是重新探测的频率,
/// 不是记录的有效期本身.
///
/// The upper bound of the backoff duration.
///
/// The closer the consecutive count is to the cap, the more likely the upstream stays
/// unavailable for a long time; the cap limits how often the record is re-probed, not how long
/// it may remain valid.
pub const EXHAUSTION_BACKOFF_CAP: Duration = Duration::from_secs(30 * 60);

/// 单次记录的有效期上限: 上游给出的恢复时间与指数退避都不得超过它.
///
/// 上限用于防御明显不合理的取值: 误解析出的巨大数值会把账号或路由长期锁死, 且不会再被
/// 任何请求重新评估. 一周远大于已经观测到的重置窗口 (Gemini 的 429 示例为 1 小时 14 分,
/// 2026-10-08 的 Opus 配额约为 65 小时), 因此不会截断真实的重置时间.
///
/// The validity cap of one record: neither a parsed recovery delay nor the exponential backoff
/// may exceed it.
///
/// The cap guards against clearly unreasonable values: a misparsed huge number would lock an
/// account or route for a long time without any request ever re-evaluating it. One week is far
/// beyond every observed reset window (the Gemini 429 example resets in 1 hour 14 minutes and
/// the Opus quotas seen on 2026-10-08 reset in about 65 hours), so real reset times are never
/// cut short.
pub const EXHAUSTION_MAX_DURATION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// 连续第 consecutive 次耗尽时的退避时长, consecutive 从 1 开始.
///
/// 取值是起始时长乘以 2 的 consecutive-1 次幂, 超过上限时取上限. 位移与乘法都用饱和运算,
/// 计数被反复累加时不会溢出.
///
/// The backoff duration of the consecutive-th exhaustion, with consecutive starting at 1.
///
/// It is the base duration doubled consecutive-1 times, clamped to the cap. Both the shift and
/// the multiplication saturate, so a repeatedly incremented counter cannot overflow.
pub fn exhaustion_backoff(consecutive: u32) -> Duration {
    let shift = consecutive.saturating_sub(1).min(31);
    let factor = 1u64 << shift;
    let seconds = EXHAUSTION_BACKOFF_BASE.as_secs().saturating_mul(factor);
    Duration::from_secs(seconds.min(EXHAUSTION_BACKOFF_CAP.as_secs()))
}

/// 解析 Retry-After 响应头的秒数形式.
///
/// RFC 9110 允许秒数与 HTTP 日期两种形式, 这里只解析秒数: 本仓库接触到的上游在需要放弃
/// 重试时都给出秒数, 日期形式解析失败就退化为退避, 不会漏掉记录. 取值不是数字时返回 None,
/// 负数字符串同样不解析.
///
/// Parses the delta-seconds form of the Retry-After header.
///
/// RFC 9110 allows both delta-seconds and an HTTP date; only delta-seconds are parsed here,
/// because every upstream this repository reaches gives the delta-seconds form when it wants
/// the caller to wait, and an unparsable form degrades to the backoff instead of dropping the
/// record. A non-numeric or negative value yields None.
pub fn retry_after_seconds(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(http::header::RETRY_AFTER)?.to_str().ok()?;
    let seconds: u64 = value.trim().parse().ok()?;
    Some(Duration::from_secs(seconds))
}

/// 从 JSON 错误体中取恢复时长.
///
/// 按文档顺序深度优先查找固定键名, 第一个可解析的取值生效. 键名覆盖两族上游写法:
/// Google RPC 的 quotaResetDelay 与 RetryInfo.retryDelay, 以及 REST 惯例的 retry_after
/// 与 retry_after_seconds. 取值可以是数字秒数, 也可以是 Go 时长文本 (例如
/// 1h14m5.453370951s). 解析失败或没有命中键名时返回 None, 由调用方退避.
///
/// Takes the recovery delay from a JSON error body.
///
/// It walks the document depth-first in document order and the first parsable value wins. The
/// key names cover two upstream families: the Google RPC quotaResetDelay and RetryInfo
/// retryDelay, and the conventional retry_after and retry_after_seconds. A value is either a
/// number of seconds or Go duration text such as 1h14m5.453370951s. A failed parse or a
/// missing key yields None and the caller falls back to the backoff.
pub fn reset_delay_from_json(body: &[u8]) -> Option<Duration> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    reset_delay_in_value(&value)
}

/// 恢复时长可能出现的键名, 顺序即深度优先查找时的判断顺序.
///
/// The key names a recovery delay may use, in the order the depth-first walk checks them.
const RESET_KEYS: [&str; 4] = [
    "quotaResetDelay",
    "retryDelay",
    "retry_after",
    "retry_after_seconds",
];

fn reset_delay_in_value(value: &serde_json::Value) -> Option<Duration> {
    match value {
        serde_json::Value::Object(fields) => {
            for key in RESET_KEYS {
                if let Some(delay) = fields.get(key).and_then(delay_of_value) {
                    return Some(delay);
                }
            }
            fields.values().find_map(reset_delay_in_value)
        }
        serde_json::Value::Array(items) => items.iter().find_map(reset_delay_in_value),
        _ => None,
    }
}

/// 把一个 JSON 取值解释为时长: 数字按秒, 字符串按 Go 时长或纯数字秒.
///
/// Interprets one JSON value as a duration: a number is seconds, a string is either Go duration
/// text or a plain number of seconds.
fn delay_of_value(value: &serde_json::Value) -> Option<Duration> {
    match value {
        serde_json::Value::Number(number) => {
            let seconds = number.as_u64().or_else(|| {
                let float = number.as_f64()?;
                (float.is_finite() && float > 0.0).then(|| float.round() as u64)
            })?;
            Some(Duration::from_secs(seconds))
        }
        serde_json::Value::String(text) => {
            let text = text.trim();
            if let Ok(seconds) = text.parse::<u64>() {
                return Some(Duration::from_secs(seconds));
            }
            parse_go_duration(text)
        }
        _ => None,
    }
}

/// 解析 Go 时长文本, 例如 4445.453370951s 或 1h14m5.453370951s.
///
/// 只接受正数: 依次读取数字与单位, 单位取 ns, us, µs, μs, ms, s, m, h 之一, 小数部分保留到
/// 纳秒, 多出的位数截断. 空串, 负数, 缺单位, 单位未知以及数值超出 u64 纳秒范围时返回 None.
/// 上游给出这种文本的地方是 Google RPC 的 quotaResetDelay 与 retryDelay, 日志中的实例是
/// 1h14m5.453370951s 与 4445.453370951s.
///
/// Parses Go duration text such as 4445.453370951s or 1h14m5.453370951s.
///
/// Only positive values are accepted: numbers and units are read in turn, a unit is one of
/// ns, us, µs, μs, ms, s, m and h, and the fractional part keeps nanosecond precision with
/// further digits truncated. An empty string, a negative value, a missing or unknown unit, and
/// a value beyond the u64 nanosecond range all yield None. Upstream emits this text in the
/// Google RPC quotaResetDelay and retryDelay fields; the instances in the log are
/// 1h14m5.453370951s and 4445.453370951s.
pub fn parse_go_duration(text: &str) -> Option<Duration> {
    if text.is_empty() {
        return None;
    }
    let mut rest = text;
    let mut total_nanos: u128 = 0;
    let mut parsed_any = false;
    while !rest.is_empty() {
        let digits_end = rest
            .find(|character: char| !character.is_ascii_digit() && character != '.')
            .unwrap_or(rest.len());
        if digits_end == 0 {
            return None;
        }
        let number = &rest[..digits_end];
        let unit_text = &rest[digits_end..];
        // 先匹配以相同字母开头的更长单位: ms 不能按 m 解析, 否则剩下的 s 会被当成缺失数字.
        //
        // Longer units sharing a first letter are matched first: ms must not parse as m, or the
        // remaining s would read as a missing number.
        let (unit_nanos, remainder) = if let Some(remainder) = unit_text.strip_prefix("ms") {
            (1_000_000u128, remainder)
        } else if let Some(remainder) = unit_text.strip_prefix("ns") {
            (1u128, remainder)
        } else if let Some(remainder) = unit_text.strip_prefix("us") {
            (1_000u128, remainder)
        } else if let Some(remainder) = unit_text.strip_prefix('µ') {
            (1_000u128, remainder.strip_prefix('s')?)
        } else if let Some(remainder) = unit_text.strip_prefix('μ') {
            (1_000u128, remainder.strip_prefix('s')?)
        } else if let Some(remainder) = unit_text.strip_prefix('h') {
            (3_600_000_000_000u128, remainder)
        } else if let Some(remainder) = unit_text.strip_prefix('m') {
            (60_000_000_000u128, remainder)
        } else {
            (1_000_000_000u128, unit_text.strip_prefix('s')?)
        };
        total_nanos = total_nanos.checked_add(component_nanos(number, unit_nanos)?)?;
        parsed_any = true;
        rest = remainder;
    }
    if !parsed_any {
        return None;
    }
    Some(Duration::from_nanos(u64::try_from(total_nanos).ok()?))
}

/// 把一个数字字符串 (可带小数) 按给定单位换算成纳秒.
///
/// Converts one number string, possibly fractional, into nanoseconds for the given unit.
fn component_nanos(number: &str, unit_nanos: u128) -> Option<u128> {
    let (integer_text, fraction_text) = match number.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (number, ""),
    };
    if integer_text.is_empty() && fraction_text.is_empty() {
        return None;
    }
    let integer: u128 = if integer_text.is_empty() {
        0
    } else {
        integer_text.parse().ok()?
    };
    // 小数最多保留九位, 即纳秒精度; 更长的位数按 Go 的截断语义舍弃.
    //
    // The fraction keeps at most nine digits, the nanosecond precision; further digits are
    // dropped, matching Go's truncation.
    let kept = &fraction_text[..fraction_text.len().min(9)];
    let fraction: u128 = if kept.is_empty() {
        0
    } else {
        kept.parse().ok()?
    };
    let fraction_nanos = fraction * unit_nanos / 10u128.pow(kept.len() as u32);
    integer.checked_mul(unit_nanos)?.checked_add(fraction_nanos)
}
