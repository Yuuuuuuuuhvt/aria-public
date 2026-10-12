//! 单账号 messages 路由的路由级不可用状态.
//!
//! 第三方单账号路由只有一份上游凭据, 没有可以换的下一个上游: 上游给出配额类拒绝后立即重发
//! 只会再次被拒. 因此这里记录一段不可用窗口, 窗口内的请求直接返回带恢复时间的错误, 不再
//! 转发上游; 期满自动恢复, 不需要人工干预. 与账号池的冷却一致, 状态只保存在内存中, 进程
//! 重启后清空.
//!
//! Route-level unavailability of the single-account messages routes.
//!
//! A third-party single-account route holds one upstream credential and has no next upstream to
//! switch to: after a quota rejection, resending at once would only be rejected again. This module
//! therefore records an unavailable window; requests inside it get an error carrying the recovery
//! time without reaching upstream, and the route recovers by itself once the window expires. Like
//! the account pool cooldowns, the state lives only in memory and a restart clears it.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use aria_core::exhaustion::{EXHAUSTION_MAX_DURATION, exhaustion_backoff};
use http::{HeaderMap, StatusCode};

/// 路由级不可用的原因.
///
/// The reason a route is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteUnavailableReason {
    /// 配额耗尽: 403 错误体命中用量窗口耗尽的说明, 见 quota_reason.
    ///
    /// Quota exhaustion: a 403 whose error body states an exhausted usage window, see
    /// quota_reason.
    Quota,
    /// 请求受限: 429.
    ///
    /// Rate limited: a 429.
    RateLimit,
}

impl RouteUnavailableReason {
    /// 健康检查中使用的取值.
    ///
    /// The value used in the health check.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Quota => "quota_exhausted",
            Self::RateLimit => "rate_limited",
        }
    }

    /// 面向客户端的说明, 包含路由名与恢复剩余秒数.
    ///
    /// The client-facing description, carrying the route name and the remaining seconds until
    /// recovery.
    pub(crate) fn message(self, route: &str, seconds: u64) -> String {
        match self {
            Self::Quota => format!("路由 {route} 的配额已耗尽, 最早 {seconds} 秒后恢复"),
            Self::RateLimit => format!("路由 {route} 请求受限, 最早 {seconds} 秒后恢复"),
        }
    }
}

/// 判断一次上游拒绝是否属于配额类, 并给出记录原因.
///
/// 判断只依据状态码与实际错误文本:
///
/// 1. 429 按请求受限处理. HTTP 429 的语义就是请求过多, 单账号路由没有可换的上游, 立即重发只会
///    再次被拒; Kimi 官方错误参考把 429 记为 "The engine is currently overloaded, please try
///    again later" 与 "We're receiving too many requests at the moment. Please wait a moment and
///    try again.", 观测中 coding-plan-dpsk-flash 路由曾连续收到 429.
/// 2. 403 只在错误消息命中 "usage limit" 时按配额耗尽处理. Kimi 官方错误参考在 403 下同时列出
///    "You've reached your 5-hour usage limit.", "You've reached your monthly usage limit for
///    this billing cycle." 与 "You've reached your weekly (7-day) usage limit." 三条配额文本,
///    以及 "(security_risk) We consider the current URL poses a security risk and are unable to
///    provide fetch service at this time." 这类请求级拒绝与 "You've reached your concurrent
///    request limit." 这类并发限制; 状态码本身无法区分它们, 只有含 usage limit 的消息说明
///    用量窗口已经耗尽. 其余 403 原样透传, 不记录, 避免把一次请求级拒绝当成整条路由不可用.
///
/// Decides whether one upstream rejection is quota-class and returns the reason to record.
///
/// The decision uses only the status code and the actual error text:
///
/// 1. A 429 is treated as rate limiting. HTTP 429 means too many requests, a single-account route
///    has no next upstream, and resending at once is rejected again; Kimi's error reference records
///    429 as "The engine is currently overloaded, please try again later" and "We're receiving too
///    many requests at the moment. Please wait a moment and try again.", and observation shows the
///    coding-plan-dpsk-flash route receiving 429s in a row.
/// 2. A 403 is treated as quota exhaustion only when its message matches "usage limit". Kimi's
///    error reference lists three quota texts under 403, as quoted above, next to request-scoped
///    rejections such as "(security_risk) We consider the current URL poses a security risk and are
///    unable to provide fetch service at this time." and the concurrency limit "You've reached your
///    concurrent request limit."; the status code alone cannot tell them apart, and only a message
///    with usage limit states that the usage window is exhausted. Every other 403 is passed through
///    unchanged and never recorded, so a request-scoped rejection cannot mark the whole route
///    unavailable.
pub(crate) fn quota_reason(status: StatusCode, body: &[u8]) -> Option<RouteUnavailableReason> {
    match status.as_u16() {
        429 => Some(RouteUnavailableReason::RateLimit),
        403 if contains_usage_limit(body) => Some(RouteUnavailableReason::Quota),
        _ => None,
    }
}

/// 错误体中是否出现用量窗口耗尽的说明.
///
/// 按整个响应体的文本判断: 上游把说明放在 error.message, 但不同实现的外层结构并不一致, 整体
/// 匹配不依赖具体字段名. 响应体在调用方已经按上限读取, 因此这里的大小写归一化不会处理无界
/// 输入.
///
/// Reports whether the error body states an exhausted usage window.
///
/// The whole body text is matched: upstream puts the statement in error.message, but the outer
/// structure differs between implementations, so matching the text does not depend on a field name.
/// The caller has already read the body under a byte cap, so this case folding never processes
/// unbounded input.
fn contains_usage_limit(body: &[u8]) -> bool {
    String::from_utf8_lossy(body)
        .to_ascii_lowercase()
        .contains("usage limit")
}

/// 从一次配额类拒绝中解析恢复时长.
///
/// 优先取 Retry-After 响应头的秒数, 其次取错误体中的恢复字段 (quotaResetDelay, retryDelay,
/// retry_after 与 retry_after_seconds). 都取不到时返回 None, 由调用方使用有上限的指数退避.
///
/// Parses the recovery delay out of one quota-class rejection.
///
/// The delta-seconds of the Retry-After header win, then the recovery fields in the error body
/// (quotaResetDelay, retryDelay, retry_after and retry_after_seconds). When none can be taken the
/// function returns None and the caller uses the capped exponential backoff.
pub(crate) fn reset_delay(headers: &HeaderMap, body: &[u8]) -> Option<Duration> {
    aria_core::exhaustion::retry_after_seconds(headers)
        .or_else(|| aria_core::exhaustion::reset_delay_from_json(body))
}

/// 一个路由的不可用窗口.
///
/// An unavailable window of one route.
struct Window {
    until: Instant,
    reason: RouteUnavailableReason,
}

/// 路由级不可用状态: 一个不可用窗口与连续配额类拒绝的计数.
///
/// Route-level unavailability: one unavailable window together with the consecutive quota-class
/// rejection count.
pub(crate) struct RouteUnavailable {
    /// 连续配额类拒绝次数, 决定没有解析值时的退避时长; 成功一次清零.
    ///
    /// The consecutive quota-class rejection count; it decides the backoff duration when no delay
    /// can be parsed, and one success clears it.
    consecutive: AtomicU32,
    window: Mutex<Option<Window>>,
}

impl RouteUnavailable {
    pub(crate) fn new() -> Self {
        Self {
            consecutive: AtomicU32::new(0),
            window: Mutex::new(None),
        }
    }

    /// 当前不可用窗口的原因与剩余时长; 没有窗口或窗口已经过期时为 None.
    ///
    /// 过期窗口在这里清除, 因此恢复不需要任何后台任务.
    ///
    /// The reason and remaining time of the current window, or None when there is none or it has
    /// expired.
    ///
    /// An expired window is cleared here, so recovery needs no background task.
    pub(crate) fn window(&self, now: Instant) -> Option<(RouteUnavailableReason, Duration)> {
        let mut slot = self.slot();
        match *slot {
            Some(ref window) if window.until > now => {
                Some((window.reason, window.until.saturating_duration_since(now)))
            }
            Some(_) => {
                *slot = None;
                None
            }
            None => None,
        }
    }

    /// 记录一次配额类拒绝: 连续计数加一, 时长取解析值或按连续次数退避, 并截断到
    /// EXHAUSTION_MAX_DURATION. 新窗口覆盖旧窗口. 返回实际记录的时长, 供日志使用.
    ///
    /// Records one quota-class rejection: the consecutive count grows by one, the duration is the
    /// parsed value or the backoff for that count, and it is cut down to EXHAUSTION_MAX_DURATION. A
    /// new window replaces the old one. The recorded duration is returned for the log line.
    pub(crate) fn record(
        &self,
        reason: RouteUnavailableReason,
        parsed: Option<Duration>,
        now: Instant,
    ) -> Duration {
        let consecutive = self
            .consecutive
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let duration = parsed
            .unwrap_or_else(|| exhaustion_backoff(consecutive))
            .min(EXHAUSTION_MAX_DURATION);
        // Instant 加法只会在单调时钟接近上限时溢出, 实际不会发生; 万一发生则不记录.
        //
        // Adding duration to an Instant overflows only near the end of the monotonic clock, which
        // does not happen in practice; if it ever did, no window is saved.
        if let Some(until) = now.checked_add(duration) {
            *self.slot() = Some(Window { until, reason });
        }
        duration
    }

    /// 记录一次成功: 清除窗口与连续计数.
    ///
    /// 一次成功说明该路由可以正常服务, 之前的拒绝与计数都不再成立.
    ///
    /// Records one success, clearing the window and the consecutive count.
    ///
    /// One success proves the route serves requests again, so the earlier rejections and the count
    /// no longer hold.
    pub(crate) fn record_success(&self) {
        self.consecutive.store(0, Ordering::SeqCst);
        *self.slot() = None;
    }

    fn slot(&self) -> std::sync::MutexGuard<'_, Option<Window>> {
        // 持锁期间只读写一个窗口, 不跨越 await, 锁中毒只可能来自无关的 panic.
        //
        // Only one window is read or written under the lock with no await in between, so poisoning
        // can only come from an unrelated panic.
        self.window.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
