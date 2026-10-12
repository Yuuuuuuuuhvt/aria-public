//! 账号冷却, 模型不可用与额度或容量耗尽记录, 规则见 architecture.md 5.11, 5.12 与 5.13.
//!
//! 账号收到上游 403 时可能已经被服务商的风控判定, 立即换号会把同一份流量扩散到整个账号池,
//! 因此该账号在一段时间内不再参与选号. 账号收到上游 404 时表明该账号没有对应模型的访问权限,
//! 同样在一段时间内针对该模型不再参与选号. 账号对某个模型返回 429 配额耗尽或 503 容量耗尽时,
//! 该模型上也会记录一段不可用时间, 期满自动恢复.
//!
//! 状态只保存在内存中: 重启进程即可恢复使用. 前三者共用底层的时间管理结构, 避免重复实现
//! 过期与清理逻辑.
//!
//! Account cooldown, model unavailability and quota or capacity exhaustion records; the rules are
//! in sections 5.11, 5.12 and 5.13 of architecture.md.
//!
//! An upstream 403 may mean the provider's risk control has flagged the account, and rotating at
//! once would spread the same traffic over the whole pool, so the account stays out of selection for
//! a while. An upstream 404 indicates the account lacks permission for that model, so it also stays
//! out of selection for that model. A 429 quota rejection or a 503 capacity rejection for one model
//! records an unavailable window for that account and model, and the account returns to selection
//! once the window expires.
//!
//! The state lives only in memory: restarting the process clears it. The first three share the
//! underlying time management structure to avoid duplicating expiry and cleanup logic.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use aria_core::exhaustion::{EXHAUSTION_MAX_DURATION, exhaustion_backoff};

/// 账号 403 冷却时长.
///
/// The account 403 cooldown duration.
pub(crate) const ACCOUNT_COOLDOWN: Duration = Duration::from_secs(6 * 60 * 60);

/// 模型不可用记录的有效期.
///
/// The duration for model unavailability records.
pub(crate) const MODEL_UNAVAILABLE_DURATION: Duration = Duration::from_secs(6 * 60 * 60);

/// 按键记录的过期时间表.
///
/// 键为泛型类型, 值为记录失效的单调时刻. 内部加锁保证并发安全,
/// 读写不跨越异步等待.
///
/// An expiry table keyed by a generic key with the instant the record expires.
/// Internal locking ensures thread safety and does not cross await points.
pub(crate) struct TimedRegistry<K> {
    until: Mutex<HashMap<K, Instant>>,
}

impl<K> Default for TimedRegistry<K> {
    fn default() -> Self {
        Self {
            until: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Eq + Hash + Clone + Ord> TimedRegistry<K> {
    /// 插入一条从 now 起持续 duration 的记录.
    ///
    /// Inserts a record lasting for duration starting from now.
    pub(crate) fn start(&self, key: K, duration: Duration, now: Instant) {
        // Instant 加法只会在单调时钟接近上限时溢出, 实际不会发生;
        // 万一发生则不记录, 避免发生 panic.
        //
        // Adding duration to an Instant overflows only near the end of the monotonic clock,
        // which does not happen in practice; if it ever did, no record is saved instead of panicking.
        if let Some(end) = now.checked_add(duration) {
            self.records().insert(key, end);
        }
    }

    /// 检查指定键在 now 时是否依然有效, 顺带清除已经失效的记录.
    ///
    /// Reports whether the key is active at now, clearing expired records.
    pub(crate) fn active(&self, key: &K, now: Instant) -> bool {
        let mut records = self.records();
        match records.get(key) {
            Some(end) if *end > now => true,
            Some(_) => {
                records.remove(key);
                false
            }
            None => false,
        }
    }

    /// 返回 now 时依然有效的键及剩余时长, 按键升序排序.
    ///
    /// Returns active keys at now with their remaining time, sorted by key in ascending order.
    pub(crate) fn remaining(&self, now: Instant) -> Vec<(K, Duration)> {
        let mut records = self.records();
        records.retain(|_, end| *end > now);
        let mut list: Vec<(K, Duration)> = records
            .iter()
            .map(|(key, end)| (key.clone(), end.saturating_duration_since(now)))
            .collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }

    fn records(&self) -> MutexGuard<'_, HashMap<K, Instant>> {
        // 持锁期间只读写映射, 不跨越 await, 锁中毒只可能来自无关的 panic.
        //
        // Only the map is accessed under the lock with no await in between, so poisoning can
        // only come from an unrelated panic.
        self.until.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// 一个账号池的 403 冷却记录, 键为账号名.
///
/// The 403 cooldown records of one account pool, keyed by account name.
#[derive(Default)]
pub(crate) struct Cooldown {
    inner: TimedRegistry<String>,
}

impl Cooldown {
    /// 让账号从 now 起冷却 ACCOUNT_COOLDOWN.
    ///
    /// Starts an ACCOUNT_COOLDOWN cooldown for the account from now.
    pub(crate) fn start(&self, name: &str, now: Instant) {
        self.inner.start(name.to_owned(), ACCOUNT_COOLDOWN, now);
    }

    /// 让账号从 now 起冷却指定的时长.
    ///
    /// Starts a cooldown of the given duration for the account from now.
    pub(crate) fn start_duration(&self, name: &str, duration: Duration, now: Instant) {
        self.inner.start(name.to_owned(), duration, now);
    }

    /// 账号在 now 时是否处于冷却.
    ///
    /// Reports whether the account is cooling down at now.
    pub(crate) fn active(&self, name: &str, now: Instant) -> bool {
        self.inner.active(&name.to_owned(), now)
    }

    /// 返回 now 时仍在冷却的账号及剩余时长, 按账号名排序.
    ///
    /// Returns the accounts still cooling down at now with their remaining time, sorted by name.
    pub(crate) fn remaining(&self, now: Instant) -> Vec<(String, Duration)> {
        self.inner.remaining(now)
    }
}

/// 账号与模型不可用记录的键.
///
/// The composite key for an account and model unavailability record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ModelKey {
    pub(crate) name: String,
    pub(crate) model: String,
}

/// Antigravity 账号池的模型不可用记录, 键为账号名与上游模型名.
///
/// Model unavailability records for the Antigravity account pool, keyed by account name and upstream model name.
#[derive(Default)]
pub(crate) struct UnavailableModels {
    inner: TimedRegistry<ModelKey>,
}

impl UnavailableModels {
    /// 记录某账号对某模型从 now 起不可用 MODEL_UNAVAILABLE_DURATION.
    ///
    /// Records that an account cannot serve a model for MODEL_UNAVAILABLE_DURATION starting from now.
    pub(crate) fn start(&self, name: &str, model: &str, now: Instant) {
        self.inner.start(
            ModelKey {
                name: name.to_owned(),
                model: model.to_owned(),
            },
            MODEL_UNAVAILABLE_DURATION,
            now,
        );
    }

    /// 账号与模型组合在 now 时是否被标记为不可用.
    ///
    /// Reports whether the account and model combination is marked unavailable at now.
    pub(crate) fn active(&self, name: &str, model: &str, now: Instant) -> bool {
        self.inner.active(
            &ModelKey {
                name: name.to_owned(),
                model: model.to_owned(),
            },
            now,
        )
    }

    /// 返回 now 时处于不可用状态的账号模型组合及剩余时长, 按键升序排序.
    ///
    /// Returns active unavailable records at now with their remaining time, sorted by key in ascending order.
    pub(crate) fn remaining(&self, now: Instant) -> Vec<(ModelKey, Duration)> {
        self.inner.remaining(now)
    }
}

/// 账号与模型组合不可用的原因, 取值同时写入健康检查.
///
/// The reason one account and model combination is unavailable; the value also appears in the
/// health check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExhaustionReason {
    /// 上游 429 配额耗尽.
    ///
    /// An upstream 429 quota rejection.
    Quota,
    /// 上游 503 容量耗尽.
    ///
    /// An upstream 503 capacity rejection.
    Capacity,
}

impl ExhaustionReason {
    /// 健康检查中使用的取值.
    ///
    /// The value used in the health check.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Quota => "quota_exhausted",
            Self::Capacity => "capacity_exhausted",
        }
    }

    /// 日志中使用的中文名称.
    ///
    /// The Chinese name used in log lines.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Quota => "配额",
            Self::Capacity => "容量",
        }
    }
}

/// 一条额度或容量耗尽记录.
///
/// One quota or capacity exhaustion record.
struct ExhaustionRecord {
    until: Instant,
    reason: ExhaustionReason,
    /// 连续耗尽次数, 决定没有解析值时的退避时长.
    ///
    /// The consecutive rejection count; it decides the backoff duration when no delay can be
    /// parsed.
    consecutive: u32,
}

/// 账号与模型的额度或容量耗尽记录, 键为账号名与上游模型名.
///
/// 上游 429 带有重置时间时按重置时间记录, 503 与不带重置时间的 429 按有上限的指数退避记录.
/// 与 403 冷却和模型不可用记录一样, 记录只影响选号, 不需要人工干预: 期满后账号自动回到
/// 选号范围, 期间该模型上的请求跳过它.
///
/// Quota or capacity exhaustion records for one account and model, keyed by account name and
/// upstream model name.
///
/// A 429 carrying a reset time is recorded until that reset, while a 503 or a 429 without one is
/// recorded for a capped exponential backoff. Like the 403 cooldown and the model unavailability
/// records, the record only affects selection and needs no manual intervention: once it expires the
/// account returns to selection, and requests for that model skip it in the meantime.
#[derive(Default)]
pub(crate) struct ExhaustedModels {
    records: Mutex<HashMap<ModelKey, ExhaustionRecord>>,
}

impl ExhaustedModels {
    /// 记录一次耗尽: 连续计数加一, 时长取解析值或按连续次数退避, 并截断到 EXHAUSTION_MAX_DURATION.
    /// 新记录覆盖同一组合的旧记录. 返回实际记录的时长, 供日志使用.
    ///
    /// Records one exhaustion: the consecutive count grows by one, the duration is the parsed
    /// value or the backoff for that count, and it is cut down to EXHAUSTION_MAX_DURATION. A new
    /// record replaces an older one for the same combination. The recorded duration is returned for
    /// the log line.
    pub(crate) fn record(
        &self,
        name: &str,
        model: &str,
        reason: ExhaustionReason,
        parsed: Option<Duration>,
        now: Instant,
    ) -> Duration {
        let key = ModelKey {
            name: name.to_owned(),
            model: model.to_owned(),
        };
        let mut records = self.records();
        // 连续计数跨过期保留: 记录过期只说明可以再次探测该账号, 上一次失败仍然是同一串连续
        // 失败, 退避因此继续加长, 直到该账号有一次成功.
        //
        // The consecutive count survives expiry: an expired record only makes the account
        // selectable again, the earlier failure still belongs to the same streak, so the backoff
        // keeps growing until the account succeeds once.
        let consecutive = records
            .get(&key)
            .map_or(1, |record| record.consecutive.saturating_add(1));
        let duration = parsed
            .unwrap_or_else(|| exhaustion_backoff(consecutive))
            .min(EXHAUSTION_MAX_DURATION);
        // Instant 加法只会在单调时钟接近上限时溢出, 实际不会发生; 万一发生则不记录,
        // 与 TimedRegistry::start 相同.
        //
        // Adding duration to an Instant overflows only near the end of the monotonic clock, which
        // does not happen in practice; if it ever did, no record is saved, as in
        // TimedRegistry::start.
        if let Some(until) = now.checked_add(duration) {
            records.insert(
                key,
                ExhaustionRecord {
                    until,
                    reason,
                    consecutive,
                },
            );
        }
        duration
    }

    /// 记录一次成功: 清除该账号在该模型上的记录与连续计数.
    ///
    /// Records one success, clearing the record and the consecutive count for that account and
    /// model.
    pub(crate) fn record_success(&self, name: &str, model: &str) {
        self.records().remove(&ModelKey {
            name: name.to_owned(),
            model: model.to_owned(),
        });
    }

    /// 账号与模型组合在 now 时是否处于耗尽记录中, 顺带清除已经失效的记录.
    ///
    /// Reports whether the account and model combination still has an active record at now,
    /// clearing expired records along the way.
    pub(crate) fn active(&self, name: &str, model: &str, now: Instant) -> bool {
        let key = ModelKey {
            name: name.to_owned(),
            model: model.to_owned(),
        };
        let mut records = self.records();
        match records.get(&key) {
            Some(record) if record.until > now => true,
            Some(_) => {
                records.remove(&key);
                false
            }
            None => false,
        }
    }

    /// 全部指定账号在该模型上的最早恢复剩余时长.
    ///
    /// 只要有一个账号没有记录或记录已经过期, 该账号仍可尝试, 因此返回 None.
    ///
    /// The earliest remaining time until recovery across every given account for one model.
    ///
    /// A single account without a record, or whose record has expired, is still worth trying, so
    /// the function returns None in that case.
    pub(crate) fn earliest_recovery(
        &self,
        names: &[String],
        model: &str,
        now: Instant,
    ) -> Option<Duration> {
        let mut records = self.records();
        let mut earliest: Option<Duration> = None;
        for name in names {
            let key = ModelKey {
                name: name.clone(),
                model: model.to_owned(),
            };
            let remaining = match records.get(&key) {
                Some(record) if record.until > now => record.until.saturating_duration_since(now),
                Some(_) => {
                    records.remove(&key);
                    return None;
                }
                None => return None,
            };
            earliest = Some(match earliest {
                Some(current) => current.min(remaining),
                None => remaining,
            });
        }
        earliest
    }

    /// 返回 now 时仍有记录的组合, 原因与剩余时长, 按键升序排序.
    ///
    /// Returns the combinations still recorded at now with their reason and remaining time,
    /// sorted by key in ascending order.
    pub(crate) fn remaining(&self, now: Instant) -> Vec<(ModelKey, ExhaustionReason, Duration)> {
        let mut records = self.records();
        records.retain(|_, record| record.until > now);
        let mut list: Vec<(ModelKey, ExhaustionReason, Duration)> = records
            .iter()
            .map(|(key, record)| {
                (
                    key.clone(),
                    record.reason,
                    record.until.saturating_duration_since(now),
                )
            })
            .collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }

    fn records(&self) -> MutexGuard<'_, HashMap<ModelKey, ExhaustionRecord>> {
        // 持锁期间只读写映射, 不跨越 await, 锁中毒只可能来自无关的 panic.
        //
        // Only the map is accessed under the lock with no await in between, so poisoning can
        // only come from an unrelated panic.
        self.records.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// 连续首字节超时计数与冷却触发器.
///
/// 记录每个账号连续发生首字节超时的次数, 达到阈值时触发冷却, 中间任意一次成功清零.
///
/// Consecutive first-byte timeout tracker and cooldown trigger.
/// Tracks the number of consecutive timeouts per account, triggers cooldown upon reaching the
/// threshold, and clears on any success.
pub(crate) struct TimeoutCooldownTracker {
    threshold: u32,
    duration: Duration,
    failures: Mutex<HashMap<String, u32>>,
}

impl TimeoutCooldownTracker {
    pub(crate) fn new(threshold: u32, duration: Duration) -> Self {
        Self {
            threshold,
            duration,
            failures: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn threshold(&self) -> u32 {
        self.threshold
    }

    pub(crate) fn duration(&self) -> Duration {
        self.duration
    }

    /// 记录一次成功, 清除该账号的连续超时计数.
    ///
    /// Records a success, clearing the consecutive timeout count for the account.
    pub(crate) fn record_success(&self, name: &str) {
        let mut map = self.failures.lock().unwrap_or_else(PoisonError::into_inner);
        map.remove(name);
    }

    /// 记录一次首字节超时, 达到阈值时触发冷却并清零计数.
    ///
    /// Records a first-byte timeout, triggers cooldown and clears the counter when reaching the threshold.
    pub(crate) fn record_timeout(&self, name: &str, cooldown: &Cooldown, now: Instant) -> bool {
        let mut map = self.failures.lock().unwrap_or_else(PoisonError::into_inner);
        let count = map.entry(name.to_owned()).or_insert(0);
        *count += 1;
        if *count >= self.threshold {
            *count = 0;
            cooldown.start_duration(name, self.duration, now);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_models_track_parsed_reset_and_recover_on_expiry() {
        let exhausted = ExhaustedModels::default();
        let now = Instant::now();
        assert!(!exhausted.active("a", "model-x", now));

        let parsed = Duration::from_secs(90);
        assert_eq!(
            exhausted.record("a", "model-x", ExhaustionReason::Quota, Some(parsed), now),
            parsed
        );
        assert!(exhausted.active("a", "model-x", now));
        assert!(!exhausted.active("a", "model-y", now), "不同模型互不影响");
        assert!(!exhausted.active("b", "model-x", now), "不同账号互不影响");
        assert_eq!(
            exhausted.remaining(now + Duration::from_secs(30)),
            vec![(
                ModelKey {
                    name: "a".to_owned(),
                    model: "model-x".to_owned()
                },
                ExhaustionReason::Quota,
                Duration::from_secs(60)
            )]
        );
        assert!(!exhausted.active("a", "model-x", now + parsed), "期满恢复");
        assert!(exhausted.remaining(now + parsed).is_empty());
    }
}
