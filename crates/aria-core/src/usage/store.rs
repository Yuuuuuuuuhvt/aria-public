//! 按日 JSONL 的用量存储与查询. 格式, 目录锁, 文件权限, fsync 规则与查询语义完全
//! 沿用 Go 版 Lyra 的 internal/usage/store.go, 服务器上已有的记录必须可以继续读取.
//!
//! 时间字段按 RFC 3339 读写, 可带纳秒与时区; 统计口径固定为 UTC+08, 名称为
//! Asia/Shanghai, 不依赖主机是否安装时区数据库.
//!
//! 全部方法是同步的. 调用方在异步运行时中必须放进 spawn_blocking, 因为目录锁,
//! fsync 与逐行读取都是阻塞系统调用.
//!
//! Daily JSONL usage storage and queries. The format, directory lock, file permissions,
//! fsync rules and query semantics follow Go Lyra's internal/usage/store.go exactly, so
//! records already present on a server stay readable.
//!
//! Time fields are read and written as RFC 3339 with optional nanoseconds and timezone.
//! The aggregation convention is fixed to UTC+08 under the name Asia/Shanghai and does
//! not depend on a timezone database being installed on the host.
//!
//! Every method is synchronous. Callers inside an async runtime must place them in
//! spawn_blocking, because the directory lock, fsync and line-by-line reads are all
//! blocking system calls.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{File, OpenOptions, Permissions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// 统计口径使用的时区名称. 对外出现在查询结果里, 也是固定偏移的来源.
///
/// The timezone name used by the aggregation convention. It appears in query results and
/// is the source of the fixed offset.
pub const TIMEZONE: &str = "Asia/Shanghai";

const DATE_FORMAT: &str = "%Y-%m-%d";
const MAX_RECORD_BYTES: usize = 64 << 10;
const MAX_METADATA_BYTES: usize = 4096;

/// 统计口径固定使用 UTC+08. 28800 秒落在 FixedOffset 的合法区间内, 因此取值在编译期
/// 就已确定; 若改成越界取值, 编译会直接失败而不是留下运行期分支.
///
/// The aggregation convention fixes UTC+08. 28800 seconds lies inside FixedOffset's valid
/// range, so the value is decided at compile time; an out-of-range value would fail the
/// build instead of leaving a runtime branch.
const SHANGHAI_OFFSET: FixedOffset = match FixedOffset::east_opt(8 * 60 * 60) {
    Some(offset) => offset,
    None => unreachable!(),
};

/// Go 的 time.Time 零值, 即公元 1 年 1 月 1 日 UTC. 反序列化时用它表示字段缺失,
/// 校验时用它判断必填字段是否存在, 与 Go 的 IsZero 一致.
///
/// Go's zero time.Time, that is 1 January of year 1 UTC. Deserialization uses it to mark
/// a missing field and validation uses it to detect one, matching Go's IsZero.
fn zero_time() -> DateTime<FixedOffset> {
    static ZERO: OnceLock<DateTime<FixedOffset>> = OnceLock::new();
    *ZERO.get_or_init(|| {
        // 该字面量是合法的 RFC 3339 时间, 因此取不到 None; 保留的回退分支只是为了让
        // 函数不依赖会 panic 的取值方式.
        //
        // The literal is a valid RFC 3339 timestamp, so it never fails to parse; the
        // fallback only exists so the function needs no panicking accessor.
        DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z")
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH.fixed_offset())
    })
}

/// 解析记录与元数据里的时间字段, 严格程度与 Go 的 time.Time.UnmarshalJSON 对齐.
///
/// Go 用 time.Parse(time.RFC3339, ...) 解析, 要求 T 分隔, 月日时分秒都补零, 时区要么
/// 是 Z 要么带冒号; chrono 的 RFC 3339 解析同时还接受空格分隔, 不补零的月日与缺少冒号
/// 的时区, 直接使用会让一行 Go 判为损坏的记录被正常汇总. 因此先按 Go 的形状校验, 再交给
/// chrono 解析并校验取值范围.
///
/// Parses the time fields of records and metadata with the strictness of Go's
/// time.Time.UnmarshalJSON.
///
/// Go parses with time.Parse(time.RFC3339, ...), which requires the T separator,
/// zero-padded month, day, hour, minute and second, and a timezone that is either Z or
/// carries a colon; chrono's RFC 3339 parser additionally accepts a space separator,
/// unpadded month and day, and a timezone without a colon, so using it directly would sum a
/// line that Go declares corrupt. The shape is therefore checked first and chrono then
/// parses it and validates the ranges.
mod strict_time {
    use super::{DateTime, FixedOffset};
    use serde::{Deserialize, Deserializer};

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        parse(&text).map_err(serde::de::Error::custom)
    }

    fn parse(text: &str) -> Result<DateTime<FixedOffset>, String> {
        if !has_go_rfc3339_shape(text) {
            return Err(format!("invalid RFC 3339 time {text}"));
        }
        // Go 的 Parse 在解析时也接受逗号作为小数分隔符, chrono 只接受句点.
        //
        // Go's Parse also accepts a comma as the fractional separator when parsing, while
        // chrono accepts only a period.
        let normalized = text.replace(',', ".");
        DateTime::parse_from_rfc3339(&normalized).map_err(|error| error.to_string())
    }

    /// 校验 Go 的 RFC 3339 形状: 2006-01-02T15:04:05[.小数](Z|±HH:MM).
    ///
    /// Checks Go's RFC 3339 shape: 2006-01-02T15:04:05[.fraction](Z|±HH:MM).
    fn has_go_rfc3339_shape(text: &str) -> bool {
        let bytes = text.as_bytes();
        // 最短形式 "2006-01-02T15:04:05Z" 共 20 个字节.
        //
        // The shortest form "2006-01-02T15:04:05Z" is 20 bytes.
        if bytes.len() < 20 {
            return false;
        }
        for (index, expected) in [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')] {
            if bytes[index] != expected {
                return false;
            }
        }
        for index in [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
            if !bytes[index].is_ascii_digit() {
                return false;
            }
        }
        let mut rest = &bytes[19..];
        if matches!(rest.first(), Some(b'.') | Some(b',')) {
            rest = &rest[1..];
            let digits = rest.iter().take_while(|byte| byte.is_ascii_digit()).count();
            if digits == 0 {
                return false;
            }
            rest = &rest[digits..];
        }
        match rest {
            [b'Z'] => true,
            [b'+' | b'-', hour_1, hour_2, b':', minute_1, minute_2] => {
                [hour_1, hour_2, minute_1, minute_2]
                    .iter()
                    .all(|byte| byte.is_ascii_digit())
            }
            _ => false,
        }
    }
}

/// 错误的分类. 与 Go 版的哨兵错误一一对应, 调用方按分类判断, 不比较错误文本.
///
/// The error categories, one for one with the Go sentinel errors. Callers branch on the
/// category and never compare error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// 对应 ErrInvalidFilter.
    InvalidFilter,
    /// 对应 ErrInvalidRecord.
    InvalidRecord,
    /// 对应 ErrClosed.
    Closed,
    /// 对应 ErrLocked.
    Locked,
    /// 对应 ErrCorruptStore.
    CorruptStore,
    /// 对应 ErrWriteUncertain.
    WriteUncertain,
    /// 汇总计数超出 int64 容量, 对应 store.go 中 Totals.add 的容量检查.
    Overflow,
    /// 打开目录或读写文件时的系统错误.
    Io,
    /// 调用方传入的统计目录为空.
    EmptyDirectory,
}

/// 统计存储返回的错误.
///
/// An error returned by the usage store.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<io::Error>,
}

impl Error {
    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    fn io(message: String, source: io::Error) -> Self {
        Self {
            kind: ErrorKind::Io,
            message,
            source: Some(source),
        }
    }

    /// 返回错误分类.
    ///
    /// Returns the error category.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// 保留分类, 在消息前加上失败位置, 供日志定位到具体日文件与行号.
    ///
    /// Keeps the category and prefixes the message with the failure location, so a log
    /// line points at the daily file and line number.
    fn with_context(self, context: &str) -> Self {
        Self {
            kind: self.kind,
            message: format!("{context}: {}", self.message),
            source: self.source,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|error| error as &(dyn std::error::Error + 'static))
    }
}

/// Tokens 的输入已经包含缓存, 输出已经包含思考. 缓存和思考字段只作明细, 汇总时
/// 不能再次加到输入或输出. total_tokens 保留上游报告的总数.
///
/// In Tokens, input already includes cache and output already includes thinking. The
/// cache and thinking fields are detail only and must not be added to input or output
/// again during aggregation. total_tokens keeps the total reported by upstream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Tokens {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_input_tokens: i64,
    pub cache_write_input_tokens: i64,
    pub reasoning_output_tokens: i64,
    pub total_tokens: i64,
    /// Qoder 网关返回的实际积分数值。
    ///
    /// The actual credit value returned by the Qoder gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credits: Option<f64>,
    /// Qoder 网关返回的原始积分数值。
    ///
    /// The original credit value returned by the Qoder gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_credits: Option<f64>,
    /// Qoder 网关返回的 billable 标志。
    ///
    /// The billable flag returned by the Qoder gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billable: Option<bool>,
}

/// Record 对应一次客户端请求的最终结果. 内部换号重试不单独生成记录.
/// usage 为 null 表示没有拿到用量, 非 null 的零值表示上游明确报告零用量.
///
/// Record corresponds to the final result of one client request. Internal rotation
/// retries do not generate separate records. A null usage means no usage was obtained,
/// while a non-null zero value means upstream explicitly reported zero usage.
///
/// 反序列化按 Go 的 json.Unmarshal 语义处理缺失字段: 缺失即为零值, 由校验函数判断
/// 必填字段. finished_at 缺失时取 Go 的零值时间, 因此校验会以同样的理由拒绝它.
///
/// Deserialization follows Go's json.Unmarshal for missing fields: a missing field is the
/// zero value and validation decides whether it is required. A missing finished_at takes
/// Go's zero time, so validation rejects it for the same reason.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    #[serde(default)]
    pub id: String,
    #[serde(default = "zero_time", deserialize_with = "strict_time::deserialize")]
    pub finished_at: DateTime<FixedOffset>,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub account: String,
    #[serde(default)]
    pub http_status: i64,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub usage: Option<Tokens>,
}

/// Filter 的日期包含首尾两天, 单次查询最多覆盖 366 天. 两个日期均省略时查询今日,
/// 只有 from 时查询至今日, 只有 to 时查询该日.
///
/// Filter dates include both ends and one query covers at most 366 days. When both dates
/// are omitted it queries today; with only from it queries through today; with only to it
/// queries that day.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    pub from: String,
    pub to: String,
    pub group_by: Vec<String>,
    pub model: String,
    pub account: String,
    pub provider: String,
}

/// Totals 是一次查询或一个分组内各项计数的合计.
///
/// Totals is the sum of the counters within one query or one group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct Totals {
    pub requests: i64,
    pub completed: i64,
    pub error: i64,
    pub incomplete: i64,
    pub usage_reported: i64,
    pub usage_missing: i64,
    #[serde(flatten)]
    pub tokens: Tokens,
}

impl Totals {
    /// 把一个记录累加到合计里. 任一计数超出 int64 时返回 Overflow, 与 store.go 的
    /// math.MaxInt64 检查对应.
    ///
    /// Adds one record to the totals. Any counter beyond int64 returns Overflow, matching
    /// store.go's math.MaxInt64 check.
    fn add(&mut self, record: &Record) -> Result<(), Error> {
        let mut increment = Self {
            requests: 1,
            ..Self::default()
        };
        match record.outcome.as_str() {
            "completed" => increment.completed = 1,
            "error" => increment.error = 1,
            "incomplete" => increment.incomplete = 1,
            _ => {}
        }
        match &record.usage {
            Some(usage) => {
                increment.usage_reported = 1;
                increment.tokens = *usage;
            }
            None => increment.usage_missing = 1,
        }
        let counters = [
            (&mut self.requests, increment.requests),
            (&mut self.completed, increment.completed),
            (&mut self.error, increment.error),
            (&mut self.incomplete, increment.incomplete),
            (&mut self.usage_reported, increment.usage_reported),
            (&mut self.usage_missing, increment.usage_missing),
            (&mut self.tokens.input_tokens, increment.tokens.input_tokens),
            (
                &mut self.tokens.output_tokens,
                increment.tokens.output_tokens,
            ),
            (
                &mut self.tokens.cache_read_input_tokens,
                increment.tokens.cache_read_input_tokens,
            ),
            (
                &mut self.tokens.cache_write_input_tokens,
                increment.tokens.cache_write_input_tokens,
            ),
            (
                &mut self.tokens.reasoning_output_tokens,
                increment.tokens.reasoning_output_tokens,
            ),
            (&mut self.tokens.total_tokens, increment.tokens.total_tokens),
        ];
        for (counter, amount) in counters {
            *counter = counter.checked_add(amount).ok_or_else(|| {
                Error::new(ErrorKind::Overflow, "usage totals exceed int64 capacity")
            })?;
        }
        Ok(())
    }
}

/// Group 是一个分组维度的合计. 未被选中的维度不出现在 JSON 里, 与 Go 的 omitempty
/// 一致.
///
/// Group is the totals for one grouping dimension. Dimensions that were not selected are
/// absent from the JSON, matching Go's omitempty.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Group {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub day: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub model: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub account: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub provider: String,
    pub totals: Totals,
}

/// Summary 是一次查询的完整返回. groups 恒为数组, 没有匹配记录时是空数组而不是
/// null, 与 Go 的初始化方式一致.
///
/// Summary is the complete return of one query. groups is always an array and is empty
/// rather than null when nothing matches, matching Go's initialization.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Summary {
    pub timezone: String,
    pub collection_started_at: DateTime<FixedOffset>,
    pub from: String,
    pub to: String,
    pub totals: Totals,
    pub groups: Vec<Group>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    #[serde(default = "zero_time", deserialize_with = "strict_time::deserialize")]
    collection_started_at: DateTime<FixedOffset>,
}

struct Inner {
    dir: PathBuf,
    /// 目录描述符只用于 fsync 目录项. close 之后为 None, 此时无法保证持久性.
    ///
    /// The directory descriptor is used only to fsync the directory entry. It is None
    /// after close, when durability can no longer be guaranteed.
    directory: Option<File>,
    lock: Option<File>,
    closed: bool,
    /// 写入持久性不确定后的固定错误消息. 与 Go 一样, 状态不确定时既不接受追加,
    /// 也不返回看似完整的汇总, 直到进程重启重新打开目录.
    ///
    /// The fixed message kept after a write whose durability is uncertain. As in Go, an
    /// uncertain state accepts no further appends and returns no seemingly complete
    /// summary until the process restarts and reopens the directory.
    write_error: Option<String>,
    collection_started_at: DateTime<FixedOffset>,
}

/// Store 使用进程锁禁止多个实例写入同一个目录. 互斥锁覆盖查询和写入, 因此一次查询
/// 不会读到正在追加的半行, 也不会混合不同写入时点的日文件. 历史记录只保存在日文件
/// 中, 内存仅保留当前查询的分组汇总.
///
/// Store uses a process lock to prevent several instances from writing the same
/// directory. The mutex covers queries and writes, so a query never reads a half-written
/// line or mixes daily files from different write instants. History lives only in the
/// daily files, and memory holds only the group totals of the current query.
pub struct Store {
    inner: Mutex<Inner>,
}

impl Store {
    /// 打开或创建统计目录, 并取得目录级进程锁. 打开过程只做权限收紧与元数据校验,
    /// 不创建日文件.
    ///
    /// Opens or creates the statistics directory and takes the directory-level process
    /// lock. Opening only tightens permissions and validates metadata; it creates no
    /// daily file.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = dir.as_ref();
        if dir.as_os_str().is_empty() || dir.to_str().is_some_and(|text| text.trim().is_empty()) {
            return Err(Error::new(
                ErrorKind::EmptyDirectory,
                "usage directory is empty",
            ));
        }
        std::fs::create_dir_all(dir)
            .map_err(|error| Error::io(format!("create usage directory: {error}"), error))?;
        let directory = open_directory(dir)
            .map_err(|error| Error::io(format!("open usage directory: {error}"), error))?;
        directory
            .set_permissions(Permissions::from_mode(0o700))
            .map_err(|error| Error::io(format!("protect usage directory: {error}"), error))?;
        let lock = open_lock_file(&dir.join(".lock"))
            .map_err(|error| Error::io(format!("open usage lock: {error}"), error))?;
        require_regular(&lock)
            .map_err(|error| Error::io(format!("open usage lock: {error}"), error))?;
        lock.set_permissions(Permissions::from_mode(0o600))
            .map_err(|error| Error::io(format!("protect usage lock: {error}"), error))?;
        // std::fs::File::try_lock 自 Rust 1.89 起稳定, 在 Unix 上以 flock 实现, 语义
        // 与 Go 的 syscall.Flock(LOCK_EX|LOCK_NB) 相同: 非阻塞独占, 同一进程内两个
        // 描述符也会互相排斥, 描述符关闭或进程退出时自动释放.
        //
        // std::fs::File::try_lock has been stable since Rust 1.89 and is implemented with
        // flock on Unix, so its semantics match Go's syscall.Flock(LOCK_EX|LOCK_NB): a
        // non-blocking exclusive lock that also excludes a second descriptor in the same
        // process and is released when the descriptor closes or the process exits.
        lock.try_lock().map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => Error::new(
                ErrorKind::Locked,
                format!("usage store is already open: {}", dir.display()),
            ),
            std::fs::TryLockError::Error(error) => {
                Error::io(format!("lock usage directory: {error}"), error)
            }
        })?;
        let mut inner = Inner {
            dir: dir.to_path_buf(),
            directory: Some(directory),
            lock: Some(lock),
            closed: false,
            write_error: None,
            collection_started_at: zero_time(),
        };
        load_or_create_metadata(&mut inner)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    /// 追加一条记录. 返回成功前已经同步文件与目录. 若追加或同步失败, 调用方不能
    /// 盲目重试, 因为原记录可能已经写入. 目录不做 ID 去重, 调用方必须只记录最终
    /// 结果一次.
    ///
    /// Appends one record and has synced the file and directory before returning success.
    /// If the append or sync fails the caller must not retry blindly, because the record
    /// may already be written. The directory does not deduplicate by ID, so the caller
    /// must record the final result exactly once.
    pub fn record(&self, record: Record) -> Result<(), Error> {
        let mut inner = self.lock();
        if inner.closed {
            return Err(Error::new(ErrorKind::Closed, "usage store is closed"));
        }
        if let Some(message) = &inner.write_error {
            return Err(Error::new(ErrorKind::WriteUncertain, message.clone()));
        }
        validate_record(&record)?;
        let mut line = serde_json::to_vec(&record).map_err(|error| {
            Error::new(
                ErrorKind::InvalidRecord,
                format!("invalid usage record: encode record: {error}"),
            )
        })?;
        line.push(b'\n');
        if line.len() > MAX_RECORD_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidRecord,
                format!("invalid usage record: record exceeds {MAX_RECORD_BYTES} bytes"),
            ));
        }
        let name = format!(
            "{}.jsonl",
            record
                .finished_at
                .with_timezone(&SHANGHAI_OFFSET)
                .format(DATE_FORMAT)
        );
        let path = inner.dir.join(&name);
        // append(true) 与 read(true) 合起来就是 Go 的 O_RDWR|O_APPEND|O_CREATE.
        //
        // append(true) together with read(true) is Go's O_RDWR|O_APPEND|O_CREATE.
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
            .map_err(|error| Error::io(format!("open usage journal {name}: {error}"), error))?;
        require_regular(&file)
            .map_err(|error| Error::io(format!("open usage journal {name}: {error}"), error))?;
        file.set_permissions(Permissions::from_mode(0o600))
            .map_err(|error| Error::io(format!("protect usage journal {name}: {error}"), error))?;
        let size = file
            .metadata()
            .map_err(|error| Error::io(format!("inspect usage journal {name}: {error}"), error))?
            .len();
        if size > 0 {
            let mut tail = [0_u8; 1];
            file.read_at(&mut tail, size - 1).map_err(|error| {
                Error::io(format!("read usage journal tail {name}: {error}"), error)
            })?;
            if tail[0] != b'\n' {
                // 原文件保持不变, 管理员可以从末尾字节判断中断位置和恢复范围.
                // The original file is left unchanged so an administrator can judge the
                // interruption point and recovery range from the final byte.
                return Err(Error::new(
                    ErrorKind::CorruptStore,
                    format!("usage journal is corrupt: {name} has an unterminated final record"),
                ));
            }
        }
        let durability =
            write_and_sync(&file, &line).and_then(|()| sync_directory(inner.directory.as_ref()));
        if let Err(error) = durability {
            // 不缓存成功计数. 状态不确定时也禁止查询返回看似完整的汇总.
            // No success count is cached. When the state is uncertain, queries are also
            // forbidden from returning a seemingly complete summary.
            let message = format!("usage journal write durability is uncertain: {name}: {error}");
            inner.write_error = Some(message.clone());
            return Err(Error::new(ErrorKind::WriteUncertain, message));
        }
        Ok(())
    }

    /// 按过滤条件汇总日文件, 返回总数与分组结果.
    ///
    /// Aggregates the daily files by filter and returns totals and groups.
    pub fn query(&self, filter: Filter) -> Result<Summary, Error> {
        let inner = self.lock();
        if inner.closed {
            return Err(Error::new(ErrorKind::Closed, "usage store is closed"));
        }
        if let Some(message) = &inner.write_error {
            return Err(Error::new(ErrorKind::WriteUncertain, message.clone()));
        }
        let (from, to, group_by) = normalize_filter(&filter, Utc::now())?;
        let mut summary = Summary {
            timezone: TIMEZONE.to_owned(),
            collection_started_at: inner.collection_started_at,
            from: from.format(DATE_FORMAT).to_string(),
            to: to.format(DATE_FORMAT).to_string(),
            totals: Totals::default(),
            groups: Vec::new(),
        };
        let mut groups: HashMap<[String; 4], Totals> = HashMap::new();
        let mut day = from;
        while day <= to {
            let date = day.format(DATE_FORMAT).to_string();
            inner.read_day(&date, &mut |record: Record| {
                if (!filter.model.is_empty() && record.model != filter.model)
                    || (!filter.account.is_empty() && record.account != filter.account)
                    || (!filter.provider.is_empty() && record.provider != filter.provider)
                {
                    return Ok(());
                }
                summary.totals.add(&record)?;
                let mut key: [String; 4] = Default::default();
                for dimension in &group_by {
                    match dimension.as_str() {
                        "day" => key[0] = date.clone(),
                        "model" => key[1] = record.model.clone(),
                        "account" => key[2] = record.account.clone(),
                        "provider" => key[3] = record.provider.clone(),
                        _ => {}
                    }
                }
                groups.entry(key).or_default().add(&record)
            })?;
            day += chrono::Duration::days(1);
        }
        let mut keys: Vec<[String; 4]> = groups.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let totals = groups.get(&key).copied().unwrap_or_else(Totals::default);
            summary.groups.push(Group {
                day: key[0].clone(),
                model: key[1].clone(),
                account: key[2].clone(),
                provider: key[3].clone(),
                totals,
            });
        }
        Ok(summary)
    }

    /// 关闭存储并释放目录锁. 重复关闭返回成功. 不删除锁文件, 避免尚未释放的旧锁与
    /// 新建文件形成两个互不相干的锁; 操作系统会在描述符关闭或进程退出时释放 flock.
    ///
    /// Closes the store and releases the directory lock. Closing twice succeeds. The lock
    /// file is not deleted, avoiding an old lock that is not yet released and a newly
    /// created file forming two unrelated locks; the operating system releases the flock
    /// when the descriptor closes or the process exits.
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.lock();
        if inner.closed {
            return Ok(());
        }
        inner.closed = true;
        // 直接释放两个描述符即可: flock 随描述符关闭释放, 关闭失败在 Rust 中不可观测.
        //
        // Dropping the two descriptors is enough: the flock is released when the
        // descriptor closes, and a close failure is unobservable in Rust.
        inner.lock = None;
        inner.directory = None;
        Ok(())
    }

    /// 取得内部互斥锁. 持锁期间不会 panic, 因此中毒只可能来自外部线程, 取回内部值
    /// 比放弃已有状态更安全.
    ///
    /// Takes the internal mutex. No panic happens while the lock is held, so poisoning
    /// could only come from an outside thread; recovering the inner value is safer than
    /// discarding the state.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Inner {
    /// 汇总一个日文件. 非阻塞打开让异常 FIFO 可以先通过 fstat 被拒绝, 避免查询一直
    /// 等待管道写入者.
    ///
    /// Aggregates one daily file. A non-blocking open lets an abnormal FIFO be rejected
    /// through fstat first, so a query does not wait forever for a pipe writer.
    fn read_day(
        &self,
        date: &str,
        consume: &mut dyn FnMut(Record) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let name = format!("{date}.jsonl");
        let path = self.dir.join(&name);
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(Error::io(
                    format!("open usage journal {name}: {error}"),
                    error,
                ));
            }
        };
        require_regular(&file)
            .map_err(|error| Error::io(format!("read usage journal {name}: {error}"), error))?;
        let mut reader = BufReader::new(file);
        let mut line_number = 0;
        loop {
            line_number += 1;
            let line = match read_journal_line(&mut reader) {
                JournalLine::Line(line) => line,
                JournalLine::End => return Ok(()),
                JournalLine::Unterminated => {
                    return Err(Error::new(
                        ErrorKind::CorruptStore,
                        format!(
                            "usage journal is corrupt: {name} line {line_number} is unterminated or oversized"
                        ),
                    ));
                }
                JournalLine::Io(error) => {
                    return Err(Error::io(
                        format!("read usage journal {name} line {line_number}: {error}"),
                        error,
                    ));
                }
            };
            let record: Record = serde_json::from_slice(&line).map_err(|_| {
                Error::new(
                    ErrorKind::CorruptStore,
                    format!(
                        "usage journal is corrupt: {name} line {line_number} contains invalid JSON"
                    ),
                )
            })?;
            if let Err(error) = validate_record(&record) {
                // 与 Go 一致: 读取路径上的校验失败归类为目录损坏, 原始原因保留在消息里.
                //
                // As in Go: a validation failure while reading is classified as a corrupt
                // store, with the original reason kept in the message.
                return Err(Error::new(
                    ErrorKind::CorruptStore,
                    format!("usage journal is corrupt: {name} line {line_number}: {error}"),
                ));
            }
            if record
                .finished_at
                .with_timezone(&SHANGHAI_OFFSET)
                .format(DATE_FORMAT)
                .to_string()
                != date
            {
                return Err(Error::new(
                    ErrorKind::CorruptStore,
                    format!(
                        "usage journal is corrupt: {name} line {line_number} belongs to another day"
                    ),
                ));
            }
            consume(record).map_err(|error| {
                error.with_context(&format!(
                    "aggregate usage journal {name} line {line_number}"
                ))
            })?;
        }
    }
}

enum JournalLine {
    Line(Vec<u8>),
    End,
    /// 行没有以换行结束, 或长度超过单条记录上限.
    ///
    /// The line does not end with a newline, or it exceeds the per-record limit.
    Unterminated,
    Io(io::Error),
}

/// 读取一行, 上限为单条记录字节数. 内存占用因此与 Go 的 ReadSlice 一样有界, 超过
/// 上限的损坏行不会被完整读入.
///
/// Reads one line up to the per-record byte limit. Memory stays bounded exactly as with
/// Go's ReadSlice, and a corrupt line beyond the limit is never fully read in.
fn read_journal_line(reader: &mut impl BufRead) -> JournalLine {
    let mut line = Vec::new();
    loop {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return JournalLine::Io(error),
        };
        if available.is_empty() {
            return if line.is_empty() {
                JournalLine::End
            } else {
                JournalLine::Unterminated
            };
        }
        match available.iter().position(|byte| *byte == b'\n') {
            Some(index) => {
                if line.len() + index + 1 > MAX_RECORD_BYTES {
                    return JournalLine::Unterminated;
                }
                line.extend_from_slice(&available[..=index]);
                reader.consume(index + 1);
                return JournalLine::Line(line);
            }
            None => {
                let length = available.len();
                if line.len() + length > MAX_RECORD_BYTES {
                    return JournalLine::Unterminated;
                }
                line.extend_from_slice(available);
                reader.consume(length);
            }
        }
    }
}

fn open_directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
}

fn open_lock_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

fn require_regular(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "journal path is not a regular file",
        ));
    }
    Ok(())
}

/// 写入并同步单个记录行. 单次 write 与 Go 的 file.Write 一样要求完整写入, 部分写入
/// 按失败处理, 因为该行已经落在文件里, 不能重试.
///
/// Writes and syncs one record line. A single write must be complete like Go's
/// file.Write; a partial write is a failure, because the line is already in the file and
/// must not be retried.
fn write_and_sync(file: &File, line: &[u8]) -> io::Result<()> {
    // Write 为 &File 实现, 因此这里重新绑定一个可变句柄而不改动调用方的所有权.
    //
    // Write is implemented for &File, so this rebinds a mutable handle without changing
    // the caller's ownership.
    let mut handle = file;
    let written = handle.write(line)?;
    if written != line.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "failed to write the whole usage record",
        ));
    }
    file.sync_all()
}

/// 同步目录项. 新日文件的目录项必须持久化, 否则文件 Sync 不能保证重启后可找到它.
/// 目录描述符已释放时无法保证持久性, 与 fsync 失败同样处理.
///
/// Syncs the directory entry. The directory entry of a new daily file must be persisted,
/// otherwise the file sync does not guarantee it can be found after a restart. A released
/// directory descriptor cannot guarantee durability and is treated like a failed fsync.
fn sync_directory(directory: Option<&File>) -> io::Result<()> {
    match directory {
        Some(file) => file.sync_all(),
        None => Err(io::Error::other("usage directory descriptor is closed")),
    }
}

/// loadOrCreateMetadata 只能在已经持有目录锁时调用. 统计起点先于任何请求记录持久化,
/// 否则首次启动中断后可能把重新启动时间误当成原始统计起点.
///
/// loadOrCreateMetadata may be called only while the directory lock is held. The
/// collection start is persisted before any request record; otherwise an interrupted
/// first start could make the restart time be mistaken for the original collection start.
fn load_or_create_metadata(inner: &mut Inner) -> Result<(), Error> {
    let path = inner.dir.join("metadata.json");
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(file) => {
            require_regular(&file).map_err(|_| {
                Error::new(
                    ErrorKind::CorruptStore,
                    "usage journal is corrupt: metadata.json is not a regular file",
                )
            })?;
            file.set_permissions(Permissions::from_mode(0o600))
                .map_err(|error| Error::io(format!("protect usage metadata: {error}"), error))?;
            let mut data = Vec::new();
            (&file)
                .take((MAX_METADATA_BYTES + 1) as u64)
                .read_to_end(&mut data)
                .map_err(|error| Error::io(format!("read usage metadata: {error}"), error))?;
            let value: Metadata = match serde_json::from_slice(&data) {
                Ok(value) if data.len() <= MAX_METADATA_BYTES => value,
                _ => {
                    return Err(Error::new(
                        ErrorKind::CorruptStore,
                        "usage journal is corrupt: metadata.json has no valid collection_started_at",
                    ));
                }
            };
            if value.collection_started_at == zero_time() {
                return Err(Error::new(
                    ErrorKind::CorruptStore,
                    "usage journal is corrupt: metadata.json has no valid collection_started_at",
                ));
            }
            inner.collection_started_at = value.collection_started_at;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let entries = std::fs::read_dir(&inner.dir).map_err(|error| {
                Error::io(
                    format!("inspect usage directory before creating metadata: {error}"),
                    error,
                )
            })?;
            for entry in entries {
                let entry = entry.map_err(|error| {
                    Error::io(
                        format!("inspect usage directory before creating metadata: {error}"),
                        error,
                    )
                })?;
                if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                    // 旧记录只能证明请求发生过, 不能证明统计从何时开始或中间是否有缺口.
                    // Old records prove only that requests happened, not when collection
                    // started or whether there are gaps in between.
                    return Err(Error::new(
                        ErrorKind::CorruptStore,
                        "usage journal is corrupt: metadata.json is missing while daily journals exist",
                    ));
                }
            }
            let value = Metadata {
                collection_started_at: Utc::now().fixed_offset(),
            };
            let mut data = serde_json::to_vec(&value).map_err(|error| {
                Error::io(
                    format!("encode usage metadata: {error}"),
                    io::Error::other(error),
                )
            })?;
            data.push(b'\n');
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&path)
                .map_err(|error| Error::io(format!("create usage metadata: {error}"), error))?;
            let durability = write_and_sync(&file, &data)
                .and_then(|()| sync_directory(inner.directory.as_ref()));
            if let Err(error) = durability {
                return Err(Error::new(
                    ErrorKind::WriteUncertain,
                    format!("usage journal write durability is uncertain: metadata.json: {error}"),
                ));
            }
            inner.collection_started_at = value.collection_started_at;
            Ok(())
        }
        Err(error) => Err(Error::io(format!("open usage metadata: {error}"), error)),
    }
}

fn validate_record(record: &Record) -> Result<(), Error> {
    if record.id.is_empty() || record.provider.is_empty() || record.finished_at == zero_time() {
        return Err(Error::new(
            ErrorKind::InvalidRecord,
            "invalid usage record: id, provider and finished_at are required",
        ));
    }
    if record.http_status != 0 && !(100..=599).contains(&record.http_status) {
        return Err(Error::new(
            ErrorKind::InvalidRecord,
            "invalid usage record: invalid http_status",
        ));
    }
    match record.outcome.as_str() {
        "completed" | "error" | "incomplete" => {}
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidRecord,
                "invalid usage record: outcome must be completed, error or incomplete",
            ));
        }
    }
    if let Some(usage) = &record.usage {
        let counts = [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_input_tokens,
            usage.cache_write_input_tokens,
            usage.reasoning_output_tokens,
            usage.total_tokens,
        ];
        if counts.iter().any(|count| *count < 0) {
            return Err(Error::new(
                ErrorKind::InvalidRecord,
                "invalid usage record: token counts cannot be negative",
            ));
        }
    }
    Ok(())
}

/// 解析筛选条件并补齐默认值. 日期解析要求严格的两段两位数字, 因为 Go 的
/// time.ParseInLocation("2006-01-02", ...) 不接受 2026-9-29, 而 chrono 的 %m 与 %d
/// 会接受一位数字.
///
/// Normalizes the filter and fills in defaults. Date parsing requires exactly two digits
/// per part, because Go's time.ParseInLocation("2006-01-02", ...) rejects 2026-9-29 while
/// chrono's %m and %d accept a single digit.
fn normalize_filter(
    filter: &Filter,
    now: DateTime<Utc>,
) -> Result<(NaiveDate, NaiveDate, Vec<String>), Error> {
    let today = now
        .with_timezone(&SHANGHAI_OFFSET)
        .format(DATE_FORMAT)
        .to_string();
    let from_text = if !filter.from.is_empty() {
        filter.from.clone()
    } else if !filter.to.is_empty() {
        filter.to.clone()
    } else {
        today.clone()
    };
    let to_text = if filter.to.is_empty() {
        today
    } else {
        filter.to.clone()
    };
    let invalid_dates = || {
        Error::new(
            ErrorKind::InvalidFilter,
            "invalid usage filter: dates must use YYYY-MM-DD",
        )
    };
    let from = parse_date(&from_text).ok_or_else(invalid_dates)?;
    let to = parse_date(&to_text).ok_or_else(invalid_dates)?;
    if from > to || (to - from).num_days() > 365 {
        return Err(Error::new(
            ErrorKind::InvalidFilter,
            "invalid usage filter: range must contain between 1 and 366 days",
        ));
    }
    let group_by = if filter.group_by.is_empty() {
        vec!["day".to_owned(), "model".to_owned(), "account".to_owned()]
    } else {
        filter.group_by.clone()
    };
    let mut seen: HashSet<&str> = HashSet::new();
    for dimension in &group_by {
        match dimension.as_str() {
            "day" | "model" | "account" | "provider" => {}
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidFilter,
                    format!("invalid usage filter: unknown group_by dimension {other}"),
                ));
            }
        }
        if !seen.insert(dimension.as_str()) {
            return Err(Error::new(
                ErrorKind::InvalidFilter,
                format!("invalid usage filter: repeated group_by dimension {dimension}"),
            ));
        }
    }
    Ok((from, to, group_by))
}

fn parse_date(text: &str) -> Option<NaiveDate> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
    {
        return None;
    }
    NaiveDate::parse_from_str(text, DATE_FORMAT).ok()
}
