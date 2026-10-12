//! 统一的标准错误日志. 每行以本地时区的时间戳开头, 格式与 Go 版 relaylog 相同,
//! 运维脚本和 journalctl 的检索习惯因此保持不变.
//!
//! 日志中不得出现请求体, 成功响应的响应体, 密钥, 令牌或带查询参数的上游地址. 上游错误
//! 响应体只允许用 single_line 去掉回车换行并按调用方的上限截断后记录.
//!
//! Unified stderr logging. Each line starts with a local-time timestamp in the same
//! format as Go's relaylog, so existing scripts and journalctl searches keep working.
//!
//! Logs must never contain request bodies, successful response bodies, keys, tokens, or
//! upstream URLs with query strings. An upstream error body may be recorded only after
//! single_line has dropped carriage returns and newlines and truncated it to the caller's
//! limit.

/// 返回日志使用的时间戳, 例如 2026-10-05T23:10:00+0800.
///
/// Returns the log timestamp, for example 2026-10-05T23:10:00+0800.
pub fn timestamp() -> String {
    chrono::Local::now()
        .format("%Y-%m-%dT%H:%M:%S%z")
        .to_string()
}

/// 把一段文本整理成单行日志文本: 去掉回车与换行, 并按字节上限在字符边界截断.
///
/// 日志按行读取, 上游错误正文与错误文本里可能出现换行, 去掉之后一条记录始终占一行. 截断在
/// 字符边界上取前缀, 因此结果始终是合法 UTF-8. 上游错误体的长度上限由调用方给出.
///
/// Turns a piece of text into single-line log text: carriage returns and newlines are
/// dropped and the result is truncated to the byte limit on a character boundary.
///
/// Logs are read line by line and upstream error bodies or error texts may contain
/// newlines; dropping them keeps one record on one line. The prefix is cut on a character
/// boundary, so the result is always valid UTF-8. The caller supplies the byte limit of the
/// upstream error body.
pub fn single_line(text: &str, limit: usize) -> String {
    let cleaned: String = text
        .chars()
        .filter(|character| *character != '\r' && *character != '\n')
        .collect();
    if cleaned.len() <= limit {
        return cleaned;
    }
    let mut end = limit;
    while !cleaned.is_char_boundary(end) {
        end -= 1;
    }
    cleaned[..end].to_owned()
}

/// 以 format! 语法写一行日志到标准错误.
///
/// Writes one log line to stderr using format! syntax.
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        eprintln!("{} {}", $crate::log::timestamp(), format_args!($($arg)*))
    };
}
