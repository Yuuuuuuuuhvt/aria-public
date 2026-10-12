//! 增量 SSE 解析器, 供协议转换读取上游事件.
//!
//! 调用方喂入任意切分的字节块, 解析器产出完整事件的 data 载荷. 事件之间以空行分隔,
//! 多个 data 行以换行拼成一条完整载荷; 其他字段行忽略, 没有 data 的空事件跳过.
//! 单行上限 8 MiB, 该上限约束的是单行长度, 由多行拼接成的事件总长度不受它限制, 与
//! Go 版 relay/sse.go 的 sseReader 相同.
//!
//! Incremental SSE parser used by the protocol translation to read upstream events.
//!
//! The caller feeds arbitrarily split byte chunks and the parser yields the data payload
//! of complete events. Events are separated by blank lines and multiple data lines are
//! joined with newlines into one payload; other field lines are ignored and empty events
//! without data are skipped. A single line is capped at 8 MiB; that cap bounds the line
//! length, so the total length of an event assembled from several lines is not covered by
//! it, matching the sseReader in the Go relay/sse.go.

use std::fmt;

use bytes::Bytes;

/// 单行上限, 与 Go 版 bufio.Scanner 的 8 MiB 缓冲上限相同.
///
/// The per-line cap, matching the 8 MiB buffer limit of the Go bufio.Scanner.
const MAX_LINE_BYTES: usize = 8 << 20;

/// 解析过程中出现的错误.
///
/// Errors raised while parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseError {
    /// 单行超过 8 MiB. 与 Go 版 Scanner 一样, 解析器此后不再可用.
    ///
    /// A single line exceeds 8 MiB. As with the Go Scanner, the parser is unusable
    /// afterwards.
    LineTooLong,
}

impl fmt::Display for SseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SseError::LineTooLong => write!(formatter, "SSE 单行超过 {MAX_LINE_BYTES} 字节"),
        }
    }
}

impl std::error::Error for SseError {}

/// 增量 SSE 解析器.
///
/// 输入结束时调用 leftover 取出尚未以空行终止的残留载荷, 上游直接关闭连接时最后一条
/// 事件可能没有尾部空行, 调用方据此判断响应是否完整.
///
/// Incremental SSE parser.
///
/// When the input ends, leftover returns the trailing payload that was not terminated by
/// a blank line. When upstream closes the connection directly the last event may lack a
/// trailing blank line, and the caller uses this to judge completeness.
pub struct SseParser {
    /// 尚未遇到换行的当前行.
    ///
    /// The current line, which has not yet seen a newline.
    line: Vec<u8>,
    /// 当前事件已积累的 data 载荷.
    ///
    /// The data payload accumulated for the current event.
    payload: Vec<u8>,
    /// 解析失败后置位, 后续调用直接返回同一错误.
    ///
    /// Set after a parse failure so later calls return the same error.
    failed: bool,
}

impl Default for SseParser {
    fn default() -> Self {
        SseParser::new()
    }
}

impl SseParser {
    /// 创建解析器.
    ///
    /// Creates a parser.
    pub fn new() -> SseParser {
        SseParser {
            line: Vec::new(),
            payload: Vec::new(),
            failed: false,
        }
    }

    /// 喂入一块字节, 返回本块中完成的全部事件载荷.
    ///
    /// 块边界可以落在任意位置, 包括 data 前缀中间与 CRLF 的两个字节之间.
    ///
    /// Feeds one byte chunk and returns every event payload completed by it.
    ///
    /// Chunk boundaries may fall anywhere, including inside a data prefix and between
    /// the two bytes of a CRLF pair.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Bytes>, SseError> {
        if self.failed {
            return Err(SseError::LineTooLong);
        }
        let mut events = Vec::new();
        let mut rest = chunk;
        while let Some(index) = rest.iter().position(|byte| *byte == b'\n') {
            let (head, tail) = rest.split_at(index);
            self.line.extend_from_slice(head);
            rest = &tail[1..];
            if self.line.len() > MAX_LINE_BYTES {
                return Err(self.fail());
            }
            if let Some(event) = self.take_line() {
                events.push(event);
            }
        }
        self.line.extend_from_slice(rest);
        if self.line.len() > MAX_LINE_BYTES {
            return Err(self.fail());
        }
        Ok(events)
    }

    /// 输入结束时取出残留载荷.
    ///
    /// 与 Go 版 sseReader.leftover 一致, 返回已经累积但尚未由空行分派的 data 载荷: 末行以
    /// 换行结束时载荷照常保留, 未以换行结束的最后一行先按完整一行并入载荷. 读取不清空载荷,
    /// 因此可以重复调用, 调用方也可以把同一份结果同时用于采集与完整性判断; 也正因为不清空,
    /// 调用方只能在流结束后调用它, 之后不得再喂入字节.
    ///
    /// Returns the trailing payload once the input has ended.
    ///
    /// Like Go's sseReader.leftover it returns the data payload accumulated but not yet dispatched
    /// by a blank line: the payload is kept when the last line ended with a newline, and a final
    /// line without a newline is folded in as a complete line first. Reading never clears the
    /// payload, so it may be called repeatedly and the same result can serve both the capture and
    /// the completeness decision; for the same reason the caller must only call it once the stream
    /// has ended and must not feed more bytes afterwards.
    pub fn leftover(&mut self) -> Option<Bytes> {
        if self.failed {
            return None;
        }
        self.fold_final_line();
        if self.payload.is_empty() {
            return None;
        }
        Some(Bytes::from(self.payload.clone()))
    }

    /// 把未以换行结束的最后一行按完整一行并入载荷.
    ///
    /// 与 take_line 的区别是不做分派: 输入已经结束, 空行在这里不再构成事件边界. 行只并入一次,
    /// 重复调用不再改变状态.
    ///
    /// Folds the final line that did not end with a newline into the payload as a complete line.
    /// Unlike take_line it never dispatches: the input has ended and a blank line is no longer an
    /// event boundary here. The line is folded in once and a repeated call changes nothing.
    fn fold_final_line(&mut self) {
        if self.line.is_empty() {
            return;
        }
        let mut line = std::mem::take(&mut self.line);
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if let Some(rest) = line.strip_prefix(b"data:".as_slice()) {
            // 只去掉一个前导空格, 其余空格属于载荷.
            //
            // Strip exactly one leading space; any further space belongs to the payload.
            let rest = rest.strip_prefix(b" ").unwrap_or(rest);
            if !self.payload.is_empty() {
                self.payload.push(b'\n');
            }
            self.payload.extend_from_slice(rest);
        }
    }

    /// 处理已经完整到达的一行, 返回该行终止的事件.
    ///
    /// Processes one complete line and returns the event that line terminates.
    fn take_line(&mut self) -> Option<Bytes> {
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        if self.line.is_empty() {
            if self.payload.is_empty() {
                return None;
            }
            return Some(Bytes::from(std::mem::take(&mut self.payload)));
        }
        if let Some(rest) = self.line.strip_prefix(b"data:".as_slice()) {
            // 只去掉一个前导空格, 其余空格属于载荷.
            //
            // Strip exactly one leading space; any further space belongs to the payload.
            let rest = rest.strip_prefix(b" ").unwrap_or(rest);
            if !self.payload.is_empty() {
                self.payload.push(b'\n');
            }
            self.payload.extend_from_slice(rest);
        }
        self.line.clear();
        None
    }

    /// 记录解析失败并返回错误.
    ///
    /// Records the parse failure and returns the error.
    fn fail(&mut self) -> SseError {
        self.failed = true;
        SseError::LineTooLong
    }
}
