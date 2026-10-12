//! 一次客户端请求的用量观测句柄, 接口见 docs/architecture.md 9.1.
//!
//! 句柄是可以克隆的共享状态: 转发路径上的接收函数持有它来喂入上游事件或响应体字节,
//! 返回给调用方的响应体在结束或被丢弃时调用 finish. 内部状态用 std::sync::Mutex 保护,
//! 所有方法都是同步的, 持锁期间不 await, 因此可以安全地从异步任务的任意位置调用.
//!
//! 每个请求恰好写入一条记录: finish 只生效一次, 句柄最后一个副本被丢弃而 finish 从未
//! 调用时按客户端已断开补记一次. 记录的时机与判定规则沿用 Go 版 relay/usage.go 的
//! usageObservation, 写盘则放进 spawn_blocking, 因为目录锁与 fsync 都是阻塞调用.
//!
//! The usage observation handle for one client request; see docs/architecture.md 9.1.
//!
//! The handle is shareable cloned state: the forwarding path holds it to feed upstream events
//! or response body bytes, and the response body handed to the caller calls finish when it
//! ends or is dropped. Internal state is protected by a std::sync::Mutex, every method is
//! synchronous and no await happens while the lock is held, so the handle is safe to use from
//! any point of an async task.
//!
//! Exactly one record is written per request: finish applies once, and when the last copy of
//! the handle is dropped without finish the request is recorded as if the client had
//! disconnected. Timing and the outcome rules follow usageObservation in Go's
//! relay/usage.go; the write itself goes into spawn_blocking because the directory lock and
//! fsync are blocking calls.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::Utc;
use rand::TryRng;

use super::capture::{Capture, CaptureBody};
use super::store::{Record, Store, Tokens};

/// 请求编号的随机字节数. 16 字节按 RFC 4648 base32 无填充编码正好 26 个字符, 与 Go 的
/// rand.Text 形式一致.
///
/// The number of random bytes in a request id. Sixteen bytes encode to exactly 26 characters
/// in RFC 4648 base32 without padding, matching the form of Go's rand.Text.
const REQUEST_ID_BYTES: usize = 16;

/// 随机源不可用时的退化序列. 编号只用于关联同一次请求的记录与响应头, 不承担安全职责,
/// 因此退化后仍然唯一即可, 不必终止进程.
///
/// The fallback sequence used when the random source is unavailable. The id only correlates a
/// request's record with its response header and carries no security role, so staying unique
/// is enough after the fallback; the process must not abort.
static FALLBACK_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 一次客户端请求的用量观测句柄.
///
/// Usage observation handle for one client request.
#[derive(Clone)]
pub struct Observation {
    /// 请求编号. 放在共享状态之外, 使 id 无需加锁即可返回, 调用方可以直接把它写进响应头.
    ///
    /// The request id, kept outside the shared state so id can be returned without taking the
    /// lock and the caller can write it straight into a response header.
    id: Arc<str>,
    state: Arc<Mutex<State>>,
}

/// 句柄内部的可变状态. 与请求一对一, 最后一个 Arc 引用消失时整体析构.
///
/// The mutable state inside the handle. It belongs to one request and is destroyed as a whole
/// when the last Arc reference goes away.
struct State {
    id: String,
    provider: String,
    /// 客户端请求使用的模型名. 上游报告模型后, 记录里使用上游的取值.
    ///
    /// The model name the client request used. Once upstream reports a model, the record
    /// carries upstream's value.
    model: String,
    account: String,
    status: u16,
    transport_complete: bool,
    write_failed: bool,
    finished: bool,
    /// 采集体按需创建: 转换线路按事件喂入, 透传线路按响应头的内容类型创建.
    ///
    /// The capture body is created on demand: the translated route feeds it events and the
    /// passthrough route creates it with the response's content type.
    capture: Option<CaptureBody>,
    store: Arc<Store>,
    write_failures: Arc<AtomicU64>,
}

impl Observation {
    /// 开始观测一次请求, 生成请求编号.
    ///
    /// Begins observing one request and generates its id.
    pub fn start(
        store: Arc<Store>,
        write_failures: Arc<AtomicU64>,
        provider: &str,
        model: &str,
    ) -> Observation {
        let id = request_id();
        Observation {
            id: Arc::from(id.as_str()),
            state: Arc::new(Mutex::new(State {
                id,
                provider: provider.to_owned(),
                model: model.to_owned(),
                account: String::new(),
                status: 0,
                transport_complete: false,
                write_failed: false,
                finished: false,
                capture: None,
                store,
                write_failures,
            })),
        }
    }

    /// 返回请求编号, 调用方把它写入 X-Relay-Request-Id 响应头.
    ///
    /// Returns the request id; the caller writes it into the X-Relay-Request-Id response
    /// header.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 记录本次请求最终使用的账号名.
    ///
    /// Records the account name the request finally used.
    pub fn set_account(&self, name: &str) {
        self.state().account = name.to_owned();
    }

    /// 喂入转换线路上游事件的载荷.
    ///
    /// 载荷来自 aria_core::sse 的事件解析, 而采集体按 SSE 帧读取, 因此这里补回字段名与
    /// 分隔空行. 载荷内部的换行在原始事件里同样由 data 行承载, 逐行补回后的解析结果与
    /// 原事件一致.
    ///
    /// Feeds the payload of one upstream event on a translated route.
    ///
    /// The payload comes from aria_core::sse, while the capture body reads SSE frames, so the
    /// field name and the separating blank line are restored here. Newlines inside the payload
    /// are carried by data lines in the original event as well, so restoring them line by line
    /// parses back to the same payload.
    pub fn capture_event(&self, payload: &[u8]) {
        let mut state = self.state();
        state.ensure_capture("text/event-stream");
        state.feed(&sse_frame(payload));
    }

    /// 透传线路按响应的内容类型创建采集体. 只生效一次, 之后重复调用不再改变内容类型.
    ///
    /// A passthrough route creates the capture body with the response's content type. It
    /// applies once; later calls no longer change the content type.
    pub fn begin_body(&self, content_type: &str) {
        self.state().ensure_capture(content_type);
    }

    /// 喂入一段响应体字节.
    ///
    /// Feeds one chunk of response body bytes.
    pub fn observe_chunk(&self, chunk: &[u8]) {
        let mut state = self.state();
        if state.capture.is_some() {
            state.feed(chunk);
        }
    }

    /// 声明响应体已经结束, 使最后一条没有尾部空行的事件也能被解析.
    ///
    /// Declares that the response body has ended, so a final event without a trailing blank
    /// line is still parsed.
    pub fn finish_chunks(&self) {
        if let Some(body) = self.state().capture.as_mut() {
            body.finish();
        }
    }

    /// 记录回给客户端的 HTTP 状态码. 与 Go 版的响应写入器一致, 只有第一次取值生效.
    ///
    /// Records the HTTP status returned to the client. Like Go's response writer, only the
    /// first value applies.
    pub fn set_status(&self, status: u16) {
        let mut state = self.state();
        if state.status == 0 {
            state.status = status;
        }
    }

    /// 记录上游传输是否正常结束.
    ///
    /// Records whether the upstream transfer ended normally.
    pub fn set_transport_complete(&self, complete: bool) {
        self.state().transport_complete = complete;
    }

    /// 记录向客户端写出时的失败. 由处理客户端连接的一侧在写响应体失败时调用.
    ///
    /// Records a failure while writing to the client, called by the side handling the client
    /// connection when writing the response body fails.
    pub fn mark_write_failed(&self) {
        self.state().write_failed = true;
    }

    /// 结束观测并写入记录. client_cancelled 表示客户端在响应完成前离开. 重复调用不再写入,
    /// 因此内部换号与重试只产生一条记录.
    ///
    /// Ends the observation and writes the record. client_cancelled reports that the client
    /// left before the response completed. Repeated calls write nothing further, so internal
    /// rotation and retries produce exactly one record.
    pub fn finish(&self, client_cancelled: bool) {
        let completed = {
            let mut state = self.state();
            if state.finished {
                return;
            }
            state.finished = true;
            state.complete(client_cancelled)
        };
        completed.persist();
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // 采集与记录都是纯内存操作, 没有跨 await 的持锁窗口, 因此锁中毒只可能来自与之
        // 无关的 panic; 继续使用内部状态比让整个请求失败更合适.
        //
        // Capture and recording are pure memory operations and no lock is held across an
        // await, so poisoning can only come from an unrelated panic; continuing with the inner
        // state is better than failing the whole request.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    /// 创建采集体. 内容类型只取第一次调用传入的取值.
    ///
    /// Creates the capture body. The content type is taken from the first call only.
    fn ensure_capture(&mut self, content_type: &str) {
        if self.capture.is_none() {
            let capture = Capture::new(self.provider.clone(), self.model.clone());
            self.capture = Some(CaptureBody::new(capture, content_type));
        }
    }

    fn feed(&mut self, bytes: &[u8]) {
        if let Some(body) = self.capture.as_mut() {
            body.observe(bytes);
        }
    }

    /// 生成本次观测的收尾结果, 收集记录与采集错误.
    ///
    /// Builds the completion result of this observation, gathering the record and the capture
    /// error.
    fn complete(&self, client_cancelled: bool) -> Completed {
        Completed {
            record: self.build_record(client_cancelled),
            capture_error: self
                .capture
                .as_ref()
                .map(CaptureBody::capture)
                .and_then(Capture::error),
            store: self.store.clone(),
            write_failures: self.write_failures.clone(),
        }
    }

    /// 按 Go 版 usageObservation.finish 的规则生成记录.
    ///
    /// Builds the record by the rules of Go's usageObservation.finish.
    fn build_record(&self, client_cancelled: bool) -> Record {
        // 上游报告过模型时以它为准, 否则沿用客户端请求的模型名.
        //
        // Upstream's model wins when it reported one; otherwise the client's model name is
        // kept.
        let model = match self.capture.as_ref().map(CaptureBody::capture) {
            Some(capture) => capture.model().to_owned(),
            None => self.model.clone(),
        };
        let usage: Option<Tokens> = self
            .capture
            .as_ref()
            .map(CaptureBody::capture)
            .and_then(Capture::usage);
        let capture_outcome = match self.capture.as_ref().map(CaptureBody::capture) {
            Some(capture) => capture.outcome().to_owned(),
            None => String::new(),
        };
        // 判定用响应写入器观察到的状态码: 没有写出任何响应时它是 0, 此时既不构成错误状态,
        // 也不满足正常完成的条件, 结果留在 incomplete.
        //
        // The outcome decision uses the status the response writer observed. It is 0 when no
        // response was written; that is neither an error status nor a normal completion, so
        // the outcome stays incomplete.
        let written = self.status;
        let outcome = if written >= 400 || capture_outcome == "error" {
            "error"
        } else if self.transport_complete
            && !self.write_failed
            && !client_cancelled
            && capture_outcome == "completed"
        {
            "completed"
        } else {
            "incomplete"
        };
        // 没有开始响应就结束时把状态码记为 499, 只用于本地统计表示客户端离开, 不会写回
        // 已经断开的连接.
        //
        // An observation that ends before a response starts records 499, used only for local
        // statistics to mean the client left; it is never written back to an already closed
        // connection.
        let http_status = if written == 0 { 499 } else { written };
        Record {
            id: self.id.clone(),
            finished_at: Utc::now().fixed_offset(),
            provider: self.provider.clone(),
            model,
            account: self.account.clone(),
            http_status: i64::from(http_status),
            outcome: outcome.to_owned(),
            usage,
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        // 句柄的最后一个副本被丢弃而 finish 从未调用时, 说明调用方没有走完任何结束路径,
        // 按客户端已断开记录一次, 使每个请求恰好产生一条记录.
        //
        // When the last copy of the handle is dropped without finish, no completion path ran,
        // so the request is recorded once as if the client had disconnected; every request
        // then produces exactly one record.
        if self.finished {
            return;
        }
        self.finished = true;
        self.complete(true).persist();
    }
}

/// 一次观测的收尾结果.
///
/// The completion result of one observation.
struct Completed {
    record: Record,
    capture_error: Option<super::capture::CaptureError>,
    store: Arc<Store>,
    write_failures: Arc<AtomicU64>,
}

impl Completed {
    /// 记录采集错误并写入记录.
    ///
    /// Logs the capture error and writes the record.
    fn persist(self) {
        if let Some(error) = self.capture_error {
            crate::log!(
                "usage capture unavailable request_id={}: {}",
                self.record.id,
                error
            );
        }
        persist(self.store, self.write_failures, self.record);
    }
}

/// 写入一条记录. 写入失败不重试: 响应已经发出, 重试会在 fsync 失败但记录已落盘时重复
/// 计数; 失败只增加计数并写日志, 查询端据此能区分统计缺口与完整统计.
///
/// Writes one record. A failure is not retried: the response has already been sent and a retry
/// would double-count when fsync failed after the record reached the file. The failure only
/// increments a counter and logs, so a query can tell a gap apart from complete statistics.
fn persist(store: Arc<Store>, write_failures: Arc<AtomicU64>, record: Record) {
    let request_id = record.id.clone();
    let write = move || {
        if let Err(error) = store.record(record) {
            write_failures.fetch_add(1, Ordering::Relaxed);
            crate::log!(
                "usage persistence failed request_id={}: {}",
                request_id,
                error
            );
        }
    };
    // 目录锁, 追加写与 fsync 都是阻塞调用, 因此在运行时内放进 spawn_blocking. 不在运行时
    // 内时 (例如句柄在同步代码里被丢弃) 没有可用的阻塞池, 只能就地写入, 语义保持一致.
    //
    // The directory lock, the append and fsync are blocking calls, so inside a runtime the
    // write goes into spawn_blocking. Outside a runtime (for example when the handle is
    // dropped in synchronous code) no blocking pool exists, so the write happens in place with
    // unchanged semantics.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn_blocking(write);
        }
        Err(_) => write(),
    }
}

/// 生成请求编号: 16 字节随机数的 base32 无填充编码, 26 个 [A-Z2-7] 字符, 与 Go 的
/// rand.Text 形式一致.
///
/// 随机源用 rand 的 SysRng, 取法与 lyra 的 new_identifier 相同: try_fill_bytes 在随机源
/// 不可用时返回错误, 不会终止进程. 退化取值与那里不同: 这里用时间与序列号而不是全零, 因为
/// 编号要用于把响应头与用量记录对上, 重复的编号会让这层对应关系失去意义.
///
/// Generates the request id: a 16-byte random value in base32 without padding, 26 characters from
/// [A-Z2-7], the same form as Go's rand.Text.
///
/// The random source is rand's SysRng and the call is the same as in lyra's new_identifier:
/// try_fill_bytes reports an error instead of aborting the process when the source is unavailable.
/// The fallback value differs: time and a sequence number rather than all zeros, because the id
/// correlates a response header with a usage record and a repeated id would make that correlation
/// meaningless.
fn request_id() -> String {
    let mut bytes = [0u8; REQUEST_ID_BYTES];
    if rand::rngs::SysRng.try_fill_bytes(&mut bytes).is_err() {
        let sequence = FALLBACK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or_default();
        bytes[..8].copy_from_slice(&nanos.to_be_bytes());
        bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    }
    base32_no_pad(&bytes)
}

/// 按 RFC 4648 的字母表做 base32 无填充编码.
///
/// Encodes base32 without padding using the RFC 4648 alphabet.
fn base32_no_pad(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let mut buffer = 0u32;
    let mut bits = 0u32;
    for byte in bytes {
        buffer = (buffer << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            // 缓冲里最多保留 12 位, 移位与掩码都在 u32 范围内, 不会溢出.
            //
            // At most 12 bits stay buffered, so the shift and mask stay inside u32 and cannot
            // overflow.
            out.push(char::from(ALPHABET[((buffer >> bits) & 0x1f) as usize]));
        }
    }
    if bits > 0 {
        out.push(char::from(
            ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize],
        ));
    }
    out
}

/// 把一条事件载荷重新封装成 SSE data 帧.
///
/// Wraps one event payload back into an SSE data frame.
fn sse_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(payload.len() + 16);
    for line in payload.split(|byte| *byte == b'\n') {
        frame.extend_from_slice(b"data: ");
        frame.extend_from_slice(line);
        frame.push(b'\n');
    }
    frame.push(b'\n');
    frame
}
