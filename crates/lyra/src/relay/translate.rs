//! Antigravity 线路的协议转换转发, 与 Go 版 relay/stream.go 的 translatedResponse 对应.
//!
//! 上游只提供 Gemini 事件流, 客户端要的是 Anthropic 报文, 因此读取循环多一层转换. 与
//! Go 版一致的关键时序有两处: 首个有效事件到达之前不提交响应头, 使上游在开始生成之前
//! 失败时仍能返回真实状态码; 响应头提交之后遇到失败只能追加一个错误事件, 不能再改状态码,
//! 也不能补发正常结束标记, 否则截断的内容会被客户端当成完整回答.
//!
//! The protocol-converting forward of the Antigravity route, corresponding to translatedResponse
//! in Go's relay/stream.go.
//!
//! Upstream only offers a Gemini event stream while the client wants Anthropic messages, so the
//! read loop carries one conversion layer. Two timings match Go: response headers are not
//! committed before the first valid event, so an upstream failure before generation still
//! returns the real status; and after the headers are committed a failure can only append an
//! error event, never change the status nor emit the normal stop marker, which would let the
//! client take truncated content as a complete answer.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use aria_core::body::{Body, BoxError};
use aria_core::errors::{anthropic_error_body, anthropic_error_type};
use aria_core::sse::SseParser;
use aria_core::usage::Observation;
use bytes::Bytes;
use http::StatusCode;
use http_body::{Body as HttpBody, Frame};

use super::attempt::{CAPACITY_RETRY_TIMEOUT, CapacityRetryGuard};
use super::forward::{BodyEnd, Completion, UpstreamEventError, upstream_event_error};
use crate::antigravity::response::AnthropicTranslator;
use crate::chatgpt::response::ChatGptTranslator;

/// 协议转换器的统合类型, 覆盖 Antigravity 与 ChatGPT 两条线路.
///
/// Unified translator enum covering both Antigravity and ChatGPT routes.
pub(crate) enum RelayTranslator {
    Antigravity(AnthropicTranslator),
    ChatGpt(ChatGptTranslator),
}

impl RelayTranslator {
    pub(crate) fn content_type(&self) -> &'static str {
        match self {
            Self::Antigravity(t) => t.content_type(),
            Self::ChatGpt(t) => t.content_type(),
        }
    }

    pub(crate) fn started(&self) -> bool {
        match self {
            Self::Antigravity(t) => t.started(),
            Self::ChatGpt(t) => t.started(),
        }
    }

    pub(crate) fn begin(&self) -> Result<Vec<u8>, TranslatedFailure> {
        match self {
            Self::Antigravity(t) => t
                .begin()
                .map_err(|error| TranslatedFailure::Translate(error.message().to_owned())),
            Self::ChatGpt(t) => t.begin().map_err(|error| {
                TranslatedFailure::TranslateWithStatus(error.status, error.message().to_owned())
            }),
        }
    }

    pub(crate) fn event(&mut self, payload: &[u8]) -> Result<Vec<u8>, TranslatedFailure> {
        match self {
            Self::Antigravity(t) => t
                .event(payload)
                .map_err(|error| TranslatedFailure::Translate(error.message().to_owned())),
            Self::ChatGpt(t) => t.event(payload).map_err(|error| {
                TranslatedFailure::TranslateWithStatus(error.status, error.message().to_owned())
            }),
        }
    }

    pub(crate) fn end(&mut self) -> Result<Vec<u8>, TranslatedFailure> {
        match self {
            Self::Antigravity(t) => t
                .end()
                .map_err(|error| TranslatedFailure::Translate(error.message().to_owned())),
            Self::ChatGpt(t) => t.end().map_err(|error| {
                TranslatedFailure::TranslateWithStatus(error.status, error.message().to_owned())
            }),
        }
    }
}

/// 提交响应头之前或之后可能出现的失败.
///
/// A failure that may appear before or after the response headers are committed.
pub(crate) enum TranslatedFailure {
    /// 容量重试预算耗尽.
    ///
    /// The capacity retry budget is exhausted.
    CapacityTimeout,
    /// 事件流内部携带的上游错误.
    ///
    /// An upstream error carried inside the event stream.
    Event(UpstreamEventError),
    /// 翻译器拒绝该事件.
    ///
    /// The translator rejected the event.
    Translate(String),
    /// 翻译器拒绝该事件并指明 HTTP 状态码.
    ///
    /// The translator rejected the event with a specific HTTP status code.
    TranslateWithStatus(StatusCode, String),
    /// 上游事件流缺少尾部空行或缺少终止原因.
    ///
    /// The upstream event stream lacks a trailing blank line or a terminal finish reason.
    UnexpectedEof,
    /// 读取上游响应体失败.
    ///
    /// Reading the upstream response body failed.
    Read(String),
}

impl TranslatedFailure {
    /// 回给客户端的错误状态码, 规则与 Go 版 fail 相同: 事件错误保留 400 到 599 的状态码,
    /// 容量预算耗尽用 503, 其余用 502.
    ///
    /// The error status returned to the client, by the rules of Go's fail: an event error keeps
    /// its 400 to 599 status, an exhausted capacity budget uses 503, and everything else uses
    /// 502.
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::CapacityTimeout => StatusCode::SERVICE_UNAVAILABLE,
            Self::Event(error) if (400..=599).contains(&error.status) => {
                StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY)
            }
            Self::TranslateWithStatus(status, _) => *status,
            _ => StatusCode::BAD_GATEWAY,
        }
    }

    /// 错误说明. 会写进日志并放入回给客户端的错误报文.
    ///
    /// The error description. It goes into the log and into the error message returned to the
    /// client.
    pub(crate) fn message(&self) -> String {
        match self {
            Self::CapacityTimeout => CAPACITY_RETRY_TIMEOUT.to_owned(),
            Self::Event(error) => error.to_string(),
            Self::Translate(message)
            | Self::TranslateWithStatus(_, message)
            | Self::Read(message) => message.clone(),
            Self::UnexpectedEof => "unexpected EOF".to_owned(),
        }
    }

    /// 记录失败, 与 Go 版 fail 的日志一致.
    ///
    /// Records the failure, matching the log of Go's fail.
    pub(crate) fn log(&self, credential: &str) {
        aria_core::log!(
            "credential {}: 上游事件流未完成: {}",
            credential,
            self.message()
        );
    }
}

/// 转换转发在事件层面的共享状态.
///
/// 读取与写出分开: 预读阶段借上游响应逐块读取直到可以提交, 提交之后的响应体自己持有这个
/// 状态继续读取. 两者共享同一套解析与转换代码, 因此不存在两份略有差异的实现.
///
/// The state shared by the event layer of the converting forward.
///
/// Reading and writing are separate: the prelude borrows the upstream response and reads chunk
/// by chunk until it can commit, and the response body handed out afterwards owns this state and
/// keeps reading. Both share one parsing and conversion implementation, so no second slightly
/// different copy exists.
pub(crate) struct TranslatedState {
    parser: SseParser,
    /// 已经解析但尚未处理的事件. 一次读取可能拿到多个事件, 预读在提交点返回时必须把剩下的
    /// 事件留给响应体继续处理, 否则同一个字节块里提交点之后的内容会丢失.
    ///
    /// Parsed events not yet handled. One read may yield several events, and when the prelude
    /// returns at the commit point the remaining events must stay for the response body;
    /// otherwise the content after the commit point inside one byte chunk would be lost.
    pending_events: VecDeque<Bytes>,
    translator: RelayTranslator,
    /// 客户端请求是否要求逐事件回写. 为假时由翻译器聚合, 事件内容在 End 时一次性输出.
    ///
    /// Whether the client asked for per-event writes. When false the translator aggregates and
    /// emits the content once at End.
    stream: bool,
    observation: Option<Observation>,
    guard: Arc<CapacityRetryGuard>,
    /// 服务本次请求的账号名, 只用于日志.
    ///
    /// The account serving this request, used only in logs.
    credential: String,
}

/// 一个上游事件的处理结果.
///
/// The result of handling one upstream event.
pub(crate) struct Consumed {
    /// 需要写给客户端的内容.
    ///
    /// The content to write to the client.
    pub(crate) output: Vec<u8>,
    /// 本次事件是否让翻译器首次产生有效候选内容; 为真时调用方先写出起始消息, 然后提交
    /// 响应头.
    ///
    /// Whether this event made the translator produce valid candidate content for the first
    /// time; when true the caller writes the start message first and then commits the response
    /// headers.
    pub(crate) committed: bool,
}

impl TranslatedState {
    pub(crate) fn new(
        translator: RelayTranslator,
        stream: bool,
        observation: Option<Observation>,
        guard: Arc<CapacityRetryGuard>,
        credential: String,
    ) -> Self {
        Self {
            parser: SseParser::new(),
            pending_events: VecDeque::new(),
            translator,
            stream,
            observation,
            guard,
            credential,
        }
    }

    pub(crate) fn content_type(&self) -> &'static str {
        self.translator.content_type()
    }

    /// 读取事件直到首个有效事件到达, 返回需要先写出的内容: 起始消息与首个内容块.
    ///
    /// 非流式请求在 End 时一次性输出, 因此不走这条路, 由 run_buffered 承担.
    ///
    /// Reads events until the first valid one arrives and returns the content to write first:
    /// the start message and the first content block. A non-streaming request emits everything
    /// at End and therefore takes run_buffered instead.
    pub(crate) async fn commit_prelude(
        &mut self,
        response: &mut reqwest::Response,
    ) -> Result<Vec<u8>, TranslatedFailure> {
        loop {
            if let Some(event) = self.take_event() {
                let consumed = self.consume(&event)?;
                if consumed.committed {
                    let begin = self.translator.begin()?;
                    let mut output = begin;
                    output.extend_from_slice(&consumed.output);
                    return Ok(output);
                }
                continue;
            }
            let Some(chunk) = read_chunk(response, &self.guard).await? else {
                // 还没有产生任何内容上游就结束了: 提交 502, 不发出成功的起始消息. 残留载荷仍
                // 交给采集器, 使已经完整读到的尾部用量不因提前失败而丢失.
                //
                // Upstream ended before producing anything: report 502 instead of emitting a
                // successful start message. A trailing payload is still handed to the capture so
                // usage read in full is not lost to the early failure.
                self.read_leftover();
                return Err(TranslatedFailure::UnexpectedEof);
            };
            self.feed(&chunk)?;
        }
    }

    /// 读完整个事件流并聚合成一条完整报文, 供非流式请求使用.
    ///
    /// 提交响应头只发生在聚合完成之后, 因此中途失败仍然返回真实的错误响应.
    ///
    /// Reads the whole event stream and aggregates one complete message for a non-streaming
    /// request. Headers are committed only after aggregation, so a failure in between still
    /// returns a real error response.
    pub(crate) async fn run_buffered(
        &mut self,
        response: &mut reqwest::Response,
    ) -> Result<Vec<u8>, TranslatedFailure> {
        loop {
            while let Some(event) = self.take_event() {
                self.consume(&event)?;
            }
            let Some(chunk) = read_chunk(response, &self.guard).await? else {
                let trailing = self.read_leftover();
                if !self.translator.started() || trailing {
                    return Err(TranslatedFailure::UnexpectedEof);
                }
                return self.translator.end();
            };
            self.feed(&chunk)?;
        }
    }

    /// 解析一块上游字节, 把完成的事件放进待处理队列.
    ///
    /// Parses one chunk of upstream bytes and puts the completed events into the queue.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<(), TranslatedFailure> {
        let events = self
            .parser
            .push(chunk)
            .map_err(|error| TranslatedFailure::Read(error.to_string()))?;
        self.pending_events.extend(events);
        Ok(())
    }

    /// 取出下一个待处理事件.
    ///
    /// Takes the next queued event.
    pub(crate) fn take_event(&mut self) -> Option<Bytes> {
        self.pending_events.pop_front()
    }

    /// 处理一个完整的上游事件.
    ///
    /// 顺序与 Go 版 consume 一致: 先记录用量, 再识别 HTTP 200 之后的错误事件, 最后交给
    /// 翻译器. 首个 Google 事件通常已经带有输入和缓存用量, 先消费它再生成 message_start,
    /// 既保留真实计数, 也无需等待后续内容.
    ///
    /// Handles one complete upstream event. The order matches Go's consume: record usage first,
    /// recognize an error event arriving after an HTTP 200, then hand the event to the
    /// translator. The first Google event usually already carries input and cache usage, so
    /// consuming it before generating message_start keeps the real counts without waiting for
    /// later content.
    pub(crate) fn consume(&mut self, event: &[u8]) -> Result<Consumed, TranslatedFailure> {
        if let Some(observation) = &self.observation {
            observation.capture_event(event);
        }
        if let Some(error) = upstream_event_error(event) {
            return Err(TranslatedFailure::Event(error));
        }
        let output = self.translator.event(event)?;
        if !self.translator.started() {
            return Ok(Consumed {
                output: Vec::new(),
                committed: false,
            });
        }
        if self.guard.progress().is_err() {
            return Err(TranslatedFailure::CapacityTimeout);
        }
        if !self.stream {
            // 非流式请求由翻译器聚合, 事件内容在 End 时一次性输出.
            //
            // A non-streaming request is aggregated by the translator and its content is emitted
            // once at End.
            return Ok(Consumed {
                output: Vec::new(),
                committed: false,
            });
        }
        Ok(Consumed {
            output,
            committed: true,
        })
    }

    /// 上游事件流结束后收尾.
    ///
    /// 原转发协议要求末事件带空行. 即使缺少分隔符而仍判为中断, 也应保存已经完整读取的尾部
    /// 用量; 截断 JSON 不送入采集器, 避免清掉之前的有效快照.
    ///
    /// Finishes after the upstream event stream ends. The original forwarding protocol requires
    /// a trailing blank line after the last event. Even when the missing separator still marks
    /// the stream as interrupted, the fully read trailing usage is saved; truncated JSON is not
    /// fed to the capture so an earlier valid snapshot is not cleared.
    pub(crate) fn finish_upstream(&mut self) -> Result<Vec<u8>, TranslatedFailure> {
        let trailing = self.read_leftover();
        if !self.translator.started() || trailing {
            return Err(TranslatedFailure::UnexpectedEof);
        }
        self.translator.end()
    }

    /// 取出尚未以空行终止的尾部载荷, 同时用于采集与完整性判断.
    ///
    /// 与 Go 版 run 一致, 残留只读取一次: 合法的 JSON 载荷交给采集器, 使最后一条事件即使缺少
    /// 尾部空行也保留已经读到的用量; 截断的 JSON 不送入采集器, 避免清掉之前的有效快照. 返回真
    /// 表示存在残留载荷, 调用方据此把这条流判为中断.
    ///
    /// Reads the trailing payload that was not terminated by a blank line and uses it both for the
    /// capture and for the completeness decision. As in Go's run the payload is read once: a valid
    /// JSON payload goes to the capture so the last event keeps its usage even without the trailing
    /// blank line, while truncated JSON is not fed to the capture so an earlier valid snapshot is
    /// not cleared. A true return means a trailing payload exists and the caller must treat the
    /// stream as interrupted.
    fn read_leftover(&mut self) -> bool {
        let leftover = self.parser.leftover();
        if let (Some(observation), Some(leftover)) = (&self.observation, &leftover)
            && serde_json::from_slice::<serde_json::Value>(leftover).is_ok()
        {
            observation.capture_event(leftover);
        }
        leftover.is_some()
    }
}

/// 读取上游响应体的下一块. 容量预算生效时与截止时刻竞速, 使预算耗尽能够取消正在等待的
/// 读取, 与 Go 版取消请求 context 的效果一致.
///
/// Reads the next chunk of the upstream response body. While the capacity budget is armed the
/// read races the deadline, so an exhausted budget cancels a read in progress, matching Go
/// cancelling the request context.
async fn read_chunk(
    response: &mut reqwest::Response,
    guard: &CapacityRetryGuard,
) -> Result<Option<Bytes>, TranslatedFailure> {
    match guard.pending_deadline() {
        Some(deadline) => {
            tokio::select! {
                result = response.chunk() => result.map_err(|_| TranslatedFailure::Read("上游响应体读取失败".to_owned())),
                () = tokio::time::sleep_until(deadline) => {
                    if guard.capacity_expired() {
                        Err(TranslatedFailure::CapacityTimeout)
                    } else {
                        Err(TranslatedFailure::Read("上游首字节读取超时".to_owned()))
                    }
                }
            }
        }
        None => response
            .chunk()
            .await
            .map_err(|_| TranslatedFailure::Read("上游响应体读取失败".to_owned())),
    }
}

/// 提交响应头之后的转换转发体.
///
/// 上游字节在调用方取下一帧时才被读取, 因此背压由拉取式读取形成; 客户端断开使响应体被
/// 丢弃时, 上游连接随之关闭.
///
/// The converting forward body after the response headers are committed. Upstream bytes are
/// read only when the caller asks for the next frame, so backpressure comes from pull-based
/// reads; when the client disconnects and the body is dropped, the upstream connection closes
/// with it.
pub(crate) struct TranslatedBody {
    inner: Body,
    state: TranslatedState,
    pending: VecDeque<Bytes>,
    completion: Completion,
    /// 上游读取与收尾已经完成, 剩余内容写出后即可结束.
    ///
    /// Upstream reading and the closing content are done; the body ends once the remaining
    /// content is written.
    upstream_done: bool,
    /// 已经写出错误事件, 之后只剩收尾.
    ///
    /// An error event has been written and only the ending is left.
    failed: bool,
}

impl TranslatedBody {
    pub(crate) fn new(
        inner: Body,
        state: TranslatedState,
        first: Vec<u8>,
        completion: Completion,
    ) -> Self {
        let mut pending = VecDeque::new();
        if !first.is_empty() {
            pending.push_back(Bytes::from(first));
        }
        Self {
            inner,
            state,
            pending,
            completion,
            upstream_done: false,
            failed: false,
        }
    }

    /// 处理一个已经解析出的上游事件, 把转换结果放入待写队列.
    ///
    /// Handles one parsed upstream event and puts the conversion result into the pending queue.
    fn consume(&mut self, event: &[u8]) {
        match self.state.consume(event) {
            Ok(consumed) => {
                if !consumed.output.is_empty() {
                    self.pending.push_back(Bytes::from(consumed.output));
                }
            }
            Err(failure) => self.fail(failure),
        }
    }

    /// 处理提交之后的失败: 补一个错误事件, 让客户端识别失败.
    ///
    /// Handles a failure after the commit: appends one error event so the client recognizes the
    /// failure.
    fn fail(&mut self, failure: TranslatedFailure) {
        if self.failed {
            return;
        }
        failure.log(&self.state.credential);
        let status = failure.status();
        let payload = anthropic_error_body(anthropic_error_type(status), &failure.message());
        let mut event = Vec::with_capacity(payload.len() + 32);
        event.extend_from_slice(b"event: error\ndata: ");
        event.extend_from_slice(&payload);
        event.extend_from_slice(b"\n\n");
        self.pending.push_back(Bytes::from(event));
        self.failed = true;
    }
}

impl HttpBody for TranslatedBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        loop {
            if let Some(frame) = this.pending.pop_front() {
                this.completion.add_bytes(frame.len());
                return Poll::Ready(Some(Ok(Frame::data(frame))));
            }
            if this.failed {
                this.completion.finish(BodyEnd::Interrupted);
                return Poll::Ready(None);
            }
            if this.upstream_done {
                this.completion.finish(BodyEnd::Complete);
                return Poll::Ready(None);
            }
            // 预读可能把提交点之后的事件留在队列里, 上游也可能一次送达多个事件, 因此每次
            // 读上游之前先清空队列.
            //
            // The prelude may leave events after the commit point in the queue and upstream may
            // deliver several events at once, so the queue is drained before every read.
            if let Some(event) = this.state.take_event() {
                this.consume(&event);
                continue;
            }
            match Pin::new(&mut this.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => match this.state.finish_upstream() {
                    Ok(end) => {
                        this.upstream_done = true;
                        if !end.is_empty() {
                            this.pending.push_back(Bytes::from(end));
                        }
                    }
                    Err(failure) => this.fail(failure),
                },
                Poll::Ready(Some(Ok(frame))) => {
                    // 尾帧不含响应内容, 直接跳过.
                    //
                    // A trailer frame carries no response content and is skipped.
                    let Ok(chunk) = frame.into_data() else {
                        continue;
                    };
                    if let Err(failure) = this.state.feed(&chunk) {
                        this.fail(failure);
                    }
                }
                Poll::Ready(Some(Err(_))) => {
                    this.fail(TranslatedFailure::Read("上游响应体读取失败".to_owned()));
                }
            }
        }
    }
}
