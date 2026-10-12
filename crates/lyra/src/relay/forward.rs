//! 转发收尾, 透传响应体与两条线路共用的错误报文构造.
//!
//! 一次转发的结束有两种形态: 响应体被读到末尾, 或者响应体在读完之前被丢弃. 两者都要
//! 写出同一条完成日志并恰好记录一次用量, 因此收尾状态集中在这里, 由各响应体在结束或
//! 析构时驱动.
//!
//! Forwarding completion, the passthrough response body, and the error encoding shared by
//! both routes.
//!
//! One forward ends in two shapes: the response body is read to the end, or it is dropped
//! before that. Both must write the same completion log and record usage exactly once, so the
//! completion state lives here and is driven by each response body on end or drop.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use aria_core::body::{Body, BoxError};
use aria_core::usage::Observation;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use http_body::{Body as HttpBody, Frame};

use crate::credential::truncate;

/// 完成日志中与结束无关的字段, 在转发开始时确定.
///
/// The fields of the completion log that do not depend on the ending; they are fixed when
/// forwarding starts.
pub(crate) struct CompletionLog {
    pub(crate) path: String,
    pub(crate) route: String,
    pub(crate) credential: String,
    pub(crate) status: u16,
    pub(crate) request_bytes: usize,
    pub(crate) started: Instant,
}

impl CompletionLog {
    /// 写一行完成日志. route 字段是 Aria 相对 Go 版新增的: 同一个二进制可以服务多条
    /// 同名路由, 日志必须能指出请求走的是哪一条.
    ///
    /// Writes one completion log line. The route field is Aria's addition to the Go format:
    /// one binary may serve several routes with the same model name, so the log must say
    /// which route the request took.
    fn write(&self, response_bytes: usize, incomplete: bool) {
        let suffix = if incomplete { " incomplete" } else { "" };
        aria_core::log!(
            "POST {} route={} credential={} upstream={} request={}B response={}B elapsed={:.1}s{}",
            self.path,
            self.route,
            self.credential,
            self.status,
            self.request_bytes,
            response_bytes,
            self.started.elapsed().as_secs_f64(),
            suffix
        );
    }
}

/// 响应体的结束方式, 决定用量记录的结果判定与完成日志的后缀.
///
/// How a response body ended; it decides the usage outcome and the completion log suffix.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyEnd {
    /// 上游传输正常结束, 客户端收到完整内容.
    ///
    /// The upstream transfer ended normally and the client received the whole content.
    Complete,
    /// 上游或客户端侧中断, 客户端收到的内容不完整.
    ///
    /// An interruption on the upstream or client side left the client with incomplete
    /// content.
    Interrupted,
    /// 一次性写出的错误或非流式报文, 不存在上游传输可判定.
    ///
    /// A one-piece error or non-streaming message; no upstream transfer can be judged.
    Buffered,
}

/// 一次转发的收尾. 结束与析构都只生效一次, 保证完成日志与用量记录各自恰好一条.
///
/// The completion of one forward. Ending and dropping each apply once, so the completion log
/// and the usage record are written exactly once apiece.
pub(crate) struct Completion {
    observation: Option<Observation>,
    log: Option<CompletionLog>,
    /// 已经交给客户端的响应字节数, 用于完成日志.
    ///
    /// The number of response bytes already handed to the client, used by the completion log.
    forwarded: usize,
    done: bool,
}

impl Completion {
    pub(crate) fn new(observation: Option<Observation>, log: Option<CompletionLog>) -> Self {
        Self {
            observation,
            log,
            forwarded: 0,
            done: false,
        }
    }

    pub(crate) fn observation(&self) -> Option<&Observation> {
        self.observation.as_ref()
    }

    /// 记下已经交给客户端的字节数.
    ///
    /// Records bytes already handed to the client.
    pub(crate) fn add_bytes(&mut self, bytes: usize) {
        self.forwarded += bytes;
    }

    /// 结束转发.
    ///
    /// Ends the forward.
    pub(crate) fn finish(&mut self, end: BodyEnd) {
        if self.done {
            return;
        }
        self.done = true;
        if let Some(observation) = &self.observation {
            observation.set_transport_complete(end == BodyEnd::Complete);
            observation.finish(false);
        }
        if let Some(log) = &self.log {
            log.write(self.forwarded, end == BodyEnd::Interrupted);
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        // 响应体在读完之前被丢弃: 客户端断开或连接任务结束. 与 Go 版 forward 写出失败
        // 时的处理一致, 记一行日志并按中断收尾.
        //
        // The response body was dropped before the end: the client disconnected or the
        // connection task ended. Matching Go's forward on a failed write, one log line is
        // written and the forward is completed as an interruption.
        if self.done {
            return;
        }
        self.done = true;
        if let Some(observation) = &self.observation {
            observation.finish(true);
        }
        aria_core::log!("client disconnected before the response completed");
        if let Some(log) = &self.log {
            log.write(self.forwarded, true);
        }
    }
}

/// 一次性写出的响应体的收尾包装: 错误报文, 非流式聚合报文, 以及 aria-core 已经构造好的
/// 响应体.
///
/// Wraps a one-piece response body for completion: an error message, an aggregated
/// non-streaming message, or a body already built by aria-core.
pub(crate) struct FinalBody {
    inner: Body,
    completion: Completion,
    end: BodyEnd,
    done: bool,
}

impl FinalBody {
    pub(crate) fn new(inner: Body, completion: Completion, end: BodyEnd) -> Self {
        Self {
            inner,
            completion,
            end,
            done: false,
        }
    }
}

impl HttpBody for FinalBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.done = true;
                this.completion.finish(this.end);
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.completion.add_bytes(data.len());
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.done = true;
                this.completion.finish(BodyEnd::Interrupted);
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

/// 透传响应体: 逐块转发上游字节, 并把每块喂给用量采集.
///
/// 拉取式读取形成背压: 只有调用方取下一帧时才读上游, 因此不会建立无界队列; 响应体被
/// 丢弃时上游连接随之关闭, 不再消耗上游额度.
///
/// The passthrough response body: it forwards upstream bytes chunk by chunk and feeds each
/// chunk to the usage capture.
///
/// Pull-based reads provide backpressure: upstream is read only when the caller asks for the
/// next frame, so no unbounded queue is built; dropping the body closes the upstream
/// connection and stops spending upstream quota.
pub(crate) struct PassthroughBody {
    inner: Body,
    completion: Completion,
    finished: bool,
}

impl PassthroughBody {
    pub(crate) fn new(inner: Body, completion: Completion) -> Self {
        Self {
            inner,
            completion,
            finished: false,
        }
    }

    fn finish(&mut self, end: BodyEnd) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(observation) = self.completion.observation() {
            observation.finish_chunks();
        }
        self.completion.finish(end);
    }
}

impl HttpBody for PassthroughBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut this.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.finish(BodyEnd::Complete);
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Ok(frame))) => {
                    // 尾帧不含响应内容, 直接跳过.
                    //
                    // A trailer frame carries no response content and is skipped.
                    let Ok(chunk) = frame.into_data() else {
                        continue;
                    };
                    if chunk.is_empty() {
                        continue;
                    }
                    if let Some(observation) = this.completion.observation() {
                        observation.observe_chunk(&chunk);
                    }
                    this.completion.add_bytes(chunk.len());
                    return Poll::Ready(Some(Ok(Frame::data(chunk))));
                }
                Poll::Ready(Some(Err(error))) => {
                    this.finish(BodyEnd::Interrupted);
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
    }
}

/// 把上游的错误响应转成 Anthropic 错误报文的载荷, 与 Go 版的 writeAnthropicError 一致.
///
/// 上游的错误体通常是 Google 风格的 {"error":{"code":..,"message":..,"status":..}}, 直接
/// 回给客户端会被告知没有 message 字段; 消息取不出来时才退化为状态码的标准文本. 状态码
/// 由调用方写入响应, 消息按字节截断到 500, 与 Go 的 `message[:500]` 相同.
///
/// Turns an upstream error response into the payload of an Anthropic error message, matching
/// Go's writeAnthropicError. The upstream body is usually Google-shaped
/// ({"error":{"code":..,"message":..,"status":..}}), which a client would read as a missing
/// message; only when no message can be extracted does it fall back to the status text. The
/// caller writes the status into the response, and the message is cut to 500 bytes, matching
/// Go's `message[:500]`.
pub(crate) fn anthropic_error_payload(status: StatusCode, payload: &[u8]) -> Vec<u8> {
    let mut message = String::from_utf8_lossy(payload).trim().to_owned();
    if let Ok(serde_json::Value::Object(object)) =
        serde_json::from_slice::<serde_json::Value>(payload)
        && let Some(text) = object
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(serde_json::Value::as_str)
        && !text.is_empty()
    {
        message = text.to_owned();
    }
    if message.is_empty() {
        message = status.canonical_reason().unwrap_or_default().to_owned();
    }
    if message.len() > MAX_ERROR_MESSAGE_BYTES {
        message = truncate(&message, MAX_ERROR_MESSAGE_BYTES);
    }
    aria_core::errors::anthropic_error_body(
        aria_core::errors::anthropic_error_type(status),
        &message,
    )
}

/// 错误消息的字节上限, 与 Go 版 writeAnthropicError 的 500 相同.
///
/// The byte limit of an error message, matching the 500 of Go's writeAnthropicError.
const MAX_ERROR_MESSAGE_BYTES: usize = 500;

/// 事件流内部携带的上游错误, 对应 Go 版的 upstreamStreamError.
///
/// 状态码来自事件体而不是 HTTP 响应头, 调用方单独取出它来回填响应状态.
///
/// An upstream error carried inside the event stream, corresponding to Go's
/// upstreamStreamError. The status comes from the event body rather than the HTTP response
/// header, so the caller takes it separately to fill in the response status.
#[derive(Debug)]
pub(crate) struct UpstreamEventError {
    pub(crate) status: u16,
    pub(crate) message: String,
}

impl std::fmt::Display for UpstreamEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "上游事件错误 {}: {}", self.status, self.message)
    }
}

/// 识别 HTTP 200 之后的 Google 错误事件, 与 Go 版的 upstreamEventError 一致.
///
/// 不识别的话, 翻译器会把这种没有候选内容的事件当成普通事件忽略, 最终给客户端发出成功
/// 结束标记.
///
/// Recognizes a Google error event arriving after an HTTP 200, matching Go's
/// upstreamEventError. Without it the translator would ignore such an event as an ordinary
/// one without candidates and end up sending a successful stop marker to the client.
pub(crate) fn upstream_event_error(payload: &[u8]) -> Option<UpstreamEventError> {
    #[derive(serde::Deserialize)]
    struct Envelope {
        #[serde(default)]
        error: Option<EventError>,
    }
    #[derive(serde::Deserialize)]
    struct EventError {
        #[serde(default)]
        message: String,
        #[serde(default)]
        code: u16,
    }
    let envelope = serde_json::from_slice::<Envelope>(payload).ok()?;
    let error = envelope.error?;
    Some(UpstreamEventError {
        status: error.code,
        message: error.message,
    })
}

/// 从上游响应头取 Retry-After 的文本取值.
///
/// Takes the Retry-After text from an upstream response header.
pub(crate) fn retry_after(headers: &HeaderMap) -> Option<String> {
    headers
        .get(http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
