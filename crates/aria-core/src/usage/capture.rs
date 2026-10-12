//! 上游用量采集. 采集器只保存一次请求的最终用量快照, 不保存生成文本或凭据, 错误
//! 只包含固定诊断文本, 因此可以安全写进日志.
//!
//! 采集与 IO 无关: 转发循环把响应体字节喂给 CaptureBody::observe, 在响应体正常结束时
//! 调用 CaptureBody::finish, 随后从 Capture 读取结果. 这样的状态机可以脱离网络单独
//! 测试, 也允许调用方在转发路径上按自己的读取边界喂数据. 读取错误后不得调用 finish,
//! 契约见 CaptureBody::finish 的说明.
//!
//! Upstream usage capture. The collector keeps only the final usage snapshot of one
//! request, never generated text or credentials, and its errors carry fixed diagnostic
//! text only, so they are safe to log.
//!
//! Capture is IO-free: the forwarding loop feeds response body bytes to
//! CaptureBody::observe, calls CaptureBody::finish when the body ends normally, and then
//! reads the result from Capture. That state machine can be tested without a network and
//! lets the caller feed data on its own read boundaries. A read error must not call finish;
//! the contract is documented on CaptureBody::finish.

use std::fmt;

use serde_json::{Map, Value};

use super::store::Tokens;

/// 单个事件与单个非流式响应体的容量上限.
///
/// The capacity limit of one event and of one non-streaming response body.
pub const MAX_CAPTURE_BYTES: usize = 8 << 20;
const MAX_CAPTURE_PROBE_BYTES: usize = 512;

/// 采集失败的分类. 对应 capture.go 中的四个哨兵错误.
///
/// The capture failure categories, one for one with the four sentinels in capture.go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureError {
    /// 对应 errCaptureJSON: 上游事件不是合法的 JSON 结构.
    Json,
    /// 对应 errCaptureUsage: 用量字段类型或取值范围不合法.
    Usage,
    /// 对应 errCaptureLimit: 事件超过 8 MiB.
    Limit,
    /// 对应 unsupported provider: 采集器被要求处理未知提供方.
    UnsupportedProvider,
}

impl fmt::Display for CaptureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Json => "usage capture: invalid upstream JSON",
            Self::Usage => "usage capture: invalid token count",
            Self::Limit => "usage capture: upstream event exceeds 8 MiB",
            Self::UnsupportedProvider => "usage capture: unsupported provider",
        })
    }
}

impl std::error::Error for CaptureError {}

/// 一次上游请求的最终用量快照.
///
/// The final usage snapshot of one upstream request.
pub struct Capture {
    provider: String,
    model: String,
    outcome: String,
    error: Option<CaptureError>,
    snapshot: TokenSnapshot,
    tokens: Tokens,
    has_usage: bool,
}

impl Capture {
    /// 构造采集器. model 是请求使用的模型名, 上游报告模型后会被替换.
    ///
    /// Builds a capture. model is the model name the request used and is replaced once
    /// upstream reports a model.
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            outcome: String::new(),
            error: None,
            snapshot: TokenSnapshot::default(),
            tokens: Tokens::default(),
            has_usage: false,
        }
    }

    /// 返回独立副本. 采集失败后不能把早期快照当成最终用量, 因此返回 None.
    ///
    /// Returns an independent copy. After a capture failure an earlier snapshot must not
    /// be treated as the final usage, so it returns None.
    pub fn usage(&self) -> Option<Tokens> {
        if !self.has_usage || self.error.is_some() {
            return None;
        }
        Some(self.tokens)
    }

    /// 返回当前模型名, 上游报告过模型时是上游的取值.
    ///
    /// Returns the current model name, which is upstream's value once it reports one.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// 返回终止结果, 尚未看到终止事件时是空串.
    ///
    /// Returns the terminal outcome, an empty string until a terminal event is seen.
    pub fn outcome(&self) -> &str {
        &self.outcome
    }

    /// 返回采集错误, 调用方可以记录它而不泄露上游正文.
    ///
    /// Returns the capture error, which the caller can log without leaking the upstream
    /// body.
    pub fn error(&self) -> Option<CaptureError> {
        self.error
    }

    fn fail(&mut self, error: CaptureError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    fn set_outcome(&mut self, outcome: &str) {
        // 错误终止之后到达的元数据不能把失败重新标成成功.
        // Metadata arriving after an error termination must not re-mark the failure as
        // success.
        if self.outcome != "error" {
            self.outcome = outcome.to_owned();
        }
    }

    /// 只消费独立的上游 JSON 载荷. 累计事件表示同一次请求的快照, 所以仅替换事件中
    /// 明确出现的字段, 不把重复快照累加成多次用量.
    ///
    /// Consumes one standalone upstream JSON payload. Accumulated events represent a
    /// snapshot of the same request, so it replaces only the fields explicitly present in
    /// the event and never adds duplicate snapshots into multiple usages.
    fn event(&mut self, payload: &[u8]) {
        if payload.len() > MAX_CAPTURE_BYTES {
            self.fail(CaptureError::Limit);
            return;
        }
        // JSON 允许的空白只有空格, 制表符与换行, 因此按 ASCII 空白裁剪与 Go 的
        // bytes.TrimSpace 在有效载荷上等价.
        //
        // JSON allows only space, tab and newlines as whitespace, so trimming ASCII
        // whitespace equals Go's bytes.TrimSpace on valid payloads.
        let payload = trim_ascii_whitespace(payload);
        if payload.is_empty() || payload == b"[DONE]" {
            return;
        }
        // xai 与 commandcode 线路返回与 Codex 字段结构一致的 Responses 事件, 用量字段同样是
        // input_tokens, cached_tokens, output_tokens 与 reasoning_tokens, 因此沿用 Codex 规则.
        // qoder 线路喂入的是转码产出的 Anthropic Messages 事件, 与 messages 线路同形, 区别只在
        // usage 里附带的积分字段.
        //
        // The xai and commandcode lines return Responses events matching Codex structurally, with
        // the same input_tokens, cached_tokens, output_tokens and reasoning_tokens usage fields,
        // so the Codex rule applies. The qoder line feeds the translated Anthropic Messages events,
        // sharing the messages rule and differing only in the credit fields attached to usage.
        let result = match self.provider.as_str() {
            "chatgpt" | "xai" | "commandcode" => self.codex_event(payload),
            "antigravity" => self.google_event(payload),
            "messages" | "qoder" => self.messages_event(payload),
            _ => Err(CaptureError::UnsupportedProvider),
        };
        if let Err(error) = result {
            self.fail(error);
        }
    }

    /// Codex Responses 事件与响应.
    ///
    /// 处理分两步: 解析阶段只读取字段并校验类型, 应用阶段才改写 model, outcome 与用量.
    /// Go 把整个载荷解进结构体, 任何被声明字段的类型错误都会在更新状态之前让事件失败,
    /// 因此解析阶段必须覆盖全部结构字段. 用量取值的错误按 errCaptureUsage 处理, 与 Go
    /// 一样发生在 model 与 outcome 更新之后.
    ///
    /// Codex Responses events and responses.
    ///
    /// Handling runs in two steps: parsing reads the fields and validates their types, and
    /// only the applying step changes model, outcome and usage. Go unmarshals the whole
    /// payload into a struct, so a type error in any declared field fails the event before
    /// any state changes, and the parsing step therefore covers every structural field. A
    /// bad usage value maps to errCaptureUsage and, as in Go, happens after model and
    /// outcome were applied.
    fn codex_event(&mut self, payload: &[u8]) -> Result<(), CaptureError> {
        let top = parse_object(payload)?;
        let response = match object_field(&top, "response")? {
            Some(response) => response,
            None => &top,
        };
        let model = string_field(response, "model")?;
        let status = string_field(response, "status")?;
        let event_type = string_field(&top, "type")?;
        let has_error =
            raw_value(&top, "error").is_some() || raw_value(response, "error").is_some();
        let usage = raw_value(response, "usage");

        if let Some(model) = model
            && !model.is_empty()
        {
            self.model = model.to_owned();
        }
        if has_error {
            self.set_outcome("error");
        }
        let mut terminal = true;
        match event_type.unwrap_or("") {
            "response.completed" => self.set_outcome("completed"),
            "response.incomplete" => self.set_outcome("incomplete"),
            "response.failed" | "error" => self.set_outcome("error"),
            "" => {
                // 非流式 Response 只有明确的状态才能证明上游已经终止.
                // Only an explicit status in a non-streaming Response proves upstream has
                // terminated.
                match status.unwrap_or("") {
                    "completed" => self.set_outcome("completed"),
                    "incomplete" => self.set_outcome("incomplete"),
                    "failed" | "cancelled" => self.set_outcome("error"),
                    _ => {}
                }
            }
            _ => terminal = false,
        }
        if !terminal {
            return Ok(());
        }
        let Some(usage) = usage else {
            return Ok(());
        };
        let usage = usage_map(usage)?;
        let input_details = usage_object(usage, "input_tokens_details")?;
        let output_details = usage_object(usage, "output_tokens_details")?;
        self.update(TokenSnapshot {
            input: token_count(usage, "input_tokens")?,
            output: token_count(usage, "output_tokens")?,
            total: token_count(usage, "total_tokens")?,
            cache_read: nested_token_count(input_details, "cached_tokens")?,
            cache_write: nested_token_count(input_details, "cache_write_tokens")?,
            reasoning: nested_token_count(output_details, "reasoning_tokens")?,
            credits: None,
            original_credits: None,
            billable: None,
        });
        Ok(())
    }

    /// Antigravity 的 Gemini 响应事件, 同样分解析与应用两步.
    ///
    /// Antigravity's Gemini response events, also split into parsing and applying.
    fn google_event(&mut self, payload: &[u8]) -> Result<(), CaptureError> {
        let top = parse_object(payload)?;
        // Go 把 response 声明为值结构体, 因此 response 不是对象时按无效 JSON 处理.
        // Go declares response as a value struct, so a non-object response is invalid JSON.
        let response = object_field(&top, "response")?;
        let model = match response {
            Some(response) => string_field(response, "modelVersion")?,
            None => None,
        };
        let has_error = raw_value(&top, "error").is_some()
            || response.is_some_and(|response| raw_value(response, "error").is_some());
        let reasons = match response {
            Some(response) => finish_reasons(response)?,
            None => Vec::new(),
        };
        // Gemini 用量字段名为 usageMetadata, 与 Codex 的 usage 不同.
        //
        // Gemini names the usage field usageMetadata, unlike Codex's usage.
        let usage = response.and_then(|response| raw_value(response, "usageMetadata"));

        if let Some(model) = model
            && !model.is_empty()
        {
            self.model = model.to_owned();
        }
        if has_error {
            self.set_outcome("error");
        }
        for reason in reasons {
            match reason {
                "" | "FINISH_REASON_UNSPECIFIED" => {}
                "STOP" | "MAX_TOKENS" => self.set_outcome("completed"),
                _ => self.set_outcome("error"),
            }
        }
        let Some(usage) = usage else {
            return Ok(());
        };
        let usage = usage_map(usage)?;
        self.update(TokenSnapshot {
            input: token_count(usage, "promptTokenCount")?,
            output: token_count(usage, "candidatesTokenCount")?,
            cache_read: token_count(usage, "cachedContentTokenCount")?,
            reasoning: token_count(usage, "thoughtsTokenCount")?,
            total: token_count(usage, "totalTokenCount")?,
            cache_write: None,
            credits: None,
            original_credits: None,
            billable: None,
        });
        Ok(())
    }

    /// Anthropic Messages 事件与响应, 规则见 docs/architecture.md 5.8 第 2 条.
    ///
    /// Anthropic Messages events and responses, per docs/architecture.md section 5.8
    /// item 2.
    fn messages_event(&mut self, payload: &[u8]) -> Result<(), CaptureError> {
        let top = parse_object(payload)?;
        // message_start 把模型与用量放在 message 对象里, message_delta 与非流式响应
        // 放在顶层.
        //
        // message_start carries the model and usage inside the message object, while
        // message_delta and non-streaming responses carry usage at the top level.
        let message = object_field(&top, "message")?;
        let model = match message {
            Some(message) => string_field(message, "model")?,
            None => string_field(&top, "model")?,
        };
        let event_type = string_field(&top, "type")?;
        let has_error = raw_value(&top, "error").is_some();
        let usage = message
            .and_then(|message| raw_value(message, "usage"))
            .or_else(|| raw_value(&top, "usage"));

        if let Some(model) = model
            && !model.is_empty()
        {
            self.model = model.to_owned();
        }
        if has_error {
            self.set_outcome("error");
        }
        match event_type.unwrap_or("") {
            // 非流式响应的 type 为 message, 它只有成功一种含义; 流式的 message_stop 表示
            // 本次生成正常结束.
            //
            // A non-streaming response has type message, which only means success, while a
            // streamed message_stop means the generation ended normally.
            "message_stop" | "message" => self.set_outcome("completed"),
            "error" => self.set_outcome("error"),
            _ => {}
        }
        let Some(usage) = usage else {
            return Ok(());
        };
        let usage = usage_map(usage)?;
        // Qoder 线路的转码事件在 usage 里附带 credits, original_credits 与 billable; messages
        // 线路没有这些字段, 读到什么记什么.
        //
        // The qoder line's translated events attach credits, original_credits and billable in
        // usage; the messages line does not carry them, so whatever appears is recorded.
        let credits = raw_value(usage, "credits").and_then(|v| v.as_f64());
        let original_credits = raw_value(usage, "original_credits").and_then(|v| v.as_f64());
        let billable = raw_value(usage, "billable").and_then(|v| v.as_bool());
        self.update(TokenSnapshot {
            input: token_count(usage, "input_tokens")?,
            output: token_count(usage, "output_tokens")?,
            cache_read: token_count(usage, "cache_read_input_tokens")?,
            cache_write: token_count(usage, "cache_creation_input_tokens")?,
            reasoning: None,
            // Anthropic 不报告总数, total_tokens 由输入加输出推导.
            // Anthropic reports no total, so total_tokens is derived from input plus
            // output.
            total: None,
            credits,
            original_credits,
            billable,
        });
        Ok(())
    }

    fn update(&mut self, patch: TokenSnapshot) {
        let mut next = self.snapshot;
        next.input = patch.input.or(next.input);
        next.output = patch.output.or(next.output);
        next.cache_read = patch.cache_read.or(next.cache_read);
        next.cache_write = patch.cache_write.or(next.cache_write);
        next.reasoning = patch.reasoning.or(next.reasoning);
        next.total = patch.total.or(next.total);
        next.credits = patch.credits.or(next.credits);
        next.original_credits = patch.original_credits.or(next.original_credits);
        next.billable = patch.billable.or(next.billable);
        if next.is_empty() {
            return;
        }
        let mut tokens = Tokens {
            input_tokens: next.input.unwrap_or(0),
            output_tokens: next.output.unwrap_or(0),
            cache_read_input_tokens: next.cache_read.unwrap_or(0),
            cache_write_input_tokens: next.cache_write.unwrap_or(0),
            reasoning_output_tokens: next.reasoning.unwrap_or(0),
            total_tokens: next.total.unwrap_or(0),
            credits: next.credits,
            original_credits: next.original_credits,
            billable: next.billable,
        };
        // Google 将思考与候选输出分开报告. 缓存输入已包含在 promptTokenCount 中,
        // Codex 的 reasoning_tokens 已包含在 output_tokens 中, 都不能再次相加.
        //
        // Google reports thinking and candidate output separately. Cached input is
        // already included in promptTokenCount and Codex's reasoning_tokens is already
        // included in output_tokens, so neither may be added again.
        if self.provider == "antigravity" {
            tokens.output_tokens = match tokens
                .output_tokens
                .checked_add(tokens.reasoning_output_tokens)
            {
                Some(value) => value,
                None => {
                    self.fail(CaptureError::Usage);
                    return;
                }
            };
        }
        // Anthropic 的 input_tokens 不含缓存, 记录时把两项缓存加回输入, 使口径与 Lyra
        // 的输入包含缓存一致. 加回在物化记录时统一进行, 而不是在事件解析里: 快照保存的
        // 是事件给出的原始字段, 后到事件重复给出 input_tokens 时会覆盖旧值, 若在解析事件
        // 时就加回, 那份缓存会随覆盖一起丢失.
        //
        // Anthropic's input_tokens excludes cache, so both cache counters are added back
        // into the input while the record is materialized. The addition happens here
        // rather than while parsing an event because the snapshot keeps the raw fields an
        // event reported: a later event repeating input_tokens replaces the old value, and
        // adding the cache during parsing would lose it together with the overwritten
        // value.
        if self.provider == "messages" {
            let cache_total = match tokens
                .cache_read_input_tokens
                .checked_add(tokens.cache_write_input_tokens)
            {
                Some(value) => value,
                None => {
                    self.fail(CaptureError::Usage);
                    return;
                }
            };
            tokens.input_tokens = match tokens.input_tokens.checked_add(cache_total) {
                Some(value) => value,
                None => {
                    self.fail(CaptureError::Usage);
                    return;
                }
            };
        }
        if next.total.is_none() {
            tokens.total_tokens = match tokens.input_tokens.checked_add(tokens.output_tokens) {
                Some(value) => value,
                None => {
                    self.fail(CaptureError::Usage);
                    return;
                }
            };
        }
        self.snapshot = next;
        self.tokens = tokens;
        self.has_usage = true;
    }
}

/// 响应体状态机. 不预读, 不合并读块, 也不接管连接关闭; 采集失败不影响转发内容.
///
/// The response body state machine. It does not read ahead, does not merge read chunks
/// and does not take over connection closing; a capture failure never changes the
/// forwarded content.
pub struct CaptureBody {
    capture: Capture,
    stream: bool,
    format_known: bool,
    probe: Vec<u8>,
    finished: bool,
    json_body: Vec<u8>,
    sse: CaptureSse,
    dropped: bool,
}

impl CaptureBody {
    /// 按响应头判断是否为 SSE. Codex 实测会把标准 SSE 标成 application/json, 因此
    /// 采集侧还会在首批字节上重新识别.
    ///
    /// Uses the response header to decide whether the body is SSE. In testing Codex
    /// labels standard SSE as application/json, so the capture side also recognizes the
    /// format from the first bytes.
    pub fn new(capture: Capture, content_type: &str) -> Self {
        let media_type = content_type.split(';').next().unwrap_or("");
        let stream = media_type.trim().eq_ignore_ascii_case("text/event-stream");
        Self {
            capture,
            stream,
            format_known: stream,
            probe: Vec::new(),
            finished: false,
            json_body: Vec::new(),
            sse: CaptureSse::default(),
            dropped: false,
        }
    }

    /// 返回底层采集器, 供调用方在 finish 之后读取结果.
    ///
    /// Returns the underlying capture so the caller can read the result after finish.
    pub fn capture(&self) -> &Capture {
        &self.capture
    }

    /// 喂入一段响应体字节. finish 之后到达的字节被忽略, 因为结果已经确定.
    ///
    /// Feeds one chunk of response body bytes. Bytes arriving after finish are ignored
    /// because the result is already decided.
    pub fn observe(&mut self, chunk: &[u8]) {
        if self.finished {
            return;
        }
        let mut payload = chunk;
        if !self.format_known {
            // 只检查已经读到的前缀, 不等待额外读块, 也不改写响应头.
            // Only the prefix already read is inspected; no extra read is awaited and the
            // response header is not rewritten.
            let room = MAX_CAPTURE_PROBE_BYTES.saturating_sub(self.probe.len());
            let taken = payload.len().min(room);
            self.probe.extend_from_slice(&payload[..taken]);
            let (stream, known) = capture_stream_prefix(&self.probe);
            if !known && self.probe.len() < MAX_CAPTURE_PROBE_BYTES {
                return;
            }
            self.stream = stream;
            self.format_known = true;
            let probe = std::mem::take(&mut self.probe);
            self.collect(&probe);
            payload = &payload[taken..];
        }
        self.collect(payload);
    }

    /// 在读取循环正常读到响应体结束时调用一次. 重复调用没有效果.
    ///
    /// 读取错误时不得调用: Go 的 captureBody 只在对底层体的读取返回 io.EOF 时终结,
    /// 读错误时既不终结也不分派未完成的事件, 已经分派过的完整事件仍然保留. 若在读错误
    /// 之后调用本方法, SSE 路径会把截断的最后一行当作完整事件送出, 由此产生的解析错误
    /// 会锁存采集失败并丢弃此前已经采集到的用量, 与 Go 的行为不同.
    ///
    /// Called once when the read loop reaches the end of the response body normally.
    /// Repeated calls have no effect.
    ///
    /// It must not be called after a read error: Go's captureBody finalizes only when a read
    /// of the underlying body returns io.EOF, so a read error neither finalizes nor
    /// dispatches an unfinished event and keeps every complete event already dispatched.
    /// Calling this method after a read error would make the SSE path send the truncated
    /// last line as a complete event, and the resulting parse error would latch a capture
    /// failure and discard usage collected so far, unlike Go.
    pub fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if !self.format_known {
            let probe = std::mem::take(&mut self.probe);
            self.collect(&probe);
        }
        if self.stream {
            // 上游关闭连接时, 最后一条完整 JSON 事件可能没有尾部空行.
            // When upstream closes the connection, the last complete JSON event may lack a
            // trailing blank line.
            let mut sse = std::mem::take(&mut self.sse);
            sse.finish(&mut self.capture);
        } else if !self.dropped {
            let body = std::mem::take(&mut self.json_body);
            self.capture.event(&body);
        }
    }

    fn collect(&mut self, payload: &[u8]) {
        if self.stream {
            self.sse.feed(&mut self.capture, payload);
        } else if !self.dropped {
            // 超过探测上限仍不能确定格式时按普通 JSON 处理, 保持内存有界.
            // When the format is still undetermined past the probe limit it is treated as
            // ordinary JSON to keep memory bounded.
            if payload.len() > MAX_CAPTURE_BYTES.saturating_sub(self.json_body.len()) {
                self.capture.fail(CaptureError::Limit);
                self.json_body = Vec::new();
                self.dropped = true;
            } else {
                self.json_body.extend_from_slice(payload);
            }
        }
    }
}

/// 判断响应体前缀是 SSE 还是普通 JSON. 第二个返回值表示是否已经可以确定.
///
/// Decides whether the body prefix is SSE or plain JSON. The second value reports
/// whether the format is already determined.
fn capture_stream_prefix(payload: &[u8]) -> (bool, bool) {
    let prefix = trim_probe_prefix(payload);
    if prefix.is_empty() {
        return (false, false);
    }
    if prefix[0] == b':' {
        return (true, true);
    }
    for field in [
        b"event:".as_slice(),
        b"data:".as_slice(),
        b"id:".as_slice(),
        b"retry:".as_slice(),
    ] {
        if prefix.starts_with(field) {
            return (true, true);
        }
        if field.starts_with(prefix) {
            return (false, false);
        }
    }
    (false, true)
}

fn trim_probe_prefix(payload: &[u8]) -> &[u8] {
    let mut start = 0;
    while start < payload.len() && matches!(payload[start], b' ' | b'\t' | b'\r' | b'\n') {
        start += 1;
    }
    &payload[start..]
}

fn trim_ascii_whitespace(payload: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = payload.len();
    while start < end && payload[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && payload[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &payload[start..end]
}

/// captureSSE 只缓存当前事件. line_length 最多保留到 2, 使超限事件即使不再保存
/// 正文, 仍能识别空行并恢复读取后续事件. CRLF 可跨任意读块边界.
///
/// captureSSE buffers only the current event. line_length is capped at 2 so that even an
/// over-limit event that no longer stores its body can still recognize a blank line and
/// resume reading later events. CRLF may cross any read boundary.
#[derive(Default)]
struct CaptureSse {
    line: Vec<u8>,
    data: Vec<u8>,
    event_bytes: usize,
    line_length: usize,
    line_is_cr: bool,
    data_lines: usize,
    dropped: bool,
}

impl CaptureSse {
    fn feed(&mut self, capture: &mut Capture, payload: &[u8]) {
        let mut rest = payload;
        while !rest.is_empty() {
            let index = rest.iter().position(|byte| *byte == b'\n');
            let (fragment, extra) = match index {
                Some(index) => (&rest[..index], 1),
                None => (rest, 0),
            };
            if !fragment.is_empty() {
                self.line_is_cr =
                    self.line_length == 0 && fragment.len() == 1 && fragment[0] == b'\r';
                self.line_length = (self.line_length + fragment.len()).min(2);
            }
            if !self.dropped {
                if fragment.len() + extra > MAX_CAPTURE_BYTES.saturating_sub(self.event_bytes) {
                    capture.fail(CaptureError::Limit);
                    self.line.clear();
                    self.data.clear();
                    self.dropped = true;
                } else {
                    self.event_bytes += fragment.len() + extra;
                    self.line.extend_from_slice(fragment);
                }
            }
            let Some(index) = index else {
                return;
            };
            self.finish_line(capture);
            rest = &rest[index + 1..];
        }
    }

    fn finish_line(&mut self, capture: &mut Capture) {
        if self.line_length == 0 || (self.line_length == 1 && self.line_is_cr) {
            self.dispatch(capture);
        } else if !self.dropped {
            // 字段名与取值之间只去掉一个空格, 与 Go 的 TrimPrefix 一致.
            //
            // Only one space between the field name and its value is removed, matching
            // Go's TrimPrefix.
            let mut range = None;
            {
                let line = match self.line.last() {
                    Some(b'\r') => &self.line[..self.line.len() - 1],
                    _ => &self.line[..],
                };
                if line == b"data" {
                    range = Some((0, 0));
                } else if let Some(value) = line.strip_prefix(b"data:") {
                    let value = value.strip_prefix(b" ").unwrap_or(value);
                    range = Some((line.len() - value.len(), value.len()));
                }
            }
            if let Some((start, length)) = range {
                if self.data_lines > 0 {
                    self.data.push(b'\n');
                }
                self.data
                    .extend_from_slice(&self.line[start..start + length]);
                self.data_lines += 1;
            }
        }
        self.line.clear();
        self.line_length = 0;
        self.line_is_cr = false;
    }

    fn dispatch(&mut self, capture: &mut Capture) {
        if !self.dropped && self.data_lines > 0 {
            capture.event(&self.data);
        }
        self.data.clear();
        self.data_lines = 0;
        self.event_bytes = 0;
        self.dropped = false;
    }

    fn finish(&mut self, capture: &mut Capture) {
        if self.line_length > 0 {
            self.finish_line(capture);
        }
        self.dispatch(capture);
    }
}

/// tokenCount 的存在标志保留明确的零值. 直接解析 i64 同时拒绝负数, 小数, 字符串
/// 与溢出, 避免先转 float64 导致大整数精度损失.
///
/// The presence flag preserves an explicit zero. Parsing i64 directly also rejects
/// negatives, decimals, strings and overflow, avoiding the large-integer precision loss
/// of converting through float64 first.
#[derive(Clone, Copy, Default)]
struct TokenSnapshot {
    input: Option<i64>,
    output: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
    reasoning: Option<i64>,
    total: Option<i64>,
    /// Qoder credits.
    credits: Option<f64>,
    original_credits: Option<f64>,
    billable: Option<bool>,
}

impl TokenSnapshot {
    fn is_empty(&self) -> bool {
        self.input.is_none()
            && self.output.is_none()
            && self.cache_read.is_none()
            && self.cache_write.is_none()
            && self.reasoning.is_none()
            && self.total.is_none()
            && self.credits.is_none()
            && self.original_credits.is_none()
            && self.billable.is_none()
    }
}

/// 解析顶层 JSON 对象. 顶层不是对象时按无效 JSON 处理, 与 Go 把载荷解进结构体的
/// 行为一致.
///
/// Parses the top-level JSON object. A non-object top level is invalid JSON, matching
/// Go's behavior of unmarshalling the payload into a struct.
fn parse_object(payload: &[u8]) -> Result<Map<String, Value>, CaptureError> {
    match serde_json::from_slice::<Value>(payload) {
        Ok(Value::Object(object)) => Ok(object),
        _ => Err(CaptureError::Json),
    }
}

/// 读取字符串字段. 缺失与 null 都表示没有取值; 其他类型报错, 因为 Go 把它们解进
/// string 字段时会失败.
///
/// Reads a string field. Missing and null both mean no value; any other type is an error
/// because Go fails when unmarshalling it into a string field.
fn string_field<'a>(
    map: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, CaptureError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(CaptureError::Json),
    }
}

fn object_field<'a>(
    map: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Map<String, Value>>, CaptureError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(object)) => Ok(Some(object)),
        Some(_) => Err(CaptureError::Json),
    }
}

fn array_field<'a>(
    map: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Vec<Value>>, CaptureError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(items)) => Ok(Some(items)),
        Some(_) => Err(CaptureError::Json),
    }
}

/// 读取用量对象. 类型错误按用量错误处理, 因为 Go 在把该字段解进用量结构体时才
/// 失败, 报的是 errCaptureUsage.
///
/// Reads a usage object. A wrong type is a usage error, because Go fails while
/// unmarshalling that field into the usage struct and reports errCaptureUsage.
fn usage_object<'a>(
    map: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Map<String, Value>>, CaptureError> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(object)) => Ok(Some(object)),
        Some(_) => Err(CaptureError::Usage),
    }
}

/// 返回出现了非 null 取值的原始字段. null 与缺失都表示没有取值, 与 Go 的
/// hasJSONValue 一致.
///
/// Returns the raw field when it carries a value other than null. Null and absence both
/// mean no value, matching Go's hasJSONValue.
fn raw_value<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    map.get(key).filter(|value| !value.is_null())
}

/// 读取已经确认存在的用量取值. 类型错误按用量错误处理, 因为 Go 在这个字段解进用量
/// 结构体时才失败, 报的是 errCaptureUsage.
///
/// Reads a usage value already known to be present. A wrong type is a usage error, because
/// Go fails while unmarshalling that field into the usage struct and reports
/// errCaptureUsage.
fn usage_map(value: &Value) -> Result<&Map<String, Value>, CaptureError> {
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(CaptureError::Usage),
    }
}

/// 收集 candidates 里的结束原因.
///
/// 与 Go 把该字段解进切片的行为逐项一致: candidates 不是数组时按无效 JSON 处理; 数组里
/// 的 null 元素被跳过, Go 对其不报错且零值结束原因不参与判定; 数字, 字符串, 数组与布尔
/// 元素都按无效 JSON 处理, 因为 Go 解码它们时会报错并让整条事件失败; 对象元素里的
/// finishReason 不是字符串时同样按无效 JSON 处理.
///
/// Collects the finish reasons from candidates.
///
/// This matches Go's behavior of unmarshalling the field into a slice item by item: a
/// non-array candidates is invalid JSON; a null element is skipped, because Go reports no
/// error for it and its zero-valued finish reason takes no part in the decision; number,
/// string, array and boolean elements are invalid JSON, because Go fails to decode them and
/// the whole event fails; and a non-string finishReason inside an object element is invalid
/// JSON as well.
fn finish_reasons(response: &Map<String, Value>) -> Result<Vec<&str>, CaptureError> {
    let Some(candidates) = array_field(response, "candidates")? else {
        return Ok(Vec::new());
    };
    let mut reasons = Vec::new();
    for candidate in candidates {
        let Value::Object(candidate) = candidate else {
            if candidate.is_null() {
                continue;
            }
            return Err(CaptureError::Json);
        };
        if let Some(reason) = string_field(candidate, "finishReason")? {
            reasons.push(reason);
        }
    }
    Ok(reasons)
}

fn token_count(map: &Map<String, Value>, key: &str) -> Result<Option<i64>, CaptureError> {
    match map.get(key) {
        None => Ok(None),
        // as_i64 只接受 JSON 整数字面量: 1.5 与 1e3 在 serde_json 中是浮点数, 因此
        // 与 Go 的 strconv.ParseInt 一样被拒绝.
        //
        // as_i64 accepts only JSON integer literals: 1.5 and 1e3 are floats in
        // serde_json and are therefore rejected exactly as Go's strconv.ParseInt does.
        Some(Value::Number(number)) => match number.as_i64() {
            Some(value) if value >= 0 => Ok(Some(value)),
            _ => Err(CaptureError::Usage),
        },
        Some(_) => Err(CaptureError::Usage),
    }
}

fn nested_token_count(
    parent: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<i64>, CaptureError> {
    match parent {
        Some(map) => token_count(map, key),
        None => Ok(None),
    }
}
