//! Gemini 事件流到 Anthropic Messages 事件流的转换.
//!
//! 上游每个事件形如:
//!
//!   {"response": {"candidates": [{"content": {"role": "model", "parts": [...]}, "finishReason": "STOP"}],
//!                 "usageMetadata": {...}, "modelVersion": "..."}, "traceId": "..."}
//!
//! 真正的载荷在 response 字段内. parts 里可能出现三类内容: 思考段落带 thought 为真, 正文段落
//! 只有 text, 工具调用是 functionCall 对象.
//!
//! Anthropic 的事件有顺序与配对要求: message_start 必须最先, 之后每个内容块都由一对
//! content_block_start 与 content_block_stop 包住, 最后以 message_delta 与 message_stop 收尾.
//! 因此这里维护当前打开的块, 在内容类型切换时先关旧再开新.
//!
//! Converts the Gemini event stream into an Anthropic Messages event stream. The real payload
//! sits inside the response field, and parts may carry three kinds of content: a thinking segment
//! with thought true, a body segment with only text, and a tool call as a functionCall object.
//!
//! Anthropic events have ordering and pairing requirements: message_start comes first, every
//! content block is wrapped by a pair of content_block_start and content_block_stop, and the
//! stream ends with message_delta and message_stop. The translator therefore tracks the currently
//! open block and closes the old one before opening a new one when the content kind changes.

use std::fmt;

use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::provider::new_identifier;
use super::request::{
    BLOCK_REDACTED_THINKING, BLOCK_SERVER_TOOL_USE, BLOCK_TEXT, BLOCK_THINKING, BLOCK_TOOL_USE,
    RawField, zero_on_null, zero_on_null_list,
};
use super::search::{GroundingMetadata, WebSearchResponse};
use super::signatures::encode_google_function_call_bridge;

/// 回给客户端的事件名. 顺序与 Anthropic 的规定一致.
///
/// Event names returned to the client; the order matches Anthropic's specification.
pub(crate) const EVENT_MESSAGE_START: &str = "message_start";
pub(crate) const EVENT_CONTENT_BLOCK_START: &str = "content_block_start";
pub(crate) const EVENT_CONTENT_BLOCK_DELTA: &str = "content_block_delta";
pub(crate) const EVENT_CONTENT_BLOCK_STOP: &str = "content_block_stop";
pub(crate) const EVENT_MESSAGE_DELTA: &str = "message_delta";
pub(crate) const EVENT_MESSAGE_STOP: &str = "message_stop";

/// 增量事件的类型取值.
///
/// Delta event type values.
pub(crate) const DELTA_TEXT: &str = "text_delta";
pub(crate) const DELTA_THINKING: &str = "thinking_delta";
pub(crate) const DELTA_JSON: &str = "input_json_delta";
pub(crate) const DELTA_CITATIONS: &str = "citations_delta";

/// 停止原因. Anthropic 的取值集合是固定的几个.
///
/// Stop reasons; Anthropic's value set is the fixed few below.
pub(crate) const STOP_END_TURN: &str = "end_turn";
pub(crate) const STOP_MAX_TOKENS: &str = "max_tokens";
pub(crate) const STOP_TOOL_USE: &str = "tool_use";

/// 转换上游响应时出现的错误.
///
/// 这些错误描述的是上游数据或本端编码问题, 与 InvalidRequest 区分的依据是: 它们出现在响应
/// 阶段, 换账号或重试可能改变结果, 因此调用方按服务端错误处理.
///
/// An error raised while converting an upstream response. These describe upstream data or local
/// encoding problems; unlike InvalidRequest they occur in the response phase, where rotation or a
/// retry may change the outcome, so the caller treats them as server errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranslateError {
    message: String,
}

impl TranslateError {
    /// 用具体原因构造错误.
    ///
    /// Builds the error from a concrete reason.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// 返回错误信息.
    ///
    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TranslateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TranslateError {}

/// 上游事件里 content 的 role 与 parts.
///
/// The role and parts of content inside an upstream event.
#[derive(Debug, Default, Deserialize)]
struct GeminiContent {
    // role 只参与解码的类型校验, 与 Go 的结构体字段一致: 类型不符时整份事件被丢弃.
    //
    // role only participates in the decode type check, matching Go's struct field: a mismatched
    // type drops the whole event.
    #[allow(dead_code)]
    #[serde(default, deserialize_with = "zero_on_null")]
    role: String,
    #[serde(default, deserialize_with = "zero_on_null_list")]
    parts: Vec<GeminiPart>,
}

/// 一个内容段落. 三类内容互斥, 因此合在一个结构里.
///
/// One content segment. The three kinds of content are mutually exclusive, so they are combined
/// in one struct.
#[derive(Debug, Default, Deserialize)]
struct GeminiPart {
    #[serde(default, deserialize_with = "zero_on_null")]
    text: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    thought: bool,
    #[serde(
        rename = "thoughtSignature",
        default,
        deserialize_with = "zero_on_null"
    )]
    thought_signature: String,
    #[serde(rename = "functionCall", default)]
    function_call: Option<GeminiFunctionCall>,
}

/// 一次工具调用. args 保留上游给出的原始文本, 与 Go 的 json.RawMessage 对应.
///
/// One tool call. args keeps the raw text given by upstream, corresponding to Go's
/// json.RawMessage.
#[derive(Debug, Default, Deserialize)]
struct GeminiFunctionCall {
    #[serde(default, deserialize_with = "zero_on_null")]
    id: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    name: String,
    /// 参数保留上游给出的原始文本, 与 Go 的 json.RawMessage 对应: 显式 null 得到文本 null,
    /// 字段缺失时为空字段.
    ///
    /// The arguments keep the raw text given by upstream, corresponding to Go's json.RawMessage:
    /// an explicit null yields the text null and a missing field stays empty.
    #[serde(default)]
    args: RawField,
}

/// 一个候选.
///
/// One candidate.
#[derive(Debug, Default, Deserialize)]
struct GeminiCandidate {
    #[serde(default, deserialize_with = "zero_on_null")]
    content: GeminiContent,
    #[serde(rename = "finishReason", default, deserialize_with = "zero_on_null")]
    finish_reason: String,
    #[serde(rename = "groundingMetadata", default)]
    grounding: Option<GroundingMetadata>,
}

/// 上游用量快照. 只声明用得上的字段.
///
/// An upstream usage snapshot; only the fields actually used are declared.
#[derive(Debug, Default, Deserialize)]
struct GeminiUsageMetadata {
    #[serde(
        rename = "promptTokenCount",
        default,
        deserialize_with = "zero_on_null"
    )]
    prompt_token_count: i64,
    #[serde(
        rename = "candidatesTokenCount",
        default,
        deserialize_with = "zero_on_null"
    )]
    candidates_token_count: i64,
    #[serde(
        rename = "cachedContentTokenCount",
        default,
        deserialize_with = "zero_on_null"
    )]
    cached_content_token_count: i64,
    #[serde(
        rename = "thoughtsTokenCount",
        default,
        deserialize_with = "zero_on_null"
    )]
    thoughts_token_count: i64,
}

/// 上游事件的 response 字段.
///
/// The response field of an upstream event.
#[derive(Debug, Default, Deserialize)]
struct GeminiResponse {
    #[serde(default, deserialize_with = "zero_on_null_list")]
    candidates: Vec<GeminiCandidate>,
    #[serde(rename = "usageMetadata", default)]
    usage_metadata: GeminiUsageMetadata,
    // modelVersion 与 responseId 只参与解码的类型校验, 与 Go 的结构体字段一致.
    //
    // modelVersion and responseId only participate in the decode type check, matching Go's struct
    // fields.
    #[allow(dead_code)]
    #[serde(rename = "modelVersion", default, deserialize_with = "zero_on_null")]
    model_version: String,
    #[allow(dead_code)]
    #[serde(rename = "responseId", default, deserialize_with = "zero_on_null")]
    response_id: String,
}

/// 一个完整的上游事件.
///
/// One complete upstream event.
#[derive(Debug, Default, Deserialize)]
struct GeminiEvent {
    #[serde(default, deserialize_with = "zero_on_null")]
    response: GeminiResponse,
}

/// 内容块的种类. 用枚举表达互斥状态, 不用多个布尔字段组合.
///
/// The kind of a content block. An enum expresses the mutually exclusive states instead of a
/// combination of boolean fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockKind {
    Text,
    Thinking,
    ToolUse,
}

impl BlockKind {
    /// 回给客户端的内容块类型名.
    ///
    /// The content block type name given to the client.
    fn as_str(self) -> &'static str {
        match self {
            Self::Text => BLOCK_TEXT,
            Self::Thinking => BLOCK_THINKING,
            Self::ToolUse => BLOCK_TOOL_USE,
        }
    }
}

/// 停止原因. 用枚举表达互斥状态.
///
/// The stop reason, expressed as an enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopReason {
    EndTurn,
    MaxTokens,
}

impl StopReason {
    /// 回给客户端的停止原因名.
    ///
    /// The stop reason name given to the client.
    fn as_str(self) -> &'static str {
        match self {
            Self::EndTurn => STOP_END_TURN,
            Self::MaxTokens => STOP_MAX_TOKENS,
        }
    }
}

/// 非流式输出时按顺序累积的内容块.
///
/// A content block accumulated in order for non-streaming output.
#[derive(Debug)]
struct AggregatedBlock {
    kind: BlockKind,
    text: String,
    id: String,
    name: String,
    args: String,
    /// 紧邻工具块输出, 保留此工具原本的签名及上游调用 id.
    ///
    /// Emitted right before the tool block, preserving this tool's original signature and
    /// upstream call id.
    bridge_data: String,
}

/// 把上游事件流转换成 Anthropic Messages 的转换器. 每个请求一份.
///
/// The translator that converts the upstream event stream into Anthropic Messages. One instance
/// per request.
#[derive(Debug)]
pub struct AnthropicTranslator {
    model: String,
    stream: bool,
    message_id: String,
    /// 工具调用标识的固定前缀, 由消息标识的前 12 位组成.
    ///
    /// The fixed prefix of a tool call id, formed from the first 12 characters of the message id.
    tool_prefix: String,

    started: bool,
    block_index: i64,
    open_kind: Option<BlockKind>,
    tool_counter: i64,

    saw_tool_call: bool,
    stop_reason: Option<StopReason>,
    input_tokens: i64,
    output_tokens: i64,
    thinking_tokens: i64,
    /// 上游报告的缓存命中量. Google 的 promptTokenCount 含这部分, 而 Anthropic 的
    /// input_tokens 不含, 因此输出时要扣除并单独回传.
    ///
    /// The cache hit count reported by upstream. Google's promptTokenCount includes it while
    /// Anthropic's input_tokens does not, so it is subtracted on output and reported separately.
    cache_read_tokens: i64,

    /// 只在非流式时使用, 用来按顺序拼出完整的 content 数组.
    ///
    /// Used only for non-streaming output, to assemble the complete content array in order.
    blocks: Vec<AggregatedBlock>,
    search: Option<WebSearchResponse>,
}

impl AnthropicTranslator {
    /// 构造翻译器. stream 为假时先聚合再一次性输出.
    ///
    /// Builds the translator. A false stream aggregates first and emits once.
    pub fn new(model: &str, stream: bool) -> Self {
        let message_id = format!("msg_{}", new_identifier());
        Self {
            tool_prefix: tool_prefix_for(&message_id),
            model: model.to_owned(),
            stream,
            message_id,
            started: false,
            block_index: 0,
            open_kind: None,
            tool_counter: 0,
            saw_tool_call: false,
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            thinking_tokens: 0,
            cache_read_tokens: 0,
            blocks: Vec::new(),
            search: None,
        }
    }

    /// 启用服务端搜索记录. required 为真时缺少执行证据必须报错.
    ///
    /// Enables the server-side search record. When required is true, missing execution evidence
    /// must error.
    pub fn enable_web_search(&mut self, required: bool) {
        self.search = Some(WebSearchResponse::new(required));
    }

    /// 回给客户端的内容类型.
    ///
    /// The content type returned to the client.
    pub fn content_type(&self) -> &'static str {
        if self.stream {
            "text/event-stream"
        } else {
            "application/json"
        }
    }

    /// 生成先于首个内容块写出的起始消息.
    ///
    /// 只有流式请求需要在最前面写出 message_start, 而非流式请求要等内容到齐后一次性输出, 因此
    /// 这里对非流式返回空. 调用方先 Event 再 Begin, 才能把上游已经报告的真实输入和缓存计数放入
    /// message_start; 起始输出保持零占位, 防止客户端按内容块重复累计输出计数.
    ///
    /// Generates the start message written before the first content block. Only a streaming
    /// request needs message_start first; a non-streaming request emits everything at once once
    /// content is complete, so this returns empty for non-streaming. The caller runs Event before
    /// Begin so the real input and cache counts already reported by upstream go into
    /// message_start, and the start output keeps a zero placeholder so the client does not
    /// accumulate the output count again per content block.
    pub fn begin(&self) -> Result<Vec<u8>, TranslateError> {
        if !self.stream {
            return Ok(Vec::new());
        }
        let mut usage = self.usage_payload(self.non_cached_input_tokens());
        usage.insert("output_tokens".to_owned(), Value::Number(0.into()));
        Ok(sse_event(
            EVENT_MESSAGE_START,
            json!({
                "type": EVENT_MESSAGE_START,
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model,
                    "content": [],
                    "stop_reason": Value::Null,
                    "stop_sequence": Value::Null,
                    "usage": usage,
                },
            }),
        ))
    }

    /// 上游是否已经产生候选内容, 而不是仅仅建立了 HTTP 连接.
    ///
    /// Reports whether upstream has produced candidate content, not merely established an HTTP
    /// connection.
    pub fn started(&self) -> bool {
        self.started
    }

    /// 处理一个上游事件, 返回需要写给客户端的内容.
    ///
    /// 异常结束可能和工具调用处于同一事件, 因此先检查本事件的全部候选, 再消费任何内容: 若先
    /// 生成工具块或成功结束标记, 客户端就会把失败当成正常回答. 单个事件解析失败不应中断整段
    /// 响应, 跳过即可, 但搜索模式必须报错, 否则搜索会被当成成功.
    ///
    /// Handles one upstream event and returns the content to write to the client. An abnormal
    /// finish may share an event with a tool call, so all candidates are checked before any
    /// content is consumed: generating a tool block or a success marker first would make the
    /// client treat the failure as a normal answer. A single event failing to parse must not
    /// interrupt the whole response and is skipped, except in search mode where it must error,
    /// otherwise the search would be taken as successful.
    pub fn event(&mut self, payload: &[u8]) -> Result<Vec<u8>, TranslateError> {
        if let Some(search) = self.search.as_mut()
            && search.add_event_bytes(payload.len())
        {
            return Err(TranslateError::new(
                "Google web search response exceeds buffer limit",
            ));
        }
        let trimmed = payload.trim_ascii();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let parsed = match serde_json::from_slice::<GeminiEvent>(trimmed) {
            Ok(parsed) => parsed,
            Err(_) => {
                if self.search.is_some() {
                    return Err(TranslateError::new("invalid Google web search event"));
                }
                return Ok(Vec::new());
            }
        };
        if self.search.is_some() && parsed.response.candidates.len() > 1 {
            return Err(TranslateError::new(
                "Google web search returned multiple candidates with ambiguous source indices",
            ));
        }
        for candidate in &parsed.response.candidates {
            match candidate.finish_reason.as_str() {
                "" | "FINISH_REASON_UNSPECIFIED" | "STOP" | "MAX_TOKENS" => {}
                other => {
                    return Err(TranslateError::new(format!(
                        "Google 上游异常结束: finishReason={other}"
                    )));
                }
            }
        }
        let usage = &parsed.response.usage_metadata;
        if usage.prompt_token_count > 0 {
            self.input_tokens = usage.prompt_token_count;
        }
        if usage.candidates_token_count > 0 {
            self.output_tokens = usage.candidates_token_count;
        }
        if usage.cached_content_token_count > 0 {
            self.cache_read_tokens = usage.cached_content_token_count;
        }
        if usage.thoughts_token_count > 0 {
            self.thinking_tokens = usage.thoughts_token_count;
        }
        if self
            .output_tokens
            .checked_add(self.thinking_tokens)
            .is_none()
        {
            return Err(TranslateError::new("Google 上游输出用量超出整数范围"));
        }

        let mut out = Vec::new();
        for candidate in &parsed.response.candidates {
            if let Some(search) = self.search.as_mut() {
                search.collect_grounding(candidate.grounding.as_ref());
            }
            let finished = matches!(candidate.finish_reason.as_str(), "STOP" | "MAX_TOKENS");
            if finished {
                self.started = true;
            }
            for part in &candidate.content.parts {
                let has_call = part
                    .function_call
                    .as_ref()
                    .is_some_and(|call| !call.name.is_empty());
                if !part.text.is_empty() || !part.thought_signature.is_empty() || has_call {
                    self.started = true;
                }
                out.extend(self.consume_part(part)?);
            }
            if finished {
                self.apply_finish_reason(&candidate.finish_reason);
            }
        }
        Ok(out)
    }

    /// 在事件流结束后收尾.
    ///
    /// HTTP 正常关闭不等于模型已经完成. 缺少终态时不能补发正常结束事件, 否则客户端会把半句话
    /// 或尚未完成的工具调用当作完整回答.
    ///
    /// Finishes after the event stream ends. A normal HTTP close does not mean the model
    /// finished: without a terminal state a success event must not be emitted, or the client
    /// would take half a sentence or an unfinished tool call as a complete answer.
    pub fn end(&mut self) -> Result<Vec<u8>, TranslateError> {
        let Some(stop_reason) = self.stop_reason else {
            return Err(TranslateError::new(
                "Google 上游缺少 finishReason 结束标记: unexpected EOF",
            ));
        };
        let search_content = match self.search.as_mut() {
            Some(search) => {
                if stop_reason != StopReason::EndTurn {
                    return Err(TranslateError::new(
                        "Google web search was truncated before completion",
                    ));
                }
                Some(search.content(&self.message_id)?)
            }
            None => None,
        };
        let mut out = Vec::new();
        if let Some(content) = search_content {
            if !self.stream {
                return self.marshal_message(content);
            }
            out.extend(self.emit_search_content(content));
        } else if !self.stream {
            return self.aggregated_response();
        }
        out.extend(self.close_block());
        out.extend(sse_event(
            EVENT_MESSAGE_DELTA,
            json!({
                "type": EVENT_MESSAGE_DELTA,
                "delta": {
                    "stop_reason": self.final_stop_reason(),
                    "stop_sequence": Value::Null,
                },
                "usage": self.usage_payload(self.non_cached_input_tokens()),
            }),
        ));
        out.extend(sse_event(
            EVENT_MESSAGE_STOP,
            json!({"type": EVENT_MESSAGE_STOP}),
        ));
        Ok(out)
    }

    /// 处理一个内容段落.
    ///
    /// Handles one content segment.
    fn consume_part(&mut self, part: &GeminiPart) -> Result<Vec<u8>, TranslateError> {
        if let Some(search) = self.search.as_mut() {
            if part.function_call.is_some() {
                return Err(TranslateError::new(
                    "Google web search returned a client function call instead of executing search",
                ));
            }
            // 搜索正文先缓冲, 直到正常终态与 grounding 数据都经过校验后再返回; 思考段落不进入
            // 搜索正文.
            //
            // The search body is buffered until both the normal terminal state and the grounding
            // data have been validated; thinking segments do not enter the search body.
            if !part.thought {
                search.push_text(&part.text);
            }
            return Ok(Vec::new());
        }
        if let Some(call) = part.function_call.as_ref()
            && !call.name.is_empty()
        {
            // 上游一次给出完整的参数对象, 缺省时按空对象处理, 与 Go 的 len(args)==0 一致:
            // 显式 null 得到文本 null, 缺失时才用空对象.
            //
            // Upstream gives the complete argument object at once; a missing one becomes an empty
            // object, matching Go's len(args)==0: an explicit null yields the text null and only a
            // missing field uses the empty object.
            let args = call
                .args
                .text()
                .map(str::to_owned)
                .unwrap_or_else(|| "{}".to_owned());
            return self.emit_tool_call(&call.name, &args, &part.thought_signature, &call.id);
        }
        if part.text.is_empty() {
            // 只有签名的段落不含可见内容, 忽略即可.
            //
            // A segment carrying only a signature has no visible content and is ignored.
            return Ok(Vec::new());
        }
        let kind = if part.thought {
            BlockKind::Thinking
        } else {
            BlockKind::Text
        };
        Ok(self.emit_textual(kind, &part.text))
    }

    /// 写出一段文本或思考内容.
    ///
    /// Emits one text or thinking segment.
    fn emit_textual(&mut self, kind: BlockKind, text: &str) -> Vec<u8> {
        if !self.stream {
            self.append_aggregated(kind, text, "", "");
            return Vec::new();
        }
        let mut out = Vec::new();
        if self.open_kind != Some(kind) {
            out.extend(self.close_block());
            out.extend(self.open_block(kind, "", ""));
        }
        let delta_type = match kind {
            BlockKind::Thinking => DELTA_THINKING,
            _ => DELTA_TEXT,
        };
        let field = match kind {
            BlockKind::Thinking => "thinking",
            _ => "text",
        };
        let mut delta = Map::new();
        delta.insert("type".to_owned(), Value::String(delta_type.to_owned()));
        delta.insert(field.to_owned(), Value::String(text.to_owned()));
        out.extend(sse_event(
            EVENT_CONTENT_BLOCK_DELTA,
            json!({
                "type": EVENT_CONTENT_BLOCK_DELTA,
                "index": self.block_index,
                "delta": delta,
            }),
        ));
        out
    }

    /// 写出一次工具调用.
    ///
    /// Gemini 一次性给出完整的参数对象, 而 Anthropic 的形式是分片的 JSON 增量. 这里用一个增量
    /// 把完整参数送出, 语义等价, 客户端按累积拼接即可得到同样的结果. 需要保留签名时, 载体以标准
    /// redacted_thinking 块紧邻工具块之前输出, 使用独立索引并在工具块之前结束.
    ///
    /// Emits one tool call. Gemini provides the complete argument object at once while Anthropic's
    /// form is a chunked JSON delta; one delta carries the complete arguments, which is
    /// semantically equivalent. When a signature must be preserved, the carrier is emitted as a
    /// standard redacted_thinking block right before the tool block, with its own index and closed
    /// before the tool block starts.
    fn emit_tool_call(
        &mut self,
        name: &str,
        args: &str,
        thought_signature: &str,
        function_call_id: &str,
    ) -> Result<Vec<u8>, TranslateError> {
        self.saw_tool_call = true;
        self.tool_counter += 1;
        let call_id = format!("{}{}", self.tool_prefix, self.tool_counter);
        let bridge_data = if thought_signature.is_empty() && function_call_id.is_empty() {
            String::new()
        } else {
            encode_google_function_call_bridge(&call_id, thought_signature, function_call_id)?
        };
        if !self.stream {
            self.append_aggregated(BlockKind::ToolUse, "", &call_id, name);
            if let Some(last) = self.blocks.last_mut() {
                last.args = args.to_owned();
                last.bridge_data = bridge_data;
            }
            return Ok(Vec::new());
        }
        let mut out = self.close_block();
        if !bridge_data.is_empty() {
            out.extend(sse_event(
                EVENT_CONTENT_BLOCK_START,
                json!({
                    "type": EVENT_CONTENT_BLOCK_START,
                    "index": self.block_index,
                    "content_block": {"type": BLOCK_REDACTED_THINKING, "data": bridge_data},
                }),
            ));
            out.extend(sse_event(
                EVENT_CONTENT_BLOCK_STOP,
                json!({"type": EVENT_CONTENT_BLOCK_STOP, "index": self.block_index}),
            ));
            self.block_index += 1;
        }
        out.extend(self.open_block(BlockKind::ToolUse, &call_id, name));
        out.extend(sse_event(
            EVENT_CONTENT_BLOCK_DELTA,
            json!({
                "type": EVENT_CONTENT_BLOCK_DELTA,
                "index": self.block_index,
                "delta": {"type": DELTA_JSON, "partial_json": args},
            }),
        ));
        // 参数已经一次送完, 因此立刻关闭这个块.
        //
        // The arguments were sent in one piece, so the block is closed immediately.
        out.extend(self.close_block());
        Ok(out)
    }

    /// 打开一个内容块.
    ///
    /// Opens one content block.
    fn open_block(&mut self, kind: BlockKind, id: &str, name: &str) -> Vec<u8> {
        let mut block = Map::new();
        block.insert("type".to_owned(), Value::String(kind.as_str().to_owned()));
        match kind {
            BlockKind::Text => {
                block.insert("text".to_owned(), Value::String(String::new()));
            }
            BlockKind::Thinking => {
                block.insert("thinking".to_owned(), Value::String(String::new()));
            }
            BlockKind::ToolUse => {
                block.insert("id".to_owned(), Value::String(id.to_owned()));
                block.insert("name".to_owned(), Value::String(name.to_owned()));
                block.insert("input".to_owned(), Value::Object(Map::new()));
            }
        }
        self.open_kind = Some(kind);
        sse_event(
            EVENT_CONTENT_BLOCK_START,
            json!({
                "type": EVENT_CONTENT_BLOCK_START,
                "index": self.block_index,
                "content_block": block,
            }),
        )
    }

    /// 关闭当前打开的内容块. 没有打开的块时返回空.
    ///
    /// Closes the currently open content block; returns empty when none is open.
    fn close_block(&mut self) -> Vec<u8> {
        if self.open_kind.take().is_none() {
            return Vec::new();
        }
        let out = sse_event(
            EVENT_CONTENT_BLOCK_STOP,
            json!({"type": EVENT_CONTENT_BLOCK_STOP, "index": self.block_index}),
        );
        self.block_index += 1;
        out
    }

    /// 记录停止原因. 上游可能多次给出该字段, 以最后一次为准.
    ///
    /// Records the stop reason. Upstream may provide the field several times and the last one
    /// wins.
    fn apply_finish_reason(&mut self, reason: &str) {
        match reason {
            "MAX_TOKENS" => self.stop_reason = Some(StopReason::MaxTokens),
            "STOP" => self.stop_reason = Some(StopReason::EndTurn),
            _ => {}
        }
    }

    /// 给出最终的停止原因. 出现过工具调用时以工具调用为准, 这与 Anthropic 的语义一致:
    /// 客户端需要据此决定是否继续执行工具.
    ///
    /// Gives the final stop reason. When a tool call occurred it wins, matching Anthropic's
    /// semantics: the client uses it to decide whether to continue executing tools.
    fn final_stop_reason(&self) -> &'static str {
        if self.saw_tool_call {
            return STOP_TOOL_USE;
        }
        self.stop_reason.map(StopReason::as_str).unwrap_or("")
    }

    /// 累加非流式输出的内容块. 相邻的同类文本会并入同一块, 与流式输出中块只在类型切换时才
    /// 关闭的行为一致.
    ///
    /// Accumulates a non-streaming content block. Adjacent text of the same kind merges into one
    /// block, matching streaming output where a block closes only when the kind changes.
    fn append_aggregated(&mut self, kind: BlockKind, text: &str, id: &str, name: &str) {
        if let Some(last) = self.blocks.last_mut()
            && last.kind == kind
            && kind != BlockKind::ToolUse
        {
            last.text.push_str(text);
            return;
        }
        self.blocks.push(AggregatedBlock {
            kind,
            text: text.to_owned(),
            id: id.to_owned(),
            name: name.to_owned(),
            args: String::new(),
            bridge_data: String::new(),
        });
    }

    /// 拼出非流式的完整响应.
    ///
    /// Assembles the complete non-streaming response.
    fn aggregated_response(&self) -> Result<Vec<u8>, TranslateError> {
        let mut content = Vec::with_capacity(self.blocks.len());
        for block in &self.blocks {
            match block.kind {
                BlockKind::Text => {
                    if block.text.is_empty() {
                        continue;
                    }
                    content.push(json!({"type": BLOCK_TEXT, "text": block.text}));
                }
                BlockKind::Thinking => {
                    if block.text.is_empty() {
                        continue;
                    }
                    content.push(json!({"type": BLOCK_THINKING, "thinking": block.text}));
                }
                BlockKind::ToolUse => {
                    if !block.bridge_data.is_empty() {
                        content.push(json!({
                            "type": BLOCK_REDACTED_THINKING,
                            "data": block.bridge_data,
                        }));
                    }
                    // 参数无法解析时退回空对象, 与 Go 保留初始值的行为一致.
                    //
                    // When the arguments cannot be parsed it falls back to an empty object,
                    // matching Go keeping its initial value.
                    let input = if block.args.is_empty() {
                        Value::Object(Map::new())
                    } else {
                        serde_json::from_str::<Value>(&block.args)
                            .unwrap_or_else(|_| Value::Object(Map::new()))
                    };
                    content.push(json!({
                        "type": BLOCK_TOOL_USE,
                        "id": block.id,
                        "name": block.name,
                        "input": input,
                    }));
                }
            }
        }
        self.marshal_message(content)
    }

    /// 编码非流式的完整报文.
    ///
    /// Encodes the complete non-streaming message.
    fn marshal_message(&self, content: Vec<Value>) -> Result<Vec<u8>, TranslateError> {
        let payload = json!({
            "id": self.message_id,
            "type": "message",
            "role": "assistant",
            "model": self.model,
            "content": content,
            "stop_reason": self.final_stop_reason(),
            "stop_sequence": Value::Null,
            "usage": self.usage_payload(self.non_cached_input_tokens()),
        });
        serde_json::to_vec(&payload)
            .map_err(|err| TranslateError::new(format!("编码 Anthropic 报文失败: {err}")))
    }

    /// 组装回给客户端的用量对象.
    ///
    /// cache_read_input_tokens 是 Anthropic 用来表示缓存命中输入量的字段, 这里由上游的
    /// cachedContentTokenCount 映射而来. 上游用量到达之前 input_tokens 为 0.
    ///
    /// Assembles the usage object returned to the client. cache_read_input_tokens is the field
    /// Anthropic uses for cached input, mapped here from upstream's cachedContentTokenCount.
    /// Before upstream usage arrives input_tokens is 0.
    fn usage_payload(&self, input_tokens: i64) -> Map<String, Value> {
        let mut usage = Map::new();
        usage.insert(
            "input_tokens".to_owned(),
            Value::Number(input_tokens.into()),
        );
        // Event 已经拒绝输出与思考相加溢出的用量, 这里用饱和加法保持无 panic.
        //
        // Event already rejects usage whose output and thinking counts overflow, so a saturating
        // add keeps this panic-free.
        usage.insert(
            "output_tokens".to_owned(),
            Value::Number(
                self.output_tokens
                    .saturating_add(self.thinking_tokens)
                    .into(),
            ),
        );
        usage.insert(
            "cache_read_input_tokens".to_owned(),
            Value::Number(self.cache_read_tokens.into()),
        );
        if self
            .search
            .as_ref()
            .is_some_and(WebSearchResponse::executed)
        {
            usage.insert(
                "server_tool_use".to_owned(),
                json!({"web_search_requests": 1}),
            );
        }
        usage
    }

    /// 返回未命中提示缓存的输入 token 数.
    ///
    /// Google 的 promptTokenCount 含命中缓存的输入, 而 Anthropic 的 input_tokens 只计未命中的
    /// 部分, 命中量另由 cache_read_input_tokens 表示. 若原样上报, 客户端会把缓存命中的部分也算
    /// 成新输入. 缓存数异常大于提示数时按 0 处理, 避免出现负数.
    ///
    /// Returns the input token count that missed the prompt cache. Google's promptTokenCount
    /// includes cache hits, while Anthropic's input_tokens counts only misses and reports hits
    /// separately as cache_read_input_tokens. Reporting the raw value would make the client count
    /// cache hits as new input. When the cache count is abnormally larger than the prompt count it
    /// is treated as 0 to avoid a negative value.
    fn non_cached_input_tokens(&self) -> i64 {
        // 两个取值都只被赋予正数, 减法在 i64 范围内; 缓存数异常大于提示数时按 0 处理, 与 Go 的
        // remaining < 0 分支相同. checked_sub 只用于排除溢出, 不改变取值.
        //
        // Both values are only ever assigned positive numbers, so the subtraction stays within
        // i64; an abnormally larger cache count is treated as 0, matching Go's remaining < 0
        // branch. checked_sub only rules out overflow and does not change the value.
        self.input_tokens
            .checked_sub(self.cache_read_tokens)
            .filter(|remaining| *remaining >= 0)
            .unwrap_or(0)
    }

    /// 保持 Anthropic 的块索引与增量顺序输出搜索结果.
    ///
    /// 搜索结果本身在 start 事件中给出, query 与文本使用标准增量, 引用用 citations_delta, 使
    /// Claude Code 能解析并统计服务端调用.
    ///
    /// Emits the search results while preserving Anthropic's block indices and delta order. The
    /// search results themselves are given in the start event, the query and text use standard
    /// deltas, and citations use citations_delta, so Claude Code can parse and count the
    /// server-side call.
    fn emit_search_content(&mut self, content: Vec<Value>) -> Vec<u8> {
        let mut out = Vec::new();
        for value in content {
            let Some(block) = value.as_object() else {
                continue;
            };
            let mut start = block.clone();
            let mut delta: Option<Map<String, Value>> = None;
            match block.get("type").and_then(Value::as_str) {
                Some(BLOCK_SERVER_TOOL_USE) => {
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    start.insert("input".to_owned(), Value::Object(Map::new()));
                    let mut created = Map::new();
                    created.insert("type".to_owned(), Value::String(DELTA_JSON.to_owned()));
                    created.insert("partial_json".to_owned(), Value::String(input.to_string()));
                    delta = Some(created);
                }
                Some(BLOCK_TEXT) => {
                    let text = block.get("text").cloned().unwrap_or(Value::Null);
                    start.insert("text".to_owned(), Value::String(String::new()));
                    start.remove("citations");
                    let mut created = Map::new();
                    created.insert("type".to_owned(), Value::String(DELTA_TEXT.to_owned()));
                    created.insert("text".to_owned(), text);
                    delta = Some(created);
                }
                _ => {}
            }
            out.extend(sse_event(
                EVENT_CONTENT_BLOCK_START,
                json!({
                    "type": EVENT_CONTENT_BLOCK_START,
                    "index": self.block_index,
                    "content_block": start,
                }),
            ));
            if let Some(delta) = delta {
                out.extend(sse_event(
                    EVENT_CONTENT_BLOCK_DELTA,
                    json!({
                        "type": EVENT_CONTENT_BLOCK_DELTA,
                        "index": self.block_index,
                        "delta": delta,
                    }),
                ));
            }
            if let Some(Value::Array(citations)) = block.get("citations") {
                for citation in citations {
                    out.extend(sse_event(
                        EVENT_CONTENT_BLOCK_DELTA,
                        json!({
                            "type": EVENT_CONTENT_BLOCK_DELTA,
                            "index": self.block_index,
                            "delta": {"type": DELTA_CITATIONS, "citation": citation},
                        }),
                    ));
                }
            }
            out.extend(sse_event(
                EVENT_CONTENT_BLOCK_STOP,
                json!({"type": EVENT_CONTENT_BLOCK_STOP, "index": self.block_index}),
            ));
            self.block_index += 1;
        }
        out
    }
}

/// 由消息标识推导工具调用标识前缀.
///
/// Go 用消息标识的第 4 到 16 个字符组成工具标识前缀; 生成器固定给出 32 位十六进制, 这里按字节取
/// 前 12 位, 用 get 避免越界 panic.
///
/// Derives the tool call id prefix from the message id. Go formed it from characters 4 to 16 of
/// the message id; the generator always produces 32 hex digits, so the first 12 are taken here,
/// with get to avoid an out-of-range panic.
fn tool_prefix_for(message_id: &str) -> String {
    let identifier = message_id.strip_prefix("msg_").unwrap_or(message_id);
    format!("toolu_{}_", identifier.get(..12).unwrap_or(identifier))
}

/// 拼一个 Anthropic 事件. 事件名同时出现在 event 行与 data 的 type 字段.
///
/// Assembles one Anthropic event. The event name appears both on the event line and in the data
/// type field.
fn sse_event(name: &str, payload: Value) -> Vec<u8> {
    // 载荷只包含字符串与数字, 序列化不会失败; Go 版在编码失败时返回空切片, 这里保持一致.
    //
    // The payload holds only strings and numbers, so serialization cannot fail; Go returned an
    // empty slice on an encoding failure, kept the same here.
    let body = serde_json::to_vec(&payload).unwrap_or_default();
    let mut out = Vec::with_capacity(name.len() + body.len() + 16);
    out.extend_from_slice(b"event: ");
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(b"\ndata: ");
    out.extend_from_slice(&body);
    out.extend_from_slice(b"\n\n");
    out
}
