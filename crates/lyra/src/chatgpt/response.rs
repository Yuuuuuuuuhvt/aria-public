//! Codex Responses 事件流到 Anthropic Messages 的响应转换.
//!
//! 按照 docs/architecture.md 3.7 节实现:
//! 1. 首个 response.created 或内容事件开启 message_start, 消息编号为 msg_ 加 32 位十六进制随机数;
//! 2. output_item.added 与 output_item.done 驱动 thinking, text 与 tool_use 块的生命周期;
//! 3. 推理条目在 output_item.done 时生成推理桥并作为 signature_delta 写出;
//! 4. function_call 的 arguments 增量按条目分别累积, 在 output_item.done 时以完成事件给出的完整
//!    参数一次性写出 tool_use 块; 两者不一致时以完成事件为准并记录长度与哈希的警告, 参数完全
//!    缺失时按流不完整报错;
//! 5. response.completed 换算缓存与输入输出用量, stop_reason 按是否出现过工具调用判定;
//! 6. response.incomplete 在 reason 为 max_output_tokens 或 length 时按 stop_reason max_tokens
//!    正常结束 (前者为 Codex 与 Grok 的取值, 后者为 Command Code Provider API 的取值), 其余原因
//!    与 failed, error 事件一样视为失败;
//! 7. 非流式请求在内存中聚合完整 content 数组后一次性返回 Anthropic 报文.
//!
//! Response conversion from Codex Responses event stream to Anthropic Messages.
//! Implemented according to section 3.7 of docs/architecture.md:
//! 1. The first response.created or content event opens message_start with a msg_ + 32 hex random id;
//! 2. output_item.added and output_item.done drive block lifecycles for thinking, text and tool_use;
//! 3. Reasoning items produce a reasoning bridge at output_item.done and emit signature_delta;
//! 4. function_call argument deltas accumulate per item and are emitted as one complete tool_use
//!    block at output_item.done; a disagreement prefers the done arguments with a warning carrying
//!    lengths and hashes, and completely missing arguments fail the stream;
//! 5. response.completed converts cached and total input/output tokens, stop_reason checks function_call;
//! 6. response.incomplete with reason max_output_tokens or length ends normally with stop_reason
//!    max_tokens (the former is the Codex and Grok value, the latter the Command Code Provider API
//!    value), other reasons like failed and error events are treated as failures;
//! 7. Non-streaming requests aggregate a full content array in memory and emit the Anthropic message once.

use http::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::signatures::{CHATGPT_REASONING_BRIDGE_TYPE, encode_reasoning_bridge};

/// 回给客户端的事件名常量.
///
/// Event name constants returned to the client.
pub(crate) const EVENT_MESSAGE_START: &str = "message_start";
pub(crate) const EVENT_CONTENT_BLOCK_START: &str = "content_block_start";
pub(crate) const EVENT_CONTENT_BLOCK_DELTA: &str = "content_block_delta";
pub(crate) const EVENT_CONTENT_BLOCK_STOP: &str = "content_block_stop";
pub(crate) const EVENT_MESSAGE_DELTA: &str = "message_delta";
pub(crate) const EVENT_MESSAGE_STOP: &str = "message_stop";

/// 转换响应时出现的错误, 携带对应 HTTP 状态码语义.
///
/// An error raised while converting responses, carrying HTTP status code semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatGptTranslateError {
    pub status: StatusCode,
    pub message: String,
}

impl ChatGptTranslateError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for ChatGptTranslateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.status, self.message)
    }
}

impl std::error::Error for ChatGptTranslateError {}

/// 当前处于打开状态的内容块.
///
/// The currently open content block.
enum CurrentBlock {
    Reasoning { id: String, thinking: String },
    Text { text: String },
}

/// 一个尚未完成的 function_call 条目.
///
/// 参数增量只在这里缓冲, 内容块等到 output_item.done 时以完成事件给出的完整参数一次性写出.
/// 条目按 output_index 与条目 id 区分: 上游允许并行工具调用, 多个条目的 added 事件可以全部先于
/// 任何一个完成事件到达, 参数增量按条目交错, 分开缓冲才不会被别的条目改写.
///
/// A function_call item that has not completed yet.
///
/// Argument deltas are buffered here and the content block is written in one piece at
/// output_item.done with the arguments that event provides. Items are told apart by output_index
/// and item id: the upstream allows parallel tool calls whose added events all arrive before any
/// completion event, so their deltas interleave and must not be merged into each other.
struct PendingFunctionCall {
    /// 条目 id, 取 output_item.added 的 item.id; 上游未提供时为空串.
    ///
    /// The item id from item.id of output_item.added; empty when the upstream omits it.
    item_id: String,
    /// 条目序号, 取事件顶层的 output_index; 上游未提供时为 None.
    ///
    /// The output_index from the event envelope; none when the upstream omits it.
    output_index: Option<i64>,
    call_id: String,
    name: String,
    /// function_call_arguments.delta 的拼接结果, 只与完成事件的参数比对, 不发给客户端.
    ///
    /// The concatenated deltas, compared against the completion event's arguments and never sent
    /// to the client on their own.
    accumulated_args: String,
    final_args: Option<String>,
}

/// ChatGPT 路由的 Anthropic 协议转换器. xai 线路的事件流与 Codex 逐字段一致, 同样使用本转换器,
/// 区别只在推理桥的类型标识.
///
/// The Anthropic protocol translator for the ChatGPT route. The xai line's event stream matches
/// Codex field by field and uses this translator too; only the reasoning bridge type differs.
pub struct ChatGptTranslator {
    model: String,
    stream: bool,
    /// 推理桥的类型标识, 决定 signature_delta 由哪条线路签发.
    ///
    /// The reasoning bridge type marker, deciding which line issues the signature_delta.
    bridge_type: &'static str,
    message_id: String,
    started: bool,
    block_index: usize,
    current_block: Option<CurrentBlock>,
    /// 尚未完成的 function_call 条目, 按到达顺序保存.
    ///
    /// function_call items that have not completed yet, kept in arrival order.
    pending_calls: Vec<PendingFunctionCall>,
    has_function_call: bool,
    completed: bool,
    stop_reason: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    aggregated_blocks: Vec<Value>,
}

impl ChatGptTranslator {
    /// 构造 chatgpt 线路的转换器. message_id 由 32 位十六进制随机数生成.
    ///
    /// Builds the translator of the chatgpt line. message_id is generated with 32 random hex
    /// digits.
    pub fn new(model: &str, stream: bool) -> Self {
        Self::for_line(model, stream, CHATGPT_REASONING_BRIDGE_TYPE)
    }

    /// 构造使用给定推理桥类型标识的转换器, xai 线路传入 XAI_REASONING_BRIDGE_TYPE.
    ///
    /// Builds a translator using the given reasoning bridge type marker; the xai line passes
    /// XAI_REASONING_BRIDGE_TYPE.
    pub fn for_line(model: &str, stream: bool, bridge_type: &'static str) -> Self {
        let identifier: [u8; 16] = rand::random();
        let message_id = format!("msg_{}", hex::encode(identifier));
        Self {
            model: model.to_owned(),
            stream,
            bridge_type,
            message_id,
            started: false,
            block_index: 0,
            current_block: None,
            pending_calls: Vec::new(),
            has_function_call: false,
            completed: false,
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            aggregated_blocks: Vec::new(),
        }
    }

    pub fn content_type(&self) -> &'static str {
        if self.stream {
            "text/event-stream"
        } else {
            "application/json"
        }
    }

    pub fn started(&self) -> bool {
        self.started
    }

    /// 起始消息: 流式在首个有效事件后写出 message_start, 非流式返回空.
    ///
    /// Start message: streaming writes message_start after the first valid event, non-streaming returns empty.
    pub fn begin(&self) -> Result<Vec<u8>, ChatGptTranslateError> {
        if !self.stream {
            return Ok(Vec::new());
        }
        let payload = json!({
            "type": EVENT_MESSAGE_START,
            "message": {
                "id": self.message_id,
                "type": "message",
                "role": "assistant",
                "model": self.model,
                "content": [],
                "stop_reason": Value::Null,
                "stop_sequence": Value::Null,
                "usage": {
                    "input_tokens": self.input_tokens,
                    "output_tokens": 0,
                    "cache_read_input_tokens": self.cache_read_tokens
                }
            }
        });
        Ok(format_sse_event(EVENT_MESSAGE_START, payload))
    }

    /// 处理一个上游 SSE 载荷.
    ///
    /// Handles one upstream SSE payload.
    pub fn event(&mut self, payload: &[u8]) -> Result<Vec<u8>, ChatGptTranslateError> {
        let trimmed = payload.trim_ascii();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let parsed: Value = match serde_json::from_slice(trimmed) {
            Ok(v) => v,
            Err(_) => return Ok(Vec::new()),
        };
        let Some(obj) = parsed.as_object() else {
            return Ok(Vec::new());
        };

        let raw_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
        let kind = raw_type.strip_prefix("response.").unwrap_or(raw_type);

        match kind {
            "created" => {
                self.started = true;
                self.extract_usage_from_response(obj.get("response"));
                Ok(Vec::new())
            }
            "in_progress" | "metadata" | "content_part.added" | "content_part.done" => {
                self.extract_usage_from_response(obj.get("response"));
                Ok(Vec::new())
            }
            "output_item.added" => {
                self.started = true;
                let output_index = obj.get("output_index").and_then(Value::as_i64);
                self.handle_output_item_added(obj.get("item"), output_index)
            }
            // Command Code 上游在 GLM-5.3 上发送 response.reasoning.delta 作为思考增量,
            // 同样转为 thinking 块的增量输出. Codex 与 Grok 不发送该事件, 因此对既有线路无影响.
            //
            // Upstream Command Code emits response.reasoning.delta for GLM-5.3 as thinking deltas,
            // converted into thinking block deltas here. Codex and Grok never emit this event,
            // so existing routes are unaffected.
            "reasoning_summary_text.delta" | "reasoning.delta" => {
                self.started = true;
                let delta = obj.get("delta").and_then(Value::as_str).unwrap_or("");
                self.handle_reasoning_delta(delta)
            }
            "output_text.delta" => {
                self.started = true;
                let delta = obj.get("delta").and_then(Value::as_str).unwrap_or("");
                self.handle_output_text_delta(delta)
            }
            "function_call_arguments.delta" => {
                self.started = true;
                let item_id = obj.get("item_id").and_then(Value::as_str).unwrap_or("");
                let output_index = obj.get("output_index").and_then(Value::as_i64);
                let delta = obj.get("delta").and_then(Value::as_str).unwrap_or("");
                self.handle_function_arguments_delta(item_id, output_index, delta)
            }
            "output_item.done" => {
                let output_index = obj.get("output_index").and_then(Value::as_i64);
                self.handle_output_item_done(obj.get("item"), output_index)
            }
            "completed" => {
                self.started = true;
                self.handle_completed(obj.get("response"))
            }
            "incomplete" => {
                self.started = true;
                self.handle_incomplete(obj.get("response"))
            }
            "failed" => {
                self.started = true;
                let (status, msg) = extract_failed_error(obj.get("response"));
                Err(ChatGptTranslateError::new(status, msg))
            }
            "error" => {
                self.started = true;
                let (status, msg) = extract_error_event(obj);
                Err(ChatGptTranslateError::new(status, msg))
            }
            _ => Ok(Vec::new()),
        }
    }

    /// 在事件流结束后收尾, 非流式在此处返回最终聚合并编码的 Anthropic 消息.
    ///
    /// Finishes after the stream closes, emitting the final aggregated Anthropic message for non-streaming.
    pub fn end(&mut self) -> Result<Vec<u8>, ChatGptTranslateError> {
        if !self.completed {
            return Err(ChatGptTranslateError::new(
                StatusCode::BAD_GATEWAY,
                "流在 response.completed 之前结束",
            ));
        }
        if self.stream {
            return Ok(Vec::new());
        }
        let message = json!({
            "id": self.message_id,
            "type": "message",
            "role": "assistant",
            "model": self.model,
            "content": self.aggregated_blocks,
            "stop_reason": self.stop_reason,
            "stop_sequence": Value::Null,
            "usage": {
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "cache_read_input_tokens": self.cache_read_tokens
            }
        });
        let bytes = serde_json::to_vec(&message).map_err(|err| {
            ChatGptTranslateError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
        })?;
        Ok(bytes)
    }

    fn handle_output_item_added(
        &mut self,
        item_val: Option<&Value>,
        output_index: Option<i64>,
    ) -> Result<Vec<u8>, ChatGptTranslateError> {
        let Some(item) = item_val.and_then(Value::as_object) else {
            return Ok(Vec::new());
        };
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
        let mut out = Vec::new();

        match item_type {
            "reasoning" => {
                let id = item
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                self.current_block = Some(CurrentBlock::Reasoning {
                    id,
                    thinking: String::new(),
                });
                if self.stream {
                    out.extend(format_sse_event(
                        EVENT_CONTENT_BLOCK_START,
                        json!({
                            "type": EVENT_CONTENT_BLOCK_START,
                            "index": self.block_index,
                            "content_block": {
                                "type": "thinking",
                                "thinking": ""
                            }
                        }),
                    ));
                }
            }
            "message" => {
                self.current_block = Some(CurrentBlock::Text {
                    text: String::new(),
                });
                if self.stream {
                    out.extend(format_sse_event(
                        EVENT_CONTENT_BLOCK_START,
                        json!({
                            "type": EVENT_CONTENT_BLOCK_START,
                            "index": self.block_index,
                            "content_block": {
                                "type": "text",
                                "text": ""
                            }
                        }),
                    ));
                }
            }
            "function_call" => {
                self.has_function_call = true;
                let item_id = item
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                self.pending_calls.push(PendingFunctionCall {
                    item_id,
                    output_index,
                    call_id,
                    name,
                    accumulated_args: String::new(),
                    final_args: None,
                });
            }
            _ => {}
        }

        Ok(out)
    }

    fn handle_reasoning_delta(&mut self, delta: &str) -> Result<Vec<u8>, ChatGptTranslateError> {
        let Some(CurrentBlock::Reasoning { thinking, .. }) = &mut self.current_block else {
            return Ok(Vec::new());
        };
        thinking.push_str(delta);
        if !self.stream || delta.is_empty() {
            return Ok(Vec::new());
        }
        Ok(format_sse_event(
            EVENT_CONTENT_BLOCK_DELTA,
            json!({
                "type": EVENT_CONTENT_BLOCK_DELTA,
                "index": self.block_index,
                "delta": {
                    "type": "thinking_delta",
                    "thinking": delta
                }
            }),
        ))
    }

    fn handle_output_text_delta(&mut self, delta: &str) -> Result<Vec<u8>, ChatGptTranslateError> {
        let Some(CurrentBlock::Text { text }) = &mut self.current_block else {
            return Ok(Vec::new());
        };
        text.push_str(delta);
        if !self.stream || delta.is_empty() {
            return Ok(Vec::new());
        }
        Ok(format_sse_event(
            EVENT_CONTENT_BLOCK_DELTA,
            json!({
                "type": EVENT_CONTENT_BLOCK_DELTA,
                "index": self.block_index,
                "delta": {
                    "type": "text_delta",
                    "text": delta
                }
            }),
        ))
    }

    // 有身份时必须匹配身份; 仅无身份的旧事件允许在唯一待完成调用上回退.
    // Identified events must match; legacy anonymous events may use the sole pending call.
    fn pending_call_index(
        &self,
        item_id: &str,
        output_index: Option<i64>,
    ) -> Result<usize, ChatGptTranslateError> {
        let mut matches = self.pending_calls.iter().enumerate().filter(|(_, call)| {
            if !item_id.is_empty() && !call.item_id.is_empty() {
                call.item_id == item_id
                    && (output_index.is_none()
                        || call.output_index.is_none()
                        || output_index == call.output_index)
            } else if let (Some(index), Some(expected)) = (output_index, call.output_index) {
                index == expected
            } else {
                item_id.is_empty() && output_index.is_none() && call.final_args.is_none()
            }
        });
        match (matches.next(), matches.next()) {
            (Some((index, _)), None) => Ok(index),
            _ => Err(ChatGptTranslateError::new(
                StatusCode::BAD_GATEWAY,
                "function call event has no unique pending item",
            )),
        }
    }

    fn handle_function_arguments_delta(
        &mut self,
        item_id: &str,
        output_index: Option<i64>,
        delta: &str,
    ) -> Result<Vec<u8>, ChatGptTranslateError> {
        let index = self.pending_call_index(item_id, output_index)?;
        self.pending_calls[index].accumulated_args.push_str(delta);
        Ok(Vec::new())
    }

    fn finish_function_call(
        &mut self,
        item: &serde_json::Map<String, Value>,
        output_index: Option<i64>,
    ) -> Result<Vec<u8>, ChatGptTranslateError> {
        let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");
        let index = self.pending_call_index(item_id, output_index)?;
        let final_args = item
            .get("arguments")
            .and_then(Value::as_str)
            .filter(|args| !args.is_empty())
            .ok_or_else(|| {
                ChatGptTranslateError::new(
                    StatusCode::BAD_GATEWAY,
                    "function call completed without final arguments",
                )
            })?;
        let input: Value = serde_json::from_str(final_args).map_err(|_| {
            ChatGptTranslateError::new(
                StatusCode::BAD_GATEWAY,
                "function call completed with invalid JSON arguments",
            )
        })?;
        if !input.is_object() {
            return Err(ChatGptTranslateError::new(
                StatusCode::BAD_GATEWAY,
                "function call arguments must be a JSON object",
            ));
        }
        let call = &mut self.pending_calls[index];
        if call.accumulated_args != final_args {
            aria_core::log!(
                "warning: function call arguments differ; using final arguments: streamed_len={} final_len={} streamed_sha256={} final_sha256={}",
                call.accumulated_args.len(),
                final_args.len(),
                hex::encode(Sha256::digest(call.accumulated_args.as_bytes())),
                hex::encode(Sha256::digest(final_args.as_bytes())),
            );
        }
        call.final_args = Some(final_args.to_owned());
        self.flush_finished_calls()
    }

    fn flush_finished_calls(&mut self) -> Result<Vec<u8>, ChatGptTranslateError> {
        let mut out = Vec::new();
        // 工具块不能挤占正在输出的文本或思考块的序号, 等该块结束后再发送.
        // Keep tool blocks from reusing the index of an open text or thinking block.
        if self.current_block.is_some() {
            return Ok(out);
        }
        while let Some(index) = self
            .pending_calls
            .iter()
            .position(|call| call.final_args.is_some())
        {
            let call = self.pending_calls.remove(index);
            let Some(args) = call.final_args else {
                continue;
            };
            if self.stream {
                out.extend(format_sse_event(
                    EVENT_CONTENT_BLOCK_START,
                    json!({
                        "type": EVENT_CONTENT_BLOCK_START, "index": self.block_index,
                        "content_block": {"type": "tool_use", "id": call.call_id,
                            "name": call.name, "input": {}}
                    }),
                ));
                out.extend(format_sse_event(
                    EVENT_CONTENT_BLOCK_DELTA,
                    json!({
                        "type": EVENT_CONTENT_BLOCK_DELTA, "index": self.block_index,
                        "delta": {"type": "input_json_delta", "partial_json": args}
                    }),
                ));
                out.extend(format_sse_event(
                    EVENT_CONTENT_BLOCK_STOP,
                    json!({
                        "type": EVENT_CONTENT_BLOCK_STOP, "index": self.block_index
                    }),
                ));
            } else {
                let input: Value = serde_json::from_str(&args).map_err(|_| {
                    ChatGptTranslateError::new(
                        StatusCode::BAD_GATEWAY,
                        "function call completed with invalid JSON arguments",
                    )
                })?;
                self.aggregated_blocks.push(json!({
                    "type": "tool_use", "id": call.call_id, "name": call.name, "input": input
                }));
            }
            self.block_index += 1;
        }
        Ok(out)
    }

    fn handle_output_item_done(
        &mut self,
        item_val: Option<&Value>,
        output_index: Option<i64>,
    ) -> Result<Vec<u8>, ChatGptTranslateError> {
        let item = item_val.and_then(Value::as_object);
        if let Some(item) =
            item.filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call"))
        {
            return self.finish_function_call(item, output_index);
        }
        let Some(block) = self.current_block.take() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();

        match block {
            CurrentBlock::Reasoning { id, thinking } => {
                let encrypted = item
                    .and_then(|it| it.get("encrypted_content"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let signature = if !encrypted.is_empty() {
                    let bridge = encode_reasoning_bridge(self.bridge_type, &id, encrypted);
                    if self.stream {
                        out.extend(format_sse_event(
                            EVENT_CONTENT_BLOCK_DELTA,
                            json!({
                                "type": EVENT_CONTENT_BLOCK_DELTA,
                                "index": self.block_index,
                                "delta": {
                                    "type": "signature_delta",
                                    "signature": bridge
                                }
                            }),
                        ));
                    }
                    Some(bridge)
                } else {
                    None
                };
                if self.stream {
                    out.extend(format_sse_event(
                        EVENT_CONTENT_BLOCK_STOP,
                        json!({
                            "type": EVENT_CONTENT_BLOCK_STOP,
                            "index": self.block_index
                        }),
                    ));
                } else {
                    let mut b = json!({
                        "type": "thinking",
                        "thinking": thinking
                    });
                    if let Some(sig) = signature {
                        b["signature"] = json!(sig);
                    }
                    self.aggregated_blocks.push(b);
                }
                self.block_index += 1;
            }
            CurrentBlock::Text { text } => {
                if self.stream {
                    out.extend(format_sse_event(
                        EVENT_CONTENT_BLOCK_STOP,
                        json!({
                            "type": EVENT_CONTENT_BLOCK_STOP,
                            "index": self.block_index
                        }),
                    ));
                } else {
                    self.aggregated_blocks.push(json!({
                        "type": "text",
                        "text": text
                    }));
                }
                self.block_index += 1;
            }
        }

        out.extend(self.flush_finished_calls()?);
        Ok(out)
    }

    fn handle_completed(
        &mut self,
        resp_val: Option<&Value>,
    ) -> Result<Vec<u8>, ChatGptTranslateError> {
        if !self.pending_calls.is_empty() {
            return Err(ChatGptTranslateError::new(
                StatusCode::BAD_GATEWAY,
                "response completed with unfinished function calls",
            ));
        }
        self.extract_usage_from_response(resp_val);
        let stop_reason = if self.has_function_call {
            "tool_use"
        } else {
            "end_turn"
        };
        self.stop_reason = Some(stop_reason.to_owned());
        self.completed = true;

        if !self.stream {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        out.extend(format_sse_event(
            EVENT_MESSAGE_DELTA,
            json!({
                "type": EVENT_MESSAGE_DELTA,
                "delta": {
                    "stop_reason": stop_reason,
                    "stop_sequence": Value::Null
                },
                "usage": {
                    "input_tokens": self.input_tokens,
                    "output_tokens": self.output_tokens,
                    "cache_read_input_tokens": self.cache_read_tokens
                }
            }),
        ));
        out.extend(format_sse_event(
            EVENT_MESSAGE_STOP,
            json!({"type": EVENT_MESSAGE_STOP}),
        ));
        Ok(out)
    }

    fn handle_incomplete(
        &mut self,
        resp_val: Option<&Value>,
    ) -> Result<Vec<u8>, ChatGptTranslateError> {
        self.extract_usage_from_response(resp_val);
        let reason = resp_val
            .and_then(|r| r.get("incomplete_details"))
            .and_then(|d| d.get("reason"))
            .and_then(Value::as_str)
            .unwrap_or("");

        // 达到输出上限是正常结束, 应转成 stop_reason 为 max_tokens, 不能当作上游响应不完整.
        // Codex 后端在 incomplete_details.reason 用 max_output_tokens 表达这一情况, Command Code
        // Provider API 说同一套 Responses 协议但用 length 表达, 见 architecture.md 3.11 第 2 条.
        // 其余原因 (例如 content_filter, 真正的断流) 仍然报错.
        //
        // Reaching the output limit is a normal end converted to stop_reason max_tokens, never a
        // truncated upstream response. The Codex backend expresses it as max_output_tokens in
        // incomplete_details.reason; the Command Code Provider API speaks the same Responses
        // protocol but uses length, see section 3.11 item 2 of architecture.md. Other reasons
        // (for example content_filter, or a real mid-stream break) still raise an error.
        if reason == "max_output_tokens" || reason == "length" {
            if !self.pending_calls.is_empty() {
                return Err(ChatGptTranslateError::new(
                    StatusCode::BAD_GATEWAY,
                    "response incomplete with unfinished function calls",
                ));
            }
            self.stop_reason = Some("max_tokens".to_owned());
            self.completed = true;

            if !self.stream {
                return Ok(Vec::new());
            }

            let mut out = Vec::new();
            out.extend(format_sse_event(
                EVENT_MESSAGE_DELTA,
                json!({
                    "type": EVENT_MESSAGE_DELTA,
                    "delta": {
                        "stop_reason": "max_tokens",
                        "stop_sequence": Value::Null
                    },
                    "usage": {
                        "input_tokens": self.input_tokens,
                        "output_tokens": self.output_tokens,
                        "cache_read_input_tokens": self.cache_read_tokens
                    }
                }),
            ));
            out.extend(format_sse_event(
                EVENT_MESSAGE_STOP,
                json!({"type": EVENT_MESSAGE_STOP}),
            ));
            return Ok(out);
        }

        let status = if reason == "content_filter" {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::BAD_GATEWAY
        };
        Err(ChatGptTranslateError::new(
            status,
            format!("上游响应不完整: reason={reason}"),
        ))
    }

    fn extract_usage_from_response(&mut self, resp_val: Option<&Value>) {
        let Some(resp) = resp_val.and_then(Value::as_object) else {
            return;
        };
        let Some(usage) = resp.get("usage").and_then(Value::as_object) else {
            return;
        };
        let input_total = usage
            .get("input_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let cached = usage
            .get("input_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let output = usage
            .get("output_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0);

        if input_total > 0 || cached > 0 {
            self.input_tokens = (input_total - cached).max(0);
            self.cache_read_tokens = cached;
        }
        if output > 0 {
            self.output_tokens = output;
        }
    }
}

/// 格式化标准 SSE 事件文本.
///
/// Formats standard SSE event text.
fn format_sse_event(event_type: &str, data: Value) -> Vec<u8> {
    let json_text = serde_json::to_string(&data).unwrap_or_default();
    format!("event: {event_type}\ndata: {json_text}\n\n").into_bytes()
}

/// 从 response.failed 事件中提取 HTTP 状态码与错误信息.
///
/// Extracts the HTTP status code and error message from a response.failed event.
fn extract_failed_error(resp_val: Option<&Value>) -> (StatusCode, String) {
    let Some(resp) = resp_val.and_then(Value::as_object) else {
        return (StatusCode::BAD_GATEWAY, "上游生成失败".to_owned());
    };
    let Some(err) = resp.get("error").and_then(Value::as_object) else {
        return (StatusCode::BAD_GATEWAY, "上游生成失败".to_owned());
    };
    let code = err.get("code").and_then(Value::as_str).unwrap_or("");
    let message = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("上游生成失败");

    let status = match code {
        "rate_limit_exceeded" | "insufficient_quota" => StatusCode::TOO_MANY_REQUESTS,
        "context_length_exceeded" | "invalid_request_error" => StatusCode::BAD_REQUEST,
        "server_is_overloaded" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_GATEWAY,
    };

    (status, message.to_owned())
}

/// 从 error 事件中提取 HTTP 状态码与错误信息.
///
/// Extracts the HTTP status code and error message from an error event.
fn extract_error_event(obj: &serde_json::Map<String, Value>) -> (StatusCode, String) {
    let err_obj = obj.get("error").and_then(Value::as_object);
    let message = err_obj
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("上游发生错误");
    let code = err_obj
        .and_then(|e| e.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("");

    let status = match code {
        "rate_limit_exceeded" => StatusCode::TOO_MANY_REQUESTS,
        "invalid_request_error" => StatusCode::BAD_REQUEST,
        _ => StatusCode::BAD_GATEWAY,
    };

    (status, message.to_owned())
}
