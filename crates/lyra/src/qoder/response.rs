//! Qoder 网关 SSE 事件流到 Anthropic Messages 响应的协议转换.
//!
//! 按照 docs/architecture.md 与参考实现构建:
//! 1. 解析每行 data: {"body": "...", "statusCodeValue": 200};
//! 2. 依次输出 message_start, thinking (content_block_start/delta/stop),
//!    text (content_block_start/delta/stop), 以及 tool_use (content_block_start/delta/stop);
//! 3. 提取 end_turn / tool_use 与 usage / total_credits 积分信息;
//! 4. 出现 401/403 或 code 105 时返回鉴权失效错误.
//!
//! Copyright (c) 2026 ggbdpq (MIT License)
//! Copyright (c) 2026 wangjunyao (MIT License)

use bytes::Bytes;
use http::StatusCode;
use serde_json::{Map, Value, json};

pub(crate) const EVENT_MESSAGE_START: &str = "message_start";
pub(crate) const EVENT_CONTENT_BLOCK_START: &str = "content_block_start";
pub(crate) const EVENT_CONTENT_BLOCK_DELTA: &str = "content_block_delta";
pub(crate) const EVENT_CONTENT_BLOCK_STOP: &str = "content_block_stop";
pub(crate) const EVENT_MESSAGE_DELTA: &str = "message_delta";
pub(crate) const EVENT_MESSAGE_STOP: &str = "message_stop";

/// 响应转换中出现的错误.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QoderTranslateError {
    pub status: StatusCode,
    pub message: String,
}

impl QoderTranslateError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

/// 网关工具调用增量片段.
#[derive(Debug, Clone, Default)]
pub struct GatewayToolCallDelta {
    pub index: Option<usize>,
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments: Option<String>,
}

/// 维护当前解析过程中的块状态.
///
/// credits 三个字段是 Option: 上游事件没有报告时保持 None, 收尾事件就不写出对应字段,
/// 而不是把缺失记成 0 积分.
#[derive(Debug, Default)]
pub struct QoderStreamState {
    pub started: bool,
    pub message_id: String,
    pub model: String,
    pub block_index: usize,
    pub current_block_type: Option<String>,
    pub tool_states: Vec<ToolState>,
    pub total_credits: Option<f64>,
    pub original_credits: Option<f64>,
    pub billable: Option<bool>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub tool_used: bool,
    /// 上游是否给过内容: 思考, 正文或工具调用增量.
    ///
    /// Whether the upstream produced content: a thinking, text or tool-call delta.
    pub content_seen: bool,
    /// 上游是否在 choices 里给过非空 finish_reason.
    ///
    /// Whether the upstream gave a non-empty finish_reason in choices.
    pub upstream_finished: bool,
}

#[derive(Debug, Clone)]
pub struct ToolState {
    pub index: usize,
    pub block_index: usize,
    pub open: bool,
}

impl QoderStreamState {
    pub fn new(model: &str) -> Self {
        let mut bytes = [0u8; 16];
        rand::fill(&mut bytes);
        let message_id = format!("msg_{}", hex::encode(bytes));
        Self {
            started: false,
            message_id,
            model: model.to_owned(),
            block_index: 0,
            current_block_type: None,
            tool_states: Vec::new(),
            total_credits: None,
            original_credits: None,
            billable: None,
            prompt_tokens: 0,
            completion_tokens: 0,
            tool_used: false,
            content_seen: false,
            upstream_finished: false,
        }
    }

    /// 报告上游是否已经给出内容或停止原因.
    ///
    /// 上游只发出 message_start 或什么都不发时它是假, 此时 finish 合成的 end_turn 与
    /// message_stop 会把空响应伪装成正常完成, 调用方据此在写出 200 之前返回错误.
    ///
    /// Reports whether the upstream has produced content or a stop reason.
    ///
    /// It is false when the upstream sent only a message_start or nothing at all; the
    /// end_turn and message_stop that finish synthesizes would then disguise the empty
    /// response as a normal completion, so callers return an error before writing a 200.
    pub fn has_output(&self) -> bool {
        self.content_seen || self.upstream_finished
    }

    /// 解析一行 SSE 文本并产出对应的 Anthropic SSE 事件字节.
    pub fn process_line(&mut self, line: &str) -> Result<Vec<Bytes>, QoderTranslateError> {
        let trimmed = line.trim();
        if !trimmed.starts_with("data:") {
            return Ok(Vec::new());
        }
        let raw = trimmed.trim_start_matches("data:").trim();
        if raw.is_empty() || raw == "[DONE]" {
            return Ok(Vec::new());
        }

        let wrapper: Value = serde_json::from_str(raw).map_err(|e| {
            QoderTranslateError::new(StatusCode::BAD_GATEWAY, format!("JSON 解析失败: {e}"))
        })?;

        if let Some(status_val) = wrapper.get("statusCodeValue").and_then(|v| v.as_i64())
            && (status_val == 401 || status_val == 403)
        {
            return Err(QoderTranslateError::new(
                StatusCode::UNAUTHORIZED,
                "Qoder 网关认证拒绝",
            ));
        }

        let body_val = match wrapper.get("body") {
            Some(Value::String(s)) => {
                if s == "[DONE]" {
                    return Ok(Vec::new());
                }
                serde_json::from_str::<Value>(s).unwrap_or(Value::Null)
            }
            Some(other) => other.clone(),
            None => Value::Null,
        };

        if let Some(code) = body_val.get("code")
            && (code.as_str() == Some("105") || code.as_i64() == Some(105))
        {
            return Err(QoderTranslateError::new(
                StatusCode::UNAUTHORIZED,
                "Qoder 登录状态已过期",
            ));
        }

        let mut events = Vec::new();

        // 首次有效数据发送 message_start
        if !self.started {
            self.started = true;
            let start_json = json!({
                "type": EVENT_MESSAGE_START,
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": self.prompt_tokens,
                        "output_tokens": 0
                    }
                }
            });
            events.push(format_sse_event(EVENT_MESSAGE_START, &start_json));
        }

        // 提取 usage 与 credits 积分字段
        if let Some(usage_obj) = body_val.get("usage") {
            if let Some(c) = usage_obj.get("credits").and_then(|v| v.as_f64()) {
                self.total_credits = Some(c);
            }
            if let Some(oc) = usage_obj.get("original_credits").and_then(|v| v.as_f64()) {
                self.original_credits = Some(oc);
            }
            if let Some(b) = usage_obj.get("billable").and_then(|v| v.as_bool()) {
                self.billable = Some(b);
            }
            if let Some(pt) = usage_obj.get("prompt_tokens").and_then(|v| v.as_u64()) {
                self.prompt_tokens = pt;
            }
            if let Some(ct) = usage_obj.get("completion_tokens").and_then(|v| v.as_u64()) {
                self.completion_tokens = ct;
            }
        }

        // 提取 choices delta
        if let Some(Value::Array(choices)) = body_val.get("choices") {
            for choice in choices {
                // 非空 finish_reason 是上游自己的停止原因; 记下来, 使流结束的判定不依赖
                // 转码器合成的 end_turn.
                //
                // A non-empty finish_reason is the upstream's own stop reason; it is
                // recorded so the end-of-stream decision does not rely on the synthesized
                // end_turn.
                if choice
                    .get("finish_reason")
                    .and_then(|value| value.as_str())
                    .is_some_and(|reason| !reason.is_empty())
                {
                    self.upstream_finished = true;
                }
                let Some(delta) = choice.get("delta") else {
                    continue;
                };

                // 1. 思考增量 (reasoning_content)
                if let Some(reasoning) = delta.get("reasoning_content").and_then(|s| s.as_str())
                    && !reasoning.is_empty()
                {
                    self.content_seen = true;
                    if self.current_block_type.as_deref() != Some("thinking") {
                        self.close_current_block(&mut events);
                        let start_evt = json!({
                            "type": EVENT_CONTENT_BLOCK_START,
                            "index": self.block_index,
                            "content_block": {
                                "type": "thinking",
                                "thinking": ""
                            }
                        });
                        events.push(format_sse_event(EVENT_CONTENT_BLOCK_START, &start_evt));
                        self.current_block_type = Some("thinking".to_owned());
                    }

                    let delta_evt = json!({
                        "type": EVENT_CONTENT_BLOCK_DELTA,
                        "index": self.block_index,
                        "delta": {
                            "type": "thinking_delta",
                            "thinking": reasoning
                        }
                    });
                    events.push(format_sse_event(EVENT_CONTENT_BLOCK_DELTA, &delta_evt));
                }

                // 2. 正文文本增量 (content)
                if let Some(content) = delta.get("content").and_then(|s| s.as_str())
                    && !content.is_empty()
                {
                    self.content_seen = true;
                    if self.current_block_type.as_deref() != Some("text") {
                        self.close_current_block(&mut events);
                        let start_evt = json!({
                            "type": EVENT_CONTENT_BLOCK_START,
                            "index": self.block_index,
                            "content_block": {
                                "type": "text",
                                "text": ""
                            }
                        });
                        events.push(format_sse_event(EVENT_CONTENT_BLOCK_START, &start_evt));
                        self.current_block_type = Some("text".to_owned());
                    }

                    let delta_evt = json!({
                        "type": EVENT_CONTENT_BLOCK_DELTA,
                        "index": self.block_index,
                        "delta": {
                            "type": "text_delta",
                            "text": content
                        }
                    });
                    events.push(format_sse_event(EVENT_CONTENT_BLOCK_DELTA, &delta_evt));
                }

                // 3. 工具调用增量 (tool_calls)
                if let Some(Value::Array(tool_calls)) = delta.get("tool_calls") {
                    self.tool_used = true;
                    if !tool_calls.is_empty() {
                        self.content_seen = true;
                    }

                    for tc in tool_calls {
                        let id = tc.get("id").and_then(|s| s.as_str()).unwrap_or("");
                        let func = tc.get("function");
                        let name = func
                            .and_then(|f| f.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or("");
                        let args = func
                            .and_then(|f| f.get("arguments"))
                            .and_then(|a| a.as_str())
                            .unwrap_or("");

                        if !id.is_empty() && !name.is_empty() {
                            self.close_current_block(&mut events);
                            let start_evt = json!({
                                "type": EVENT_CONTENT_BLOCK_START,
                                "index": self.block_index,
                                "content_block": {
                                    "type": "tool_use",
                                    "id": id,
                                    "name": name,
                                    "input": {}
                                }
                            });
                            events.push(format_sse_event(EVENT_CONTENT_BLOCK_START, &start_evt));
                            self.current_block_type = Some("tool_use".to_owned());
                        }

                        if !args.is_empty() {
                            let delta_evt = json!({
                                "type": EVENT_CONTENT_BLOCK_DELTA,
                                "index": self.block_index,
                                "delta": {
                                    "type": "input_json_delta",
                                    "partial_json": args
                                }
                            });
                            events.push(format_sse_event(EVENT_CONTENT_BLOCK_DELTA, &delta_evt));
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    /// 关闭当前正在打开的文本或思考块.
    pub fn close_current_block(&mut self, events: &mut Vec<Bytes>) {
        if self.current_block_type.is_some() {
            let stop_evt = json!({
                "type": EVENT_CONTENT_BLOCK_STOP,
                "index": self.block_index
            });
            events.push(format_sse_event(EVENT_CONTENT_BLOCK_STOP, &stop_evt));
            self.block_index += 1;
            self.current_block_type = None;
        }
    }

    /// 流结束时收尾并生成 message_delta 与 message_stop.
    ///
    /// message_delta 的 usage 携带最终令牌计数与积分字段: 令牌计数上游总会报告, 积分字段
    /// 只有上游报告过才写出. 采集侧从这一帧读出记录所需的全部用量.
    pub fn finish(&mut self) -> Vec<Bytes> {
        let mut events = Vec::new();
        self.close_current_block(&mut events);

        let stop_reason = if self.tool_used {
            "tool_use"
        } else {
            "end_turn"
        };
        let mut usage = Map::new();
        usage.insert("output_tokens".to_owned(), json!(self.completion_tokens));
        usage.insert("input_tokens".to_owned(), json!(self.prompt_tokens));
        if let Some(credits) = self.total_credits {
            usage.insert("credits".to_owned(), json!(credits));
        }
        if let Some(original_credits) = self.original_credits {
            usage.insert("original_credits".to_owned(), json!(original_credits));
        }
        if let Some(billable) = self.billable {
            usage.insert("billable".to_owned(), json!(billable));
        }
        let delta_evt = json!({
            "type": EVENT_MESSAGE_DELTA,
            "delta": {
                "stop_reason": stop_reason,
                "stop_sequence": null
            },
            "usage": Value::Object(usage)
        });
        events.push(format_sse_event(EVENT_MESSAGE_DELTA, &delta_evt));

        let stop_evt = json!({
            "type": EVENT_MESSAGE_STOP
        });
        events.push(format_sse_event(EVENT_MESSAGE_STOP, &stop_evt));

        events
    }
}

fn format_sse_event(name: &str, data: &Value) -> Bytes {
    let payload = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_owned());
    let formatted = format!("event: {name}\ndata: {payload}\n\n");
    Bytes::from(formatted.into_bytes())
}

/// 把 QoderStreamState 产出的 Anthropic SSE 事件流汇总成一条完整消息.
///
/// 与非流式 Anthropic Messages 响应同形: 顶层包含 type, role, model, content,
/// stop_reason, stop_sequence 与 usage. 搜索编排的每一轮都需要这种完整报文, 而网关只提供
/// 事件流, 因此这里复用同一份转换结果做聚合, 不再单独实现一份网关解析逻辑.
///
/// Aggregates the Anthropic SSE event stream produced by QoderStreamState into one complete
/// message.
///
/// It matches the shape of a non-streaming Anthropic Messages response: the top level holds
/// type, role, model, content, stop_reason, stop_sequence and usage. Every search round
/// needs such a complete payload while the gateway only offers an event stream, so the
/// aggregation reuses the same conversion output rather than parsing the gateway again.
pub(crate) fn aggregate_message(events: &Bytes) -> Result<Vec<u8>, QoderTranslateError> {
    let mut message_id = String::new();
    let mut model = String::new();
    let mut usage = Map::new();
    let mut blocks: Vec<Value> = Vec::new();
    let mut current: Option<Map<String, Value>> = None;
    let mut open_index: Option<usize> = None;
    let mut stop_reason: Value = Value::Null;
    let mut stop_sequence: Value = Value::Null;

    let text = std::str::from_utf8(events).map_err(|err| {
        QoderTranslateError::new(StatusCode::BAD_GATEWAY, format!("事件编码无效: {err}"))
    })?;
    for block in text.split("\n\n") {
        let Some(data) = block.lines().find_map(|line| line.strip_prefix("data: ")) else {
            continue;
        };
        let event: Value = serde_json::from_str(data).map_err(|err| {
            QoderTranslateError::new(StatusCode::BAD_GATEWAY, format!("事件解析失败: {err}"))
        })?;
        match event.get("type").and_then(|value| value.as_str()) {
            Some(EVENT_MESSAGE_START) => {
                let start = event.get("message").unwrap_or(&Value::Null);
                message_id = start
                    .get("id")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_owned();
                model = start
                    .get("model")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_owned();
                if let Some(map) = start.get("usage").and_then(|value| value.as_object()) {
                    usage = map.clone();
                }
            }
            Some(EVENT_CONTENT_BLOCK_START) => {
                let index = block_index(&event)?;
                let mut content_block = event
                    .get("content_block")
                    .and_then(|value| value.as_object())
                    .cloned()
                    .unwrap_or_default();
                let kind = content_block
                    .get("type")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                // 文本与思考块在流末累积文本, 参数块累积 partial_json, 在块结束时才组装成
                // input. 起始事件里的空占位值在这里去掉, 步进增量时按类型回填.
                //
                // Text and thinking blocks accumulate text at stream end, and argument blocks
                // accumulate partial_json, assembling input only when the block stops. The empty
                // placeholder from the start event is dropped here and refilled by kind as deltas
                // arrive.
                match kind {
                    "text" => {
                        content_block.remove("text");
                    }
                    "thinking" => {
                        content_block.remove("thinking");
                    }
                    "tool_use" => {
                        content_block.remove("input");
                    }
                    _ => {}
                }
                blocks.push(Value::Object(content_block));
                open_index = Some(index);
                current = Some(Map::new());
            }
            Some(EVENT_CONTENT_BLOCK_DELTA) => {
                let index = block_index(&event)?;
                let Some(delta) = event.get("delta").and_then(|value| value.as_object()) else {
                    continue;
                };
                let Some(accumulator) = current.as_mut().filter(|_| open_index == Some(index))
                else {
                    continue;
                };
                for (key, value) in delta {
                    match key.as_str() {
                        "type" => {}
                        "text" | "thinking" => {
                            let piece = value.as_str().unwrap_or_default();
                            let slot = accumulator
                                .entry(key.clone())
                                .or_insert_with(|| Value::String(String::new()));
                            if let Value::String(text) = slot {
                                text.push_str(piece);
                            }
                        }
                        "partial_json" => {
                            let piece = value.as_str().unwrap_or_default();
                            let slot = accumulator
                                .entry("partial_json".to_owned())
                                .or_insert_with(|| Value::String(String::new()));
                            if let Value::String(text) = slot {
                                text.push_str(piece);
                            }
                        }
                        _ => {
                            accumulator.insert(key.clone(), value.clone());
                        }
                    }
                }
            }
            Some(EVENT_CONTENT_BLOCK_STOP) => {
                let index = block_index(&event)?;
                if open_index != Some(index) {
                    continue;
                }
                let Some(accumulator) = current.take() else {
                    continue;
                };
                let Some(block) = blocks
                    .get_mut(index)
                    .and_then(|value| value.as_object_mut())
                else {
                    continue;
                };
                if let Some(Value::String(text)) = accumulator.get("text") {
                    block.insert("text".to_owned(), Value::String(text.clone()));
                }
                if let Some(Value::String(thinking)) = accumulator.get("thinking") {
                    block.insert("thinking".to_owned(), Value::String(thinking.clone()));
                }
                if let Some(Value::String(partial)) = accumulator.get("partial_json") {
                    // 参数无法解析成 JSON 对象时退回空对象, 与 antigravity 非流式输出的处理
                    // 一致, 避免半个参数让整条消息不可用.
                    //
                    // When the arguments do not parse into a JSON object it falls back to an
                    // empty object, matching the antigravity non-streaming output, so half an
                    // argument never makes the whole message unusable.
                    let input = serde_json::from_str::<Value>(partial)
                        .ok()
                        .filter(Value::is_object)
                        .unwrap_or_else(|| Value::Object(Map::new()));
                    block.insert("input".to_owned(), input);
                }
                for (key, value) in accumulator {
                    if key != "text" && key != "thinking" && key != "partial_json" {
                        block.insert(key, value);
                    }
                }
                open_index = None;
            }
            Some(EVENT_MESSAGE_DELTA) => {
                if let Some(delta) = event.get("delta") {
                    stop_reason = delta.get("stop_reason").cloned().unwrap_or(Value::Null);
                    stop_sequence = delta.get("stop_sequence").cloned().unwrap_or(Value::Null);
                }
                if let Some(update) = event.get("usage").and_then(|value| value.as_object()) {
                    usage.extend(update.iter().map(|(k, v)| (k.clone(), v.clone())));
                }
            }
            _ => {}
        }
    }

    let payload = json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": blocks,
        "stop_reason": stop_reason,
        "stop_sequence": stop_sequence,
        "usage": Value::Object(usage),
    });
    serde_json::to_vec(&payload).map_err(|err| {
        QoderTranslateError::new(StatusCode::BAD_GATEWAY, format!("编码消息失败: {err}"))
    })
}

/// 读取一个内容块事件的 index 字段, 缺失或超出 usize 时返回错误.
///
/// Reads the index field of a content block event, erroring when it is missing or out of
/// usize range.
fn block_index(event: &Value) -> Result<usize, QoderTranslateError> {
    event
        .get("index")
        .and_then(|value| value.as_u64())
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            QoderTranslateError::new(StatusCode::BAD_GATEWAY, "事件缺少合法的内容块序号")
        })
}
