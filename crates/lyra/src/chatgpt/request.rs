//! Anthropic Messages 请求到 Codex Responses 请求的协议转换.
//!
//! 按照 docs/architecture.md 3.7 节实现:
//! 1. model 采用路由设定的上游模型名, stream 恒为 true, store 为 false;
//! 2. include 包含 reasoning.encrypted_content;
//! 3. reasoning.effort 由客户端 output_config.effort 映射, 缺省使用路由配置;
//! 4. parallel_tool_calls 默认为 true, 除非显式禁用并行工具;
//! 5. prompt_cache_key 读取客户端会话头;
//! 6. tools, tool_choice, instructions 与 input 条目严格映射并支持推理桥回放.
//!
//! Protocol conversion from Anthropic Messages requests to Codex Responses requests.
//! Implemented according to section 3.7 of docs/architecture.md:
//! 1. model takes the upstream model configured on the route, stream is constantly true, store is false;
//! 2. include contains reasoning.encrypted_content;
//! 3. reasoning.effort maps from client output_config.effort, defaulting to the route config;
//! 4. parallel_tool_calls defaults to true unless disable_parallel_tool_use is true;
//! 5. prompt_cache_key is read from the client session header;
//! 6. tools, tool_choice, instructions and input items are strictly mapped and support reasoning bridge replay.

use bytes::Bytes;
use http::HeaderMap;
use serde_json::{Map, Value, json};

use super::signatures::{CHATGPT_REASONING_BRIDGE_TYPE, decode_reasoning_bridge};

/// 客户端会话标识请求头名.
///
/// The client session identifier header name.
const HEADER_SESSION_ID: &str = "x-claude-code-session-id";

/// 不支持的独立网页搜索工具名.
///
/// The unsupported standalone web search tool name.
const UNSUPPORTED_WEB_SEARCH_TOOL: &str = "web_search_20250305";

/// 同一种 Responses 请求体在一条线路上的差异.
///
/// chatgpt 与 xai 两条线路共用本模块的转换, 两个后端在请求体上只有这里列出的不同, 由各线路的
/// 常量给出, 使转换代码本身只有一份.
///
/// The differences of one line on the shared Responses request body. The chatgpt and xai lines
/// share this module's conversion and the two backends differ only in what is listed here, given
/// by each line's constant, so the conversion code exists once.
pub struct ResponsesDialect {
    /// 推理桥的类型标识, 只有该线路签发的 thinking 签名会还原为 reasoning 条目.
    ///
    /// The reasoning bridge type marker; only thinking signatures issued by the line turn back
    /// into reasoning items.
    pub bridge_type: &'static str,
    /// 请求没有工具时是否仍写出 tool_choice. Codex 接受没有工具的 tool_choice; Grok 对它返回 400
    /// invalid-argument, 文本为 A tool_choice was set on the request but no tools were
    /// specified, 2026-10-09 冒烟实测, 同样没有工具时的 parallel_tool_calls 则被接受.
    ///
    /// Whether tool_choice is still written when the request has no tools. Codex accepts a
    /// tool_choice without tools; Grok answers it with 400 invalid-argument, reading A tool_choice
    /// was set on the request but no tools were specified, as measured in the 2026-10-09 smoke
    /// test, while parallel_tool_calls without tools is accepted.
    pub tool_choice_without_tools: bool,
    /// 是否把客户端请求中的 max_tokens 写为 Responses 请求体的 max_output_tokens.
    /// Command Code 线路写出, chatgpt 与 xai 线路保持不写.
    ///
    /// Whether to write client max_tokens as max_output_tokens in the Responses body.
    /// The Command Code line writes it; chatgpt and xai keep it unwritten.
    pub emit_max_output_tokens: bool,
}

/// chatgpt 线路的请求体差异: 保持 3.7 节的形状, 没有工具时同样写出 tool_choice.
///
/// The request body differences of the chatgpt line: the section 3.7 shape, writing tool_choice
/// even without tools.
pub const CHATGPT_DIALECT: ResponsesDialect = ResponsesDialect {
    bridge_type: CHATGPT_REASONING_BRIDGE_TYPE,
    tool_choice_without_tools: true,
    emit_max_output_tokens: false,
};

/// 将 Anthropic Messages 请求转换为 Codex Responses API 请求体.
///
/// xai 线路的 Grok 后端接受与 Codex 几乎相同的请求体形状, 因此两条线路共用本转换, 不同之处由
/// dialect 给出, 见 ResponsesDialect.
///
/// Converts an Anthropic Messages request into a Codex Responses API request body.
///
/// The Grok backend of the xai line accepts almost the same body shape as Codex, so both lines
/// share this conversion and dialect supplies the differences; see ResponsesDialect.
pub fn build_responses_request(
    headers: &HeaderMap,
    body: &[u8],
    upstream_model: &str,
    default_reasoning_effort: &str,
    dialect: &ResponsesDialect,
) -> Result<Bytes, String> {
    let root: Value =
        serde_json::from_slice(body).map_err(|err| format!("解析请求体失败: {err}"))?;
    let Some(obj) = root.as_object() else {
        return Err("请求体必须是 JSON 对象".to_owned());
    };

    // 1. instructions 处理
    let instructions = extract_instructions(obj.get("system"));

    // 2. reasoning effort 映射
    let effort = resolve_reasoning_effort(obj.get("output_config"), default_reasoning_effort);

    // 3. parallel_tool_calls 与 tool_choice
    let (tool_choice, parallel_tool_calls) = resolve_tool_choice(obj.get("tool_choice"));

    // 4. tools 转换
    let tools = convert_tools(obj.get("tools"))?;

    // 5. input 条目序列
    let input = convert_messages(obj.get("messages"), dialect.bridge_type)?;

    // 6. prompt_cache_key
    let prompt_cache_key = headers
        .get(HEADER_SESSION_ID)
        .and_then(|val| val.to_str().ok())
        .filter(|val| !val.trim().is_empty())
        .map(|val| val.trim().to_owned());

    let mut out = Map::new();
    out.insert("model".to_owned(), json!(upstream_model));
    out.insert("stream".to_owned(), json!(true));
    out.insert("store".to_owned(), json!(false));
    out.insert("include".to_owned(), json!(["reasoning.encrypted_content"]));
    out.insert(
        "reasoning".to_owned(),
        json!({
            "effort": effort,
            "summary": "auto"
        }),
    );
    out.insert("parallel_tool_calls".to_owned(), json!(parallel_tool_calls));
    if tools.is_some() || dialect.tool_choice_without_tools {
        out.insert("tool_choice".to_owned(), tool_choice);
    }
    if dialect.emit_max_output_tokens
        && let Some(max_tokens) = obj.get("max_tokens")
        && (max_tokens.is_i64() || max_tokens.is_u64())
    {
        out.insert("max_output_tokens".to_owned(), max_tokens.clone());
    }
    if let Some(inst) = instructions {
        out.insert("instructions".to_owned(), json!(inst));
    }
    if let Some(key) = prompt_cache_key {
        out.insert("prompt_cache_key".to_owned(), json!(key));
    }
    if let Some(tools_val) = tools {
        out.insert("tools".to_owned(), tools_val);
    }
    out.insert("input".to_owned(), Value::Array(input));

    let json_bytes = serde_json::to_vec(&Value::Object(out))
        .map_err(|err| format!("序列化 Responses 请求失败: {err}"))?;
    Ok(Bytes::from(json_bytes))
}

/// 提取系统提示作为 instructions.
///
/// Extracts the system prompt as instructions.
fn extract_instructions(system: Option<&Value>) -> Option<String> {
    let val = system?;
    if let Some(s) = val.as_str() {
        if !s.is_empty() {
            return Some(s.to_owned());
        }
        return None;
    }
    if let Some(array) = val.as_array() {
        let texts: Vec<&str> = array
            .iter()
            .filter_map(|block| {
                if block.get("type").and_then(Value::as_str) == Some("text") {
                    block.get("text").and_then(Value::as_str)
                } else {
                    None
                }
            })
            .collect();
        if !texts.is_empty() {
            return Some(texts.join("\n\n"));
        }
    }
    None
}

/// 解析推理强度档位.
///
/// Resolves the reasoning effort tier.
fn resolve_reasoning_effort(output_config: Option<&Value>, default_effort: &str) -> String {
    let fallback = match default_effort {
        "low" | "medium" | "high" | "xhigh" => default_effort,
        _ => "high",
    };
    let Some(config) = output_config.and_then(Value::as_object) else {
        return fallback.to_owned();
    };
    let Some(effort) = config.get("effort").and_then(Value::as_str) else {
        return fallback.to_owned();
    };
    match effort {
        "low" => "low".to_owned(),
        "medium" => "medium".to_owned(),
        "high" => "high".to_owned(),
        "xhigh" | "max" => "xhigh".to_owned(),
        _ => fallback.to_owned(),
    }
}

/// 解析 tool_choice 与 parallel_tool_calls.
///
/// Resolves tool_choice and parallel_tool_calls.
fn resolve_tool_choice(choice_val: Option<&Value>) -> (Value, bool) {
    let mut parallel = true;
    let Some(choice) = choice_val else {
        return (json!("auto"), parallel);
    };
    if let Some(s) = choice.as_str() {
        return match s {
            "auto" => (json!("auto"), parallel),
            "any" => (json!("required"), parallel),
            "none" => (json!("none"), parallel),
            _ => (json!("auto"), parallel),
        };
    }
    let Some(obj) = choice.as_object() else {
        return (json!("auto"), parallel);
    };
    if obj
        .get("disable_parallel_tool_use")
        .and_then(Value::as_bool)
        == Some(true)
    {
        parallel = false;
    }
    let kind = obj.get("type").and_then(Value::as_str).unwrap_or("auto");
    match kind {
        "auto" => (json!("auto"), parallel),
        "any" => (json!("required"), parallel),
        "none" => (json!("none"), parallel),
        "tool" => {
            let name = obj.get("name").and_then(Value::as_str).unwrap_or("");
            (
                json!({
                    "type": "function",
                    "name": name
                }),
                parallel,
            )
        }
        _ => (json!("auto"), parallel),
    }
}

/// 转换 tools 工具定义.
///
/// Converts tool definitions.
fn convert_tools(tools_val: Option<&Value>) -> Result<Option<Value>, String> {
    let Some(val) = tools_val else {
        return Ok(None);
    };
    let Some(array) = val.as_array() else {
        return Ok(None);
    };
    if array.is_empty() {
        return Ok(None);
    }
    let mut out_tools = Vec::with_capacity(array.len());
    for item in array {
        let Some(tool_obj) = item.as_object() else {
            continue;
        };
        let tool_type = tool_obj.get("type").and_then(Value::as_str).unwrap_or("");
        if tool_type == UNSUPPORTED_WEB_SEARCH_TOOL {
            return Err("chatgpt 路由不支持网页搜索工具".to_owned());
        }
        let name = tool_obj.get("name").and_then(Value::as_str).unwrap_or("");
        let description = tool_obj
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("");
        let parameters = tool_obj
            .get("input_schema")
            .cloned()
            .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
        out_tools.push(json!({
            "type": "function",
            "name": name,
            "description": description,
            "strict": false,
            "parameters": parameters
        }));
    }
    if out_tools.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Value::Array(out_tools)))
    }
}

/// 转换 messages 消息序列为 Responses input 条目序列.
///
/// Converts a messages sequence into a Responses input item sequence.
fn convert_messages(messages_val: Option<&Value>, bridge_type: &str) -> Result<Vec<Value>, String> {
    let Some(val) = messages_val else {
        return Ok(Vec::new());
    };
    let Some(messages) = val.as_array() else {
        return Ok(Vec::new());
    };
    let mut input = Vec::new();

    for msg in messages {
        let Some(msg_obj) = msg.as_object() else {
            continue;
        };
        let role = msg_obj.get("role").and_then(Value::as_str).unwrap_or("");
        let content_val = msg_obj.get("content");

        match role {
            "system" => {
                // 中途出现的 system 消息生成 role 为 developer 的 message 条目
                let mut dev_content = Vec::new();
                append_user_or_dev_content(&mut dev_content, content_val)?;
                if !dev_content.is_empty() {
                    input.push(json!({
                        "type": "message",
                        "role": "developer",
                        "content": dev_content
                    }));
                }
            }
            "user" => {
                convert_user_message(&mut input, content_val)?;
            }
            "assistant" => {
                convert_assistant_message(&mut input, content_val, bridge_type)?;
            }
            _ => {}
        }
    }

    Ok(input)
}

/// 转换 user 消息内容, 区分普通内容与 tool_result.
///
/// Converts user message content, separating regular items from tool_result.
fn convert_user_message(input: &mut Vec<Value>, content_val: Option<&Value>) -> Result<(), String> {
    let Some(content) = content_val else {
        return Ok(());
    };
    if let Some(text) = content.as_str() {
        input.push(json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text}]
        }));
        return Ok(());
    }
    let Some(blocks) = content.as_array() else {
        return Ok(());
    };

    let mut pending_user_content: Vec<Value> = Vec::new();

    for block in blocks {
        let Some(b) = block.as_object() else {
            continue;
        };
        let b_type = b.get("type").and_then(Value::as_str).unwrap_or("");
        if b_type == "tool_result" {
            // 先将前面积攒的 user message 刷入 input
            if !pending_user_content.is_empty() {
                input.push(json!({
                    "type": "message",
                    "role": "user",
                    "content": std::mem::take(&mut pending_user_content)
                }));
            }
            // 构造 function_call_output 条目
            let call_id = b.get("tool_use_id").and_then(Value::as_str).unwrap_or("");
            let is_error = b.get("is_error").and_then(Value::as_bool).unwrap_or(false);
            let output = format_tool_result_output(b.get("content"), is_error)?;
            input.push(json!({
                "type": "function_call_output",
                "call_id": call_id,
                "output": output
            }));
        } else if let Some(item) = format_input_content_item(b)? {
            pending_user_content.push(item);
        }
    }

    if !pending_user_content.is_empty() {
        input.push(json!({
            "type": "message",
            "role": "user",
            "content": pending_user_content
        }));
    }

    Ok(())
}

/// 将 user 或 developer 文本内容追加到 content 列表中.
///
/// Appends user or developer text content to the content list.
fn append_user_or_dev_content(
    content_list: &mut Vec<Value>,
    content_val: Option<&Value>,
) -> Result<(), String> {
    let Some(content) = content_val else {
        return Ok(());
    };
    if let Some(text) = content.as_str() {
        content_list.push(json!({"type": "input_text", "text": text}));
        return Ok(());
    }
    if let Some(blocks) = content.as_array() {
        for block in blocks {
            if let Some(b) = block.as_object()
                && let Some(item) = format_input_content_item(b)?
            {
                content_list.push(item);
            }
        }
    }
    Ok(())
}

/// 格式化单个 input_text 或 input_image 内容块.
///
/// Formats a single input_text or input_image content block.
fn format_input_content_item(block: &Map<String, Value>) -> Result<Option<Value>, String> {
    let b_type = block.get("type").and_then(Value::as_str).unwrap_or("");
    match b_type {
        "text" => {
            let text = block.get("text").and_then(Value::as_str).unwrap_or("");
            Ok(Some(json!({"type": "input_text", "text": text})))
        }
        "image" => {
            let Some(source) = block.get("source").and_then(Value::as_object) else {
                return Ok(None);
            };
            let src_type = source.get("type").and_then(Value::as_str).unwrap_or("");
            let image_url = match src_type {
                "base64" => {
                    let media_type = source
                        .get("media_type")
                        .and_then(Value::as_str)
                        .unwrap_or("image/jpeg");
                    let data = source.get("data").and_then(Value::as_str).unwrap_or("");
                    format!("data:{media_type};base64,{data}")
                }
                "url" => {
                    let url = source.get("url").and_then(Value::as_str).unwrap_or("");
                    url.to_owned()
                }
                _ => return Ok(None),
            };
            Ok(Some(json!({"type": "input_image", "image_url": image_url})))
        }
        _ => Ok(None),
    }
}

/// 格式化 tool_result 的 output 字段.
///
/// Formats the output field of a tool_result.
fn format_tool_result_output(content_val: Option<&Value>, is_error: bool) -> Result<Value, String> {
    let Some(content) = content_val else {
        let text = if is_error { "Error: " } else { "" };
        return Ok(Value::String(text.to_owned()));
    };
    if let Some(s) = content.as_str() {
        let text = if is_error {
            format!("Error: {s}")
        } else {
            s.to_owned()
        };
        return Ok(Value::String(text));
    }
    let Some(blocks) = content.as_array() else {
        let text = if is_error { "Error: " } else { "" };
        return Ok(Value::String(text.to_owned()));
    };

    let has_image = blocks
        .iter()
        .any(|b| b.get("type").and_then(Value::as_str) == Some("image"));

    if !has_image {
        let mut full_text = String::new();
        for b in blocks {
            if b.get("type").and_then(Value::as_str) == Some("text")
                && let Some(t) = b.get("text").and_then(Value::as_str)
            {
                full_text.push_str(t);
            }
        }
        let text = if is_error {
            format!("Error: {full_text}")
        } else {
            full_text
        };
        return Ok(Value::String(text));
    }

    // 含有图片时转换为数组
    let mut items = Vec::new();
    let mut error_prefixed = false;
    for b in blocks {
        let Some(obj) = b.as_object() else {
            continue;
        };
        let b_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
        if b_type == "text" {
            let t = obj.get("text").and_then(Value::as_str).unwrap_or("");
            let text = if is_error && !error_prefixed {
                error_prefixed = true;
                format!("Error: {t}")
            } else {
                t.to_owned()
            };
            items.push(json!({"type": "input_text", "text": text}));
        } else if let Some(img_item) = format_input_content_item(obj)? {
            items.push(img_item);
        }
    }
    if is_error && !error_prefixed {
        items.insert(0, json!({"type": "input_text", "text": "Error: "}));
    }
    Ok(Value::Array(items))
}

/// 转换 assistant 消息内容.
///
/// Converts assistant message content.
fn convert_assistant_message(
    input: &mut Vec<Value>,
    content_val: Option<&Value>,
    bridge_type: &str,
) -> Result<(), String> {
    let Some(content) = content_val else {
        return Ok(());
    };
    if let Some(text) = content.as_str() {
        input.push(json!({
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text}]
        }));
        return Ok(());
    }
    let Some(blocks) = content.as_array() else {
        return Ok(());
    };

    let mut pending_assistant_text: Vec<Value> = Vec::new();

    for block in blocks {
        let Some(b) = block.as_object() else {
            continue;
        };
        let b_type = b.get("type").and_then(Value::as_str).unwrap_or("");
        match b_type {
            "text" => {
                let text = b.get("text").and_then(Value::as_str).unwrap_or("");
                pending_assistant_text.push(json!({"type": "output_text", "text": text}));
            }
            "tool_use" => {
                if !pending_assistant_text.is_empty() {
                    input.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": std::mem::take(&mut pending_assistant_text)
                    }));
                }
                let id = b.get("id").and_then(Value::as_str).unwrap_or("");
                let name = b.get("name").and_then(Value::as_str).unwrap_or("");
                let input_val = b.get("input").cloned().unwrap_or_else(|| json!({}));
                let arguments =
                    serde_json::to_string(&input_val).unwrap_or_else(|_| "{}".to_owned());
                input.push(json!({
                    "type": "function_call",
                    "call_id": id,
                    "name": name,
                    "arguments": arguments
                }));
            }
            "thinking" => {
                if !pending_assistant_text.is_empty() {
                    input.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": std::mem::take(&mut pending_assistant_text)
                    }));
                }
                let signature = b.get("signature").and_then(Value::as_str).unwrap_or("");
                let thinking_text = b.get("thinking").and_then(Value::as_str).unwrap_or("");
                // 只有属于本推理桥的 signature 才生成 reasoning 条目, 其他来源丢弃; 已识别为本协议
                // 却损坏的推理桥按 3.7 第 7 条报错, 静默丢弃会让上游丢失推理上下文且难以定位.
                //
                // Only signatures of this reasoning bridge become reasoning items and other sources
                // are dropped; a bridge recognized as this protocol but corrupt is an error under 3.7
                // item 7, because dropping it silently loses upstream reasoning context.
                if let Some(bridge) = decode_reasoning_bridge(bridge_type, signature)? {
                    let summary = if thinking_text.is_empty() {
                        Vec::new()
                    } else {
                        vec![json!({
                            "type": "summary_text",
                            "text": thinking_text
                        })]
                    };
                    input.push(json!({
                        "type": "reasoning",
                        "id": bridge.id,
                        "summary": summary,
                        "encrypted_content": bridge.encrypted_content
                    }));
                }
            }
            // redacted_thinking 与其他块丢弃
            _ => {}
        }
    }

    if !pending_assistant_text.is_empty() {
        input.push(json!({
            "type": "message",
            "role": "assistant",
            "content": pending_assistant_text
        }));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::*;
    use crate::chatgpt::signatures::{
        COMMANDCODE_REASONING_BRIDGE_TYPE, ChatGptReasoningBridge, XAI_REASONING_BRIDGE_TYPE,
        decode_chatgpt_reasoning_bridge, encode_chatgpt_reasoning_bridge, encode_reasoning_bridge,
    };
    use crate::commandcode::COMMANDCODE_DIALECT;
    use crate::xai::XAI_DIALECT;

    /// 标准 base64 编码, 用于把推理桥载体的期望文本与编码结果对照.
    ///
    /// Standard base64 encoding, used to compare the expected carrier text with the encoded
    /// result.
    fn standard(text: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(text)
    }

    #[test]
    fn converts_tool_use_and_tool_result_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let headers = HeaderMap::new();
        let body = json!({
            "model": "gpt-5.6-sol",
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "Calling calculator"},
                        {"type": "tool_use", "id": "call_123", "name": "calculator", "input": {"a": 1, "b": 2}}
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "call_123", "content": "3"}
                    ]
                }
            ]
        });
        let res = build_responses_request(
            &headers,
            &serde_json::to_vec(&body)?,
            "gpt-5.6-sol",
            "high",
            &CHATGPT_DIALECT,
        )
        .map_err(|e| e.to_string())?;
        let val: Value = serde_json::from_slice(&res)?;
        let input = val["input"].as_array().ok_or("input is not array")?;
        assert_eq!(input.len(), 3);
        // assistant text
        assert_eq!(input[0]["type"], "message");
        assert_eq!(input[0]["role"], "assistant");
        // assistant tool_use
        assert_eq!(input[1]["type"], "function_call");
        assert_eq!(input[1]["call_id"], "call_123");
        assert_eq!(input[1]["name"], "calculator");
        assert_eq!(input[1]["arguments"], "{\"a\":1,\"b\":2}");
        // user tool_result
        assert_eq!(input[2]["type"], "function_call_output");
        assert_eq!(input[2]["call_id"], "call_123");
        assert_eq!(input[2]["output"], "3");

        // 没有工具时 tool_choice 的输出跟随线路方言: chatgpt 保持原有形状也写出 tool_choice;
        // Grok 拒绝没有工具的 tool_choice 但接受 parallel_tool_calls, 因此 xai 省略 tool_choice.
        //
        // Without tools the tool_choice output follows the line dialect: chatgpt keeps its shape and
        // writes tool_choice, while Grok rejects tool_choice without tools but accepts
        // parallel_tool_calls, so the xai line omits tool_choice.
        let convert = |body: &[u8], dialect: &ResponsesDialect| -> Result<Value, String> {
            let bytes = build_responses_request(&headers, body, "grok-4.7", "high", dialect)?;
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())
        };
        let plain = serde_json::to_vec(&json!({
            "model": "grok",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "你好"}],
        }))?;
        let chatgpt = convert(&plain, &CHATGPT_DIALECT)?;
        assert_eq!(chatgpt["tool_choice"], json!("auto"));
        assert!(chatgpt.get("max_output_tokens").is_none(), "{chatgpt}");
        let xai = convert(&plain, &XAI_DIALECT)?;
        assert!(xai.get("tool_choice").is_none(), "{xai}");
        assert_eq!(xai["parallel_tool_calls"], json!(true));
        assert!(xai.get("tools").is_none(), "{xai}");
        assert!(xai.get("max_output_tokens").is_none(), "{xai}");
        let commandcode = convert(&plain, &COMMANDCODE_DIALECT)?;
        assert_eq!(commandcode["tool_choice"], json!("auto"));
        assert_eq!(commandcode["parallel_tool_calls"], json!(true));
        assert!(commandcode.get("tools").is_none(), "{commandcode}");
        assert_eq!(commandcode["max_output_tokens"], json!(1024));

        // chatgpt 线路的载体按字节锁定: 参数化之后客户端已经保存的签名必须仍能原样回放; xai 线路
        // 使用同一载体格式但类型标识不同, 两条线路互不解码对方的推理桥.
        //
        // The chatgpt carrier is pinned byte for byte: signatures clients saved before the
        // parameterization must still replay unchanged. The xai line uses the same carrier format
        // with a different type marker, so neither line decodes the other's reasoning bridge.
        let chatgpt_bridge = encode_chatgpt_reasoning_bridge("rs_1", "enc-a");
        assert_eq!(
            chatgpt_bridge,
            standard(
                br#"{"type":"aria.chatgpt.reasoning","version":1,"id":"rs_1","encrypted_content":"enc-a"}"#
            )
        );
        let xai_bridge = encode_reasoning_bridge(XAI_REASONING_BRIDGE_TYPE, "rs_2", "enc-b");
        let expected = ChatGptReasoningBridge {
            id: "rs_2".to_owned(),
            encrypted_content: "enc-b".to_owned(),
        };
        assert_eq!(
            decode_reasoning_bridge(XAI_REASONING_BRIDGE_TYPE, &xai_bridge),
            Ok(Some(expected))
        );
        assert_eq!(decode_chatgpt_reasoning_bridge(&xai_bridge), Ok(None));
        assert_eq!(
            decode_reasoning_bridge(XAI_REASONING_BRIDGE_TYPE, &chatgpt_bridge),
            Ok(None)
        );
        let commandcode_bridge =
            encode_reasoning_bridge(COMMANDCODE_REASONING_BRIDGE_TYPE, "rs_3", "enc-c");
        let expected_commandcode = ChatGptReasoningBridge {
            id: "rs_3".to_owned(),
            encrypted_content: "enc-c".to_owned(),
        };
        assert_eq!(
            decode_reasoning_bridge(COMMANDCODE_REASONING_BRIDGE_TYPE, &commandcode_bridge),
            Ok(Some(expected_commandcode))
        );
        assert_eq!(
            decode_chatgpt_reasoning_bridge(&commandcode_bridge),
            Ok(None)
        );
        assert_eq!(
            decode_reasoning_bridge(XAI_REASONING_BRIDGE_TYPE, &commandcode_bridge),
            Ok(None)
        );
        assert_eq!(
            decode_reasoning_bridge(COMMANDCODE_REASONING_BRIDGE_TYPE, &chatgpt_bridge),
            Ok(None)
        );
        assert_eq!(
            decode_reasoning_bridge(COMMANDCODE_REASONING_BRIDGE_TYPE, &xai_bridge),
            Ok(None)
        );
        Ok(())
    }
}
