//! 函数调用签名桥的编码, 解码与校验.
//!
//! 本反代的历史回传协议不是 Google 或 Anthropic 的官方签名格式: JSON 经 base64 编码后放入
//! 标准 redacted_thinking.data, 客户端原样保存即可. 签名只回填到同一助手消息内由
//! tool_use_id 指定的 functionCall part, 不使用进程缓存, 也不能用此协议解释其他模型的不透明
//! redacted_thinking. 已用真实上游验证过该协议: 上游需要原签名, 且 Claude Code 会保留
//! 承载签名的载体.
//!
//! Encoding, decoding and validation of the function-call signature bridge. This is this
//! relay's history replay protocol, not an official signature format of Google or Anthropic:
//! the JSON is base64-encoded into a standard redacted_thinking.data that the client stores
//! unchanged. The signature is restored only to the functionCall part named by tool_use_id
//! within the same assistant message; no process cache is used, and this protocol must not be
//! used to interpret another model's opaque redacted_thinking.

use std::collections::HashMap;

use base64::Engine;
use serde::Serialize;
use serde_json::{Map, Value};

use super::request::{
    AnthropicBlock, AnthropicTurn, BLOCK_REDACTED_THINKING, BLOCK_TOOL_USE, InvalidRequest,
    ROLE_ASSISTANT,
};
use super::response::TranslateError;

/// 载体类型标识, 同时是识别本反代载体的前缀.
///
/// The carrier type marker, which also serves as the prefix identifying this relay's carrier.
pub(crate) const GOOGLE_FUNCTION_CALL_BRIDGE_TYPE: &str = "self-infra.google.function-call";

/// 标准编码下类型标识位于 JSON 开头. 即使后半段损坏也能识别本反代的载体, 但不能把普通加密
/// 字节或未知 JSON 类型误当成 Google 签名.
///
/// With standard encoding the type marker sits at the start of the JSON. Even a corrupted tail
/// still identifies this relay's carrier, but ordinary encrypted bytes or an unknown JSON type
/// must not be mistaken for a Google signature.
const BRIDGE_MARKER: &str = r#"{"type":"self-infra.google.function-call""#;

/// 当前载体版本. 只有版本 1 存在, 其他版本必须报错而不是被忽略.
///
/// The current carrier version. Only version 1 exists; other versions must error rather than be
/// ignored.
const BRIDGE_VERSION: i64 = 1;

/// 载体解码后的字段.
///
/// The fields of a decoded carrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoogleFunctionCallBridge {
    pub(crate) tool_use_id: String,
    pub(crate) thought_signature: String,
    pub(crate) function_call_id: String,
}

/// 编码载体时的字段顺序. 字段声明顺序即 JSON 键顺序, 必须与 Go 的结构体一致, 否则解码时的
/// 前缀识别会失败.
///
/// The field order used when encoding the carrier. Declaration order is JSON key order and must
/// match the Go struct, otherwise prefix recognition on decode fails.
#[derive(Debug, Serialize)]
struct BridgePayload {
    #[serde(rename = "type")]
    kind: String,
    version: i64,
    tool_use_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    thought_signature: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    function_call_id: String,
}

/// 编码函数调用签名载体.
///
/// Encodes the function-call signature carrier.
pub(crate) fn encode_google_function_call_bridge(
    tool_use_id: &str,
    signature: &str,
    function_call_id: &str,
) -> Result<String, TranslateError> {
    let payload = BridgePayload {
        kind: GOOGLE_FUNCTION_CALL_BRIDGE_TYPE.to_owned(),
        version: BRIDGE_VERSION,
        tool_use_id: tool_use_id.to_owned(),
        thought_signature: signature.to_owned(),
        function_call_id: function_call_id.to_owned(),
    };
    let encoded = serde_json::to_vec(&payload)
        .map_err(|err| TranslateError::new(format!("编码函数调用签名载体失败: {err}")))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(encoded))
}

/// 解码函数调用签名载体.
///
/// 忽略没有本协议类型标识的其他模型数据; 已经识别为本协议的损坏数据必须报错, 否则静默丢失
/// 签名会在上游变成难以定位的工具失败.
///
/// Decodes the function-call signature carrier. Data from other models that lacks this
/// protocol's type marker is ignored; data already identified as this protocol but corrupted
/// must error, because a silently lost signature becomes a hard-to-locate tool failure upstream.
pub(crate) fn decode_google_function_call_bridge(
    data: &str,
) -> Result<Option<GoogleFunctionCallBridge>, InvalidRequest> {
    let (decoded, truncated) = decode_base64_prefix(data);
    let has_marker = decoded.starts_with(BRIDGE_MARKER.as_bytes());
    if truncated {
        if has_marker {
            return Err(bridge_error("invalid base64"));
        }
        return Ok(None);
    }
    let mut values = serde_json::Deserializer::from_slice(&decoded).into_iter::<Value>();
    let value = match values.next() {
        Some(Ok(value)) => value,
        _ => {
            if has_marker {
                return Err(bridge_error("invalid JSON"));
            }
            return Ok(None);
        }
    };
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.get("type").and_then(Value::as_str) != Some(GOOGLE_FUNCTION_CALL_BRIDGE_TYPE) {
        return Ok(None);
    }
    // 已经确认是本反代的载体, 之后的结构问题一律报错.
    //
    // The carrier is confirmed to belong to this relay; every later structural problem errors.
    let fields = parse_bridge_fields(object)?;
    if values.next().is_some() {
        return Err(bridge_error("trailing data"));
    }
    if fields.version != BRIDGE_VERSION {
        return Err(bridge_error(&format!(
            "unsupported version {}",
            fields.version
        )));
    }
    if fields.tool_use_id.is_empty()
        || (fields.thought_signature.is_empty() && fields.function_call_id.is_empty())
    {
        return Err(bridge_error("missing tool id or replay data"));
    }
    Ok(Some(GoogleFunctionCallBridge {
        tool_use_id: fields.tool_use_id,
        thought_signature: fields.thought_signature,
        function_call_id: fields.function_call_id,
    }))
}

/// 按 Go 的结构体解码规则读取载体字段: 未知字段报错, null 视为不赋值.
///
/// 字段名按原样匹配. Go 的解码器还会做不区分大小写的匹配, 但本协议的载体只由本模块编码,
/// 键名固定为小写, 因此两种匹配方式对本协议的数据没有差别.
///
/// Reads the carrier fields with Go's struct decoding rules: an unknown field errors and null is
/// a no-op. Field names are matched exactly; Go's decoder also matched case-insensitively, but
/// this protocol's carrier is written only by this module with fixed lowercase keys, so the two
/// rules cannot differ for this protocol's data.
fn parse_bridge_fields(object: &Map<String, Value>) -> Result<BridgeFields, InvalidRequest> {
    let mut fields = BridgeFields {
        version: 0,
        tool_use_id: String::new(),
        thought_signature: String::new(),
        function_call_id: String::new(),
    };
    for (key, value) in object {
        match key.as_str() {
            // type 在进入本函数前已确认取值为本协议标识.
            //
            // The type value is already known to be this protocol's marker.
            "type" => {}
            "version" => match value {
                Value::Null => {}
                Value::Number(number) => {
                    let Some(parsed) = number.as_i64() else {
                        return Err(bridge_error("malformed fields"));
                    };
                    fields.version = parsed;
                }
                _ => return Err(bridge_error("malformed fields")),
            },
            "tool_use_id" => fields.tool_use_id = optional_string(value)?,
            "thought_signature" => fields.thought_signature = optional_string(value)?,
            "function_call_id" => fields.function_call_id = optional_string(value)?,
            _ => return Err(bridge_error("malformed fields")),
        }
    }
    Ok(fields)
}

/// 载体字段的中间结果.
///
/// The intermediate result of the carrier fields.
struct BridgeFields {
    version: i64,
    tool_use_id: String,
    thought_signature: String,
    function_call_id: String,
}

/// 读取一个字符串字段: null 得到空串, 类型不符报错.
///
/// Reads one string field: null gives an empty string and a type mismatch errors.
fn optional_string(value: &Value) -> Result<String, InvalidRequest> {
    match value {
        Value::Null => Ok(String::new()),
        Value::String(text) => Ok(text.clone()),
        _ => Err(bridge_error("malformed fields")),
    }
}

/// 构造载体错误. 错误信息只描述问题, 不携带签名载荷或原始请求.
///
/// Builds a carrier error. The message describes the problem only and carries neither the
/// signature payload nor the raw request.
fn bridge_error(detail: &str) -> InvalidRequest {
    InvalidRequest::new(format!(
        "invalid Google function-call signature bridge: {detail}"
    ))
}

/// 复现 Go 的 base64.StdEncoding.DecodeString 在出错时仍返回已解码前缀的行为.
///
/// 逐量子处理: 遇到非法字符, 坏填充或不足一个量子就停止, 返回此前完整解出的字节以及是否出错.
/// 这个前缀正是判断载体类型标识所需的全部内容.
///
/// Reproduces Go's base64.StdEncoding.DecodeString behavior of returning the already decoded
/// prefix when it fails. It processes quantum by quantum and stops at an invalid character, bad
/// padding or an incomplete quantum, returning the bytes decoded so far plus whether it failed.
/// That prefix is exactly what is needed to detect the carrier type marker.
pub(crate) fn decode_base64_prefix(data: &str) -> (Vec<u8>, bool) {
    let bytes = data.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut index = 0;
    while index < bytes.len() {
        let mut quantum = [0u8; 4];
        let mut filled = 0;
        while filled < 4 && index < bytes.len() {
            let byte = bytes[index];
            index += 1;
            // Go 的解码器在量子内跳过换行.
            //
            // Go's decoder skips newlines inside a quantum.
            if byte == b'\n' || byte == b'\r' {
                continue;
            }
            quantum[filled] = byte;
            filled += 1;
        }
        if filled < 4 {
            return (decoded, true);
        }
        // 先解出这个量子再判断填充位置: Go 在读到填充之后才失败于下一个字符, 因此带填充的末尾
        // 量子本身已经写入结果. 这一步对识别类型标识是必需的, 标识的最后一个字节正落在带填充的
        // 量子内.
        //
        // The quantum is decoded before padding position is judged: Go fails on the character after
        // the padding, so a final padded quantum is already written. This matters for recognizing
        // the type marker, whose last byte sits in the padded quantum.
        match base64::engine::general_purpose::STANDARD.decode(quantum) {
            Ok(chunk) => decoded.extend_from_slice(&chunk),
            Err(_) => return (decoded, true),
        }
        if quantum.contains(&b'=') {
            // 填充只能出现在输入的末尾; 其后除换行外还有内容时 Go 同样报错.
            //
            // Padding may only appear at the end of the input; Go also errors when anything but a
            // newline follows.
            if bytes[index..]
                .iter()
                .any(|byte| *byte != b'\n' && *byte != b'\r')
            {
                return (decoded, true);
            }
            return (decoded, false);
        }
    }
    (decoded, false)
}

/// 收集单条助手消息内的载体.
///
/// 按调用标识而非工具名称关联, 防止同名工具并行调用或连续调用时把一个签名分配到另一个
/// functionCall. 每个载体的 tool_use_id 在同一消息内必须恰好对应一个 tool_use.
///
/// Collects the carriers within one assistant message. Association is by call id rather than
/// tool name, preventing a signature from being assigned to the wrong functionCall when
/// same-named tools are called in parallel or in sequence. Each carrier's tool_use_id must match
/// exactly one tool_use in the same message.
pub(crate) fn google_function_call_bridges(
    turn: &AnthropicTurn,
    blocks: &[AnthropicBlock],
) -> Result<HashMap<String, GoogleFunctionCallBridge>, InvalidRequest> {
    let mut bridges: HashMap<String, GoogleFunctionCallBridge> = HashMap::new();
    let mut tool_counts: HashMap<&str, usize> = HashMap::new();
    for block in blocks {
        if block.kind == BLOCK_TOOL_USE {
            *tool_counts.entry(block.id.as_str()).or_insert(0) += 1;
        }
        if block.kind != BLOCK_REDACTED_THINKING {
            continue;
        }
        let Some(bridge) = decode_google_function_call_bridge(&block.data)? else {
            continue;
        };
        if turn.role != ROLE_ASSISTANT {
            return Err(bridge_error("requires assistant message"));
        }
        if bridges.contains_key(&bridge.tool_use_id) {
            return Err(bridge_error("duplicate tool id"));
        }
        bridges.insert(bridge.tool_use_id.clone(), bridge);
    }
    for tool_use_id in bridges.keys() {
        if tool_counts.get(tool_use_id.as_str()).copied().unwrap_or(0) != 1 {
            return Err(bridge_error(
                "expected one matching tool_use in the same message",
            ));
        }
    }
    Ok(bridges)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::antigravity::request::BLOCK_TOOL_RESULT;
    use crate::antigravity::response::{
        AnthropicTranslator, DELTA_JSON, DELTA_TEXT, DELTA_THINKING, EVENT_CONTENT_BLOCK_DELTA,
        EVENT_CONTENT_BLOCK_START, EVENT_CONTENT_BLOCK_STOP,
    };
    use crate::antigravity::test_support::{
        collect, event_field, event_object, inner_request, parse_events, parts_of,
    };

    /// 模拟客户端按标准 Anthropic 事件累积内容. 测试随后把这些内容原样回传, 从响应转换到历史
    /// 转换验证签名, 不依赖服务内存中的关联状态.
    ///
    /// Simulates a client accumulating content from standard Anthropic events. The test then passes
    /// that content back unchanged, verifying the signature across the response and history
    /// conversions without relying on association state in service memory.
    fn translated_content(stream: bool, event: &str) -> Vec<Value> {
        let mut translator = AnthropicTranslator::new("gemini-3.8-flash-high", stream);
        let out = collect(&mut translator, &[event]);
        assert!(translator.started(), "有效函数调用应保留 Started 状态");
        if !stream {
            let response: Value = match serde_json::from_slice(&out) {
                Ok(response) => response,
                Err(err) => panic!("非流式输出不是 JSON: {err}"),
            };
            return match response["content"].as_array() {
                Some(content) => content.clone(),
                None => panic!("非流式输出缺少 content"),
            };
        }
        let mut content: Vec<Value> = Vec::new();
        let mut open: Option<usize> = None;
        let mut partial = String::new();
        for event in parse_events(&out) {
            match event.0.as_str() {
                EVENT_CONTENT_BLOCK_START => {
                    let index = event_field(&event, "index").as_u64().unwrap_or(u64::MAX) as usize;
                    assert!(
                        open.is_none() && index == content.len(),
                        "内容块没有按索引依次配对"
                    );
                    open = Some(index);
                    partial.clear();
                    content.push(event_field(&event, "content_block").clone());
                }
                EVENT_CONTENT_BLOCK_DELTA => {
                    let index = event_field(&event, "index").as_u64().unwrap_or(u64::MAX) as usize;
                    assert_eq!(Some(index), open, "增量与打开的内容块不匹配");
                    let delta = event_object(&event, "delta").clone();
                    let Some(block) = content.get_mut(index).and_then(Value::as_object_mut) else {
                        panic!("内容块不存在: {index}");
                    };
                    match delta["type"].as_str() {
                        Some(DELTA_JSON) => {
                            partial.push_str(delta["partial_json"].as_str().unwrap_or_default());
                        }
                        Some(DELTA_TEXT) => {
                            let text = block["text"].as_str().unwrap_or_default().to_owned();
                            block["text"] =
                                Value::String(text + delta["text"].as_str().unwrap_or_default());
                        }
                        Some(DELTA_THINKING) => {
                            let text = block["thinking"].as_str().unwrap_or_default().to_owned();
                            block["thinking"] = Value::String(
                                text + delta["thinking"].as_str().unwrap_or_default(),
                            );
                        }
                        other => panic!("出现非标准测试增量: {other:?}"),
                    }
                }
                EVENT_CONTENT_BLOCK_STOP => {
                    let index = event_field(&event, "index").as_u64().unwrap_or(u64::MAX) as usize;
                    assert_eq!(Some(index), open, "关闭事件与打开的内容块不匹配");
                    if !partial.is_empty() {
                        let input: Value = match serde_json::from_str(&partial) {
                            Ok(input) => input,
                            Err(err) => panic!("增量不是合法 JSON: {partial:?} {err}"),
                        };
                        content[index]["input"] = input;
                    }
                    open = None;
                }
                _ => {}
            }
        }
        assert!(open.is_none(), "事件流结束后仍有未关闭的块");
        content
    }

    /// 构造带函数调用段落的上游事件.
    ///
    /// Builds an upstream event carrying function-call segments.
    fn function_call_event(parts: Vec<Value>) -> String {
        json!({"response": {"candidates": [{
            "content": {"role": "model", "parts": parts}, "finishReason": "STOP",
        }]}})
        .to_string()
    }

    /// 把一组消息回放成上游信封的 request 内容.
    ///
    /// Replays a set of messages into the request content of an upstream envelope.
    fn replay_content(messages: Value) -> Map<String, Value> {
        let body = json!({"model": "gemini-3.8-flash-high", "messages": messages});
        let envelope =
            crate::antigravity::test_support::envelope_with_project(&body.to_string(), "project");
        inner_request(&envelope).clone()
    }

    /// 取出内容块里的工具调用标识, 并核对标识本身不承载签名.
    ///
    /// Extracts the tool call ids from content blocks and checks that an id carries no signature.
    fn tool_ids(content: &[Value]) -> Vec<String> {
        let mut ids = Vec::new();
        for value in content {
            let Some(block) = value.as_object() else {
                panic!("内容块不是对象: {value}");
            };
            if block.get("type").and_then(Value::as_str) != Some(BLOCK_TOOL_USE) {
                continue;
            }
            let Some(id) = block.get("id").and_then(Value::as_str) else {
                panic!("tool_use 缺少 id: {value}");
            };
            assert!(
                id.starts_with("toolu_") && id.len() <= 64,
                "工具标识不应承载签名: length={}",
                id.len()
            );
            assert_eq!(block.len(), 4, "tool_use 不应携带非标准签名字段");
            ids.push(id.to_owned());
        }
        ids
    }

    #[test]
    fn signature_replay_handles_mixed_carrier_history() {
        let signature = "opaque-mixed-roundtrip-signature";
        for stream in [false, true] {
            let content = translated_content(
                stream,
                &function_call_event(vec![
                    json!({
                        "functionCall": {"name": "lookup", "args": {"key": "a"}},
                        "thoughtSignature": signature,
                    }),
                    json!({"functionCall": {"name": "lookup", "args": {"key": "b"}}}),
                ]),
            );
            let ids = tool_ids(&content);
            assert_eq!(ids.len(), 2, "并行调用应有两个普通调用标识");
            let messages = json!([
                {"role": "user", "content": "Read both values."},
                {"role": "assistant", "content": content},
                {"role": "user", "content": [
                    {"type": BLOCK_TOOL_RESULT, "tool_use_id": ids[0], "content": "A"},
                    {"type": BLOCK_TOOL_RESULT, "tool_use_id": ids[1], "content": "B"},
                ]},
                {"role": "assistant", "content": [
                    {"type": BLOCK_TOOL_USE, "id": "call_00_foreign_step",
                     "name": "lookup", "input": {"key": "c"}}]},
                {"role": "user", "content": [
                    {"type": BLOCK_TOOL_RESULT, "tool_use_id": "call_00_foreign_step",
                     "content": "C"}]},
            ]);
            let inner = replay_content(messages);
            let captured = parts_of(&inner, 1);
            assert_eq!(captured.len(), 2, "载体不能变成多余的 part: {captured:?}");
            assert_eq!(
                captured[0]["thoughtSignature"],
                json!(signature),
                "有载体的签名必须原值回放"
            );
            assert!(
                captured[1].get("thoughtSignature").is_none(),
                "并行调用的第二个 part 不得带签名: {captured:?}"
            );
            let foreign = parts_of(&inner, 3);
            assert_eq!(
                foreign[0]["thoughtSignature"],
                json!("skip_thought_signature_validator"),
                "无载体历史应按官方方式兜底"
            );
        }
    }
}
