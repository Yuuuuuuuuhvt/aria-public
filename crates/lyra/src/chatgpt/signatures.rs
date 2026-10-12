//! ChatGPT 推理桥的编码, 解码与校验.
//!
//! 推理桥把上游 Codex 生成的 reasoning 条目及其 encrypted_content 放入 Anthropic
//! thinking 块的 signature 字段, 客户端在下一轮按原样回放. 校验规则与 Antigravity 签名桥相同:
//! 非本协议数据跳过, 识别为本协议后字段必须完整且不允许未知字段.
//!
//! Encoding, decoding and validation of the ChatGPT reasoning bridge. The reasoning bridge puts
//! upstream Codex reasoning items and their encrypted_content into the signature field of an
//! Anthropic thinking block, and the client replays it unchanged in subsequent turns. Validation
//! rules match the Antigravity signature bridge: data from other protocols is skipped, while data
//! identified as this protocol must have complete fields and rejects unknown fields.
//!
//! xai 线路使用同一套载体格式, 但类型标识不同. encrypted_content 只能由签发它的后端解密, 客户
//! 端在同一会话里切换 chatgpt 与 xai 路由时, 另一条线路签发的推理桥按其他来源丢弃, 不会被发
//! 往无法解密它的后端.
//!
//! The xai line uses the same carrier format with a different type marker. encrypted_content can
//! only be decrypted by the backend that issued it, so when a client switches between chatgpt and
//! xai routes within one conversation, a bridge issued by the other line is dropped as data from
//! another source and never sent to a backend that cannot decrypt it.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// 尾部损坏时整体解码只会得到空结果, 无法再识别类型标识, 因此复用签名桥的逐量子前缀解码.
//
// Decoding the whole string yields nothing when the tail is corrupt, losing the type marker, so
// the quantum-by-quantum prefix decoder of the signature bridge is reused.
use crate::antigravity::signatures::decode_base64_prefix;

/// chatgpt 线路的载体类型标识.
///
/// The carrier type marker of the chatgpt line.
pub const CHATGPT_REASONING_BRIDGE_TYPE: &str = "aria.chatgpt.reasoning";

/// xai 线路的载体类型标识.
///
/// The carrier type marker of the xai line.
pub const XAI_REASONING_BRIDGE_TYPE: &str = "aria.xai.reasoning";

/// commandcode 线路的载体类型标识.
///
/// The carrier type marker of the commandcode line.
pub const COMMANDCODE_REASONING_BRIDGE_TYPE: &str = "aria.commandcode.reasoning";

/// 标准编码下类型标识之前的固定前缀. 前缀之后紧跟类型标识与结尾引号时, 数据即属于该协议,
/// 用于在字段校验之前提前识别本协议载体.
///
/// The fixed prefix before the type marker under standard encoding. When the type marker and its
/// closing quote follow the prefix, the data belongs to that protocol; this recognizes the carrier
/// early, before field validation.
const BRIDGE_MARKER_PREFIX: &str = r#"{"type":""#;

/// 当前载体版本.
///
/// The current carrier version.
const BRIDGE_VERSION: i64 = 1;

/// 解码后的推理桥数据.
///
/// Decoded reasoning bridge data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatGptReasoningBridge {
    pub id: String,
    pub encrypted_content: String,
}

/// 推理桥的序列化载荷结构.
///
/// The serialized payload structure of the reasoning bridge.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BridgePayload {
    #[serde(rename = "type")]
    kind: String,
    version: i64,
    id: String,
    encrypted_content: String,
}

/// 编码 ChatGPT 推理桥载体.
///
/// Encodes the ChatGPT reasoning bridge carrier.
pub fn encode_chatgpt_reasoning_bridge(id: &str, encrypted_content: &str) -> String {
    encode_reasoning_bridge(CHATGPT_REASONING_BRIDGE_TYPE, id, encrypted_content)
}

/// 解码并校验 ChatGPT 推理桥载体.
///
/// Decodes and validates the ChatGPT reasoning bridge carrier.
pub fn decode_chatgpt_reasoning_bridge(
    data: &str,
) -> Result<Option<ChatGptReasoningBridge>, String> {
    decode_reasoning_bridge(CHATGPT_REASONING_BRIDGE_TYPE, data)
}

/// 按给定线路的类型标识编码推理桥载体.
///
/// Encodes the reasoning bridge carrier with the given line's type marker.
pub fn encode_reasoning_bridge(bridge_type: &str, id: &str, encrypted_content: &str) -> String {
    let payload = BridgePayload {
        kind: bridge_type.to_owned(),
        version: BRIDGE_VERSION,
        id: id.to_owned(),
        encrypted_content: encrypted_content.to_owned(),
    };
    let encoded = serde_json::to_vec(&payload).unwrap_or_default();
    base64::engine::general_purpose::STANDARD.encode(encoded)
}

/// 按给定线路的类型标识解码并校验推理桥载体.
///
/// 忽略没有该类型标识的其他数据, 其中包括另一条线路签发的推理桥; 已经识别为该协议的损坏或
/// 未知字段数据返回错误.
///
/// Decodes and validates the reasoning bridge carrier with the given line's type marker. Data
/// lacking that type marker is ignored, including a bridge issued by the other line; corrupted
/// data or data with unknown fields recognized as that protocol returns an error.
pub fn decode_reasoning_bridge(
    bridge_type: &str,
    data: &str,
) -> Result<Option<ChatGptReasoningBridge>, String> {
    let (decoded, truncated) = decode_base64_prefix(data);
    let has_marker = decoded
        .strip_prefix(BRIDGE_MARKER_PREFIX.as_bytes())
        .and_then(|rest| rest.strip_prefix(bridge_type.as_bytes()))
        .is_some_and(|rest| rest.first() == Some(&b'"'));
    if truncated {
        if has_marker {
            return Err("推理桥 base64 格式无效".to_owned());
        }
        return Ok(None);
    }
    let mut values = serde_json::Deserializer::from_slice(&decoded).into_iter::<Value>();
    let value = match values.next() {
        Some(Ok(value)) => value,
        _ => {
            if has_marker {
                return Err("推理桥 JSON 格式无效".to_owned());
            }
            return Ok(None);
        }
    };
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.get("type").and_then(Value::as_str) != Some(bridge_type) {
        return Ok(None);
    }
    if values.next().is_some() {
        return Err("推理桥包含多余尾部数据".to_owned());
    }
    let payload: BridgePayload = match serde_json::from_value(Value::Object(object.clone())) {
        Ok(payload) => payload,
        Err(err) => return Err(format!("推理桥字段校验失败: {err}")),
    };
    if payload.version != BRIDGE_VERSION {
        return Err(format!("不支持的推理桥版本: {}", payload.version));
    }
    Ok(Some(ChatGptReasoningBridge {
        id: payload.id,
        encrypted_content: payload.encrypted_content,
    }))
}
