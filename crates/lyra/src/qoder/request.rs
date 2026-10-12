//! Anthropic Messages 请求到 Qoder 网关协议请求的无状态转换.
//!
//! 按照公开协议参考实现构建:
//! 1. 将 Anthropic 的 system, messages 与 tools 转换为 Qoder 网关的 chatTemplate 结构;
//! 2. 工具调用块 tool_use 转换为 function call, 工具返回块 tool_result 转换为 tool 角色消息;
//! 3. 最终请求体通过 codec::encode 执行自定义字母表置换与重排编码.
//!
//! Copyright (c) 2026 ggbdpq (MIT License)
//! Copyright (c) 2026 wangjunyao (MIT License)

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::codec::encode;

fn generate_id(prefix: &str) -> String {
    let mut bytes = [0u8; 16];
    rand::fill(&mut bytes);
    format!("{prefix}_{}", hex::encode(bytes))
}

/// 网关请求单条内容片段.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GatewayContentPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image_url")]
    ImageUrl { image_url: GatewayImageUrl },
}

/// 图片链接片段.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayImageUrl {
    pub url: String,
}

/// 网关工具调用声明.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayWireToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: GatewayWireFunctionCall,
}

/// 网关函数调用内容.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayWireFunctionCall {
    pub name: String,
    pub arguments: String,
}

/// 网关工具定义.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayWireTool {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: GatewayWireFunctionDef,
}

/// 网关函数原型定义.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayWireFunctionDef {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

/// 网关消息记录.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayWireMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents: Option<Vec<GatewayContentPart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<GatewayWireToolCall>>,
}

/// Qoder 网关 chatTemplate 请求体.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayChatRequest {
    pub request_id: String,
    pub request_set_id: String,
    pub chat_record_id: String,
    pub stream: bool,
    pub chat_task: String,
    pub chat_context: Value,
    pub session_id: String,
    pub source: u32,
    pub version: String,
    pub aliyun_user_type: String,
    pub session_type: String,
    pub agent_id: String,
    pub task_id: String,
    pub model_config: Value,
    pub messages: Vec<GatewayWireMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<GatewayWireTool>>,
    pub business: Value,
}

/// 将 Anthropic Messages 请求体转换为 Qoder 网关请求体并执行自定义编码.
pub fn build_gateway_request(
    body: &[u8],
    upstream_model: &str,
    session_id: &str,
) -> Result<Bytes, String> {
    let root: Value =
        serde_json::from_slice(body).map_err(|err| format!("解析请求体失败: {err}"))?;
    let Some(obj) = root.as_object() else {
        return Err("请求体必须是 JSON 对象".to_owned());
    };

    let req_id = generate_id("req");
    let record_id = generate_id("rec");
    let now_ms = chrono::Utc::now().timestamp_millis();

    let mut wire_messages = Vec::new();

    // 1. 系统提示词转换
    if let Some(system_val) = obj.get("system") {
        let system_text = match system_val {
            Value::String(s) => s.clone(),
            Value::Array(arr) => {
                let mut parts = Vec::new();
                for item in arr {
                    if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                        parts.push(text);
                    }
                }
                parts.join("\n\n")
            }
            _ => String::new(),
        };
        if !system_text.is_empty() {
            wire_messages.push(GatewayWireMessage {
                role: "system".to_owned(),
                content: Some(system_text),
                contents: None,
                name: None,
                tool_call_id: None,
                tool_calls: None,
            });
        }
    }

    // 2. 上下文消息转换
    if let Some(Value::Array(msgs)) = obj.get("messages") {
        for msg in msgs {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
            let content_val = msg.get("content");

            match content_val {
                Some(Value::String(text)) => {
                    wire_messages.push(GatewayWireMessage {
                        role: role.to_owned(),
                        content: Some(text.clone()),
                        contents: None,
                        name: None,
                        tool_call_id: None,
                        tool_calls: None,
                    });
                }
                Some(Value::Array(blocks)) => {
                    let mut text_parts = Vec::new();
                    let mut tool_calls = Vec::new();
                    let mut tool_results = Vec::new();

                    for block in blocks {
                        let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        match block_type {
                            "text" => {
                                if let Some(t) = block.get("text").and_then(|s| s.as_str()) {
                                    text_parts.push(t);
                                }
                            }
                            "tool_use" => {
                                let id = block.get("id").and_then(|s| s.as_str()).unwrap_or("");
                                let name = block.get("name").and_then(|s| s.as_str()).unwrap_or("");
                                let input_val = block.get("input").cloned().unwrap_or(json!({}));
                                let args_str = serde_json::to_string(&input_val)
                                    .unwrap_or_else(|_| "{}".to_owned());

                                tool_calls.push(GatewayWireToolCall {
                                    id: id.to_owned(),
                                    kind: "function".to_owned(),
                                    function: GatewayWireFunctionCall {
                                        name: name.to_owned(),
                                        arguments: args_str,
                                    },
                                });
                            }
                            "tool_result" => {
                                let tool_use_id = block
                                    .get("tool_use_id")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("");
                                let content = match block.get("content") {
                                    Some(Value::String(s)) => s.clone(),
                                    Some(other) => serde_json::to_string(other).unwrap_or_default(),
                                    None => String::new(),
                                };
                                tool_results.push((tool_use_id.to_owned(), content));
                            }
                            _ => {}
                        }
                    }

                    if !tool_results.is_empty() {
                        for (tid, res_content) in tool_results {
                            wire_messages.push(GatewayWireMessage {
                                role: "tool".to_owned(),
                                content: Some(res_content),
                                contents: None,
                                name: None,
                                tool_call_id: Some(tid),
                                tool_calls: None,
                            });
                        }
                    } else if role == "assistant" {
                        let combined_text = text_parts.join("");
                        wire_messages.push(GatewayWireMessage {
                            role: "assistant".to_owned(),
                            content: if combined_text.is_empty() {
                                None
                            } else {
                                Some(combined_text)
                            },
                            contents: None,
                            name: None,
                            tool_call_id: None,
                            tool_calls: if tool_calls.is_empty() {
                                None
                            } else {
                                Some(tool_calls)
                            },
                        });
                    } else {
                        let combined_text = text_parts.join("");
                        wire_messages.push(GatewayWireMessage {
                            role: "user".to_owned(),
                            content: Some(combined_text),
                            contents: None,
                            name: None,
                            tool_call_id: None,
                            tool_calls: None,
                        });
                    }
                }
                _ => {}
            }
        }
    }

    // 3. 工具定义转换
    let tools = if let Some(Value::Array(raw_tools)) = obj.get("tools") {
        let mut list = Vec::new();
        for tool in raw_tools {
            let name = tool.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let desc = tool
                .get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("");
            let schema = tool.get("input_schema").cloned().unwrap_or(json!({}));

            list.push(GatewayWireTool {
                kind: "function".to_owned(),
                function: GatewayWireFunctionDef {
                    name: name.to_owned(),
                    description: desc.to_owned(),
                    parameters: schema,
                },
            });
        }
        if list.is_empty() { None } else { Some(list) }
    } else {
        None
    };

    let chat_req = GatewayChatRequest {
        request_id: req_id.clone(),
        request_set_id: req_id,
        chat_record_id: record_id,
        stream: true,
        chat_task: "FREE_INPUT".to_owned(),
        chat_context: json!({
            "extra": {
                "modelConfig": { "is_reasoning": false, "key": upstream_model },
                "originalContent": { "type": "text", "text": "" }
            },
            "text": { "type": "text", "text": "" }
        }),
        session_id: if session_id.is_empty() {
            generate_id("sess")
        } else {
            session_id.to_owned()
        },
        source: 1,
        version: "3".to_owned(),
        aliyun_user_type: "personal_standard".to_owned(),
        session_type: "qodercli".to_owned(),
        agent_id: "agent_common".to_owned(),
        task_id: "common".to_owned(),
        model_config: json!({
            "key": upstream_model,
            "is_vl": false,
            "is_reasoning": false,
            "source": "system"
        }),
        messages: wire_messages,
        tools,
        business: json!({
            "id": "",
            "name": "",
            "begin_at": now_ms
        }),
    };

    let raw_json = serde_json::to_vec(&chat_req).map_err(|e| format!("序列化网关请求失败: {e}"))?;
    let encoded_str = encode(&raw_json);
    Ok(Bytes::from(encoded_str.into_bytes()))
}
