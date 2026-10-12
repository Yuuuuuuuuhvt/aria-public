//! 两种对外错误格式. Responses 端点服务 OpenAI 协议的客户端, 沿用 Go 版 Lyra 的
//! OpenAI 风格错误; 其余端点, 包括 Messages, 模型列表, 健康检查与用量查询, 都使用
//! Anthropic 错误格式, 因为 Claude Code 是这些端点的主要调用方.
//!
//! Two client-facing error formats. Responses endpoints serve OpenAI-protocol clients
//! and keep the Go Lyra OpenAI-style error; every other endpoint, including Messages,
//! the model list, health and usage, uses the Anthropic error format because Claude
//! Code is their main caller.

use http::{HeaderValue, Response, StatusCode, header};
use serde_json::json;

use crate::body::{Body, full};

/// 按 HTTP 状态码选择 Anthropic 错误类型, 规则与 Go 版 Lyra 的 anthropicErrorType
/// 相同.
///
/// Picks the Anthropic error type for a status code, matching Go Lyra's
/// anthropicErrorType.
pub fn anthropic_error_type(status: StatusCode) -> &'static str {
    match status.as_u16() {
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        429 => "rate_limit_error",
        413 => "request_too_large",
        500..=599 => "api_error",
        _ => "invalid_request_error",
    }
}

/// 编码 Anthropic 错误响应体.
///
/// Encodes an Anthropic error body.
pub fn anthropic_error_body(kind: &str, message: &str) -> Vec<u8> {
    json!({"type": "error", "error": {"type": kind, "message": message}})
        .to_string()
        .into_bytes()
}

/// 构造 Anthropic 错误响应. kind 为 None 时按状态码推导错误类型.
///
/// Builds an Anthropic error response. When kind is None the type is derived from
/// the status code.
pub fn anthropic_error(status: StatusCode, kind: Option<&str>, message: &str) -> Response<Body> {
    let kind = kind.unwrap_or_else(|| anthropic_error_type(status));
    json_response(status, anthropic_error_body(kind, message))
}

/// 构造 OpenAI 风格错误响应. 错误类型 codex_relay_error 与 Go 版 Lyra 相同, Codex
/// 客户端和实验框架见到的错误因此不变. 响应要求关闭连接, 与原实现一致.
///
/// Builds an OpenAI-style error response. The codex_relay_error type matches Go Lyra,
/// so Codex clients and the experiment framework see unchanged errors. Like the
/// original, the response asks to close the connection.
pub fn openai_error(status: StatusCode, message: &str) -> Response<Body> {
    let body = json!({"error": {"message": message, "type": "codex_relay_error"}});
    let mut response = json_response(status, body.to_string().into_bytes());
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));
    response
}

/// 构造带 no-store 缓存头的 JSON 响应.
///
/// Builds a JSON response with a no-store cache header.
pub fn json_response(status: StatusCode, body: Vec<u8>) -> Response<Body> {
    let mut response = Response::new(full(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
