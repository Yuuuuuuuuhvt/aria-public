//! 两个订阅提供方共用的上游请求类型与构造错误.
//!
//! ChatGPT 透传与 Antigravity 转换构造出的结果形态相同: 一个地址, 一组请求头与一个请求体. 两条
//! 线路各自的构造错误也归成同一种类型, 这样 relay 可以用一套匹配处理两种线路的失败, 不必为每条
//! 线路复制一份分类.
//!
//! The upstream request type and construction error shared by both subscription providers. The
//! ChatGPT passthrough and the Antigravity conversion produce the same shape: a URL, a set of
//! headers and a body. Their construction errors are also unified into one type, so the relay can
//! handle both routes' failures with one match instead of one classification per route.

use std::fmt;

use http::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use crate::antigravity::request::InvalidRequest;

/// 构造完成的上游请求: 地址, 请求头与请求体.
///
/// 请求体长度由调用方在发送时交给 HTTP 客户端, 因此这里不再单独保存长度.
///
/// A built upstream request: URL, headers and body. The body length is handed to the HTTP client
/// when sending, so it is not stored separately here.
#[derive(Debug)]
pub struct UpstreamRequest {
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

/// 构造上游请求时的失败原因.
///
/// 只有 InvalidRequest 属于客户端输入错误; 另外两种来自账号或本端配置, 调用方不能把它们当作客户
/// 端请求错误返回 400.
///
/// The failure reasons when building an upstream request. Only InvalidRequest is a client input
/// error; the other two come from account or local configuration and must not be reported as a
/// client 400.
#[derive(Debug)]
pub enum BuildError {
    /// 客户端请求无法转换, 对应 Go 的 ErrInvalidAnthropicRequest. 只有 Antigravity 线路会产生它,
    /// ChatGPT 线路的正文原样透传.
    ///
    /// The client request cannot be converted, corresponding to Go's ErrInvalidAnthropicRequest.
    /// Only the Antigravity route produces it; the ChatGPT route passes the body through unchanged.
    InvalidRequest(InvalidRequest),
    /// 账号缺少项目标识. 只有 Antigravity 线路会产生它, 这是账号配置问题.
    ///
    /// The account has no project id. Only the Antigravity route produces it; this is an account
    /// configuration problem.
    MissingProjectId,
    /// 上游地址或请求头无法编码, 这是本端配置或凭据问题.
    ///
    /// The upstream URL or a request header cannot be encoded; this is a local configuration or
    /// credential problem.
    InvalidUpstream,
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(error) => error.fmt(formatter),
            Self::MissingProjectId => formatter.write_str("缺少项目标识, 无法构造上游请求"),
            Self::InvalidUpstream => formatter.write_str("上游地址或请求头无法编码"),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidRequest(error) => Some(error),
            _ => None,
        }
    }
}

/// 写入一个请求头. 头名与取值非法时返回错误而不是 panic.
///
/// Go 的 setRawHeader 直接写入 map, 非法取值会在发送阶段才失败; 这里提前到构造阶段, 失败与否的判
/// 断不变, 只是更早暴露. 头名在 http 层规范化为小写, 与 Go 写入的小写形态一致.
///
/// Writes one request header, returning an error instead of panicking on an invalid name or value.
/// Go's setRawHeader wrote straight into the map and an invalid value failed only when sending;
/// this moves the failure to construction time, which changes when it surfaces, not whether. Header
/// names are normalized to lowercase by the http layer, matching the lowercase form Go wrote.
pub fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), BuildError> {
    let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| BuildError::InvalidUpstream)?;
    let value = HeaderValue::from_str(value).map_err(|_| BuildError::InvalidUpstream)?;
    headers.insert(name, value);
    Ok(())
}
