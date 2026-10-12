//! ChatGPT 提供方: Codex Responses 透传的请求构造.
//!
//! 正文原样透传, 不做任何改写; 请求头由认证头, 固定标识与客户端透传头组成. 本模块只构造请求,
//! 账号池与轮换由 credential 与 relay 模块承担.
//!
//! The ChatGPT route: request construction for passing the Codex Responses protocol through. The
//! body is passed through unchanged; the headers consist of authentication headers, fixed
//! identifiers and client headers. This module only builds requests; the account pool and rotation
//! belong to the credential and relay modules.
//!
//! chatgpt 路由的 Messages 协议转换放在子模块中: request 把 Anthropic Messages 请求转换为
//! Responses 请求, response 把 Responses 事件流转换回 Anthropic 格式, signatures 负责推理桥.
//! 转换后的请求仍由本模块的 build_request 发出.
//!
//! The Messages conversion of chatgpt routes lives in submodules: request converts Anthropic
//! Messages requests into Responses requests, response converts the Responses event stream back
//! into the Anthropic format, and signatures owns the reasoning bridge. The converted request is
//! still sent through this module's build_request.
//!
//! xai 线路的 Grok 后端说同一种 Responses 协议, 因此复用这三个子模块, 两个后端的请求体差异与
//! 推理桥类型标识由 crate::xai::XAI_DIALECT 给出; 发往 Grok 的请求由 crate::xai::build_request
//! 构造.
//!
//! The Grok backend of the xai line speaks the same Responses protocol, so it reuses these three
//! submodules, with the request body differences and the reasoning bridge type marker given by
//! crate::xai::XAI_DIALECT; requests to Grok are built by crate::xai::build_request.
//!
//! 请求头按小写写入. Go 版用 setRawHeader 绕过 http.Header 的首字母大写规范化, 保留改写前的
//! Python 实现发往上游的 originator, chatgpt-account-id 与 session_id 全小写形态; Rust 的 http
//! 头名本身就规范化为小写, 因此发出的形态与 Go 版一致. HTTP 头名不区分大小写, 这一差异对上游
//! 没有影响.
//!
//! Request headers are written in lowercase. Go used setRawHeader to bypass http.Header's
//! capitalization and keep the lowercase originator, chatgpt-account-id and session_id the Python
//! implementation sent; Rust's header names are normalized to lowercase anyway, so the wire form
//! matches Go's. HTTP header names are case-insensitive, so this difference has no effect on
//! upstream.

pub mod request;
pub mod response;
pub mod signatures;

use http::HeaderMap;
use url::Url;

use crate::upstream::{BuildError, UpstreamRequest, insert_header};

/// 固定标识. 上游按 originator 判断请求来自官方 Codex 客户端, 取值需要与官方客户端一致.
///
/// 版本号不再写死在这里, 而是由 3.8 的标识配置渲染出 User-Agent, 见 crate::client_id.
///
/// Fixed identifier. Upstream uses originator to decide that a request comes from the official
/// Codex client, so the value must match the official client.
///
/// The version is no longer hard-coded here; the 3.8 identifier configuration renders the
/// User-Agent, see crate::client_id.
pub const CHATGPT_ORIGINATOR: &str = "codex_cli_rs";

/// 拼接在上游前缀之后的路径.
///
/// The path appended after the upstream prefix.
pub const CHATGPT_SUFFIX: &str = "/responses";

/// 本线路接受的 POST 路径, 与改写前的 Python 实现的 POST_PATHS 一致.
///
/// Aria 的入口按路径比较并丢弃查询串, 因此带查询串的请求也会命中; Go 版按含查询串的原始请求
/// 目标比较, 那种情况下不命中. 这一差异属于 architecture.md 第 5.1 节记录的路径匹配规则.
///
/// The POST paths this route accepts, matching POST_PATHS in the Python implementation before the
/// rewrite. Aria's entry point compares by path and drops the query string, so a request with a
/// query string also matches; Go compared the raw request target including the query string and
/// did not match then. That difference is the path matching rule recorded in section 5.1 of
/// architecture.md.
pub const CHATGPT_POST_PATHS: [&str; 3] = [
    "/v1/responses",
    "/responses",
    "/backend-api/codex/responses",
];

/// 触发换号的响应码. 改写前的 Python 实现的 ROTATE_STATUSES 还包含 403; Aria 改为冷却收到
/// 403 的账号且不换号, 见 architecture.md 5.11.
///
/// 这些状态描述的是某一个账号, 换一个账号可能成功, 因此不等待就换下一个. 400 与 500 不在其中:
/// 400 是请求本身的问题, 换账号不会改变结果.
///
/// The response codes that trigger rotation. ROTATE_STATUSES in the Python implementation before
/// the rewrite also held 403; Aria instead cools down the account that received a 403 without
/// rotating, see section 5.11 of architecture.md. These statuses describe one account and another
/// account may succeed, so the relay rotates without waiting. 400 and 500 are not included: a 400
/// is a problem in the request itself and another account would not change the result.
pub const CHATGPT_ROTATE_STATUSES: [u16; 3] = [401, 402, 429];

/// 需要原样透传的客户端头, 用于保持多轮会话标识等语义.
///
/// Client headers passed through unchanged, preserving semantics such as the multi-turn session
/// identifier.
pub const CHATGPT_PASSTHROUGH_HEADERS: [&str; 3] = ["session_id", "conversation_id", "openai-beta"];

/// 默认模型列表, 与改写前的 Python 实现的 MODEL_IDS 一致.
///
/// 只用于配置缺省时的 /v1/models 展示; 实际使用哪个模型由客户端请求决定. Aria 的模型列表以
/// 配置为准, 这份列表是 providers.chatgpt.models 的默认值.
///
/// The default model list, matching MODEL_IDS in the Python implementation before the rewrite. It
/// is used only for the /v1/models listing when the configuration omits the list; which model is
/// actually used is decided by the client request. Aria takes the model list from configuration,
/// and this list is the default for providers.chatgpt.models.
pub const CHATGPT_MODELS: [&str; 8] = [
    "gpt-6-astra",
    "gpt-6-luna",
    "gpt-6-sol",
    "gpt-5.6-luna",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.5",
    "gpt-reserve",
];

/// 构造一次上游请求. 请求正文原样透传, 不做任何改写.
///
/// 头部取值顺序与 Go 版一致: 先写认证头, 再写固定标识, 然后写 Content-Type 与 Accept, 最后写
/// 客户端透传头. 同名头以后写的为准, 因此顺序会决定冲突时的结果. base 是上游前缀, 末尾斜杠在
/// 这里去掉. user_agent 由调用方按 3.8 的标识配置渲染, 取值在构造请求时确定, 一次获取失败不会
/// 改变已经发出去的请求.
///
/// Builds one upstream request. The body is passed through unchanged. The header order matches Go:
/// authentication headers, fixed identifiers, Content-Type and Accept, then the client headers.
/// A later header of the same name wins, so the order decides conflicts. base is the upstream
/// prefix and a trailing slash is trimmed here. user_agent is rendered by the caller from the 3.8
/// identifier configuration; its value is fixed when the request is built, so one failed fetch
/// does not change a request already sent.
pub fn build_request(
    base: &str,
    auth_headers: &[(&str, &str)],
    inbound_headers: &HeaderMap,
    body: &[u8],
    user_agent: &str,
) -> Result<UpstreamRequest, BuildError> {
    let base = base.strip_suffix('/').unwrap_or(base);
    let url =
        Url::parse(&format!("{base}{CHATGPT_SUFFIX}")).map_err(|_| BuildError::InvalidUpstream)?;
    let mut headers = HeaderMap::new();
    for (name, value) in auth_headers {
        insert_header(&mut headers, name, value)?;
    }
    insert_header(&mut headers, "originator", CHATGPT_ORIGINATOR)?;
    insert_header(&mut headers, "User-Agent", user_agent)?;
    let content_type = header_text(inbound_headers, "Content-Type")
        .unwrap_or_else(|| "application/json".to_owned());
    insert_header(&mut headers, "Content-Type", &content_type)?;
    let accept =
        header_text(inbound_headers, "Accept").unwrap_or_else(|| "text/event-stream".to_owned());
    insert_header(&mut headers, "Accept", &accept)?;
    for name in CHATGPT_PASSTHROUGH_HEADERS {
        if let Some(value) = header_text(inbound_headers, name) {
            insert_header(&mut headers, name, &value)?;
        }
    }
    Ok(UpstreamRequest {
        url,
        headers,
        body: body.to_vec(),
    })
}

/// 读取一个客户端头. 缺失, 空值与无法按文本读取都视为没有取值, 与 Go 的 Header.Get 加空串判断
/// 一致.
///
/// Reads one client header. A missing header, an empty value and a value that cannot be read as
/// text all count as absent, matching Go's Header.Get plus the empty-string check.
fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?;
    if value.is_empty() {
        return None;
    }
    Some(value.to_owned())
}
