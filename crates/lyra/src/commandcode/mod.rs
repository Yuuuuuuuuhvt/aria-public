//! commandcode 提供方: Command Code Responses 接口与 Provider API 的请求构造.
//!
//! api.commandcode.ai/provider/v1 是 Command Code 提供的 Provider API, 说 OpenAI Responses 协议,
//! 事件流与 Codex 类似但思考事件形态不同, 请求体携带 max_output_tokens 与 tool_choice, 因此
//! Messages 与 Responses 之间的转换复用 crate::chatgpt 的子模块并按 COMMANDCODE_DIALECT 区分.
//! 本模块负责上游基址常量, 默认模型清单, 请求构造以及单密钥凭据来源.
//!
//! The commandcode provider: request construction for Command Code's Responses endpoint and
//! Provider API.
//!
//! api.commandcode.ai/provider/v1 is Command Code's Provider API speaking the OpenAI Responses
//! protocol. Its event stream resembles Codex but uses distinct reasoning event shapes, and its
//! request body takes max_output_tokens and tool_choice without tools. Conversion between Messages
//! and Responses reuses the crate::chatgpt submodules with COMMANDCODE_DIALECT. This module owns
//! the upstream base URL constant, default model listing, request building and the static-key
//! credential source.

use http::HeaderMap;
use serde_json::{Map, Value, json};
use url::Url;

use crate::chatgpt::request::ResponsesDialect;
use crate::chatgpt::signatures::COMMANDCODE_REASONING_BRIDGE_TYPE;
use crate::credential::{AuthHeaders, Entry, Error, Status, bearer_headers_of};
use crate::upstream::{BuildError, UpstreamRequest, insert_header};

/// commandcode 线路的请求体差异: 使用专用推理桥标识, 没有工具时仍写出 tool_choice,
/// 并且将客户端的 max_tokens 写出为 max_output_tokens.
///
/// The request body differences of the commandcode line: its own reasoning bridge type marker,
/// tool_choice emitted even without tools, and client max_tokens written as max_output_tokens.
pub const COMMANDCODE_DIALECT: ResponsesDialect = ResponsesDialect {
    bridge_type: COMMANDCODE_REASONING_BRIDGE_TYPE,
    tool_choice_without_tools: true,
    emit_max_output_tokens: true,
};

/// 默认基址.
///
/// The default base address.
pub const COMMANDCODE_BASE_URL: &str = "https://api.commandcode.ai/provider/v1";

/// 拼接在基址之后的路径后缀.
///
/// The path suffix appended to the base address.
pub const COMMANDCODE_SUFFIX: &str = "/responses";

/// 默认模型清单, 第一批只启用 GLM-5.3.
///
/// The default model listing, enabling only GLM-5.3 in the first batch.
pub const COMMANDCODE_MODELS: [&str; 1] = ["zai-org/GLM-5.3"];

/// 固定 User-Agent 取值, 不做客户端版本跟进.
///
/// The fixed User-Agent value without client version tracking.
pub const COMMANDCODE_USER_AGENT: &str = "aria";

/// 构造一次发往 Command Code Provider API 的请求. body 是已经转换好的 Responses 请求体.
///
/// 请求头只写 Authorization, User-Agent, Content-Type: application/json 与
/// Accept: text/event-stream; User-Agent 取固定值, 不发送任何客户端指纹头.
///
/// Builds one request to the Command Code Provider API. body is the already converted Responses body.
///
/// Headers contain only Authorization, User-Agent, Content-Type: application/json and
/// Accept: text/event-stream; User-Agent uses a fixed value without client fingerprint headers.
pub fn build_request(
    base: &str,
    auth_headers: &[(&str, &str)],
    body: &[u8],
) -> Result<UpstreamRequest, BuildError> {
    let base = base.strip_suffix('/').unwrap_or(base);
    let url = Url::parse(&format!("{base}{COMMANDCODE_SUFFIX}"))
        .map_err(|_| BuildError::InvalidUpstream)?;
    let mut headers = HeaderMap::new();
    for (name, value) in auth_headers {
        insert_header(&mut headers, name, value)?;
    }
    insert_header(&mut headers, "User-Agent", COMMANDCODE_USER_AGENT)?;
    insert_header(&mut headers, "Content-Type", "application/json")?;
    insert_header(&mut headers, "Accept", "text/event-stream")?;
    Ok(UpstreamRequest {
        url,
        headers,
        body: body.to_vec(),
    })
}

/// Command Code 线路的固定密钥凭据来源.
///
/// 该线路只有一把固定 API 密钥, 不是账号池, 不需要刷新与换号. 提供单个启用的凭据条目,
/// 在健康检查中可见且不暴露密钥.
///
/// The static key credential source of the Command Code line.
///
/// The line operates with a single static API key rather than an account pool, needing no
/// refresh or rotation. It provides a single enabled credential entry visible in health checks
/// without exposing the key material.
#[derive(Clone)]
pub struct CommandCodeSource {
    api_key: String,
}

/// 调试输出不含密钥, 防止密钥经日志或错误信息泄露.
///
/// The debug output omits the key, so it cannot leak through logs or error messages.
impl std::fmt::Debug for CommandCodeSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandCodeSource")
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl CommandCodeSource {
    /// 用给定的 API 密钥构造凭据来源.
    ///
    /// Builds a credential source with the given API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
        }
    }

    /// 返回包含单个条目的凭据快照.
    ///
    /// Returns a credential snapshot containing the single entry.
    pub async fn snapshot(&self) -> Result<Vec<Entry>, Error> {
        let mut entry = Map::new();
        entry.insert("name".to_owned(), json!("commandcode"));
        entry.insert("access_token".to_owned(), json!(self.api_key));
        entry.insert("enabled".to_owned(), json!(true));
        entry.insert("priority".to_owned(), json!(0));
        Ok(vec![entry])
    }

    /// 生成鉴权请求头.
    ///
    /// Generates authentication headers.
    pub async fn authorize(
        &self,
        entry: Entry,
        _force_refresh: bool,
    ) -> Result<AuthHeaders, Error> {
        bearer_headers_of(&entry)
    }

    /// 返回公开健康状态, 不暴露密钥.
    ///
    /// Returns public health status without exposing secret keys.
    pub async fn status(&self) -> Result<Vec<Status>, Error> {
        let status = Status {
            name: "commandcode".to_owned(),
            account_id_prefix: String::new(),
            plan: Value::Null,
            enabled: true,
            priority: 0,
            note: Value::Null,
            expires_at: 0,
            expires_in_seconds: 0,
        };
        Ok(vec![status])
    }
}

impl crate::credential::Source for CommandCodeSource {
    async fn snapshot(&self) -> Result<Vec<Entry>, Error> {
        self.snapshot().await
    }

    async fn authorize(&self, entry: Entry, force_refresh: bool) -> Result<AuthHeaders, Error> {
        self.authorize(entry, force_refresh).await
    }

    async fn status(&self) -> Result<Vec<Status>, Error> {
        self.status().await
    }
}
