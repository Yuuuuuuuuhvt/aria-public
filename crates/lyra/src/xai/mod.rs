//! xai 提供方: SuperGrok 订阅经官方 Grok CLI 渠道的 Responses 请求构造.
//!
//! cli-chat-proxy.grok.com 是官方 Grok CLI 使用的订阅后端, 说 OpenAI Responses 协议, 事件流与
//! Codex 后端逐字段一致, 请求体只有 XAI_DIALECT 列出的差异, 因此 Messages 与 Responses 之间的
//! 转换复用 crate::chatgpt 的子模块. 本模块负责与 Codex 不同的部分: OAuth 客户端参数, 上游地址,
//! 四个客户端指纹头与请求体差异. 账号池与轮换由 credential 与 relay 模块承担.
//!
//! 协议事实取自对真实订阅后端的一次探针, 默认基址与回调形态另外核对过官方 grok CLI
//! 1.0.50 二进制中的默认取值.
//!
//! The xai provider: request construction for SuperGrok subscriptions through the official Grok
//! CLI channel.
//!
//! cli-chat-proxy.grok.com is the subscription backend of the official Grok CLI. It speaks the
//! OpenAI Responses protocol with event streams matching the Codex backend field by field and
//! request bodies differing only as XAI_DIALECT lists, so the conversion between Messages and
//! Responses reuses the crate::chatgpt submodules. This module owns what differs from Codex: the
//! OAuth client parameters, the upstream address, the four client fingerprint headers and the
//! request body differences. The account pool and rotation belong to the credential and relay
//! modules.
//!
//! The protocol facts come from a probe of the real subscription backend; the default base address
//! and the callback form were also checked against the defaults inside the official grok CLI 1.0.50
//! binary.

use http::HeaderMap;
use url::Url;

use crate::chatgpt::request::ResponsesDialect;
use crate::chatgpt::signatures::XAI_REASONING_BRIDGE_TYPE;
use crate::upstream::{BuildError, UpstreamRequest, insert_header};

/// xai 线路的请求体差异: 使用自己的推理桥类型标识, 请求没有工具时省略 tool_choice, 原因见
/// ResponsesDialect 的字段说明.
///
/// The request body differences of the xai line: its own reasoning bridge type marker, and no
/// tool_choice when the request has no tools; see the ResponsesDialect fields for the reasons.
pub const XAI_DIALECT: ResponsesDialect = ResponsesDialect {
    bridge_type: XAI_REASONING_BRIDGE_TYPE,
    tool_choice_without_tools: false,
    emit_max_output_tokens: false,
};

/// 官方 Grok CLI 的公开 OAuth 客户端标识. 登录与刷新都必须使用它, 刷新令牌绑定在签发它的客户端
/// 上, 换用其他取值会被授权服务拒绝.
///
/// The public OAuth client identifier of the official Grok CLI. Both login and refresh must use
/// it: a refresh token is bound to the client it was issued to, and any other value is rejected by
/// the authorization service.
pub const XAI_OAUTH_CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";

/// 登录与刷新申请的授权范围. 官方 CLI 还申请 conversations 与 workspaces 的读写权限; 网关只调用
/// 推理接口, 因此业务权限只取 grok-cli:access 与 api:access, 另加 OIDC 身份与离线刷新需要的四项.
///
/// The scopes requested at login and refresh. The official CLI also asks for read and write
/// access to conversations and workspaces; the gateway only calls the inference endpoint, so the
/// business scopes are limited to grok-cli:access and api:access, plus the four needed for the
/// OIDC identity and offline refresh.
pub const XAI_OAUTH_SCOPE: &str = "openid profile email offline_access grok-cli:access api:access";

/// 令牌交换与刷新共用的端点.
///
/// The endpoint shared by the code exchange and the refresh.
pub const XAI_TOKEN_URL: &str = "https://auth.x.ai/oauth2/token";

/// 订阅推理后端的默认基址, 与官方 CLI 内置的 GROK_CLI_CHAT_PROXY_BASE_URL 默认值相同.
///
/// api.x.ai 不能作为默认值: 它是按量计费的余额池, 同一访问令牌在那里返回
/// personal-team-blocked:spending-limit, 订阅额度只在 cli-chat-proxy.grok.com 上可用.
///
/// The default base address of the subscription inference backend, equal to the default of
/// GROK_CLI_CHAT_PROXY_BASE_URL built into the official CLI.
///
/// api.x.ai must not be the default: it is the pay-as-you-go balance pool, the same access token
/// gets personal-team-blocked:spending-limit there, and the subscription quota is only available
/// on cli-chat-proxy.grok.com.
pub const XAI_BASE_URL: &str = "https://cli-chat-proxy.grok.com/v1";

/// 拼接在基址之后的路径.
///
/// The path appended after the base address.
pub const XAI_SUFFIX: &str = "/responses";

/// 触发换号的响应码, 与 chatgpt 线路相同: 收到 403 的账号进入冷却且不换号, 见 architecture.md
/// 5.11.
///
/// The response codes that trigger rotation, the same as on the chatgpt line: an account that
/// receives a 403 cools down without rotating, see section 5.11 of architecture.md.
pub const XAI_ROTATE_STATUSES: [u16; 3] = [401, 402, 429];

/// 提前刷新的余量.
///
/// 访问令牌的有效期约 6 小时, 令牌响应的 expires_in 为 21600. 沿用 ChatGPT 线路的 24 小时余量会
/// 让令牌永远不够新鲜, 每个请求都会刷新一次并轮换刷新令牌. 这里取 3600 秒, 与令牌分发索取端的
/// 余量相同: 令牌大约每 5 小时刷新一次, 剩下的一小时足以覆盖时钟偏差与长时间的流式请求.
///
/// The early-refresh margin.
///
/// An access token lives for about 6 hours; the token response carries expires_in 21600. Reusing
/// the 24-hour margin of the ChatGPT line would keep the token permanently stale, so every request
/// would refresh it and rotate the refresh token. 3600 seconds are used instead, the same margin
/// as the token broker client: the token is refreshed roughly every 5 hours, and the remaining
/// hour covers clock skew and long streaming requests.
pub const XAI_REFRESH_MARGIN_SECONDS: i64 = 3600;

/// X-XAI-Token-Auth 请求头的取值. 推理端点据此把 Bearer 令牌当作 CLI 令牌验证; 探针中写成
/// grok-cli 时返回 no auth context.
///
/// The value of the X-XAI-Token-Auth header. The inference endpoint uses it to validate the Bearer
/// token as a CLI token; in the probe the value grok-cli was answered with no auth context.
pub const XAI_TOKEN_AUTH_VALUE: &str = "xai-grok-cli";

/// x-grok-client-identifier 请求头的取值, 与官方 CLI 的终端客户端一致.
///
/// The value of the x-grok-client-identifier header, matching the official CLI's terminal client.
pub const XAI_CLIENT_IDENTIFIER: &str = "grok-shell";

/// 默认模型列表, 取自订阅账号在 cli-chat-proxy.grok.com/v1/models 上的实测清单. 只用于配置缺省时
/// 的 /v1/models 展示, 是 providers.xai.models 的默认值.
///
/// The default model list, taken from the subscription account's measured listing at
/// cli-chat-proxy.grok.com/v1/models. It is only used for the /v1/models listing when the
/// configuration omits the list and is the default of providers.xai.models.
pub const XAI_MODELS: [&str; 4] = ["grok-4.7", "grok-4.7-build-fast", "grok-4.6", "grok-4.5"];

/// 构造一次发往 Grok 订阅后端的请求. body 是已经转换好的 Responses 请求体, 原样发送.
///
/// 请求头依次写入鉴权头, User-Agent, x-grok-client-version, x-grok-client-identifier,
/// X-XAI-Token-Auth, Content-Type 与 Accept; 缺少任何一个指纹头时推理端点返回 "Your Grok CLI
/// version (none) is outdated". 与 chatgpt 线路有三处不同:
///
/// 1. 不写 originator, 也不透传 session_id 一类客户端头. 它们是 Codex 的标识, 官方 Grok CLI 不
///    发送; 会话缓存键已经由请求转换写进请求体的 prompt_cache_key.
/// 2. Content-Type 与 Accept 是固定值, 不取客户端请求头. 本线路只服务 Messages 转换, 客户端的
///    Accept 描述的是它期望的 Anthropic 响应, 例如 Anthropic SDK 默认的 application/json, 而发往
///    上游的请求体恒为 stream 为 true 的 JSON, 上游必须按事件流应答.
/// 3. user_agent 与 client_version 由调用方从同一份标识快照渲染, 保证两个头给出同一个版本.
///
/// base 是上游前缀, 末尾斜杠在这里去掉.
///
/// Builds one request to the Grok subscription backend. body is the already converted Responses
/// body and is sent unchanged.
///
/// Headers are written in this order: the auth headers, User-Agent, x-grok-client-version,
/// x-grok-client-identifier, X-XAI-Token-Auth, Content-Type and Accept; without any one of the
/// fingerprint headers the inference endpoint answers "Your Grok CLI version (none) is outdated".
/// Three points differ from the chatgpt line:
///
/// 1. No originator is written and no session_id-like client header is passed through. Those
///    identify Codex and the official Grok CLI never sends them; the session cache key is already
///    carried in the body's prompt_cache_key by the request conversion.
/// 2. Content-Type and Accept are fixed values rather than client headers. This line only serves
///    the Messages conversion, where the client's Accept describes the Anthropic response it
///    expects, such as the Anthropic SDK's default application/json, while the upstream body is
///    always JSON with stream set to true and upstream must answer with an event stream.
/// 3. user_agent and client_version are rendered by the caller from one identifier snapshot, so
///    both headers carry the same version.
///
/// base is the upstream prefix and a trailing slash is trimmed here.
pub fn build_request(
    base: &str,
    auth_headers: &[(&str, &str)],
    body: &[u8],
    user_agent: &str,
    client_version: &str,
) -> Result<UpstreamRequest, BuildError> {
    let base = base.strip_suffix('/').unwrap_or(base);
    let url =
        Url::parse(&format!("{base}{XAI_SUFFIX}")).map_err(|_| BuildError::InvalidUpstream)?;
    let mut headers = HeaderMap::new();
    for (name, value) in auth_headers {
        insert_header(&mut headers, name, value)?;
    }
    insert_header(&mut headers, "User-Agent", user_agent)?;
    insert_header(&mut headers, "x-grok-client-version", client_version)?;
    insert_header(
        &mut headers,
        "x-grok-client-identifier",
        XAI_CLIENT_IDENTIFIER,
    )?;
    insert_header(&mut headers, "X-XAI-Token-Auth", XAI_TOKEN_AUTH_VALUE)?;
    insert_header(&mut headers, "Content-Type", "application/json")?;
    insert_header(&mut headers, "Accept", "text/event-stream")?;
    Ok(UpstreamRequest {
        url,
        headers,
        body: body.to_vec(),
    })
}
