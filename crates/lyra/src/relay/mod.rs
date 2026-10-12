//! 订阅账号线路的请求处理: 账号选择, 401 刷新重试, 503 容量重试预算, Responses 透传,
//! Antigravity 转换转发与用量观测. 接口见 docs/architecture.md 9.2.
//!
//! 入口按线路分派: messages 走 Google 账号池与协议转换, responses 走 ChatGPT 账号池的透传,
//! chatgpt_messages 与 xai_messages 把 Messages 请求转换为 Responses 协议, 分别走 ChatGPT 与
//! SuperGrok 账号池. 两类客户端看到的错误格式不同, 因此错误报文的构造也分开: Messages 使用
//! Anthropic 格式, Responses 沿用 Go 版 Lyra 的 OpenAI 风格.
//!
//! 与 Go 版的账号选择, 重试与日志格式逐项对齐; 差异只有 architecture.md 第 7 节列出的
//! 那几项, 以及完成日志新增的 route 字段. 访问日志由调用方写, 本模块只写 Go 版 relay 包
//! 负责的账号日志与完成日志.
//!
//! Request handling for subscription-backed routes: account selection, the 401 refresh retry,
//! the 503 capacity retry budget, Responses passthrough, Antigravity translated forwarding and
//! usage observation. See docs/architecture.md 9.2.
//!
//! The entry points dispatch by route: messages uses the Google account pool and protocol
//! conversion, responses passes through the ChatGPT account pool, and chatgpt_messages and
//! xai_messages convert Messages requests into the Responses protocol on the ChatGPT and
//! SuperGrok account pools. The two clients see different error formats, so error encoding is
//! separate as well: Messages uses the Anthropic format while Responses keeps the OpenAI style of
//! Go Lyra.
//!
//! Account selection, retries and log formats align item by item with Go; the only differences
//! are those listed in section 7 of architecture.md plus the route field added to the completion
//! log. Access logs are written by the caller; this module writes only the account logs and
//! completion logs that the Go relay package owns.

mod attempt;
mod cooldown;
mod forward;
mod quota;
mod translate;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aria_core::body::{Body, BoxError, empty, full};
use aria_core::errors::openai_error;
use aria_core::usage::Observation;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use http_body::{Body as HttpBody, Frame};
use http_body_util::{BodyExt, StreamBody};

use crate::antigravity::provider::{session_key, translator_for};
use crate::chatgpt::request::{CHATGPT_DIALECT, ResponsesDialect};
use crate::client_id::ClientIdentity;
use crate::credential::google::GOOGLE_TOKEN_URL;
use crate::credential::{BrokerSource, GoogleSource, Pool, Status, TOKEN_URL};
use crate::xai::{XAI_DIALECT, XAI_REFRESH_MARGIN_SECONDS};
use attempt::{
    AttemptError, AttemptFailure, AttemptResponse, CapacityRetryGuard, Inbound, Route,
    select_credential,
};
use cooldown::{Cooldown, ExhaustedModels, TimeoutCooldownTracker, UnavailableModels};
use forward::{
    BodyEnd, Completion, CompletionLog, FinalBody, PassthroughBody, anthropic_error_payload,
};
use serde::Serialize;
use translate::{RelayTranslator, TranslatedBody, TranslatedFailure, TranslatedState};

/// 搜索轮次响应体的读取上限, 与 canto 侧 readSearchResponse 的 4 MiB 相同.
///
/// The read limit of a search round response body, matching the 4 MiB of readSearchResponse on
/// the canto side.
const MAX_SEARCH_RESPONSE_BYTES: usize = 4 << 20;

/// Antigravity 提供方的配置.
///
/// The configuration of the Antigravity provider.
pub struct AntigravityConfig {
    pub credentials: PathBuf,
    pub base_url: String,
    pub client_id: String,
    pub client_secret: String,
    /// 共享的 Antigravity 客户端标识, 由 3.8 的后台任务更新.
    ///
    /// The shared Antigravity client identifier, updated by the 3.8 background task.
    pub user_agent: Arc<ClientIdentity>,
    /// 连续首字节超时触发冷却的次数上限.
    pub timeout_cooldown_threshold: u32,
    /// 连续首字节超时冷却时长.
    pub timeout_cooldown_duration: Duration,
}

/// ChatGPT 凭据来源配置.
///
/// Credential source configuration for ChatGPT.
pub enum ChatGptSourceConfig {
    /// 本地凭据文件.
    ///
    /// Local credential file.
    Credentials(PathBuf),
    /// 令牌分发来源.
    ///
    /// Token broker source.
    Broker(BrokerConfig),
}

/// 令牌分发配置.
///
/// Token broker configuration.
pub struct BrokerConfig {
    pub url: String,
    pub key: String,
}

/// ChatGPT 提供方的配置.
///
/// The configuration of the ChatGPT provider.
pub struct ChatGptConfig {
    pub source: ChatGptSourceConfig,
    pub base_url: String,
    pub models: Vec<String>,
    /// 共享的 Codex 客户端标识, 由 3.8 的后台任务更新.
    ///
    /// The shared Codex client identifier, updated by the 3.8 background task.
    pub user_agent: Arc<ClientIdentity>,
}

/// xai 提供方的配置. 凭据文件格式与 ChatGPT 账号池相同; 本实例持有 refresh token 并负责刷新,
/// 刷新使用这里的令牌端点, client_id 与 scope.
///
/// The configuration of the xai provider. The credential file has the ChatGPT pool's format; this
/// instance holds the refresh token and refreshes it with the token endpoint, client_id and scope
/// given here.
pub struct XaiConfig {
    pub credentials: PathBuf,
    pub base_url: String,
    pub token_url: String,
    pub client_id: String,
    pub scope: String,
    pub models: Vec<String>,
    /// 共享的 Grok CLI 客户端标识, 由 3.8 的后台任务更新.
    ///
    /// The shared Grok CLI client identifier, updated by the 3.8 background task.
    pub user_agent: Arc<ClientIdentity>,
}

/// Command Code 提供方的配置. 该线路只有一把固定 API 密钥, 使用固定的 Provider API 端点.
///
/// The configuration of the Command Code provider. The line operates with a single static
/// API key against the fixed Provider API endpoint.
pub struct CommandCodeConfig {
    pub base_url: String,
    pub api_key: String,
    pub models: Vec<String>,
}

/// 中继的完整配置.
///
/// The complete relay configuration.
pub struct RelayConfig {
    pub antigravity: Option<AntigravityConfig>,
    pub chatgpt: Option<ChatGptConfig>,
    pub xai: Option<XaiConfig>,
    pub commandcode: Option<CommandCodeConfig>,
    /// 收到 503 后继续等待容量的时间上限.
    ///
    /// The upper bound of waiting for capacity after a 503.
    pub capacity_budget: Duration,
    /// 等待上游响应头与首个有效事件的首字节超时.
    ///
    /// The timeout for waiting for upstream response headers and the first valid event.
    pub first_byte_timeout: Duration,
}

/// 一次缓冲转发的结果, 供搜索轮次使用.
///
/// The result of one buffered forward, used by the search rounds.
pub struct BufferedResponse {
    pub status: StatusCode,
    pub retry_after: Option<String>,
    pub body: Bytes,
}

/// 中继.
///
/// The relay.
pub struct Relay {
    antigravity: Option<AntigravityRoute>,
    chatgpt: Option<ChatGptRoute>,
    xai: Option<XaiRoute>,
    commandcode: Option<CommandCodeRoute>,
    client: reqwest::Client,
    capacity_budget: Duration,
    first_byte_timeout: Duration,
    chatgpt_models: Vec<String>,
    xai_models: Vec<String>,
    commandcode_models: Vec<String>,
}

struct AntigravityRoute {
    source: GoogleSource,
    base_url: String,
    cooldown: Cooldown,
    user_agent: Arc<ClientIdentity>,
    unavailable: UnavailableModels,
    exhausted: ExhaustedModels,
    timeout_tracker: TimeoutCooldownTracker,
}

struct ChatGptRoute {
    source: ChatGptSource,
    base_url: String,
    cooldown: Cooldown,
    user_agent: Arc<ClientIdentity>,
}

struct XaiRoute {
    source: Pool,
    base_url: String,
    cooldown: Cooldown,
    user_agent: Arc<ClientIdentity>,
}

struct CommandCodeRoute {
    source: crate::commandcode::CommandCodeSource,
    base_url: String,
}

/// 健康检查中的一条冷却记录, 见 architecture.md 5.11.
///
/// One cooldown record in the health check; see section 5.11 of architecture.md.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CooldownStatus {
    pub provider: &'static str,
    pub name: String,
    pub remaining_seconds: u64,
}

/// 健康检查中的一条不可用模型记录, 见 architecture.md 5.12 与 5.13.
///
/// One unavailable model record in the health check; see sections 5.12 and 5.13 of
/// architecture.md.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnavailableModelStatus {
    pub provider: &'static str,
    pub name: String,
    pub model: String,
    /// 不可用原因: model_not_found, quota_exhausted 或 capacity_exhausted.
    ///
    /// The reason: model_not_found, quota_exhausted or capacity_exhausted.
    pub reason: &'static str,
    pub remaining_seconds: u64,
}

pub(crate) enum ChatGptSource {
    Pool(Pool),
    Broker(BrokerSource),
}

/// 一次 Responses 转换请求的目标: 日志用的路由名, 上游模型, 默认思考强度与该线路的请求体差异.
/// chatgpt 与 xai 两条线路共用.
///
/// The target of one Responses conversion request: the route name for logs, the upstream model,
/// the default reasoning effort and the line's request body differences. Shared by the chatgpt
/// and xai lines.
struct ChatGptTarget<'a> {
    route: &'a str,
    model: &'a str,
    effort: &'a str,
    dialect: &'static ResponsesDialect,
}

impl Relay {
    /// 构造中继. client 用于上游请求, refresh_client 用于令牌刷新与项目标识推导.
    ///
    /// 凭据文件在每次取号时读取, 构造阶段不触碰磁盘: 文件缺失或损坏在第一个请求上暴露,
    /// 与 Go 版一致.
    ///
    /// Builds the relay. client serves upstream requests and refresh_client serves token
    /// refreshes and project id discovery.
    ///
    /// Credential files are read on every selection and are not touched while constructing: a
    /// missing or corrupt file surfaces on the first request, matching Go.
    pub fn new(
        config: RelayConfig,
        client: reqwest::Client,
        refresh_client: reqwest::Client,
    ) -> Result<Relay, String> {
        Self::with_token_endpoints(config, client, refresh_client, TOKEN_URL, GOOGLE_TOKEN_URL)
    }

    /// 用指定的令牌端点构造中继. 生产入口使用默认端点, 测试据此把刷新请求指向本机桩服务,
    /// 与 GoogleSource::new 把 token_url 作为参数的理由相同: 刷新路径必须能被真实覆盖,
    /// 而不是用替身跳过请求构造与响应解析.
    ///
    /// xai 线路的令牌端点属于 providers.xai 的配置项, 已经由 XaiConfig.token_url 携带, 测试直接
    /// 在配置中指向桩服务, 因此这里不再为它增加参数, 避免同一取值出现两个来源.
    ///
    /// Builds the relay with the given token endpoints. The production entry point uses the
    /// defaults, while tests point refresh requests at a local stub for the same reason
    /// GoogleSource::new takes token_url as a parameter: the refresh path must be really covered
    /// rather than skipped behind a stand-in.
    ///
    /// The token endpoint of the xai line is a providers.xai option already carried by
    /// XaiConfig.token_url, and tests point it at a stub through the configuration, so no
    /// parameter is added here and the value never has two sources.
    pub(crate) fn with_token_endpoints(
        config: RelayConfig,
        client: reqwest::Client,
        refresh_client: reqwest::Client,
        chatgpt_token_url: &str,
        google_token_url: &str,
    ) -> Result<Relay, String> {
        // 标识在两条线路间共享: 生成请求, 项目标识推导与登录命令都从同一份状态读版本, 因此
        // 同一时刻整条线路只有一个 User-Agent 取值.
        //
        // The identifier is shared across one line: generation requests, project id discovery
        // and the login command all read the version from the same state, so a line has exactly
        // one User-Agent value at any moment.
        let antigravity = config.antigravity.map(|route| AntigravityRoute {
            source: GoogleSource::new(
                route.credentials,
                route.client_id,
                route.client_secret,
                route.base_url.clone(),
                google_token_url,
                refresh_client.clone(),
                route.user_agent.clone(),
            ),
            base_url: route.base_url,
            cooldown: Cooldown::default(),
            user_agent: route.user_agent,
            unavailable: UnavailableModels::default(),
            exhausted: ExhaustedModels::default(),
            timeout_tracker: TimeoutCooldownTracker::new(
                route.timeout_cooldown_threshold,
                route.timeout_cooldown_duration,
            ),
        });
        let (xai, xai_models) = match config.xai {
            Some(provider) => (
                Some(XaiRoute {
                    source: Pool::with_oauth(
                        provider.credentials,
                        provider.token_url,
                        refresh_client.clone(),
                        provider.client_id,
                        provider.scope,
                        XAI_REFRESH_MARGIN_SECONDS,
                    ),
                    base_url: provider.base_url,
                    cooldown: Cooldown::default(),
                    user_agent: provider.user_agent,
                }),
                provider.models,
            ),
            None => (None, Vec::new()),
        };
        let (chatgpt, chatgpt_models) = match config.chatgpt {
            Some(provider) => {
                let source = match provider.source {
                    ChatGptSourceConfig::Credentials(credentials) => ChatGptSource::Pool(
                        Pool::new(credentials, chatgpt_token_url, refresh_client),
                    ),
                    ChatGptSourceConfig::Broker(broker) => ChatGptSource::Broker(
                        BrokerSource::new(broker.url, broker.key, refresh_client),
                    ),
                };
                (
                    Some(ChatGptRoute {
                        source,
                        base_url: provider.base_url,
                        cooldown: Cooldown::default(),
                        user_agent: provider.user_agent,
                    }),
                    provider.models,
                )
            }
            None => (None, Vec::new()),
        };
        let (commandcode, commandcode_models) = match config.commandcode {
            Some(provider) => (
                Some(CommandCodeRoute {
                    source: crate::commandcode::CommandCodeSource::new(provider.api_key),
                    base_url: provider.base_url,
                }),
                provider.models,
            ),
            None => (None, Vec::new()),
        };
        Ok(Relay {
            antigravity,
            chatgpt,
            xai,
            commandcode,
            client,
            capacity_budget: config.capacity_budget,
            first_byte_timeout: config.first_byte_timeout,
            chatgpt_models,
            xai_models,
            commandcode_models,
        })
    }

    /// 是否配置了 Google 账号池.
    ///
    /// Whether the Google account pool is configured.
    pub fn has_antigravity(&self) -> bool {
        self.antigravity.is_some()
    }

    /// 是否配置了 ChatGPT 账号池. 配置了提供方即可透传, 模型列表可以为空.
    ///
    /// Whether the ChatGPT account pool is configured. Pass-through needs only the provider;
    /// the model list may be empty.
    pub fn has_chatgpt(&self) -> bool {
        self.chatgpt.is_some()
    }

    pub fn chatgpt_models(&self) -> &[String] {
        &self.chatgpt_models
    }

    /// 是否配置了 SuperGrok 账号池.
    ///
    /// Whether the SuperGrok account pool is configured.
    pub fn has_xai(&self) -> bool {
        self.xai.is_some()
    }

    pub fn xai_models(&self) -> &[String] {
        &self.xai_models
    }

    /// 是否配置了 Command Code 线路.
    ///
    /// Whether the Command Code line is configured.
    pub fn has_commandcode(&self) -> bool {
        self.commandcode.is_some()
    }

    /// 配置的 Command Code 模型列表, 供 /v1/models 使用.
    ///
    /// The configured Command Code model list, used by /v1/models.
    pub fn commandcode_models(&self) -> &[String] {
        &self.commandcode_models
    }

    /// ChatGPT 账号池的转发能力, 未配置提供方时为 None.
    ///
    /// The forwarding capabilities of the ChatGPT pool, or None without the provider.
    fn chatgpt_route(&self) -> Option<Route<'_>> {
        self.chatgpt.as_ref().map(|provider| Route::ChatGpt {
            source: &provider.source,
            base_url: &provider.base_url,
            cooldown: &provider.cooldown,
            user_agent: &provider.user_agent,
        })
    }

    /// SuperGrok 账号池的转发能力, 未配置提供方时为 None.
    ///
    /// The forwarding capabilities of the SuperGrok pool, or None without the provider.
    fn xai_route(&self) -> Option<Route<'_>> {
        self.xai.as_ref().map(|provider| Route::Xai {
            source: &provider.source,
            base_url: &provider.base_url,
            cooldown: &provider.cooldown,
            user_agent: &provider.user_agent,
        })
    }

    /// Command Code 线路的转发能力, 未配置提供方时为 None.
    ///
    /// The forwarding capabilities of the Command Code line, or None without the provider.
    fn commandcode_route(&self) -> Option<Route<'_>> {
        self.commandcode
            .as_ref()
            .map(|provider| Route::CommandCode {
                source: &provider.source,
                base_url: &provider.base_url,
            })
    }

    /// 汇总各线路的凭据状态. 顺序固定为 Google 账号池在前, ChatGPT 账号池其次, SuperGrok
    /// 账号池再次, Command Code 线路最后.
    ///
    /// Aggregates the credential statuses of every route. The order is fixed: the Google pool
    /// first, the ChatGPT pool second, the SuperGrok pool next and the Command Code line last.
    pub async fn statuses(&self) -> Result<Vec<Status>, String> {
        let mut all = Vec::new();
        if let Some(route) = &self.antigravity {
            let statuses = Route::Antigravity {
                source: &route.source,
                base_url: &route.base_url,
                cooldown: &route.cooldown,
                user_agent: &route.user_agent,
                unavailable: &route.unavailable,
                upstream_model: "",
                timeouts: &route.timeout_tracker,
                exhausted: &route.exhausted,
            }
            .status()
            .await
            .map_err(|error| error.to_string())?;
            all.extend(statuses);
        }
        for route in [
            self.chatgpt_route(),
            self.xai_route(),
            self.commandcode_route(),
        ]
        .into_iter()
        .flatten()
        {
            let statuses = route.status().await.map_err(|error| error.to_string())?;
            all.extend(statuses);
        }
        Ok(all)
    }

    /// 列出各账号池中仍在冷却的账号, 顺序与 statuses 相同: Google 账号池在前.
    ///
    /// Lists the accounts still cooling down in every pool, in the same order as statuses: the
    /// Google pool first.
    pub fn cooldowns(&self) -> Vec<CooldownStatus> {
        let now = Instant::now();
        let pools = [
            (
                "antigravity",
                self.antigravity.as_ref().map(|route| &route.cooldown),
            ),
            (
                "chatgpt",
                self.chatgpt.as_ref().map(|route| &route.cooldown),
            ),
            ("xai", self.xai.as_ref().map(|route| &route.cooldown)),
        ];
        let mut list = Vec::new();
        for (provider, cooldown) in pools {
            let Some(cooldown) = cooldown else {
                continue;
            };
            list.extend(
                cooldown
                    .remaining(now)
                    .into_iter()
                    .map(|(name, remaining)| CooldownStatus {
                        provider,
                        name,
                        remaining_seconds: remaining.as_secs(),
                    }),
            );
        }
        list
    }

    /// 列出 Antigravity 账号池中按账号与模型记录的不可用项, 供健康检查展示.
    ///
    /// 两类记录合并输出: 404 模型不可用记录的原因是 model_not_found, 额度或容量耗尽记录的原因
    /// 是 quota_exhausted 或 capacity_exhausted. 每项都带原因与剩余秒数, 按键升序排序.
    ///
    /// Lists the per account and model unavailable entries of the Antigravity account pool for the
    /// health check.
    ///
    /// Two kinds of records are merged: a 404 model unavailability record carries the reason
    /// model_not_found, while a quota or capacity exhaustion record carries quota_exhausted or
    /// capacity_exhausted. Every entry states its reason and remaining seconds, sorted by key in
    /// ascending order.
    pub fn unavailable_models(&self) -> Vec<UnavailableModelStatus> {
        let now = Instant::now();
        let Some(route) = &self.antigravity else {
            return Vec::new();
        };
        let mut list: Vec<UnavailableModelStatus> = route
            .unavailable
            .remaining(now)
            .into_iter()
            .map(|(key, remaining)| UnavailableModelStatus {
                provider: "antigravity",
                name: key.name,
                model: key.model,
                reason: "model_not_found",
                remaining_seconds: remaining.as_secs(),
            })
            .collect();
        list.extend(
            route
                .exhausted
                .remaining(now)
                .into_iter()
                .map(|(key, reason, remaining)| UnavailableModelStatus {
                    provider: "antigravity",
                    name: key.name,
                    model: key.model,
                    reason: reason.as_str(),
                    remaining_seconds: remaining.as_secs(),
                }),
        );
        list.sort_by(|a, b| (&a.name, &a.model, a.reason).cmp(&(&b.name, &b.model, b.reason)));
        list
    }

    /// 为分发端点提供刷新后的账号条目.
    ///
    /// Provides refreshed account entries for the broker endpoint.
    pub async fn broker_chatgpt(
        &self,
        account: Option<&str>,
        force: bool,
    ) -> Result<Vec<crate::credential::Entry>, crate::credential::Error> {
        let Some(provider) = &self.chatgpt else {
            return Err(crate::credential::Error::message("ChatGPT 提供方未配置"));
        };
        match &provider.source {
            ChatGptSource::Pool(pool) => pool.broker_entries(account, force).await,
            ChatGptSource::Broker(_) => Err(crate::credential::Error::message(
                "分发端点仅在使用 credentials 凭据文件时可用",
            )),
        }
    }

    /// 处理一次 Antigravity 线路的 Messages 请求. body 已由调用方改写为上游模型名, route 只
    /// 用于日志.
    ///
    /// Handles one Messages request on the Antigravity route. The body has already been
    /// rewritten to the upstream model name by the caller; route is used only in logs.
    pub async fn messages(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        route: &str,
        observation: Option<Observation>,
    ) -> Response<Body> {
        let (response, _) = self
            .antigravity_response(headers, &body, route, observation, None)
            .await;
        response
    }

    /// 以非流式方式完成一次 Antigravity 请求并返回状态码与完整响应体, 供搜索轮次使用.
    ///
    /// 强制 stream 为 false 后走与常规生成相同的一条转换路径, 因此搜索轮次的账号选择,
    /// 重试与转换行为与常规请求一致, 区别只在于读取完整响应体并带上游的 Retry-After.
    ///
    /// Completes one Antigravity request without streaming and returns the status with the whole
    /// response body for the search rounds. With stream forced to false it takes the same
    /// conversion path as ordinary generation, so account selection, retries and conversion
    /// behave the same; the differences are reading the whole body and carrying upstream's
    /// Retry-After.
    pub async fn messages_buffered(&self, headers: &HeaderMap, body: Bytes) -> BufferedResponse {
        // 搜索轮次没有路由名参数, 完成日志的 route 字段用线路名代替.
        //
        // A search round carries no route name, so the completion log's route field uses the
        // route kind.
        let (response, retry_after) = self
            .antigravity_response(headers, &body, "antigravity", None, Some(false))
            .await;
        let status = response.status();
        let Some(body) = collect_body(response, MAX_SEARCH_RESPONSE_BYTES).await else {
            // 超过上限时按网关错误返回空响应体, 与 Go 版 readSearchResponse 对超限的处理相同:
            // 调用方把任何非 200 映射为搜索失败, 因此这里不返回被截断的内容, 避免半个报文被当成
            // 有效结果.
            //
            // Beyond the limit, a gateway error with an empty body is returned, matching how Go's
            // readSearchResponse treats an oversized body: the caller maps any non-200 to a search
            // failure, so truncated content is not returned and a half message is never taken as a
            // valid result.
            return BufferedResponse {
                status: StatusCode::BAD_GATEWAY,
                retry_after,
                body: Bytes::new(),
            };
        };
        BufferedResponse {
            status,
            retry_after,
            body,
        }
    }

    /// 处理一次 ChatGPT 线路的 Messages 转换请求. body 已由调用方解析, route 只用于日志.
    ///
    /// Handles one Messages conversion request on the ChatGPT route. body has been parsed by
    /// the caller; route is used only in logs.
    pub async fn chatgpt_messages(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        route: &str,
        observation: Option<Observation>,
        reasoning_effort: &str,
        upstream_model: &str,
    ) -> Response<Body> {
        let target = ChatGptTarget {
            route,
            model: upstream_model,
            effort: reasoning_effort,
            dialect: &CHATGPT_DIALECT,
        };
        let (response, _) = self
            .responses_conversion(
                self.chatgpt_route(),
                headers,
                &body,
                target,
                observation,
                None,
            )
            .await;
        response
    }

    /// 以非流式方式完成一次 ChatGPT 请求并返回状态码与完整响应体, 供搜索轮次使用.
    ///
    /// Completes one ChatGPT request without streaming and returns the status with the whole
    /// response body for the search rounds.
    pub async fn chatgpt_messages_buffered(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        reasoning_effort: &str,
        upstream_model: &str,
    ) -> BufferedResponse {
        let target = ChatGptTarget {
            route: "chatgpt",
            model: upstream_model,
            effort: reasoning_effort,
            dialect: &CHATGPT_DIALECT,
        };
        self.buffered_conversion(self.chatgpt_route(), headers, &body, target)
            .await
    }

    /// 处理一次 xai 线路的 Messages 转换请求. body 已由调用方解析, route 只用于日志.
    ///
    /// Handles one Messages conversion request on the xai route. body has been parsed by the
    /// caller; route is used only in logs.
    pub async fn xai_messages(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        route: &str,
        observation: Option<Observation>,
        reasoning_effort: &str,
        upstream_model: &str,
    ) -> Response<Body> {
        let target = ChatGptTarget {
            route,
            model: upstream_model,
            effort: reasoning_effort,
            dialect: &XAI_DIALECT,
        };
        let (response, _) = self
            .responses_conversion(self.xai_route(), headers, &body, target, observation, None)
            .await;
        response
    }

    /// 以非流式方式完成一次 xai 请求并返回状态码与完整响应体, 供搜索轮次使用.
    ///
    /// Completes one xai request without streaming and returns the status with the whole
    /// response body for the search rounds.
    pub async fn xai_messages_buffered(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        reasoning_effort: &str,
        upstream_model: &str,
    ) -> BufferedResponse {
        let target = ChatGptTarget {
            route: "xai",
            model: upstream_model,
            effort: reasoning_effort,
            dialect: &XAI_DIALECT,
        };
        self.buffered_conversion(self.xai_route(), headers, &body, target)
            .await
    }

    /// 处理一次 Command Code 线路的 Messages 转换请求. body 已由调用方解析, route 只用于日志.
    ///
    /// Handles one Messages conversion request on the Command Code route. body has been parsed
    /// by the caller; route is used only in logs.
    pub async fn commandcode_messages(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        route: &str,
        observation: Option<Observation>,
        reasoning_effort: &str,
        upstream_model: &str,
    ) -> Response<Body> {
        let target = ChatGptTarget {
            route,
            model: upstream_model,
            effort: reasoning_effort,
            dialect: &crate::commandcode::COMMANDCODE_DIALECT,
        };
        let (response, _) = self
            .responses_conversion(
                self.commandcode_route(),
                headers,
                &body,
                target,
                observation,
                None,
            )
            .await;
        response
    }

    /// 以非流式方式完成一次 Command Code 请求并返回状态码与完整响应体, 供搜索轮次使用.
    ///
    /// Completes one Command Code request without streaming and returns the status with the whole
    /// response body for the search rounds.
    pub async fn commandcode_messages_buffered(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        reasoning_effort: &str,
        upstream_model: &str,
    ) -> BufferedResponse {
        let target = ChatGptTarget {
            route: "commandcode",
            model: upstream_model,
            effort: reasoning_effort,
            dialect: &crate::commandcode::COMMANDCODE_DIALECT,
        };
        self.buffered_conversion(self.commandcode_route(), headers, &body, target)
            .await
    }

    /// 处理一次 ChatGPT 线路的 Responses 透传. path 是客户端请求的路径, 用于日志.
    ///
    /// Handles one Responses pass-through on the ChatGPT route. path is the client request
    /// path, used in logs.
    pub async fn responses(
        &self,
        headers: &HeaderMap,
        path: &str,
        body: Bytes,
        observation: Option<Observation>,
    ) -> Response<Body> {
        // Responses 透传没有路由名, 完成日志的 route 字段用线路名代替.
        //
        // The Responses pass-through carries no route name, so the completion log's route field
        // uses the route kind.
        let context = RequestContext::new(path, "chatgpt", body.len());
        let Some(route) = self.chatgpt_route() else {
            return context.openai_reject(observation, StatusCode::NOT_FOUND, "unsupported path");
        };
        let queue = match route.snapshot(None).await {
            Ok(queue) => queue,
            Err(error) => {
                return context.openai_reject(
                    observation,
                    StatusCode::BAD_GATEWAY,
                    &error.to_string(),
                );
            }
        };
        let guard = CapacityRetryGuard::new(self.capacity_budget, self.first_byte_timeout);
        let inbound = Inbound {
            headers,
            body: &body,
            session: None,
            stream: false,
        };
        let selected = match select_credential(
            &route,
            &self.client,
            &queue,
            &inbound,
            &guard,
            observation.as_ref(),
        )
        .await
        {
            Ok(selected) => selected,
            Err(failure) => {
                // ChatGPT 线路没有转换层, 失败只有"全部账号都不可用"一类, 与 Go 版 reject
                // 一致地使用 OpenAI 风格错误.
                //
                // The ChatGPT route has no conversion layer and its only failure class is that
                // every account is unusable, reported with an OpenAI-style error like Go's
                // reject.
                return context.openai_reject(
                    observation,
                    failure_status(&failure),
                    failure.error.message(),
                );
            }
        };
        let served = selected.account;
        match selected.response {
            AttemptResponse::DirectError { status, body, .. } => {
                context.buffered(observation, &served, status, body, BodyEnd::Buffered)
            }
            AttemptResponse::Upstream(upstream)
            | AttemptResponse::StreamPrelude {
                response: upstream, ..
            } => {
                if upstream.status().as_u16() >= 400 {
                    self.forward_error(context, observation, served, upstream, &guard)
                        .await
                } else {
                    self.forward(context, observation, served, upstream)
                }
            }
            AttemptResponse::Replay(response) => {
                // 容量重试保留的响应只有 antigravity 线路会产生, 这里只是兜底.
                //
                // Only the antigravity route produces a kept capacity response; this is a
                // fallback.
                self.forward_error_buffered(context, observation, served, response)
            }
        }
    }

    /// 透传一次上游响应: 状态码与响应头转发, 响应体按块流式回传.
    ///
    /// Passes one upstream response through: status and headers are forwarded and the body is
    /// streamed back chunk by chunk.
    fn forward(
        &self,
        context: RequestContext,
        observation: Option<Observation>,
        served: String,
        upstream: reqwest::Response,
    ) -> Response<Body> {
        let status = upstream.status();
        let content_type = upstream_content_type(&upstream);
        // 请求标识必须在响应被移动进响应体之前取出.
        //
        // The request identifiers must be taken before the response is moved into the body.
        let request_ids: Vec<(http::HeaderName, HeaderValue)> =
            ["x-request-id", "x-oai-request-id"]
                .into_iter()
                .filter_map(|name| {
                    upstream
                        .headers()
                        .get(name)
                        .map(|value| (http::HeaderName::from_static(name), value.clone()))
                })
                .collect();
        if let Some(observation) = &observation {
            observation.begin_body(&content_type);
            observation.set_status(status.as_u16());
        }
        let completion = Completion::new(
            observation,
            Some(context.completion_log(&served, status.as_u16())),
        );
        let mut response = Response::new(BodyExt::boxed(PassthroughBody::new(
            stream_body(upstream),
            completion,
        )));
        *response.status_mut() = status;
        let headers = response.headers_mut();
        insert_content_type(headers, &content_type);
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        // 上游的请求标识原样带回, 便于把客户端看到的失败与上游日志对上.
        //
        // Upstream's request identifiers are carried back so a client-visible failure can be
        // matched against the upstream log.
        for (name, value) in request_ids {
            headers.insert(name, value);
        }
        response
    }

    /// 上游返回错误状态时的透传: 先记录错误响应体, 再按普通透传回给客户端.
    ///
    /// 上游的错误体通常只有一两百字节且写明原因, 不记录的话运维侧只能看到状态码, 无法判断
    /// 是限流, 容量还是参数问题.
    ///
    /// Pass-through of an upstream error status: the error body is recorded first and then the
    /// response is forwarded like any other. Upstream error bodies are usually one or two
    /// hundred bytes and state the reason; without them operators would see only a status code
    /// and could not tell rate limiting, capacity and parameter problems apart.
    async fn forward_error(
        &self,
        context: RequestContext,
        observation: Option<Observation>,
        served: String,
        upstream: reqwest::Response,
        guard: &CapacityRetryGuard,
    ) -> Response<Body> {
        let replay = attempt::buffered_error_response(upstream, &served, Some(guard)).await;
        self.forward_error_buffered(context, observation, served, Arc::new(replay))
    }

    /// 把已经读完的上游错误响应回给客户端.
    ///
    /// Returns an already read upstream error response to the client.
    fn forward_error_buffered(
        &self,
        context: RequestContext,
        observation: Option<Observation>,
        served: String,
        replay: Arc<attempt::CapacityResponse>,
    ) -> Response<Body> {
        let status = replay.status;
        let content_type = replay
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .unwrap_or("application/json")
            .to_owned();
        if let Some(observation) = &observation {
            observation.begin_body(&content_type);
            observation.set_status(status.as_u16());
        }
        let completion = Completion::new(
            observation,
            Some(context.completion_log(&served, status.as_u16())),
        );
        let mut response = Response::new(BodyExt::boxed(FinalBody::new(
            full(replay.body.clone()),
            completion,
            BodyEnd::Buffered,
        )));
        *response.status_mut() = status;
        let headers = response.headers_mut();
        insert_content_type(headers, &content_type);
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        for name in ["x-request-id", "x-oai-request-id"] {
            if let Some(value) = replay.headers.get(name) {
                headers.insert(name, value.clone());
            }
        }
        if let Some(value) = replay.headers.get(http::header::RETRY_AFTER) {
            headers.insert(http::header::RETRY_AFTER, value.clone());
        }
        response
    }

    /// messages 与 messages_buffered 的共同实现. 第二个返回值是上游的 Retry-After.
    ///
    /// The shared implementation of messages and messages_buffered. The second value is
    /// upstream's Retry-After.
    async fn antigravity_response(
        &self,
        headers: &HeaderMap,
        body: &Bytes,
        route_name: &str,
        observation: Option<Observation>,
        stream_override: Option<bool>,
    ) -> (Response<Body>, Option<String>) {
        // 完成日志里的 POST 路径固定为 /v1/messages: 本入口只服务这一条路径, 而调用方给出的
        // route 是路由名 (模型名), 两者是不同的字段. 查询串在 aria 的分派阶段已被丢弃, 与
        // Go 版按含查询串的请求目标记录日志略有不同, 属于 architecture.md 5.1 的路径匹配规则.
        //
        // The POST path in the completion log is fixed to /v1/messages: this entry point serves
        // only that path, while the route name the caller passes is a different field. The query
        // string was already dropped by aria's dispatch, which differs slightly from Go logging the
        // request target with its query string and belongs to the path matching rule of
        // architecture.md 5.1.
        let context = RequestContext::new("/v1/messages", route_name, body.len());
        let Some(provider) = &self.antigravity else {
            // 配置校验要求 antigravity 路由必须配置提供方, 因此这里只是防御分支: 与 Go 版
            // providerFor 找不到线路时一样按未知路径处理.
            //
            // Configuration validation requires an antigravity route to have a provider, so
            // this is only a defensive branch: like Go's providerFor finding no route, it
            // reports an unknown path.
            return (
                context.notice(observation, StatusCode::NOT_FOUND, "unsupported path"),
                None,
            );
        };
        let (model, requested_stream) = request_shape(body);
        let upstream_model = crate::antigravity::provider::upstream_model(body);
        let stream = stream_override.unwrap_or(requested_stream);
        let session = session_key(headers, body);
        let session = if session.is_empty() {
            None
        } else {
            Some(session.as_str())
        };
        let route = Route::Antigravity {
            source: &provider.source,
            base_url: &provider.base_url,
            cooldown: &provider.cooldown,
            user_agent: &provider.user_agent,
            unavailable: &provider.unavailable,
            upstream_model: &upstream_model,
            timeouts: &provider.timeout_tracker,
            exhausted: &provider.exhausted,
        };
        let queue = match route.snapshot(session).await {
            Ok(queue) => queue,
            Err(error) => {
                // 现有实现在这里会抛出未捕获异常并断开连接, 客户端拿不到响应. 改为明确返回
                // 502, 便于排查配置问题.
                //
                // The existing implementation throws an uncaught exception here and drops the
                // connection, leaving the client with no response. An explicit 502 makes
                // configuration problems easier to diagnose.
                return (
                    context.notice(observation, StatusCode::BAD_GATEWAY, &error.to_string()),
                    None,
                );
            }
        };
        let guard = CapacityRetryGuard::new(self.capacity_budget, self.first_byte_timeout);
        let inbound = Inbound {
            headers,
            body,
            session,
            stream,
        };
        let selected = match select_credential(
            &route,
            &self.client,
            &queue,
            &inbound,
            &guard,
            observation.as_ref(),
        )
        .await
        {
            Ok(selected) => selected,
            Err(failure) => return (self.attempt_failure(context, observation, failure), None),
        };
        let retry_after = selected.response.retry_after();
        let served = selected.account;
        let mut response = match selected.response {
            AttemptResponse::DirectError { status, body, .. } => {
                context.buffered(observation, &served, status, body, BodyEnd::Buffered)
            }
            AttemptResponse::Replay(response) => {
                // 预算耗尽时保留下来的容量错误响应: 回给客户端之前真正收到的状态与说明,
                // 不把已知容量错误改成其他类型.
                //
                // A capacity error response kept when the budget ran out: the client receives
                // the status and description actually received earlier, and the known capacity
                // error is not turned into another type.
                let payload = anthropic_error_payload(response.status, &response.body);
                context.buffered(
                    observation,
                    &served,
                    response.status,
                    payload,
                    BodyEnd::Buffered,
                )
            }
            AttemptResponse::StreamPrelude {
                response: upstream,
                state,
                first,
            } => {
                let content_type = state.content_type();
                let status = upstream.status();
                if let Some(observation) = &observation {
                    observation.set_status(status.as_u16());
                }
                let completion = Completion::new(
                    observation,
                    Some(context.completion_log(&served, status.as_u16())),
                );
                let body = TranslatedBody::new(stream_body(upstream), *state, first, completion);
                let mut response = Response::new(BodyExt::boxed(body));
                *response.status_mut() = status;
                let headers = response.headers_mut();
                insert_content_type(headers, content_type);
                headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
                response
            }
            AttemptResponse::Upstream(upstream) => {
                if upstream.status().as_u16() >= 400 {
                    // 上游错误状态不走翻译器: 响应体是普通 JSON 错误对象而不是事件流, 交给
                    // 翻译器会被逐条丢弃.
                    //
                    // An upstream error status does not go through the translator: the body is
                    // an ordinary JSON error object rather than an event stream, and feeding it
                    // to the translator would drop every line.
                    let replay =
                        attempt::buffered_error_response(upstream, &served, Some(&guard)).await;
                    let payload = anthropic_error_payload(replay.status, &replay.body);
                    context.buffered(
                        observation,
                        &served,
                        replay.status,
                        payload,
                        BodyEnd::Buffered,
                    )
                } else {
                    let translator =
                        RelayTranslator::Antigravity(translator_for(&model, stream, body));
                    self.translated(
                        Serving {
                            context,
                            observation,
                            served,
                            guard,
                        },
                        upstream,
                        translator,
                        stream,
                    )
                    .await
                }
            }
        };
        if let Some(retry_after) = &retry_after
            && let Ok(val) = HeaderValue::from_str(retry_after)
        {
            response
                .headers_mut()
                .insert(http::header::RETRY_AFTER, val);
        }
        (response, retry_after)
    }

    /// chatgpt 与 xai 两条线路 Messages 转换转发的共同实现, 第二个返回值是上游的 Retry-After.
    ///
    /// 两条线路的上游都说 Responses 协议, 请求转换, 选号, 重试与响应转换因此只有这一份实现; 区别
    /// 只在 route 选出的账号池与上游请求头, 以及 target 携带的请求体差异与推理桥类型标识. route 为
    /// None 表示该线路的提供方没有配置.
    ///
    /// The shared implementation of the Messages conversion of the chatgpt and xai lines; the
    /// second value is upstream's Retry-After.
    ///
    /// Both upstreams speak the Responses protocol, so request conversion, selection, retries and
    /// response conversion exist only here; the lines differ only in the account pool and upstream
    /// headers chosen by route and in the request body differences and reasoning bridge type
    /// carried by target. A None route means the line's provider is not configured.
    async fn responses_conversion(
        &self,
        route: Option<Route<'_>>,
        headers: &HeaderMap,
        body: &Bytes,
        target: ChatGptTarget<'_>,
        observation: Option<Observation>,
        stream_override: Option<bool>,
    ) -> (Response<Body>, Option<String>) {
        let context = RequestContext::new("/v1/messages", target.route, body.len());
        let Some(route) = route else {
            return (
                context.notice(observation, StatusCode::NOT_FOUND, "unsupported path"),
                None,
            );
        };
        let (_, requested_stream) = request_shape(body);
        let stream = stream_override.unwrap_or(requested_stream);

        // 1. 将 Anthropic Messages 请求转换为 Responses 请求体
        let responses_body = match crate::chatgpt::request::build_responses_request(
            headers,
            body,
            target.model,
            target.effort,
            target.dialect,
        ) {
            Ok(bytes) => bytes,
            Err(err) => {
                let payload =
                    aria_core::errors::anthropic_error_body("invalid_request_error", &err);
                return (
                    context.buffered(
                        observation,
                        target.route,
                        StatusCode::BAD_REQUEST,
                        payload,
                        BodyEnd::Buffered,
                    ),
                    None,
                );
            }
        };

        // 2. 在该线路的账号池中选号
        let queue = match route.snapshot(None).await {
            Ok(queue) => queue,
            Err(error) => {
                return (
                    context.notice(observation, StatusCode::BAD_GATEWAY, &error.to_string()),
                    None,
                );
            }
        };
        let guard = CapacityRetryGuard::new(self.capacity_budget, self.first_byte_timeout);
        let inbound = Inbound {
            headers,
            body: &responses_body,
            session: None,
            stream,
        };
        let selected = match select_credential(
            &route,
            &self.client,
            &queue,
            &inbound,
            &guard,
            observation.as_ref(),
        )
        .await
        {
            Ok(selected) => selected,
            Err(failure) => return (self.attempt_failure(context, observation, failure), None),
        };
        let retry_after = selected.response.retry_after();
        let served = selected.account;

        let mut response = match selected.response {
            AttemptResponse::DirectError { status, body, .. } => {
                context.buffered(observation, &served, status, body, BodyEnd::Buffered)
            }
            AttemptResponse::Replay(response) => {
                let payload = anthropic_error_payload(response.status, &response.body);
                context.buffered(
                    observation,
                    &served,
                    response.status,
                    payload,
                    BodyEnd::Buffered,
                )
            }
            AttemptResponse::Upstream(upstream)
            | AttemptResponse::StreamPrelude {
                response: upstream, ..
            } => {
                if upstream.status().as_u16() >= 400 {
                    let replay =
                        attempt::buffered_error_response(upstream, &served, Some(&guard)).await;
                    let payload = anthropic_error_payload(replay.status, &replay.body);
                    context.buffered(
                        observation,
                        &served,
                        replay.status,
                        payload,
                        BodyEnd::Buffered,
                    )
                } else {
                    let translator = RelayTranslator::ChatGpt(
                        crate::chatgpt::response::ChatGptTranslator::for_line(
                            target.model,
                            stream,
                            target.dialect.bridge_type,
                        ),
                    );
                    self.translated(
                        Serving {
                            context,
                            observation,
                            served,
                            guard,
                        },
                        upstream,
                        translator,
                        stream,
                    )
                    .await
                }
            }
        };

        if let Some(retry_after) = &retry_after
            && let Ok(val) = HeaderValue::from_str(retry_after)
        {
            response
                .headers_mut()
                .insert(http::header::RETRY_AFTER, val);
        }

        (response, retry_after)
    }

    /// chatgpt_messages_buffered 与 xai_messages_buffered 的共同实现: 强制非流式走同一条转换路径,
    /// 读取至多 4 MiB 的完整响应体.
    ///
    /// The shared implementation of chatgpt_messages_buffered and xai_messages_buffered: it takes
    /// the same conversion path with streaming forced off and reads the whole body up to 4 MiB.
    async fn buffered_conversion(
        &self,
        route: Option<Route<'_>>,
        headers: &HeaderMap,
        body: &Bytes,
        target: ChatGptTarget<'_>,
    ) -> BufferedResponse {
        let (response, retry_after) = self
            .responses_conversion(route, headers, body, target, None, Some(false))
            .await;
        let status = response.status();
        let Some(body) = collect_body(response, MAX_SEARCH_RESPONSE_BYTES).await else {
            return BufferedResponse {
                status: StatusCode::BAD_GATEWAY,
                retry_after,
                body: Bytes::new(),
            };
        };
        BufferedResponse {
            status,
            retry_after,
            body,
        }
    }

    /// 走协议转换的一条上游响应.
    ///
    /// 流式请求先在预读阶段读到首个有效事件再提交响应头; 非流式请求在预读阶段读完整个
    /// 事件流并聚合, 两者共用同一套解析与转换代码.
    ///
    /// One upstream response taking the protocol conversion. A streaming request reads up to the
    /// first valid event in a prelude before committing the response headers; a non-streaming
    /// request reads and aggregates the whole event stream there. Both share one parsing and
    /// conversion implementation.
    async fn translated(
        &self,
        serving: Serving,
        mut upstream: reqwest::Response,
        translator: RelayTranslator,
        stream: bool,
    ) -> Response<Body> {
        let Serving {
            context,
            observation,
            served,
            guard,
        } = serving;
        let content_type = translator.content_type();
        let status = upstream.status();
        let mut state = TranslatedState::new(
            translator,
            stream,
            observation.clone(),
            guard.clone(),
            served.clone(),
        );
        if stream {
            match state.commit_prelude(&mut upstream).await {
                Ok(first) => {
                    if let Some(observation) = &observation {
                        observation.set_status(status.as_u16());
                    }
                    let completion = Completion::new(
                        observation,
                        Some(context.completion_log(&served, status.as_u16())),
                    );
                    let body = TranslatedBody::new(stream_body(upstream), state, first, completion);
                    let mut response = Response::new(BodyExt::boxed(body));
                    *response.status_mut() = status;
                    let headers = response.headers_mut();
                    insert_content_type(headers, content_type);
                    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
                    response
                }
                Err(failure) => {
                    translated_failure_response(context, observation, &served, &guard, failure)
                }
            }
        } else {
            match state.run_buffered(&mut upstream).await {
                Ok(payload) => {
                    context.buffered(observation, &served, status, payload, BodyEnd::Complete)
                }
                Err(failure) => {
                    translated_failure_response(context, observation, &served, &guard, failure)
                }
            }
        }
    }

    /// 账号选择失败时回给客户端的响应.
    ///
    /// Go 的 doPost 按错误类别选择回写方式: 输入错误返回 Anthropic 400, 容量预算耗尽返回
    /// 503 并保留已知错误说明, 其余返回 502. 只有前两者属于会写完成日志的路径, 其余走
    /// reject 路径, 只有访问日志.
    ///
    /// The response returned to the client when account selection fails. Go's doPost chooses how
    /// to write by error class: invalid input becomes an Anthropic 400, an exhausted capacity
    /// budget becomes a 503 keeping the known error description, and anything else becomes a
    /// 502. Only the first two take the path that writes a completion log; the rest take the
    /// reject path with the access log only.
    fn attempt_failure(
        &self,
        context: RequestContext,
        observation: Option<Observation>,
        failure: AttemptFailure,
    ) -> Response<Body> {
        match failure.error {
            // 容量预算耗尽时没有选中的账号, 完成日志与 Go 版一样不记账号名, 即使队列里已经试过
            // 若干账号.
            //
            // When the capacity budget is exhausted no account was selected, so the completion log
            // records no account name as in Go, even though several accounts may have been tried.
            AttemptError::CapacityTimeout => {
                let status = failure_status(&failure);
                let payload = anthropic_error_payload(status, failure.error.message().as_bytes());
                context.buffered(observation, "", status, payload, BodyEnd::Buffered)
            }
            AttemptError::InvalidRequest(_) => {
                let status = failure_status(&failure);
                let payload = anthropic_error_payload(status, failure.error.message().as_bytes());
                context.buffered(
                    observation,
                    &failure.account,
                    status,
                    payload,
                    BodyEnd::Buffered,
                )
            }
            AttemptError::Exhausted {
                message,
                retry_after,
            } => {
                // 全部账号耗尽时没有选中的账号, 完成日志与容量预算耗尽一样不记账号名; 状态码与
                // 容量预算耗尽相同, 并额外用 Retry-After 给出最早恢复的秒数.
                //
                // When every account is exhausted no account was selected, so like the exhausted
                // capacity budget the completion log records no account name; the status matches
                // that path and Retry-After additionally carries the seconds until the earliest
                // recovery.
                let status = StatusCode::SERVICE_UNAVAILABLE;
                let payload = anthropic_error_payload(status, message.as_bytes());
                let mut response =
                    context.buffered(observation, "", status, payload, BodyEnd::Buffered);
                if let Ok(value) = HeaderValue::from_str(&retry_after.as_secs().max(1).to_string())
                {
                    response
                        .headers_mut()
                        .insert(http::header::RETRY_AFTER, value);
                }
                response
            }
            AttemptError::ModelUnavailable(ref message) => {
                let status = failure_status(&failure);
                let payload = aria_core::errors::anthropic_error_body("not_found_error", message);
                context.buffered(
                    observation,
                    &failure.account,
                    status,
                    payload,
                    BodyEnd::Buffered,
                )
            }
            AttemptError::Other(_) | AttemptError::FirstByteTimeout => context.notice(
                observation,
                failure_status(&failure),
                failure.error.message(),
            ),
        }
    }
}

/// 一次已选定账号的转发上下文.
///
/// 转换转发需要同时用到日志上下文, 观测句柄, 账号名与容量预算守卫, 因此合成一个值传入.
///
/// The forwarding context of one selected account. The converting forward needs the log context,
/// the observation handle, the account name and the capacity guard together, so they are passed
/// as one value.
struct Serving {
    context: RequestContext,
    observation: Option<Observation>,
    served: String,
    guard: Arc<CapacityRetryGuard>,
}

/// 一次请求在日志与用量上的公共上下文.
///
/// The context shared by the logs and the usage of one request.
struct RequestContext {
    /// 完成日志里的 POST 路径.
    ///
    /// The POST path in the completion log.
    path: String,
    /// 完成日志里的 route 字段. 搜索轮次与 Responses 透传没有路由名参数时取线路名.
    ///
    /// The route field of the completion log. It takes the route kind for search rounds and the
    /// Responses pass-through, which carry no route name.
    route: String,
    request_bytes: usize,
    started: Instant,
}

impl RequestContext {
    fn new(path: &str, route: &str, request_bytes: usize) -> Self {
        Self {
            path: path.to_owned(),
            route: route.to_owned(),
            request_bytes,
            started: Instant::now(),
        }
    }

    fn completion_log(&self, credential: &str, status: u16) -> CompletionLog {
        CompletionLog {
            path: self.path.clone(),
            route: self.route.clone(),
            credential: credential.to_owned(),
            status,
            request_bytes: self.request_bytes,
            started: self.started,
        }
    }

    /// 构造一次性写出的响应: 错误报文或非流式聚合报文, 并写完成日志.
    ///
    /// Builds a response written in one piece, an error message or an aggregated non-streaming
    /// message, and writes the completion log.
    fn buffered(
        &self,
        observation: Option<Observation>,
        credential: &str,
        status: StatusCode,
        payload: Vec<u8>,
        end: BodyEnd,
    ) -> Response<Body> {
        // 状态码必须先写进观测再交出响应体: 记录里的状态码决定结果判定, 漏记会让一次成功的
        // 请求被当成客户端断开.
        //
        // The status must reach the observation before the response body is handed out: the status
        // in the record decides the outcome, and omitting it would turn a successful request into a
        // client disconnect.
        if let Some(observation) = &observation {
            observation.set_status(status.as_u16());
        }
        let completion = Completion::new(
            observation,
            Some(self.completion_log(credential, status.as_u16())),
        );
        json_response(status, payload, completion, end)
    }

    /// 构造只有访问日志的 Anthropic 错误响应, 与 Go 版 reject 一致.
    ///
    /// Builds an Anthropic error response with the access log only, matching Go's reject.
    fn notice(
        &self,
        observation: Option<Observation>,
        status: StatusCode,
        message: &str,
    ) -> Response<Body> {
        if let Some(observation) = &observation {
            observation.set_status(status.as_u16());
        }
        let completion = Completion::new(observation, None);
        let payload = anthropic_error_payload(status, message.as_bytes());
        json_response(status, payload, completion, BodyEnd::Buffered)
    }

    /// 构造 OpenAI 风格错误响应, ChatGPT 线路沿用 Go 版 Lyra 的错误格式.
    ///
    /// Builds an OpenAI-style error response; the ChatGPT route keeps the error format of Go
    /// Lyra.
    fn openai_reject(
        &self,
        observation: Option<Observation>,
        status: StatusCode,
        message: &str,
    ) -> Response<Body> {
        if let Some(observation) = &observation {
            observation.set_status(status.as_u16());
        }
        let completion = Completion::new(observation, None);
        let mut response = openai_error(status, message);
        let body = std::mem::replace(response.body_mut(), empty());
        *response.body_mut() = BodyExt::boxed(FinalBody::new(body, completion, BodyEnd::Buffered));
        response
    }
}

/// 构造带 Anthropic 错误报文的 JSON 响应.
///
/// Builds a JSON response carrying an Anthropic error message.
fn json_response(
    status: StatusCode,
    payload: Vec<u8>,
    completion: Completion,
    end: BodyEnd,
) -> Response<Body> {
    let mut response = Response::new(BodyExt::boxed(FinalBody::new(
        full(payload),
        completion,
        end,
    )));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// 提交响应头之前失败时回给客户端的响应, 与 Go 版 fail 的未提交分支一致.
///
/// 响应头尚未提交时返回真正的错误响应; 预算耗尽而重试已经拿到 200 时, 回给客户端之前真正
/// 收到的 503, 不把已知容量错误改成其他类型.
///
/// The response returned to the client when a failure happens before the response headers are
/// committed, matching the uncommitted branch of Go's fail. While the headers are not yet
/// committed a real error response is returned; when the budget ran out after a retry had
/// already received a 200, the client receives the 503 actually received earlier and the known
/// capacity error is not turned into another type.
fn translated_failure_response(
    context: RequestContext,
    observation: Option<Observation>,
    served: &str,
    guard: &CapacityRetryGuard,
    failure: TranslatedFailure,
) -> Response<Body> {
    failure.log(served);
    if matches!(failure, TranslatedFailure::CapacityTimeout)
        && let Some(previous) = guard.take_capacity_response()
    {
        let payload = anthropic_error_payload(previous.status, &previous.body);
        return context.buffered(
            observation,
            served,
            previous.status,
            payload,
            BodyEnd::Buffered,
        );
    }
    let status = failure.status();
    let payload = anthropic_error_payload(status, failure.message().as_bytes());
    context.buffered(observation, served, status, payload, BodyEnd::Buffered)
}

/// 失败对应的客户端状态码, 与 Go 版 doPost 的三个分支一致.
///
/// The client status of a failure, matching the three branches of Go's doPost.
fn failure_status(failure: &AttemptFailure) -> StatusCode {
    match failure.error {
        AttemptError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        AttemptError::CapacityTimeout | AttemptError::Exhausted { .. } => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        AttemptError::FirstByteTimeout => StatusCode::GATEWAY_TIMEOUT,
        AttemptError::ModelUnavailable(_) => StatusCode::NOT_FOUND,
        AttemptError::Other(_) => StatusCode::BAD_GATEWAY,
    }
}

/// 取客户端请求中的模型名与是否流式, 与 Go 版 requestShape 相同.
///
/// 只解析这两个字段: 翻译器需要模型名回填响应, 需要流式标记决定是逐事件回写还是聚合成
/// 完整报文. 解析失败时两个取值都退化为零值, 与 Go 把载荷解进结构体失败时的返回一致.
///
/// Extracts the model name and the streaming flag from the client request, matching Go's
/// requestShape. Only these two fields are parsed: the translator needs the model to fill in
/// the response and the streaming flag to decide between per-event writes and aggregation into
/// a complete message. A parse failure degrades both values to their zero form, matching Go
/// returning zeros when the payload fails to unmarshal into the struct.
fn request_shape(body: &[u8]) -> (String, bool) {
    #[derive(serde::Deserialize)]
    struct Shape {
        #[serde(default)]
        model: String,
        #[serde(default)]
        stream: bool,
    }
    match serde_json::from_slice::<Shape>(body) {
        Ok(shape) => (shape.model, shape.stream),
        Err(_) => (String::new(), false),
    }
}

/// 把上游响应体转成按块流式的响应体.
///
/// Turns an upstream response body into a chunk-streamed response body.
fn stream_body(response: reqwest::Response) -> Body {
    let stream = response
        .bytes_stream()
        .map_err(|error| -> BoxError { Box::new(error) })
        .map_ok(Frame::data);
    BodyExt::boxed(StreamBody::new(stream))
}

/// 读取响应体, 至多 limit 字节; 超过上限时返回 None.
///
/// 上限之后还读到内容即判为超限, 因此调用方能够区分正好等于上限与超过上限, 而不是拿到被截断的
/// 内容.
///
/// Reads a response body up to limit bytes, returning None once the limit is exceeded. Content
/// arriving after the limit marks it as exceeded, so the caller can tell exactly-limit from
/// over-limit instead of receiving truncated content.
async fn collect_body(response: Response<Body>, limit: usize) -> Option<Bytes> {
    let mut body = response.into_body();
    let mut collected = Vec::new();
    loop {
        let frame = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await;
        let Some(Ok(frame)) = frame else {
            return Some(Bytes::from(collected));
        };
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if chunk.len() > limit - collected.len() {
            return None;
        }
        collected.extend_from_slice(&chunk);
    }
}

/// 取上游响应的内容类型, 缺失或为空时使用 application/json, 与 Go 版 forward 相同.
///
/// Takes the content type of an upstream response, using application/json when it is missing or
/// empty, matching Go's forward.
fn upstream_content_type(response: &reqwest::Response) -> String {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("application/json")
        .to_owned()
}

/// 写入内容类型响应头.
///
/// Writes the content type response header.
fn insert_content_type(headers: &mut HeaderMap, content_type: &str) {
    match HeaderValue::from_str(content_type) {
        Ok(value) => {
            headers.insert(CONTENT_TYPE, value);
        }
        Err(_) => {
            // 上游给出的内容类型无法编码时按默认值处理, 不让整个响应失败.
            //
            // An unencodable upstream content type falls back to the default instead of failing
            // the whole response.
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
    }
}
