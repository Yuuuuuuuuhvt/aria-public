//! 请求分派: 端点, 鉴权, 并发槽位, 请求体读取, 模型路由, 模型列表, 健康检查与用量查询.
//!
//! 每个请求只在这里解析一次路径与方法, 之后各分支复用同一份判断, 因此鉴权顺序, 错误格式与
//! 日志口径在整个服务内保持一致.
//!
//! 并发槽位在读取请求体之前获取, 许可随响应体一起丢弃, 因此慢客户端无法在不占用槽位的
//! 情况下消耗内存, 而槽位也一定持有到响应体发送完毕或客户端断开.
//!
//! Request dispatch: endpoints, authentication, concurrency slots, body reading, model
//! routing, the model list, health checks and the usage query.
//!
//! Every request parses its path and method exactly once here and all branches reuse that
//! judgement, so the authentication order, error formats and log fields stay consistent
//! across the service.
//!
//! The concurrency slot is taken before the body is read and the permit is dropped together
//! with the response body, so a slow client cannot consume memory without also holding a
//! slot, and the slot is always held until the body is sent or the client disconnects.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use aria_core::auth::{KeySet, presented_key};
use aria_core::body::{Body, BoxError, empty};
use aria_core::errors::{anthropic_error, json_response, openai_error};
use aria_core::upstream::{ClientOptions, build_client};
use aria_core::usage::{ErrorKind, Filter, Observation, Store};
use bytes::{Bytes, BytesMut};
use canto::forward::{MessagesDestination, RawObject, parse_messages_request, rewrite_model};
use canto::search::{
    SearchLimits, SearchService, SearchTurn, run_search, search_options, search_stream,
    write_search_message,
};
use http::request::Parts;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode, header};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use lyra::client_id::{ClientIdentity, VersionSnapshot};
use lyra::relay::{AntigravityConfig, ChatGptConfig, Relay, RelayConfig, XaiConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{Config, RouteSpec};
use crate::search_turn::{AntigravityTurn, QoderTurn, ResponsesLine, ResponsesTurn};

/// 并发槽位耗尽时返回的 Retry-After 值, 与 Canto 的取值相同.
///
/// The Retry-After value returned when the concurrency slots are exhausted, matching Canto.
const SLOT_RETRY_AFTER: &str = "1";

/// 请求体的读取期限. 超过期限的慢速上传返回 400, 与 Canto 的读截止时间相同.
///
/// The read deadline of the request body. A slow upload past the deadline returns 400,
/// matching Canto's read deadline.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Google 线路的容量重试预算, 见 architecture.md 5.6 第 2 条.
///
/// The capacity retry budget of the Google line, see section 5.6 rule 2 of architecture.md.
const CAPACITY_BUDGET: Duration = Duration::from_secs(25);

/// messages 路由与搜索服务共用的客户端参数: 允许 HTTP/2, 连接超时 15 秒.
///
/// The client parameters shared by messages routes and the search service: HTTP/2 allowed,
/// 15 second connect timeout.
const MESSAGES_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// 两个订阅账号提供方共用的客户端参数: 只用 HTTP/1.1, 连接超时 30 秒.
///
/// The client parameters shared by both subscription providers: HTTP/1.1 only, 30 second
/// connect timeout.
const RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// 客户端标识的更新间隔, 与 architecture.md 3.8 的每 3 小时一致.
///
/// The interval at which the client identifiers are refreshed, matching section 3.8 of
/// architecture.md.
const IDENTITY_REFRESH_INTERVAL: Duration = Duration::from_secs(3 * 60 * 60);

/// Responses 透传的三个路径. 这些端点服务 OpenAI 协议的客户端, 因此错误使用 OpenAI 风格.
///
/// The three Responses passthrough paths. These endpoints serve OpenAI-protocol clients,
/// so their errors use the OpenAI style.
const RESPONSE_PATHS: [&str; 3] = [
    "/v1/responses",
    "/responses",
    "/backend-api/codex/responses",
];

/// 用量查询允许的参数名, 与 Lyra relay/usage.go 的 serveUsage 相同.
///
/// The parameter names the usage query accepts, matching serveUsage in Lyra's
/// relay/usage.go.
const USAGE_PARAMETERS: [&str; 6] = ["from", "to", "model", "account", "provider", "group_by"];

/// 一次生成请求要服务的端点.
///
/// The endpoint one generation request is served by.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    /// POST /v1/messages.
    Messages,
    /// POST /v1/messages/count_tokens.
    CountTokens,
    /// 三个 Responses 透传路径之一.
    ///
    /// One of the three Responses passthrough paths.
    Responses,
}

/// 客户端可见的错误格式.
///
/// The client-visible error format.
#[derive(Clone, Copy)]
enum ErrorStyle {
    /// Messages, 模型列表, 健康检查与用量查询使用的 Anthropic 格式.
    ///
    /// The Anthropic format used by Messages, the model list, health checks and the usage
    /// query.
    Anthropic,
    /// Responses 端点使用的 OpenAI 格式.
    ///
    /// The OpenAI format used by the Responses endpoints.
    OpenAi,
}

impl ErrorStyle {
    /// 按请求路径选择错误格式.
    ///
    /// Picks the error format for a request path.
    fn for_path(path: &str) -> ErrorStyle {
        if RESPONSE_PATHS.contains(&path) {
            ErrorStyle::OpenAi
        } else {
            ErrorStyle::Anthropic
        }
    }

    /// 构造该格式下的错误响应.
    ///
    /// Builds an error response in this format.
    fn error(self, status: StatusCode, kind: &str, message: &str) -> Response<Body> {
        match self {
            ErrorStyle::Anthropic => anthropic_error(status, Some(kind), message),
            ErrorStyle::OpenAi => openai_error(status, message),
        }
    }

    /// 构造鉴权失败响应.
    ///
    /// 两个消息分别沿用各自的 Go 来源: Anthropic 格式来自 Canto 的 writeError, OpenAI 格式
    /// 来自 Lyra 的 reject.
    ///
    /// Builds the authentication failure response.
    ///
    /// The two messages keep their own Go sources: the Anthropic one comes from Canto's
    /// writeError and the OpenAI one from Lyra's reject.
    fn unauthorized(self) -> Response<Body> {
        match self {
            ErrorStyle::Anthropic => {
                anthropic_error(StatusCode::UNAUTHORIZED, None, "invalid client key")
            }
            ErrorStyle::OpenAi => openai_error(StatusCode::UNAUTHORIZED, "invalid api key"),
        }
    }
}

/// 已经解析完成的一条路由.
///
/// One resolved route.
struct RouteEntry {
    /// 路由名, 用于日志与用量记录的 account 字段.
    ///
    /// The route name, used in logs and as the account field of the usage record.
    name: String,
    /// 发给上游的模型名.
    ///
    /// The model name sent upstream.
    model: String,
    /// 为真时独立网页搜索走 Kimi 搜索服务.
    ///
    /// When true, standalone web search goes to the Kimi search service.
    moonshot_search: bool,
    /// 路由允许的最大输出 token 数, 仅 messages 与 antigravity 路由支持.
    ///
    /// The maximum output tokens allowed for the route, supported only by messages and antigravity.
    max_output_tokens: Option<i64>,
    kind: RouteKind,
}

/// 路由的处理方式.
///
/// How a route is served.
enum RouteKind {
    /// 已经构造好的 messages 目标, 包含上游地址与凭据.
    ///
    /// The prepared messages destination, holding upstream addresses and credentials.
    Messages(MessagesDestination),
    /// 交给 lyra 完成账号选择与协议转换.
    ///
    /// Handed to lyra for account selection and protocol translation.
    Antigravity,
    /// 经 Codex 渠道使用 GPT 模型, 由 lyra 完成协议转换.
    ///
    /// Using GPT models via Codex; lyra performs protocol translation.
    ChatGpt { reasoning_effort: String },
    /// 经 SuperGrok 订阅使用 Grok 模型, 由 lyra 完成协议转换.
    ///
    /// Using Grok models via a SuperGrok subscription; lyra performs protocol translation.
    Xai { reasoning_effort: String },
    /// 经 Command Code Provider API 使用 GLM-5.3, 由 lyra 完成协议转换.
    ///
    /// Using GLM-5.3 via Command Code's Provider API; lyra performs protocol translation.
    CommandCode { reasoning_effort: String },
    /// Qoder 网关线路上的路由.
    ///
    /// A route on the Qoder gateway line.
    Qoder,
}

/// 用量存储与写入失败计数.
///
/// 写入失败计数供 /v1/usage 暴露, 使统计缺口不会被误认为完整数据.
///
/// The usage store together with its write-failure counter.
///
/// The counter is exposed by /v1/usage so a gap in the statistics cannot be mistaken for
/// complete data.
struct Usage {
    store: Arc<Store>,
    write_failures: Arc<AtomicU64>,
}

/// 健康检查中的一条路由级不可用记录, 见 architecture.md 5.13.
///
/// One route-level unavailability record in the health check; see section 5.13 of architecture.md.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct UnavailableRouteStatus {
    /// 路由名.
    ///
    /// The route name.
    route: String,
    /// 不可用原因: quota_exhausted 或 rate_limited.
    ///
    /// The reason: quota_exhausted or rate_limited.
    reason: &'static str,
    /// 距离恢复的剩余秒数.
    ///
    /// The remaining seconds until recovery.
    remaining_seconds: u64,
}

/// 服务运行期的全部共享状态.
///
/// All shared state of a running server.
pub struct AppState {
    clients: KeySet,
    broker_clients: Option<KeySet>,
    routes: BTreeMap<String, Arc<RouteEntry>>,
    /// 预先编码的模型列表, 请求期间只读.
    ///
    /// The pre-encoded model list, read-only while serving.
    models: Bytes,
    semaphore: Arc<Semaphore>,
    max_body: usize,
    relay: Relay,
    search: Option<SearchService>,
    usage: Option<Usage>,
    /// 各订阅线路的共享客户端标识. 请求路径从中渲染 User-Agent, 后台任务定期更新它, 见
    /// architecture.md 3.8. 令牌刷新客户端在这里保留一份, 供后台任务获取版本清单使用.
    ///
    /// The shared client identifiers of the subscription lines. The request path renders the
    /// User-Agent from them and a background task updates them on a schedule, see section 3.8 of
    /// architecture.md. The token refresh client is kept here for the background task that
    /// fetches the version manifests.
    antigravity_user_agent: Option<Arc<ClientIdentity>>,
    chatgpt_user_agent: Option<Arc<ClientIdentity>>,
    xai_user_agent: Option<Arc<ClientIdentity>>,
    identity_client: reqwest::Client,
    qoder_auth: Option<Arc<lyra::qoder::auth::GatewayAuth>>,
    qoder_semaphore: Option<Arc<Semaphore>>,
}

impl AppState {
    /// 校验配置并构造运行期状态.
    ///
    /// 上游客户端在这里构造一次并共享: messages 路由与搜索服务共用一个, 两个订阅账号提供方
    /// 共用另一个, 令牌刷新使用第三个.
    ///
    /// Validates the configuration and builds the runtime state.
    ///
    /// The upstream clients are built once and shared: one for messages routes and the
    /// search service, one for both subscription providers, and a third for token refresh,
    /// whose 60 second overall timeout is applied per request inside lyra.
    pub fn new(
        config: &Config,
        clients: KeySet,
        broker_clients: Option<KeySet>,
    ) -> Result<AppState, String> {
        let messages_client = build_client(&ClientOptions {
            http2: true,
            connect_timeout: MESSAGES_CONNECT_TIMEOUT,
            idle_timeout: config.upstream_idle_timeout,
        })?;
        let relay_client = build_client(&ClientOptions {
            http2: false,
            connect_timeout: RELAY_CONNECT_TIMEOUT,
            idle_timeout: config.upstream_idle_timeout,
        })?;
        // 令牌刷新与项目标识推导的 60 秒总超时由 lyra 在每个请求上设置, 见
        // credential/pool.rs 与 credential/google.rs 的 timeout(60s), 因此这里的客户端参数与
        // 两个提供方的客户端保持一致, 不再用连接超时与读空闲超时逼近整请求超时.
        //
        // The 60 second overall timeout of token refresh and project id discovery is set per
        // request inside lyra, see the timeout(60s) calls in credential/pool.rs and
        // credential/google.rs, so this client keeps the same parameters as the two
        // providers instead of approximating a whole-request timeout with a connect timeout
        // and a read-idle timeout.
        let refresh_client = build_client(&ClientOptions {
            http2: false,
            connect_timeout: RELAY_CONNECT_TIMEOUT,
            idle_timeout: config.upstream_idle_timeout,
        })?;

        // 搜索服务在路由之前构造, 与 Canto 的 New 相同: 服务缺失或配置无效时立即失败,
        // 而不是等到第一次搜索请求.
        //
        // The search service is built before the routes, as in Canto's New: a missing or
        // invalid service fails immediately instead of at the first search request.
        let search = match &config.moonshot_search {
            Some(service) => Some(SearchService::new(
                service,
                messages_client.clone(),
                &getenv,
            )?),
            None => None,
        };

        // 每条线路各建一份共享标识. 标识在请求路径, 项目标识推导与后台任务之间共享, 因此构造
        // 一次, 克隆进 relay 配置, 再保留一份在 AppState. 每条线路的标识存在与否和对应提供方
        // 完全一致, 所以按提供方匹配构造.
        //
        // One shared identifier per line. The identifier is shared between the request path,
        // project id discovery and the background task, so it is built once, cloned into the relay
        // configuration, and kept in AppState. Each line's identifier exists exactly when its
        // provider is configured, so it is built per provider.
        let (antigravity, antigravity_user_agent) = match &config.antigravity {
            Some(provider) => {
                let user_agent = ClientIdentity::new(provider.client.clone());
                let antigravity = AntigravityConfig {
                    credentials: provider.credentials.clone(),
                    base_url: provider.base_url.clone(),
                    client_id: provider.client_id.clone(),
                    client_secret: provider.client_secret.clone(),
                    user_agent: user_agent.clone(),
                    timeout_cooldown_threshold: provider.timeout_cooldown_threshold,
                    timeout_cooldown_duration: provider.timeout_cooldown_duration,
                };
                (Some(antigravity), Some(user_agent))
            }
            None => (None, None),
        };
        let (chatgpt, chatgpt_user_agent) = match &config.chatgpt {
            Some(provider) => {
                let source = match &provider.source {
                    crate::config::ChatGptSource::Credentials(credentials) => {
                        lyra::relay::ChatGptSourceConfig::Credentials(credentials.clone())
                    }
                    crate::config::ChatGptSource::Broker { url, key_env } => {
                        let key = std::env::var(key_env).unwrap_or_default();
                        lyra::relay::ChatGptSourceConfig::Broker(lyra::relay::BrokerConfig {
                            url: url.clone(),
                            key,
                        })
                    }
                };
                let user_agent = ClientIdentity::new(provider.client.clone());
                let chatgpt = ChatGptConfig {
                    source,
                    base_url: provider.base_url.clone(),
                    models: provider.models.clone(),
                    user_agent: user_agent.clone(),
                };
                (Some(chatgpt), Some(user_agent))
            }
            None => (None, None),
        };
        let (xai, xai_user_agent) = match &config.xai {
            Some(provider) => {
                let user_agent = ClientIdentity::new(provider.client.clone());
                let xai = XaiConfig {
                    credentials: provider.credentials.clone(),
                    base_url: provider.base_url.clone(),
                    token_url: provider.token_url.clone(),
                    client_id: provider.client_id.clone(),
                    scope: provider.scope.clone(),
                    models: provider.models.clone(),
                    user_agent: user_agent.clone(),
                };
                (Some(xai), Some(user_agent))
            }
            None => (None, None),
        };

        let commandcode = match &config.commandcode {
            Some(provider) => {
                let api_key = std::env::var(&provider.key_env).map_err(|_| {
                    format!("commandcode key_env {:?} is missing", provider.key_env)
                })?;
                Some(lyra::relay::CommandCodeConfig {
                    base_url: provider.base_url.clone(),
                    api_key,
                    models: provider.models.clone(),
                })
            }
            None => None,
        };

        // 后台任务用令牌刷新客户端获取版本清单, 因此遵守进程环境的代理设置. 这里克隆一份, 原件
        // 交给 relay 做刷新与推导.
        //
        // The background task fetches version manifests with the token refresh client, so it
        // honors the process environment proxy. A copy is kept here; the original goes to the
        // relay for refresh and discovery.
        let identity_client = refresh_client.clone();

        let relay = Relay::new(
            RelayConfig {
                antigravity,
                chatgpt,
                xai,
                commandcode,
                capacity_budget: CAPACITY_BUDGET,
                first_byte_timeout: config.upstream_first_byte_timeout,
            },
            relay_client,
            refresh_client,
        )?;

        let mut routes = BTreeMap::new();
        let mut models: Vec<Value> = Vec::new();
        for spec in &config.routes {
            let entry = Arc::new(build_route(spec, &messages_client)?);
            for name in std::iter::once(&spec.name).chain(spec.aliases.iter()) {
                routes.insert(name.clone(), entry.clone());
            }
            models.push(model_item(
                spec.aliases.first().unwrap_or(&spec.name),
                display_name(spec),
            ));
        }
        for model in relay
            .chatgpt_models()
            .iter()
            .chain(relay.xai_models())
            .chain(relay.commandcode_models())
        {
            models.push(model_item(model, model));
        }
        let first = models
            .first()
            .map(|item| item["id"].clone())
            .unwrap_or(Value::Null);
        let last = models
            .last()
            .map(|item| item["id"].clone())
            .unwrap_or(Value::Null);
        let payload = json!({
            "object": "list",
            "data": models,
            "has_more": false,
            "first_id": first,
            "last_id": last,
        });
        // 载荷由本模块在内存中构造, 编码不会失败; 失败时保留空列表而不是让服务无法启动.
        //
        // The payload is built in memory by this module and encoding cannot fail; on a
        // failure an empty list is kept rather than preventing startup.
        let models = serde_json::to_vec(&payload).unwrap_or_default();

        let usage = match &config.usage_dir {
            Some(dir) => {
                let store = Store::open(dir)
                    .map_err(|error| format!("cannot open usage statistics: {error}"))?;
                Some(Usage {
                    store: Arc::new(store),
                    write_failures: Arc::new(AtomicU64::new(0)),
                })
            }
            None => None,
        };

        let qoder_semaphore = config
            .qoder
            .as_ref()
            .map(|p| Arc::new(Semaphore::new(p.max_concurrency)));

        let qoder_auth = match &config.qoder {
            Some(provider) => {
                let machine = match &provider.machine_file {
                    Some(path) => {
                        lyra::qoder::auth::GatewayMachineInfo::load_or_generate(Some(path))
                    }
                    None => {
                        aria_core::log!(
                            "info: providers.qoder.machine_file is not configured; machine identity will change upon restart"
                        );
                        lyra::qoder::auth::GatewayMachineInfo::load_or_generate(None)
                    }
                };
                let token = std::env::var(&provider.token_env).unwrap_or_default();
                if !token.is_empty() {
                    Some(Arc::new(lyra::qoder::auth::GatewayAuth::new(
                        token,
                        Some(&provider.base_url),
                        Some(machine),
                        None,
                    )?))
                } else {
                    aria_core::log!(
                        "warning: providers.qoder is configured but environment variable {} is empty",
                        &provider.token_env
                    );
                    None
                }
            }
            None => None,
        };

        Ok(AppState {
            clients,
            broker_clients,
            routes,
            models: Bytes::from(models),
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            max_body: config.max_body_bytes,
            relay,
            search,
            usage,
            antigravity_user_agent,
            chatgpt_user_agent,
            xai_user_agent,
            identity_client,
            qoder_auth,
            qoder_semaphore,
        })
    }

    /// 启动客户端标识的后台更新任务.
    ///
    /// 服务启动后立即获取一次, 之后每 3 小时一次. 获取用令牌刷新客户端, 因此遵守进程环境的
    /// 代理设置; 获取失败不影响请求处理, 只沿用当前值. 规则见 architecture.md 3.8.
    ///
    /// Starts the background task that refreshes the client identifiers. It fetches once at
    /// startup and then every 3 hours. The fetch uses the token refresh client, so it honors the
    /// process environment proxy; a failed fetch does not affect request handling and only keeps
    /// the current value. See section 3.8 of architecture.md.
    pub fn start_identity_refresh(self: &Arc<Self>) {
        let lines: [Option<Arc<ClientIdentity>>; 3] = [
            self.antigravity_user_agent.clone(),
            self.chatgpt_user_agent.clone(),
            self.xai_user_agent.clone(),
        ];
        if lines.iter().all(Option::is_none) {
            return;
        }
        let client = self.identity_client.clone();
        tokio::spawn(async move {
            // 启动时立即获取一次, 再进入固定间隔.
            //
            // Fetch once at startup, then enter a fixed interval.
            refresh_lines(&lines, &client).await;
            let mut ticker = tokio::time::interval(IDENTITY_REFRESH_INTERVAL);
            // 第一次 tick 立即触发, 跳过它, 避免与启动时的那次获取背靠背.
            //
            // The first tick fires immediately; skip it so it does not run back to back with the
            // startup fetch.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                refresh_lines(&lines, &client).await;
            }
        });
    }

    /// antigravity, chatgpt 与 xai 三条线路当前标识的版本与来源, 按这个顺序给出, 供健康检查使用.
    /// 没有配置线路时该条目为 None.
    ///
    /// The current version and source of the antigravity, chatgpt and xai identifiers in that
    /// order, for the health check. An entry is None when the line is not configured.
    pub fn identity_snapshots(
        &self,
    ) -> (
        Option<VersionSnapshot>,
        Option<VersionSnapshot>,
        Option<VersionSnapshot>,
    ) {
        let snapshot = |line: &Option<Arc<ClientIdentity>>| line.as_ref().map(|id| id.snapshot());
        (
            snapshot(&self.antigravity_user_agent),
            snapshot(&self.chatgpt_user_agent),
            snapshot(&self.xai_user_agent),
        )
    }

    /// 报告是否配置了 ChatGPT 提供方.
    ///
    /// Reports whether a ChatGPT provider is configured.
    pub fn has_chatgpt(&self) -> bool {
        self.relay.has_chatgpt()
    }

    /// 列出当前不可用的 messages 路由, 按路由名排序.
    ///
    /// 别名与路由名指向同一条路由, 这里按路由名只列一次; 只有处于不可用窗口中的路由才会出现,
    /// 见 architecture.md 5.13.
    ///
    /// Lists the messages routes that are currently unavailable, sorted by route name.
    ///
    /// An alias and the route name point at the same route, so the route appears once under its
    /// name; only routes inside an unavailable window appear, see section 5.13 of architecture.md.
    fn unavailable_routes(&self) -> Vec<UnavailableRouteStatus> {
        let mut seen = BTreeSet::new();
        let mut list = Vec::new();
        for entry in self.routes.values() {
            if !seen.insert(Arc::as_ptr(entry)) {
                continue;
            }
            let RouteKind::Messages(destination) = &entry.kind else {
                continue;
            };
            if let Some(status) = destination.unavailable_status() {
                list.push(UnavailableRouteStatus {
                    route: entry.name.clone(),
                    reason: status.reason,
                    remaining_seconds: status.remaining_seconds,
                });
            }
        }
        list.sort_by(|a, b| a.route.cmp(&b.route));
        list
    }

    /// 返回不同路由的数量, 别名不计入.
    ///
    /// Returns the number of distinct routes; aliases are not counted.
    pub fn route_count(&self) -> usize {
        self.routes
            .values()
            .map(Arc::as_ptr)
            .collect::<BTreeSet<_>>()
            .len()
    }

    /// 为一次生成请求创建用量观测句柄, 未启用用量时返回 None.
    ///
    /// Creates the usage observation handle for one generation request, or None when usage
    /// capture is disabled.
    fn observe(&self, provider: &str, model: &str) -> Option<Observation> {
        self.usage.as_ref().map(|usage| {
            Observation::start(
                usage.store.clone(),
                usage.write_failures.clone(),
                provider,
                model,
            )
        })
    }

    /// 关闭用量存储, 使目录锁与目录项同步在退出前完成.
    ///
    /// Closes the usage store so the directory lock and the directory entry sync complete
    /// before exit.
    pub fn close_usage(&self) {
        if let Some(usage) = &self.usage
            && let Err(error) = usage.store.close()
        {
            aria_core::log!("usage store close failed: {error}");
        }
    }
}

/// 依次刷新各线路的标识. 各线路串行获取, 3 小时一次的间隔下没有并发必要.
///
/// Refreshes the lines' identifiers in turn. They are fetched serially; at a 3-hour interval
/// there is no need for concurrency.
async fn refresh_lines(lines: &[Option<Arc<ClientIdentity>>], client: &reqwest::Client) {
    for line in lines.iter().flatten() {
        line.refresh(client).await;
    }
}

/// 构造一条路由的运行期描述.
///
/// Builds the runtime description of one route.
fn build_route(spec: &RouteSpec, client: &reqwest::Client) -> Result<RouteEntry, String> {
    let kind = match &spec.kind {
        crate::config::RouteKind::Messages { .. } => {
            let route = spec.messages_route().ok_or("invalid messages route")?;
            RouteKind::Messages(MessagesDestination::new(&route, client.clone(), &getenv)?)
        }
        crate::config::RouteKind::Antigravity => RouteKind::Antigravity,
        crate::config::RouteKind::ChatGpt { reasoning_effort } => RouteKind::ChatGpt {
            reasoning_effort: reasoning_effort.clone(),
        },
        crate::config::RouteKind::Xai { reasoning_effort } => RouteKind::Xai {
            reasoning_effort: reasoning_effort.clone(),
        },
        crate::config::RouteKind::CommandCode { reasoning_effort } => RouteKind::CommandCode {
            reasoning_effort: reasoning_effort.clone(),
        },
        crate::config::RouteKind::Qoder => RouteKind::Qoder,
    };
    Ok(RouteEntry {
        name: spec.name.clone(),
        model: spec.model.clone(),
        moonshot_search: spec.moonshot_search,
        max_output_tokens: spec.max_output_tokens,
        kind,
    })
}

/// 返回模型列表中的展示名: 未配置时取路由名.
///
/// Returns the display name of the model list: the route name when it is not configured.
fn display_name(spec: &RouteSpec) -> &str {
    if spec.display_name.is_empty() {
        &spec.name
    } else {
        &spec.display_name
    }
}

/// 构造模型列表中的一项.
///
/// Builds one item of the model list.
fn model_item(id: &str, display_name: &str) -> Value {
    json!({
        "id": id,
        "type": "model",
        "object": "model",
        "display_name": display_name,
        "owned_by": "aria",
    })
}

/// 读取进程环境变量的取值函数.
///
/// The value getter reading the process environment.
fn getenv(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// 处理一个已经建立连接上的请求, 写出访问日志并返回响应.
///
/// Handles one request on an established connection, writes the access log and returns the
/// response.
pub async fn handle(
    state: Arc<AppState>,
    peer: Option<SocketAddr>,
    request: http::Request<Incoming>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    let target = match parts.uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.clone(),
    };
    let style = ErrorStyle::for_path(&path);
    let method = parts.method.clone();
    let response = route(&state, parts, body, &path, style).await;
    access_log(peer, &method, &target, response.status());
    response
}

/// 按方法与路径分派请求.
///
/// 鉴权顺序与两个 Go 实现一致: 存活检查不需要鉴权, 其余 GET 与 POST 先鉴权再判断路径,
/// 其他方法直接返回 501 而不鉴权.
///
/// Dispatches a request by method and path.
///
/// The authentication order matches both Go implementations: the liveliness check needs no
/// key, every other GET and POST is authenticated before its path is judged, and other
/// methods return 501 without authentication.
async fn route(
    state: &Arc<AppState>,
    parts: Parts,
    body: Incoming,
    path: &str,
    style: ErrorStyle,
) -> Response<Body> {
    match parts.method {
        Method::GET => {
            if path == "/health/liveliness" {
                return json_response(StatusCode::OK, br#"{"status":"ok"}"#.to_vec());
            }
            if path == "/v1/broker/chatgpt" {
                let Some(broker_clients) = &state.broker_clients else {
                    return not_found(style);
                };
                if !broker_clients.authorized(&presented_key(&parts.headers)) {
                    return style.unauthorized();
                }
                return broker_chatgpt(state, parts).await;
            }
            if state
                .broker_clients
                .as_ref()
                .is_some_and(|b| b.authorized(&presented_key(&parts.headers)))
            {
                return style.unauthorized();
            }
            if !state.clients.authorized(&presented_key(&parts.headers)) {
                return style.unauthorized();
            }
            match path {
                "/healthz" | "/v1/healthz" => healthz(state).await,
                "/v1/models" => json_response(StatusCode::OK, state.models.to_vec()),
                "/v1/usage" => usage(state, parts.uri.query()).await,
                _ => not_found(style),
            }
        }
        Method::POST => {
            if state
                .broker_clients
                .as_ref()
                .is_some_and(|b| b.authorized(&presented_key(&parts.headers)))
            {
                return style.unauthorized();
            }
            if !state.clients.authorized(&presented_key(&parts.headers)) {
                return style.unauthorized();
            }
            let endpoint = match path {
                "/v1/messages" => Endpoint::Messages,
                "/v1/messages/count_tokens" => Endpoint::CountTokens,
                other if RESPONSE_PATHS.contains(&other) => Endpoint::Responses,
                _ => return not_found(style),
            };
            generation(state.clone(), parts, body, endpoint, style).await
        }
        // Lyra 的未定义方法返回 501, 且不读取请求体也不鉴权; 已知路径配错方法走这里, 未知
        // 路径在 GET 与 POST 分支中返回 404.
        //
        // Lyra returns 501 for an undefined method without reading the body or checking the
        // key; a known path with the wrong method lands here, while an unknown path returns
        // 404 in the GET and POST branches.
        _ => not_implemented(),
    }
}

/// 处理一次生成请求.
///
/// 并发槽位在读取请求体之前获取, 许可一直持有到响应体发送完毕或客户端断开.
///
/// Serves one generation request.
///
/// The concurrency slot is taken before the body is read and the permit is held until the
/// response body has been sent or the client disconnects.
async fn generation(
    state: Arc<AppState>,
    parts: Parts,
    body: Incoming,
    endpoint: Endpoint,
    style: ErrorStyle,
) -> Response<Body> {
    let Ok(permit) = state.semaphore.clone().try_acquire_owned() else {
        let mut response = style.error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "gateway concurrency limit reached",
        );
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_static(SLOT_RETRY_AFTER),
        );
        return response;
    };
    let body = match read_body(body, state.max_body).await {
        Ok(body) => body,
        Err(BodyError::TooLarge) => {
            return guard_plain(
                style.error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request_too_large",
                    "request body exceeds limit",
                ),
                permit,
            );
        }
        Err(BodyError::Unreadable) => {
            return guard_plain(
                style.error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    "request body could not be read",
                ),
                permit,
            );
        }
    };
    let target = match parts.uri.query() {
        Some(query) => format!("{}?{query}", parts.uri.path()),
        None => parts.uri.path().to_owned(),
    };
    let request = GenerationRequest {
        state,
        parts,
        body,
        endpoint,
        style,
        target,
        started: Instant::now(),
    };
    request.serve().await.guard(permit)
}

/// 把并发许可附加到一个不带用量采集与完成日志的响应上.
///
/// Attaches the concurrency permit to a response without usage capture or a completion log.
fn guard_plain(response: Response<Body>, permit: OwnedSemaphorePermit) -> Response<Body> {
    Generation::plain(response).guard(permit)
}

/// 读取请求体时可能出现的两种失败.
///
/// The two failures possible while reading a request body.
enum BodyError {
    /// 超过 max_body_bytes.
    ///
    /// Beyond max_body_bytes.
    TooLarge,
    /// 读取超时或传输失败.
    ///
    /// A read timeout or a transport failure.
    Unreadable,
}

/// 在读取期限内读完全部请求体, 超过上限时立即停止读取.
///
/// 允许分块传输: 不检查 Content-Length, 只按实际读取的字节数判断大小.
///
/// Reads the whole request body within the read deadline and stops reading as soon as the
/// limit is exceeded.
///
/// Chunked transfer is allowed: Content-Length is not inspected and only the bytes actually
/// read decide the size.
async fn read_body(body: Incoming, limit: usize) -> Result<Bytes, BodyError> {
    let read = async {
        let mut body = body;
        let mut collected = BytesMut::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| BodyError::Unreadable)?;
            if let Ok(data) = frame.into_data() {
                // 用 saturating_sub 而不是相加后比较: collected 始终不超过 limit, 因此这里既
                // 不会下溢, 也不会出现两个长度相加的溢出.
                //
                // saturating_sub instead of adding the two lengths: collected never exceeds
                // limit, so this neither underflows nor overflows the sum.
                if data.len() > limit.saturating_sub(collected.len()) {
                    return Err(BodyError::TooLarge);
                }
                collected.extend_from_slice(&data);
            }
        }
        Ok(collected.freeze())
    };
    match tokio::time::timeout(BODY_READ_TIMEOUT, read).await {
        Ok(result) => result,
        Err(_) => Err(BodyError::Unreadable),
    }
}

/// 一次已经读完请求体的生成请求.
///
/// One generation request whose body has already been read.
struct GenerationRequest {
    state: Arc<AppState>,
    parts: Parts,
    body: Bytes,
    endpoint: Endpoint,
    style: ErrorStyle,
    /// 发往上游的请求目标: 请求路径加客户端查询串.
    ///
    /// The upstream request target: the request path plus the client query string.
    target: String,
    started: Instant,
}

impl GenerationRequest {
    /// 按端点分派请求.
    ///
    /// Dispatches the request by endpoint.
    async fn serve(&self) -> Generation {
        if self.endpoint == Endpoint::Responses {
            return self.serve_responses().await;
        }
        let Some((fields, model)) = parse_messages_request(&self.body) else {
            return Generation::plain(self.style.error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "expected JSON object with a model",
            ));
        };
        let Some(entry) = self.state.routes.get(&model).cloned() else {
            return Generation::plain(self.style.error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "unknown model",
            ));
        };
        match &entry.kind {
            // antigravity 路由只接受配置中的模型, count_tokens 没有对应的转换入口.
            //
            // An antigravity route accepts only configured models, and count_tokens has no
            // translation entry point.
            RouteKind::Antigravity if self.endpoint == Endpoint::CountTokens => {
                Generation::plain(anthropic_error(
                    StatusCode::NOT_FOUND,
                    Some("not_found_error"),
                    "unsupported gateway endpoint",
                ))
            }
            RouteKind::ChatGpt { .. } | RouteKind::Xai { .. } | RouteKind::CommandCode { .. }
                if self.endpoint == Endpoint::CountTokens =>
            {
                Generation::plain(anthropic_error(
                    StatusCode::NOT_FOUND,
                    Some("not_found_error"),
                    "unsupported gateway endpoint",
                ))
            }
            RouteKind::Qoder if self.endpoint == Endpoint::CountTokens => {
                Generation::plain(anthropic_error(
                    StatusCode::NOT_FOUND,
                    Some("not_found_error"),
                    "unsupported gateway endpoint",
                ))
            }
            RouteKind::Antigravity => self.serve_antigravity(&entry, fields, model).await,
            RouteKind::ChatGpt { reasoning_effort } => {
                self.serve_responses_line(
                    &entry,
                    fields,
                    model,
                    reasoning_effort,
                    ResponsesLine::ChatGpt,
                )
                .await
            }
            RouteKind::Xai { reasoning_effort } => {
                self.serve_responses_line(
                    &entry,
                    fields,
                    model,
                    reasoning_effort,
                    ResponsesLine::Xai,
                )
                .await
            }
            RouteKind::CommandCode { reasoning_effort } => {
                self.serve_responses_line(
                    &entry,
                    fields,
                    model,
                    reasoning_effort,
                    ResponsesLine::CommandCode,
                )
                .await
            }
            RouteKind::Qoder => self.serve_qoder(&entry, fields, model).await,
            RouteKind::Messages(destination) => {
                self.serve_messages(&entry, destination, fields, model)
                    .await
            }
        }
    }

    /// 尝试获取一个 Qoder 并发许可.
    ///
    /// 未配置信号量时返回 None; 许可不足时返回已经构造好的 429 响应, 带 Retry-After, 与常规
    /// 转发的失败行为一致.
    ///
    /// Tries to take one Qoder concurrency permit.
    ///
    /// It returns None when no semaphore is configured, and an already built 429 response with
    /// Retry-After when no permit is free, matching the failure of ordinary forwarding.
    fn try_qoder_permit(
        &self,
    ) -> Result<Option<tokio::sync::SemaphorePermit<'_>>, Box<Response<Body>>> {
        let Some(sem) = &self.state.qoder_semaphore else {
            return Ok(None);
        };
        match sem.try_acquire() {
            Ok(permit) => Ok(Some(permit)),
            Err(_) => {
                let mut response = anthropic_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    Some("rate_limit_error"),
                    "Qoder upstream concurrency limit exceeded",
                );
                response
                    .headers_mut()
                    .insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
                Err(Box::new(response))
            }
        }
    }

    /// 构造搜索与拒绝分支公用的用量观测与完成日志.
    ///
    /// 搜索编排与其拒绝分支返回 Anthropic 格式的聚合响应, 因此用量按 messages 规则采集,
    /// provider 记为 messages, 账号取路由名, 并在结束时补上完成日志, 与 antigravity 与
    /// Responses 线路相同.
    ///
    /// Builds the usage observation and completion log shared by the search and rejection
    /// branches.
    ///
    /// The orchestration and its rejection branch return aggregated Anthropic responses, so
    /// usage follows the messages rule with provider messages and the route name as the account,
    /// and a completion log is emitted at the end, matching the antigravity and Responses lines.
    fn search_observation(
        &self,
        entry: &RouteEntry,
        model: &str,
    ) -> (Option<Observation>, Completion) {
        let observation = self.state.observe("messages", model);
        if let Some(observation) = &observation {
            observation.set_account(&entry.name);
        }
        let completion = Completion {
            started: self.started,
            target: self.target.clone(),
            route: entry.name.clone(),
            status: 0,
            request_bytes: self.body.len(),
        };
        (observation, completion)
    }

    /// 服务一条 qoder 路由上的请求.
    ///
    /// 与 antigravity 与 Responses 线路相同, 声明了 moonshot_search 的路由会拦截 /v1/messages
    /// 上的独立网页搜索请求: 搜索与拒绝分支返回 Anthropic 格式的聚合响应, 用量按 messages
    /// 规则采集; 其余请求直接交给 lyra 的 Qoder 转发.
    ///
    /// Serves a request on a qoder route.
    ///
    /// As on the antigravity and Responses lines, a route declaring moonshot_search intercepts
    /// the isolated web search request on /v1/messages: the search and rejection branches return
    /// aggregated Anthropic responses with usage captured under the messages rule, while every
    /// other request goes straight to lyra's Qoder forward.
    async fn serve_qoder(
        &self,
        entry: &RouteEntry,
        mut fields: RawObject,
        model: String,
    ) -> Generation {
        let clamped =
            match canto::forward::clamp_output_ceiling(&mut fields, entry.max_output_tokens) {
                Ok(c) => c,
                Err(message) => return Generation::plain(invalid_body(message)),
            };
        // 搜索编排只拦截 /v1/messages 上的独立网页搜索请求, count_tokens 一律透传.
        //
        // The search orchestration only intercepts the isolated web search request on
        // /v1/messages; count_tokens is always forwarded.
        if entry.moonshot_search && self.endpoint == Endpoint::Messages {
            let Some(auth) = &self.state.qoder_auth else {
                return Generation::plain(anthropic_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Some("api_error"),
                    "Qoder access token is not configured in environment variable",
                ));
            };
            match search_plan(&fields) {
                SearchPlan::Rejected(response) => {
                    let (observation, completion) = self.search_observation(entry, &model);
                    return Generation::observed(response, observation, Some(completion));
                }
                SearchPlan::Search { limits, stream } => {
                    let (observation, completion) = self.search_observation(entry, &model);
                    // 搜索编排全程持有一个 qoder 并发许可: 许可在这里获取, 直到整个分支返回才
                    // 释放, 因此一次搜索的每一轮都占用同一个槽位. 许可不足时的 429 同样带观测,
                    // 使它与其他搜索失败一样写入用量与完成日志.
                    //
                    // The whole orchestration holds one qoder concurrency permit: it is taken here
                    // and released only when the branch returns, so every round of one search
                    // occupies the same slot. The 429 of an exhausted limit also carries the
                    // observation, so it is recorded like every other search failure.
                    let _permit = match self.try_qoder_permit() {
                        Ok(permit) => permit,
                        Err(response) => {
                            return Generation::observed(*response, observation, Some(completion));
                        }
                    };
                    let turn = QoderTurn::new(&self.state.identity_client, auth, &entry.model);
                    let response = self
                        .perform_search(fields, limits, &entry.model, &turn, stream)
                        .await;
                    return Generation::observed(response, observation, Some(completion));
                }
                SearchPlan::Native => {}
            }
        }
        let body = if !clamped && model == entry.model {
            self.body.clone()
        } else {
            match rewrite_model(&mut fields, &entry.model) {
                Ok(body) => body,
                Err(message) => return Generation::plain(invalid_body(message)),
            }
        };

        let Some(auth) = &self.state.qoder_auth else {
            return Generation::plain(anthropic_error(
                StatusCode::SERVICE_UNAVAILABLE,
                Some("api_error"),
                "Qoder access token is not configured in environment variable",
            ));
        };

        let _permit = match self.try_qoder_permit() {
            Ok(permit) => permit,
            Err(response) => return Generation::plain(*response),
        };

        let observation = self.state.observe("qoder", &model);
        if let Some(obs) = &observation {
            obs.set_account(&entry.name);
        }

        // stream 为 false 的非流式请求改走缓冲入口, 由它把网关事件流汇总成一条 messages 消息
        // 并返回 application/json, 与非流式 Anthropic 响应同形; 其余请求保持流式转码.
        //
        // A stream-false request takes the buffered entry point, which folds the gateway event
        // stream into one messages payload returned as application/json, matching a
        // non-streaming Anthropic response; every other request keeps the streaming conversion.
        let buffered = matches!(search_stream(&fields), Ok(false));
        let mut response = if buffered {
            lyra::qoder::qoder_buffered_request(
                self.state.identity_client.clone(),
                auth,
                &entry.model,
                &body,
                observation.clone(),
            )
            .await
        } else {
            lyra::qoder::forward_qoder_request(
                self.state.identity_client.clone(),
                auth,
                &entry.model,
                &body,
                observation.clone(),
            )
            .await
        };

        if let Some(obs) = &observation {
            obs.set_status(response.status().as_u16());
        }
        // 事件由 lyra 在转发过程中喂入观测, 收尾由响应体在完整写出后执行; 这里不能再调用
        // finish_chunks 或 set_transport_complete, 否则流式响应尚未写完就会被提前终结, 之后
        // 喂入的事件被采集体丢弃.
        //
        // Events are fed into the observation by lyra while forwarding, and the response body
        // tears the observation down once it has been written out in full; finish_chunks and
        // set_transport_complete must not be called here, or a streaming response would be
        // finalized before it finished and every event fed afterwards would be dropped.
        set_request_id(&mut response, &observation);
        Generation::plain(response)
    }

    /// 服务一条 antigravity 路由上的请求.
    ///
    /// 请求体的模型名先改写为路由的模型, 再交给 lyra 的转换逻辑. 常规生成的用量采集与完成
    /// 日志由 lyra 写出, 因为只有它知道选中的账号; 独立搜索的结果由 dispatch 自己采集, 因为
    /// 搜索轮次走的是 lyra 的缓冲入口.
    ///
    /// Serves a request on an antigravity route.
    ///
    /// The body's model name is rewritten to the route model before lyra translates it.
    /// Lyra writes the usage record and completion log of ordinary generation because only
    /// it knows the selected account; dispatch captures the isolated search result itself,
    /// because those rounds use lyra's buffered entry point.
    async fn serve_antigravity(
        &self,
        entry: &RouteEntry,
        mut fields: RawObject,
        model: String,
    ) -> Generation {
        let clamped =
            match canto::forward::clamp_output_ceiling(&mut fields, entry.max_output_tokens) {
                Ok(c) => c,
                Err(message) => return Generation::plain(invalid_body(message)),
            };
        if entry.moonshot_search && self.endpoint == Endpoint::Messages {
            let turn = AntigravityTurn::new(&self.state.relay);
            match search_plan(&fields) {
                SearchPlan::Rejected(response) => {
                    // 独立搜索分支与拒绝分支返回 Anthropic 格式的聚合响应, 因此用量按 messages 规则
                    // 采集且 provider 记为 messages, 账号取路由名, 并在结束时补上完成日志.
                    //
                    // The standalone search and rejection branches return aggregated responses in
                    // the Anthropic format, so usage is captured under the messages rule with
                    // provider messages, the account is the route name, and a completion log is
                    // emitted at the end.
                    let observation = self.state.observe("messages", &model);
                    if let Some(observation) = &observation {
                        observation.set_account(&entry.name);
                    }
                    let completion = Completion {
                        started: self.started,
                        target: self.target.clone(),
                        route: entry.name.clone(),
                        status: 0,
                        request_bytes: self.body.len(),
                    };
                    return Generation::observed(response, observation, Some(completion));
                }
                SearchPlan::Search { limits, stream } => {
                    let observation = self.state.observe("messages", &model);
                    if let Some(observation) = &observation {
                        observation.set_account(&entry.name);
                    }
                    let completion = Completion {
                        started: self.started,
                        target: self.target.clone(),
                        route: entry.name.clone(),
                        status: 0,
                        request_bytes: self.body.len(),
                    };
                    let response = self
                        .perform_search(fields, limits, &entry.model, &turn, stream)
                        .await;
                    return Generation::observed(response, observation, Some(completion));
                }
                SearchPlan::Native => {}
            }
        }
        let body = if !clamped && model == entry.model {
            self.body.clone()
        } else {
            match rewrite_model(&mut fields, &entry.model) {
                Ok(body) => body,
                Err(message) => return Generation::plain(invalid_body(message)),
            }
        };
        let observation = self.state.observe("antigravity", &model);
        let mut response = self
            .state
            .relay
            .messages(&self.parts.headers, body, &entry.name, observation.clone())
            .await;
        if let Some(observation) = &observation {
            observation.set_status(response.status().as_u16());
        }
        set_request_id(&mut response, &observation);
        Generation::plain(response)
    }

    /// 服务一条 messages 路由上的请求: 独立搜索或直接转发.
    ///
    /// Serves a request on a messages route: either the isolated search or a direct
    /// forward.
    async fn serve_messages(
        &self,
        entry: &RouteEntry,
        destination: &MessagesDestination,
        mut fields: RawObject,
        model: String,
    ) -> Generation {
        // count_tokens 不是生成请求, 不记录用量与完成日志; messages 路由没有账号, 常规生成的
        // account 取路由名, 与 architecture 5.8.3 相同.
        //
        // count_tokens is not a generation request and records neither usage nor completion logs;
        // a messages route has no account and the account of ordinary generation is the route name,
        // as section 5.8.3 requires.
        let (observation, completion) = if self.endpoint == Endpoint::CountTokens {
            (None, None)
        } else {
            let observation = self.state.observe("messages", &model);
            if let Some(observation) = &observation {
                observation.set_account(&entry.name);
            }
            let completion = Completion {
                started: self.started,
                target: self.target.clone(),
                route: entry.name.clone(),
                status: 0,
                request_bytes: self.body.len(),
            };
            (observation, Some(completion))
        };
        let clamped = if self.endpoint == Endpoint::CountTokens {
            false
        } else {
            match canto::forward::clamp_output_ceiling(&mut fields, entry.max_output_tokens) {
                Ok(c) => c,
                Err(message) => {
                    return Generation::observed(invalid_body(message), observation, completion);
                }
            }
        };
        // 搜索编排只拦截 /v1/messages 上的独立网页搜索请求, count_tokens 一律透传.
        //
        // The search orchestration only intercepts the isolated web search request on
        // /v1/messages; count_tokens is always forwarded.
        if entry.moonshot_search && self.endpoint == Endpoint::Messages {
            match search_plan(&fields) {
                SearchPlan::Rejected(response) => {
                    return Generation::observed(response, observation, completion);
                }
                SearchPlan::Search { limits, stream } => {
                    let response = self
                        .perform_search(fields, limits, destination.model(), destination, stream)
                        .await;
                    return Generation::observed(response, observation, completion);
                }
                SearchPlan::Native => {}
            }
        }
        let body = if !clamped && model == destination.model() {
            self.body.clone()
        } else {
            match rewrite_model(&mut fields, destination.model()) {
                Ok(body) => body,
                Err(message) => {
                    return Generation::observed(invalid_body(message), observation, completion);
                }
            }
        };
        let response = destination
            .forward(&self.parts.headers, &self.target, body)
            .await;
        Generation::observed(response, observation, completion)
    }

    /// 服务一条 chatgpt 或 xai 路由上的请求: 独立搜索或协议转换转发.
    ///
    /// 两条线路的分派规则完全相同, 由 line 选择 lyra 的入口与用量记录的 provider.
    ///
    /// Serves a request on a chatgpt or xai route: either isolated search or protocol
    /// translation. Both lines follow the same dispatch rules; line selects the lyra entry point
    /// and the provider of the usage record.
    async fn serve_responses_line(
        &self,
        entry: &RouteEntry,
        fields: RawObject,
        model: String,
        reasoning_effort: &str,
        line: ResponsesLine,
    ) -> Generation {
        if entry.moonshot_search && self.endpoint == Endpoint::Messages {
            let turn = ResponsesTurn::new(&self.state.relay, line, reasoning_effort, &entry.model);
            match search_plan(&fields) {
                SearchPlan::Rejected(response) => {
                    // 与 antigravity 路由相同: 搜索与拒绝分支返回 Anthropic 格式的聚合响应, 用量按
                    // messages 规则采集, 账号取路由名.
                    //
                    // As on antigravity routes, the search and rejection branches return aggregated
                    // Anthropic responses, so usage follows the messages rule with the route name as
                    // the account.
                    let observation = self.state.observe("messages", &model);
                    if let Some(observation) = &observation {
                        observation.set_account(&entry.name);
                    }
                    let completion = Completion {
                        started: self.started,
                        target: self.target.clone(),
                        route: entry.name.clone(),
                        status: 0,
                        request_bytes: self.body.len(),
                    };
                    return Generation::observed(response, observation, Some(completion));
                }
                SearchPlan::Search { limits, stream } => {
                    let observation = self.state.observe("messages", &model);
                    if let Some(observation) = &observation {
                        observation.set_account(&entry.name);
                    }
                    let completion = Completion {
                        started: self.started,
                        target: self.target.clone(),
                        route: entry.name.clone(),
                        status: 0,
                        request_bytes: self.body.len(),
                    };
                    let response = self
                        .perform_search(fields, limits, &entry.model, &turn, stream)
                        .await;
                    return Generation::observed(response, observation, Some(completion));
                }
                SearchPlan::Native => {}
            }
        } else if !entry.moonshot_search
            && self.endpoint == Endpoint::Messages
            && let Ok(Some(_)) = canto::search::search_options(&fields)
        {
            return Generation::plain(anthropic_error(
                StatusCode::BAD_REQUEST,
                Some("invalid_request_error"),
                "route does not support web search",
            ));
        }
        let observation = self.state.observe(line.provider(), &model);
        if let Some(observation) = &observation {
            observation.set_account(&entry.name);
        }
        let relay = &self.state.relay;
        let headers = &self.parts.headers;
        let mut response = match line {
            ResponsesLine::ChatGpt => {
                relay
                    .chatgpt_messages(
                        headers,
                        self.body.clone(),
                        &entry.name,
                        observation.clone(),
                        reasoning_effort,
                        &entry.model,
                    )
                    .await
            }
            ResponsesLine::Xai => {
                relay
                    .xai_messages(
                        headers,
                        self.body.clone(),
                        &entry.name,
                        observation.clone(),
                        reasoning_effort,
                        &entry.model,
                    )
                    .await
            }
            ResponsesLine::CommandCode => {
                relay
                    .commandcode_messages(
                        headers,
                        self.body.clone(),
                        &entry.name,
                        observation.clone(),
                        reasoning_effort,
                        &entry.model,
                    )
                    .await
            }
        };
        if let Some(observation) = &observation {
            observation.set_status(response.status().as_u16());
        }
        set_request_id(&mut response, &observation);
        Generation::plain(response)
    }

    /// 执行一次受限搜索编排.
    ///
    /// 搜索轮次由 turn 执行: messages 路由用 canto 的转发目标, antigravity 路由用经过 lyra
    /// 的缓冲入口.
    ///
    /// Runs one bounded search orchestration.
    ///
    /// The rounds run through turn: a messages route uses canto's forwarding destination and
    /// an antigravity route uses the buffered entry point through lyra.
    async fn perform_search(
        &self,
        fields: RawObject,
        limits: SearchLimits,
        model: &str,
        turn: &dyn SearchTurn,
        stream: bool,
    ) -> Response<Body> {
        let Some(search) = &self.state.search else {
            // 配置校验保证声明 moonshot_search 的路由一定有搜索服务, 因此这里只在配置被绕过
            // 时出现, 返回与搜索服务失败相同的 502.
            //
            // Configuration validation guarantees that a route declaring moonshot_search
            // has a search service, so this only happens if that validation is bypassed;
            // it returns the same 502 as a failed search service.
            return anthropic_error(
                StatusCode::BAD_GATEWAY,
                Some("api_error"),
                "search workflow failed",
            );
        };
        match run_search(
            fields,
            limits,
            model,
            self.state.max_body,
            turn,
            search,
            &self.parts.headers,
        )
        .await
        {
            Ok(message) => write_search_message(&message, stream),
            Err(failure) => failure.into_response(),
        }
    }

    /// 服务 Responses 透传请求.
    ///
    /// Serves a Responses passthrough request.
    async fn serve_responses(&self) -> Generation {
        if !self.state.has_chatgpt() {
            return Generation::plain(openai_error(StatusCode::NOT_FOUND, "unsupported path"));
        }
        let model = probe_model(&self.body);
        let observation = self.state.observe("chatgpt", &model);
        let path = self.parts.uri.path();
        let mut response = self
            .state
            .relay
            .responses(
                &self.parts.headers,
                path,
                self.body.clone(),
                observation.clone(),
            )
            .await;
        if let Some(observation) = &observation {
            observation.set_status(response.status().as_u16());
        }
        set_request_id(&mut response, &observation);
        Generation::plain(response)
    }
}

/// 一次 messages 请求的搜索判定结果.
///
/// 用枚举而不是 Result<Option<..>, Response<..>>: 被拒绝时携带的是完整响应, 放进 Err 会让
/// 每个 Result 都带上一个很大的错误类型.
///
/// The search decision for one messages request.
///
/// An enum rather than Result<Option<..>, Response<..>>: a rejection carries a whole
/// response, and putting that in Err would make every Result of this shape large.
enum SearchPlan {
    /// 请求必须按原生方式转发.
    ///
    /// The request must be forwarded natively.
    Native,
    /// 请求是独立搜索, 携带轮次预算与是否流式输出.
    ///
    /// The request is the isolated search, carrying its round budget and streaming flag.
    Search { limits: SearchLimits, stream: bool },
    /// 请求被拒绝, 已经带有 400 响应.
    ///
    /// The request was rejected, carrying its 400 response.
    Rejected(Response<Body>),
}

/// 判断一次 messages 请求是否属于独立搜索, 并准备编排所需的参数.
///
/// 校验失败时直接给出 400 响应, 因此调用方不需要重复映射错误信息.
///
/// Decides whether a messages request is the isolated search and prepares the parameters of
/// the orchestration.
///
/// A failed check already carries its 400 response, so the caller never maps an error
/// message twice.
fn search_plan(fields: &RawObject) -> SearchPlan {
    match search_options(fields) {
        Err(message) => SearchPlan::Rejected(anthropic_error(
            StatusCode::BAD_REQUEST,
            Some("invalid_request_error"),
            &message,
        )),
        Ok(None) => SearchPlan::Native,
        Ok(Some(limits)) => match search_stream(fields) {
            Ok(stream) => SearchPlan::Search { limits, stream },
            Err(message) => SearchPlan::Rejected(anthropic_error(
                StatusCode::BAD_REQUEST,
                Some("invalid_request_error"),
                &message,
            )),
        },
    }
}

/// 构造请求体改写失败的响应.
///
/// Builds the response for a failed body rewrite.
fn invalid_body(message: String) -> Response<Body> {
    anthropic_error(
        StatusCode::BAD_REQUEST,
        Some("invalid_request_error"),
        &message,
    )
}

/// 解析 Responses 请求体中的模型名, 供用量记录使用.
///
/// 只读取一个字段, 正文本身原样透传; 解析失败时返回空字符串, 与 Lyra 的 requestShape 相同.
///
/// Reads the model name of a Responses body for the usage record.
///
/// Only one field is read and the body itself is passed through unchanged; a parse failure
/// yields an empty string, matching Lyra's requestShape.
fn probe_model(body: &[u8]) -> String {
    #[derive(Deserialize)]
    struct Probe {
        #[serde(default)]
        model: String,
    }
    serde_json::from_slice::<Probe>(body)
        .map(|probe| probe.model)
        .unwrap_or_default()
}

/// 写入 X-Relay-Request-Id 响应头.
///
/// Writes the X-Relay-Request-Id response header.
fn set_request_id(response: &mut Response<Body>, observation: &Option<Observation>) {
    if let Some(observation) = observation
        && let Ok(value) = HeaderValue::from_str(observation.id())
    {
        response.headers_mut().insert("x-relay-request-id", value);
    }
}

/// 处理 GET /v1/broker/chatgpt 分发请求.
///
/// 查询参数 account 可选, force 可选. 对每个目标账号调用凭据池的 authorize, 不新鲜时刷新并
/// 写回, 返回规格规定的 7 个字段. 刷新失败的账号不出现在结果中; 全部失败时返回 502.
///
/// Serves GET /v1/broker/chatgpt broker requests.
///
/// The query parameters account and force are optional. Calls the credential pool's authorize
/// for each target account, refreshing and writing back when stale, returning the 7 fields
/// specified. Accounts failing to refresh are omitted; returns 502 if all fail.
async fn broker_chatgpt(state: &AppState, parts: Parts) -> Response<Body> {
    let (account, force) = parse_broker_query(parts.uri.query());
    match state.relay.broker_chatgpt(account.as_deref(), force).await {
        Ok(credentials) => {
            let payload = json!({"credentials": credentials});
            json_response(
                StatusCode::OK,
                serde_json::to_vec(&payload).unwrap_or_default(),
            )
        }
        Err(error) => {
            aria_core::log!("GET /v1/broker/chatgpt: broker request failed: {}", error);
            anthropic_error(
                StatusCode::BAD_GATEWAY,
                Some("api_error"),
                &error.to_string(),
            )
        }
    }
}

/// 解析分发端点的查询参数 account 与 force.
///
/// Parses the account and force query parameters of the broker endpoint.
fn parse_broker_query(query: Option<&str>) -> (Option<String>, bool) {
    let mut account = None;
    let mut force = false;
    if let Some(query) = query {
        let fake_url = format!("http://localhost/?{query}");
        if let Ok(url) = reqwest::Url::parse(&fake_url) {
            for (key, val) in url.query_pairs() {
                if key == "account" && !val.is_empty() {
                    account = Some(val.into_owned());
                } else if key == "force" && val == "1" {
                    force = true;
                }
            }
        }
    }
    (account, force)
}

/// 处理 /healthz 与 /v1/healthz.
///
/// 除账号状态与冷却记录, 还给出按账号与模型区分的不可用记录 (unavailable_models, 见
/// architecture.md 5.12 与 5.13) 与按路由的不可用记录 (unavailable_routes, 见 5.13), 供派发
/// 子会话前查询.
///
/// Serves /healthz and /v1/healthz.
///
/// Besides the account statuses and cooldown records it reports the per account and model
/// unavailable records (unavailable_models, see sections 5.12 and 5.13 of architecture.md) and the
/// per route unavailable records (unavailable_routes, see 5.13), so a caller can check them before
/// dispatching a sub-session.
async fn healthz(state: &AppState) -> Response<Body> {
    match state.relay.statuses().await {
        Ok(statuses) => {
            // client 字段给出各线路当前使用的版本与来源, 来源取 fetched, cached 或 fallback,
            // 见 architecture.md 3.8.
            //
            // The client field states the version and source currently in use on each line; the
            // source is fetched, cached or fallback, see section 3.8 of architecture.md.
            let (antigravity, chatgpt, xai) = state.identity_snapshots();
            let payload = json!({
                "status": "ok",
                "credentials": statuses,
                "cooldowns": state.relay.cooldowns(),
                "client": {
                    "antigravity": antigravity,
                    "chatgpt": chatgpt,
                    "xai": xai,
                },
                "unavailable_models": state.relay.unavailable_models(),
                "unavailable_routes": state.unavailable_routes(),
            });
            // 载荷由本模块在内存中构造, 编码不会失败; 失败时返回空对象而不是中断请求.
            //
            // The payload is built in memory and encoding cannot fail; on a failure an
            // empty object is returned instead of breaking the request.
            json_response(
                StatusCode::OK,
                serde_json::to_vec(&payload).unwrap_or_default(),
            )
        }
        Err(message) => anthropic_error(StatusCode::INTERNAL_SERVER_ERROR, None, &message),
    }
}

/// 处理 /v1/usage.
///
/// 参数校验与响应格式同 Lyra relay/usage.go 的 serveUsage; 存储查询是同步文件操作, 因此放进
/// spawn_blocking, 不阻塞运行时线程.
///
/// Serves /v1/usage.
///
/// Parameter validation and the response format match serveUsage in Lyra's relay/usage.go;
/// the store query is a synchronous file operation and therefore runs in spawn_blocking
/// instead of blocking a runtime thread.
async fn usage(state: &AppState, raw_query: Option<&str>) -> Response<Body> {
    let Some(usage) = &state.usage else {
        return anthropic_error(
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            "usage statistics are disabled",
        );
    };
    let filter = match parse_usage_filter(raw_query.unwrap_or_default()) {
        Ok(filter) => filter,
        Err(message) => {
            return anthropic_error(
                StatusCode::BAD_REQUEST,
                Some("invalid_request_error"),
                &message,
            );
        }
    };
    let store = usage.store.clone();
    let queried = tokio::task::spawn_blocking(move || store.query(filter)).await;
    match queried {
        Ok(Ok(summary)) => {
            let mut payload = serde_json::to_value(&summary).unwrap_or(Value::Null);
            if let Value::Object(fields) = &mut payload {
                fields.insert(
                    "write_failures_since_start".to_owned(),
                    json!(usage.write_failures.load(Ordering::Relaxed)),
                );
            }
            json_response(
                StatusCode::OK,
                serde_json::to_vec(&payload).unwrap_or_default(),
            )
        }
        Ok(Err(error)) => match error.kind() {
            ErrorKind::InvalidFilter => anthropic_error(
                StatusCode::BAD_REQUEST,
                Some("invalid_request_error"),
                &error.to_string(),
            ),
            _ => {
                aria_core::log!("usage query failed: {error}");
                anthropic_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    None,
                    "usage statistics could not be read",
                )
            }
        },
        Err(_) => {
            aria_core::log!("usage query task failed");
            anthropic_error(
                StatusCode::SERVICE_UNAVAILABLE,
                None,
                "usage statistics could not be read",
            )
        }
    }
}

/// 解析用量查询参数.
///
/// 规则与 Go 的 url.ParseQuery 加 serveUsage 的校验相同: 解析失败, 未知参数, 重复参数与空
/// 取值都返回 400.
///
/// Parses the usage query parameters.
///
/// The rules match Go's url.ParseQuery plus the serveUsage checks: a parse failure, an
/// unknown parameter, a repeated parameter and an empty value all return 400.
fn parse_usage_filter(raw_query: &str) -> Result<Filter, String> {
    let pairs = parse_query(raw_query)?;
    for (name, _) in &pairs {
        if !USAGE_PARAMETERS.contains(&name.as_str()) {
            return Err("unknown usage query parameter".to_owned());
        }
    }
    let mut seen = BTreeSet::new();
    for (name, value) in &pairs {
        if value.is_empty() || !seen.insert(name.clone()) {
            return Err("usage query parameters require one nonempty value".to_owned());
        }
    }
    let mut filter = Filter::default();
    for (name, value) in pairs {
        match name.as_str() {
            "from" => filter.from = value,
            "to" => filter.to = value,
            "model" => filter.model = value,
            "account" => filter.account = value,
            "provider" => filter.provider = value,
            "group_by" => filter.group_by = value.split(',').map(str::to_owned).collect(),
            _ => {}
        }
    }
    Ok(filter)
}

/// 解析查询串为键值对, 失败时返回固定的 400 信息.
///
/// 与 Go 的 url.ParseQuery 保持一致的两处细节: 分号是明确拒绝的分隔符, 百分号转义不完整或
/// 不是十六进制时整个查询无效, 而不是把该取值当成原文.
///
/// Parses a query string into pairs, returning the fixed 400 message on failure.
///
/// Two details follow Go's url.ParseQuery: a semicolon is an explicitly rejected separator,
/// and an incomplete or non-hexadecimal percent escape makes the whole query invalid
/// instead of being taken literally.
fn parse_query(raw_query: &str) -> Result<Vec<(String, String)>, String> {
    if raw_query.contains(';') {
        return Err("invalid usage query".to_owned());
    }
    let mut pairs = Vec::new();
    for pair in raw_query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (name, value) = match pair.split_once('=') {
            Some((name, value)) => (name, value),
            None => (pair, ""),
        };
        let name = percent_decode(name).map_err(|_| "invalid usage query".to_owned())?;
        let value = percent_decode(value).map_err(|_| "invalid usage query".to_owned())?;
        pairs.push((name, value));
    }
    Ok(pairs)
}

/// 解码一个查询串片段: 加号是空格, 百分号转义按十六进制解析.
///
/// Decodes one query fragment: a plus is a space and a percent escape is parsed as
/// hexadecimal.
fn percent_decode(text: &str) -> Result<String, ()> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let high = bytes.get(index + 1).and_then(|byte| hex_value(*byte));
                let low = bytes.get(index + 2).and_then(|byte| hex_value(*byte));
                match (high, low) {
                    (Some(high), Some(low)) => decoded.push(high * 16 + low),
                    _ => return Err(()),
                }
                index += 3;
            }
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    // Go 的字符串可以承载非法 UTF-8, Rust 的字符串不能; 查询参数是过滤条件而不是要原样转发
    // 的数据, 因此有损转换即可.
    //
    // Go strings may hold invalid UTF-8 while Rust strings may not; a query parameter is a
    // filter value rather than data to forward verbatim, so a lossy conversion is enough.
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}

/// 返回一个十六进制字符的数值.
///
/// Returns the value of one hexadecimal digit.
fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// 构造 404 响应.
///
/// Builds a 404 response.
fn not_found(style: ErrorStyle) -> Response<Body> {
    style.error(StatusCode::NOT_FOUND, "not_found_error", "unsupported path")
}

/// 构造 501 响应.
///
/// 与 Lyra 相同: 不写响应体, 也不设置内容类型.
///
/// Builds a 501 response.
///
/// As in Lyra: no response body and no content type.
fn not_implemented() -> Response<Body> {
    let mut response = Response::new(empty());
    *response.status_mut() = StatusCode::NOT_IMPLEMENTED;
    response
}

/// 写一行访问日志, 格式与 Lyra 的 accessLog 相同.
///
/// Writes one access log line in the same shape as Lyra's accessLog.
fn access_log(peer: Option<SocketAddr>, method: &Method, target: &str, status: StatusCode) {
    let host = peer
        .map(|address| address.ip().to_string())
        .unwrap_or_default();
    // 与 Go 的访问日志一致: 记录远端主机, 方法, 原始请求目标与状态码.
    //
    // Matching Go's access log: the remote host, the method, the raw request target and the
    // status code.
    aria_core::log!(
        "{} \"{} {} HTTP/1.1\" {} -",
        host,
        method,
        target,
        status.as_u16()
    );
}

/// 一次生成请求的响应及其附属状态.
///
/// The response of one generation request together with its attached state.
struct Generation {
    response: Response<Body>,
    /// 用量观测. messages 路由与 antigravity 路由的搜索分支由 dispatch 采集响应体字节;
    /// 其余分支的采集在 lyra 内部完成, 这里只用于写入请求编号响应头.
    ///
    /// The usage observation. Dispatch captures the body bytes of a messages route and of
    /// the search branch of an antigravity route; the other branches capture inside lyra and
    /// this only carries the request identifier header.
    usage: Option<Observation>,
    /// 完成日志, 只有 messages 路由写出.
    ///
    /// The completion log, written only by messages routes.
    completion: Option<Completion>,
}

impl Generation {
    /// 构造一个不带用量采集与完成日志的响应.
    ///
    /// Builds a response without usage capture or a completion log.
    fn plain(response: Response<Body>) -> Generation {
        Generation {
            response,
            usage: None,
            completion: None,
        }
    }

    /// 构造带用量观测的响应, 并写入请求编号响应头.
    ///
    /// Builds a response carrying a usage observation and writing the request identifier
    /// header.
    fn observed(
        mut response: Response<Body>,
        usage: Option<Observation>,
        completion: Option<Completion>,
    ) -> Generation {
        set_request_id(&mut response, &usage);
        Generation {
            response,
            usage,
            completion,
        }
    }

    /// 把并发许可, 用量采集与完成日志附加到响应体上.
    ///
    /// 状态码在响应头确定时写入观测句柄, 因为此时它已经不会再变化.
    ///
    /// Attaches the concurrency permit, usage capture and completion log to the response
    /// body.
    ///
    /// The status code is written into the observation as soon as the response headers
    /// exist, because it cannot change afterwards.
    fn guard(self, permit: OwnedSemaphorePermit) -> Response<Body> {
        let (parts, body) = self.response.into_parts();
        if let Some(usage) = &self.usage {
            usage.set_status(parts.status.as_u16());
            usage.begin_body(content_type(&parts.headers));
        }
        let completion = self
            .completion
            .map(|completion| completion.with_status(parts.status.as_u16()));
        let guarded = GuardedBody {
            inner: body,
            _permit: permit,
            usage: self.usage,
            completion,
            response_bytes: 0,
            expected_bytes: content_length(&parts.headers),
            completed: false,
        };
        Response::from_parts(parts, BodyExt::boxed(guarded))
    }
}

/// 完成日志的固定字段.
///
/// The fixed fields of a completion log line.
struct Completion {
    started: Instant,
    target: String,
    route: String,
    status: u16,
    request_bytes: usize,
}

impl Completion {
    /// 补上最终状态码.
    ///
    /// Fills in the final status code.
    fn with_status(mut self, status: u16) -> Completion {
        self.status = status;
        self
    }

    /// 写出一行完成日志.
    ///
    /// 字段与 Lyra 的完成日志相同, 其中 credential 换成 route: messages 路由没有账号, 选择
    /// 上游的是路由名; 账号线路的完成日志由 lyra 自己写出, 它才知道账号名.
    ///
    /// Writes one completion log line.
    ///
    /// The fields match Lyra's completion log with route in place of credential: a messages
    /// route has no account and the route name selects the upstream, while the
    /// account-backed lines write their own completion log inside lyra, which is the only
    /// place that knows the account name.
    fn emit(&self, response_bytes: usize, completed: bool) {
        let suffix = if completed { "" } else { " incomplete" };
        aria_core::log!(
            "POST {} route={} upstream={} request={}B response={}B elapsed={:.1}s{}",
            self.target,
            self.route,
            self.status,
            self.request_bytes,
            response_bytes,
            self.started.elapsed().as_secs_f64(),
            suffix
        );
    }
}

/// 附加在响应体上的并发许可, 用量采集与完成日志.
///
/// 三件事都依赖响应体的生命周期, 因此放在同一个包装里: 许可必须持有到响应体结束, 采集需要
/// 逐块读取响应体字节, 完成日志需要最终的字节数与是否读完. 分开包装会让三者各自持有一份
/// 响应体, 徒增复杂度.
///
/// The concurrency permit, usage capture and completion log attached to a response body.
///
/// All three depend on the body's lifetime, so they share one wrapper: the permit must be
/// held until the body ends, the capture reads the body chunk by chunk, and the completion
/// log needs the final byte count and whether the body finished. Separate wrappers would
/// each hold a copy of the body and add complexity for nothing.
struct GuardedBody {
    inner: Body,
    /// 许可只被持有, 不参与读写.
    ///
    /// The permit is only held; it takes no part in reading or writing.
    _permit: OwnedSemaphorePermit,
    usage: Option<Observation>,
    completion: Option<Completion>,
    response_bytes: usize,
    /// 响应头给出的响应体长度.
    ///
    /// hyper 在中继带 Content-Length 的响应时按该长度结束响应, 不会再多轮询一次去取结束帧,
    /// 因此这里用它判断响应体是否完整送出; 上游流到一半断开时长度达不到, 结果仍是未完成.
    ///
    /// The body length given by the response headers.
    ///
    /// When relaying a response with Content-Length, hyper ends the response at that length and
    /// never polls once more for the closing frame, so this value decides whether the body was
    /// delivered in full; a stream that breaks half way never reaches it and stays incomplete.
    expected_bytes: Option<usize>,
    completed: bool,
}

impl GuardedBody {
    /// 记录响应体正常读完.
    ///
    /// 结束信号有三个: 达到响应头给出的长度, 中继协议自己声明响应体结束, 或者上游流返回结束
    /// 帧. 三者覆盖了 hyper 结束响应的全部方式. 结束时显式调用 finish(false) 表示客户端没有
    /// 断开; 客户端提前断开时不会走到这里, Observation 的析构会按客户端断开记录一次, 因此每个
    /// 请求恰好记录一次.
    ///
    /// Records that the response body was read to its end.
    ///
    /// finish(false) is called explicitly here because the client did not disconnect; an
    /// early disconnect never reaches this point and the Observation destructor records
    /// that case instead, so every request is recorded exactly once.
    fn complete(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        if let Some(usage) = &self.usage {
            usage.finish_chunks();
            usage.set_transport_complete(true);
            usage.finish(false);
        }
    }
}

impl HttpBody for GuardedBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        // GuardedBody 的全部字段都是 Unpin, 因此可以安全地取得可变引用.
        //
        // Every field of GuardedBody is Unpin, so a mutable reference can be taken safely.
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.response_bytes += data.len();
                    if let Some(usage) = &this.usage {
                        usage.observe_chunk(data);
                    }
                }
                // hyper 在写出每一帧之后用 is_end_stream 判断响应体是否结束, 知道长度时不会再
                // 轮询一次, 因此这里用同一次判断记录完成状态; 只在 None 上判断会让这类响应永远
                // 记为未完成. 当响应头声明了 Content-Length 时, 只有实际写出字节数达到该长度才判为
                // 完成, 避免底层提前中断却被误判为正常结束.
                //
                // hyper decides the body has ended through is_end_stream after writing each frame
                // and does not poll again when the length is known, so the same check records the
                // completion here; relying on the None frame alone would leave those responses
                // recorded as incomplete forever. When Content-Length is declared, the body is
                // considered completed only when the bytes written reach that length, preventing
                // truncated streams from being falsely marked completed.
                let delivered = this
                    .expected_bytes
                    .is_some_and(|expected| this.response_bytes >= expected);
                if delivered || (this.expected_bytes.is_none() && this.inner.is_end_stream()) {
                    this.complete();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => {
                let finished = match this.expected_bytes {
                    Some(expected) => this.response_bytes >= expected,
                    None => true,
                };
                if finished {
                    this.complete();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

impl Drop for GuardedBody {
    fn drop(&mut self) {
        // 完成日志在响应体结束时写出, 因此它记录的是最终字节数以及流是否被读完.
        //
        // The completion log is written when the body ends, so it records the final byte
        // count and whether the stream finished.
        if let Some(completion) = &self.completion {
            completion.emit(self.response_bytes, self.completed);
        }
    }
}

/// 读取响应头给出的响应体长度, 缺失或不是数字时返回 None.
///
/// Reads the body length from the response headers, or None when it is absent or not a number.
fn content_length(headers: &HeaderMap) -> Option<usize> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

/// 读取响应头中的内容类型, 缺失时返回空字符串.
///
/// Reads the content type of the response headers, or an empty string when absent.
fn content_type(headers: &HeaderMap) -> &str {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
}
