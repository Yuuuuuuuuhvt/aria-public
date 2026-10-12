//! aria serve 的统一配置解析与校验, 格式见 docs/architecture.md 第 3 节.
//!
//! 解析拒绝未知字段并且只接受一个 JSON 对象, 因此拼写错误的选项会在启动阶段报错, 而不是
//! 被静默忽略. 所有校验失败都指出出错的字段与路由名, 并且不输出任何密钥.
//!
//! 校验分为两步: 本模块负责字段组合, 路由名唯一性与提供方依赖等结构性规则; 基址与上游密钥
//! 的规则沿用 Canto, 由构造 canto::forward::MessagesDestination 时执行, 因此两条路径共用
//! 同一份判断, 不会出现两套不一致的规则.
//!
//! Configuration parsing and validation for aria serve; the format is section 3 of
//! docs/architecture.md.
//!
//! Parsing rejects unknown fields and accepts exactly one JSON object, so a misspelled
//! option fails at startup instead of being ignored. Every validation failure names the
//! offending field and route and never prints a secret.
//!
//! Validation happens in two steps: this module enforces structural rules such as field
//! combinations, unique route names and provider requirements; the base URL and upstream
//! key rules are Canto's and run when canto::forward::MessagesDestination is built, so both
//! paths share one judgement instead of two rule sets that could disagree.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use canto::forward::{MessagesRoute, MessagesUpstream};
use canto::search::SearchServiceConfig;
use lyra::client_id::ClientConfig;
use lyra::credential::google::GOOGLE_CLIENT_SECRET_ENV;
use serde::Deserialize;

/// 顶层字段的默认值, 与 docs/architecture.md 3.1 的默认值表一致.
///
/// The defaults of the top-level fields, matching the default table in section 3.1 of
/// docs/architecture.md.
const DEFAULT_LISTEN: &str = "127.0.0.1:20182";
const DEFAULT_MAX_CONCURRENCY: u64 = 100;
const DEFAULT_MAX_BODY_BYTES: u64 = 33_554_432;
const DEFAULT_UPSTREAM_IDLE_TIMEOUT_SECS: u64 = 600;
const DEFAULT_UPSTREAM_FIRST_BYTE_TIMEOUT_SECS: u64 = 120;
const DEFAULT_TIMEOUT_COOLDOWN_THRESHOLD: u64 = 2;
const DEFAULT_TIMEOUT_COOLDOWN_SECS: u64 = 300;

/// 并发上限的取值上限.
///
/// tokio 的 Semaphore 对许可数量有内部上限, 超过时会 panic; 这里在配置阶段拒绝不合理的取值,
/// 使错误在启动时以配置错误的形式出现, 而不是进程崩溃.
///
/// The upper bound of the concurrency limit.
///
/// tokio's Semaphore has an internal permit limit and panics beyond it; an unreasonable value
/// is rejected here so the failure appears at startup as a configuration error instead of a
/// process crash.
const MAX_CONCURRENCY_LIMIT: u64 = 1_000_000;

/// providers.antigravity 的默认值.
///
/// The defaults of providers.antigravity.
const DEFAULT_ANTIGRAVITY_BASE_URL: &str = "https://daily-cloudcode-pa.googleapis.com";
const DEFAULT_GOOGLE_CLIENT_ID: &str =
    "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";

/// providers.chatgpt 的默认基址.
///
/// The default base URL of providers.chatgpt.
const DEFAULT_CHATGPT_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";

/// providers.qoder 的默认基址与环境变量名.
///
/// The default base URL and environment variable of providers.qoder.
const DEFAULT_QODER_BASE_URL: &str = "https://gateway.qoder.com.cn";
const DEFAULT_QODER_TOKEN_ENV: &str = "QODER_CN_PAT";

/// 一条路由的解析结果.
///
/// 两种路由类型共享名称, 展示名, 别名, 上游模型名与搜索声明; 类型特有的字段放在 RouteKind
/// 中, 因此读取方必须显式区分类型, 不会把 messages 的上游字段误用到 antigravity 路由上.
///
/// One parsed route.
///
/// Both route types share the name, display name, aliases, upstream model name and search
/// declaration; type-specific fields live in RouteKind, so a reader must discriminate
/// explicitly and cannot apply messages upstream fields to an antigravity route.
pub struct RouteSpec {
    /// 路由名, 也是可请求的模型名.
    ///
    /// The route name, which is also a requestable model name.
    pub name: String,
    /// 模型列表中的展示名.
    ///
    /// The display name shown in the model list.
    pub display_name: String,
    /// 其他可请求的模型名.
    ///
    /// Further requestable model names.
    pub aliases: Vec<String>,
    /// 发给上游的模型名.
    ///
    /// The model name sent upstream.
    pub model: String,
    /// 为真时独立网页搜索走 Kimi 搜索服务.
    ///
    /// When true, standalone web search goes to the Kimi search service.
    pub moonshot_search: bool,
    /// 路由允许的最大输出 token 数, 仅 messages 与 antigravity 路由支持.
    ///
    /// The maximum output tokens allowed for the route, supported only by messages and antigravity.
    pub max_output_tokens: Option<i64>,
    /// 路由类型特有的字段.
    ///
    /// The fields specific to this route type.
    pub kind: RouteKind,
}

/// 路由类型及其特有字段.
///
/// The route type and its specific fields.
pub enum RouteKind {
    /// 上游已经实现 Anthropic Messages 协议的路由. 单上游写法与多上游写法在这里已经归一为
    /// 一份数据, 因此后续代码不需要再区分两种写法.
    ///
    /// A route whose upstream already implements Anthropic Messages. The single-upstream
    /// and multi-upstream forms are already normalized into one shape here, so later code
    /// never distinguishes them.
    Messages {
        /// 上游 Messages 基址, 网关追加 /v1/messages.
        ///
        /// The upstream Messages base URL; the gateway appends /v1/messages.
        base_url: String,
        /// 环境变量非空时覆盖 base_url.
        ///
        /// Overrides base_url when the environment variable is non-empty.
        base_url_env: String,
        /// 上游密钥所在的环境变量.
        ///
        /// The environment variable holding the upstream key.
        key_env: String,
        /// 多上游写法中的上游列表, 为空表示单上游写法.
        ///
        /// The upstream list of the multi-upstream form; empty selects the single-upstream
        /// form.
        upstreams: Vec<UpstreamSpec>,
    },
    /// 订阅账号线路上的路由, 由 lyra 完成协议转换.
    ///
    /// A route on a subscription-backed line; lyra performs the protocol translation.
    Antigravity,
    /// 经 Codex 渠道使用 GPT 模型的路由, 由 lyra 完成 Responses 协议转换.
    ///
    /// A route using GPT models via Codex; lyra performs Responses protocol conversion.
    ChatGpt {
        /// 默认思考强度档位: low, medium, high 或 xhigh.
        ///
        /// Default reasoning effort tier: low, medium, high or xhigh.
        reasoning_effort: String,
    },
    /// 经 SuperGrok 订阅使用 Grok 模型的路由, 由 lyra 完成 Responses 协议转换.
    ///
    /// A route using Grok models via a SuperGrok subscription; lyra performs Responses protocol
    /// conversion.
    Xai {
        /// 默认思考强度档位: low, medium, high 或 xhigh.
        ///
        /// Default reasoning effort tier: low, medium, high or xhigh.
        reasoning_effort: String,
    },
    /// 经 Command Code Provider API 使用 GLM-5.3 的路由, 由 lyra 完成 Responses 协议转换.
    ///
    /// A route using GLM-5.3 via Command Code's Provider API; lyra performs Responses protocol
    /// conversion.
    CommandCode {
        /// 默认思考强度档位: low, medium, high 或 xhigh.
        ///
        /// Default reasoning effort tier: low, medium, high or xhigh.
        reasoning_effort: String,
    },
    /// Qoder 网关线路上的路由, 由 lyra 完成请求编码, Bearer 鉴权与流式转码.
    ///
    /// A route on the Qoder gateway line; lyra performs request encoding, Bearer signing,
    /// and streaming translation.
    Qoder,
}

/// 多上游写法中的一条上游.
///
/// One upstream of the multi-upstream form.
pub struct UpstreamSpec {
    pub base_url: String,
    pub base_url_env: String,
    pub key_env: String,
}

impl RouteSpec {
    /// 构造 canto 的 messages 路由描述. antigravity 路由返回 None.
    ///
    /// 每次调用都新建一份值, 因为 canto::forward::MessagesRoute 不实现 Clone, 而配置中的
    /// 一条路由恰好只构造一个目标.
    ///
    /// Builds the canto messages route description; an antigravity route returns None.
    ///
    /// Every call builds a fresh value because canto::forward::MessagesRoute does not
    /// implement Clone and one configured route builds exactly one destination.
    pub fn messages_route(&self) -> Option<MessagesRoute> {
        match &self.kind {
            RouteKind::Messages {
                base_url,
                base_url_env,
                key_env,
                upstreams,
            } => Some(MessagesRoute {
                name: self.name.clone(),
                display_name: self.display_name.clone(),
                aliases: self.aliases.clone(),
                model: self.model.clone(),
                base_url: base_url.clone(),
                base_url_env: base_url_env.clone(),
                key_env: key_env.clone(),
                upstreams: upstreams
                    .iter()
                    .map(|upstream| MessagesUpstream {
                        base_url: upstream.base_url.clone(),
                        base_url_env: upstream.base_url_env.clone(),
                        key_env: upstream.key_env.clone(),
                    })
                    .collect(),
                moonshot_search: self.moonshot_search,
            }),
            RouteKind::Antigravity
            | RouteKind::ChatGpt { .. }
            | RouteKind::Xai { .. }
            | RouteKind::CommandCode { .. }
            | RouteKind::Qoder => None,
        }
    }
}

/// providers.antigravity 的解析结果.
///
/// The parsed providers.antigravity.
pub struct AntigravityProvider {
    pub credentials: PathBuf,
    pub base_url: String,
    pub client_id: String,
    pub client_secret: String,
    /// 客户端标识配置, 见 architecture.md 3.8.
    ///
    /// The client-identifier configuration, see section 3.8 of architecture.md.
    pub client: ClientConfig,
    /// 连续首字节超时触发冷却的次数上限.
    pub timeout_cooldown_threshold: u32,
    /// 连续首字节超时冷却时长.
    pub timeout_cooldown_duration: Duration,
}

/// ChatGPT 的凭据来源: 本地文件或令牌分发来源.
///
/// Credential source for ChatGPT: local file or token broker.
#[derive(Clone, Debug, PartialEq)]
pub enum ChatGptSource {
    Credentials(PathBuf),
    Broker { url: String, key_env: String },
}

/// providers.chatgpt 的解析结果.
///
/// The parsed providers.chatgpt.
pub struct ChatGptProvider {
    pub source: ChatGptSource,
    pub base_url: String,
    pub models: Vec<String>,
    pub broker_clients: Option<PathBuf>,
    /// 客户端标识配置, 见 architecture.md 3.8.
    ///
    /// The client-identifier configuration, see section 3.8 of architecture.md.
    pub client: ClientConfig,
}

/// providers.xai 的解析结果. 凭据来源只有本地凭据文件一种, 令牌分发留给以后.
///
/// The parsed providers.xai. The only credential source is a local credential file; token
/// brokering is left for later.
pub struct XaiProvider {
    pub credentials: PathBuf,
    pub base_url: String,
    pub token_url: String,
    pub client_id: String,
    pub scope: String,
    pub models: Vec<String>,
    /// 客户端标识配置, 见 architecture.md 3.8.
    ///
    /// The client-identifier configuration, see section 3.8 of architecture.md.
    pub client: ClientConfig,
}

/// providers.commandcode 的解析结果.
///
/// The parsed providers.commandcode.
pub struct CommandCodeProvider {
    pub base_url: String,
    pub key_env: String,
    pub models: Vec<String>,
}

/// providers.qoder 的解析结果.
///
/// The parsed providers.qoder.
pub struct QoderProvider {
    pub base_url: String,
    pub token_env: String,
    pub machine_file: Option<PathBuf>,
    pub max_concurrency: usize,
    pub models: Vec<String>,
}

/// aria serve 的完整配置.
///
/// 限制项已经转换成运行期类型, 因此使用方不需要再次解析或判断正负.
///
/// The complete configuration of aria serve.
///
/// Limits are already converted into runtime types, so no consumer parses them again or
/// checks their sign.
pub struct Config {
    /// 监听地址, 命令行 --listen 优先.
    ///
    /// The listen address; the command line --listen takes precedence.
    pub listen: String,
    /// 客户端密钥文件路径, 命令行 --clients 优先.
    ///
    /// The client key file path; the command line --clients takes precedence.
    pub clients: Option<String>,
    pub max_concurrency: usize,
    pub max_body_bytes: usize,
    /// 等待上游响应头与响应体任意字节的最长空闲时间.
    ///
    /// The longest idle time while waiting for upstream response headers or body bytes.
    pub upstream_idle_timeout: Duration,
    /// 等待上游响应头与首个有效事件的最长首字节时间.
    ///
    /// The longest first-byte time while waiting for upstream response headers and the first event.
    pub upstream_first_byte_timeout: Duration,
    /// 设置后启用用量采集与 GET /v1/usage.
    ///
    /// When set, usage capture and GET /v1/usage are enabled.
    pub usage_dir: Option<PathBuf>,
    pub moonshot_search: Option<SearchServiceConfig>,
    pub antigravity: Option<AntigravityProvider>,
    pub chatgpt: Option<ChatGptProvider>,
    pub xai: Option<XaiProvider>,
    pub commandcode: Option<CommandCodeProvider>,
    pub qoder: Option<QoderProvider>,
    pub routes: Vec<RouteSpec>,
}

/// 读取并校验配置文件.
///
/// Reads and validates the configuration file.
pub fn load(path: &std::path::Path) -> Result<Config, String> {
    let data = std::fs::read(path).map_err(|error| format!("读取配置文件失败: {error}"))?;
    parse(&data)
}

/// 解析并校验配置字节.
///
/// 独立于文件读取, 使校验分支可以在单元测试中直接覆盖, 不必为每种错误创建文件.
///
/// Parses and validates configuration bytes.
///
/// Kept separate from file reading so unit tests cover every validation branch without
/// creating a file per case.
pub fn parse(data: &[u8]) -> Result<Config, String> {
    let mut deserializer = serde_json::Deserializer::from_slice(data);
    let raw = RawConfig::deserialize(&mut deserializer)
        .map_err(|error| format!("invalid aria config: {error}"))?;
    // 与 Canto 的 Load 相同: 解析出一个值后必须到达输入末尾, 因此两个拼接的 JSON 对象会被
    // 拒绝, 而不是静默使用第一个.
    //
    // As in Canto's Load: after one value the input must be at its end, so two concatenated
    // JSON objects are rejected instead of silently using the first.
    deserializer
        .end()
        .map_err(|_| "aria config must contain one JSON object".to_owned())?;
    raw.validate()
}

/// 配置文件的原样结构. 所有对象都拒绝未知字段.
///
/// The configuration file as written. Every object rejects unknown fields.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_listen")]
    listen: String,
    #[serde(default)]
    clients: Option<String>,
    #[serde(default = "default_max_concurrency")]
    max_concurrency: u64,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: u64,
    #[serde(default = "default_upstream_idle_timeout_secs")]
    upstream_idle_timeout_secs: u64,
    #[serde(default)]
    upstream_first_byte_timeout_secs: Option<u64>,
    #[serde(
        default,
        alias = "first_byte_timeout_cooldown_threshold",
        alias = "cooldown_threshold"
    )]
    timeout_cooldown_threshold: Option<u64>,
    #[serde(
        default,
        alias = "first_byte_timeout_cooldown_secs",
        alias = "cooldown_secs"
    )]
    timeout_cooldown_secs: Option<u64>,
    #[serde(default)]
    usage_dir: Option<String>,
    #[serde(default)]
    moonshot_search: Option<RawSearchService>,
    #[serde(default)]
    providers: RawProviders,
    #[serde(default)]
    routes: Vec<RawRoute>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSearchService {
    url: String,
    key_env: String,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawProviders {
    #[serde(default)]
    antigravity: Option<RawAntigravity>,
    #[serde(default)]
    chatgpt: Option<RawChatGpt>,
    #[serde(default)]
    xai: Option<RawXai>,
    #[serde(default)]
    commandcode: Option<RawCommandCode>,
    #[serde(default)]
    qoder: Option<RawQoder>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAntigravity {
    credentials: String,
    #[serde(default = "default_antigravity_base_url")]
    base_url: String,
    #[serde(default = "default_google_client_id")]
    client_id: String,
    /// 缺省时回退到环境变量 ARIA_ANTIGRAVITY_CLIENT_SECRET, 两者都没有则校验失败.
    ///
    /// Falls back to the environment variable ARIA_ANTIGRAVITY_CLIENT_SECRET when absent; with
    /// neither present, validation fails.
    #[serde(default)]
    client_secret: Option<String>,
    #[serde(default)]
    client: Option<RawClient>,
    #[serde(
        default,
        alias = "first_byte_timeout_cooldown_threshold",
        alias = "cooldown_threshold"
    )]
    timeout_cooldown_threshold: Option<u64>,
    #[serde(
        default,
        alias = "first_byte_timeout_cooldown_secs",
        alias = "cooldown_secs"
    )]
    timeout_cooldown_secs: Option<u64>,
}

/// providers.*.client 的原样结构. 三个字段都可选, 缺省时按线路默认值补齐.
///
/// The as-written providers.*.client. All three fields are optional and fall back to the line
/// defaults when absent.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawClient {
    user_agent_template: Option<String>,
    version_url: Option<String>,
    fallback_version: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawChatGpt {
    #[serde(default)]
    credentials: Option<String>,
    #[serde(default)]
    broker: Option<RawBroker>,
    #[serde(default)]
    broker_clients: Option<String>,
    #[serde(default = "default_chatgpt_base_url")]
    base_url: String,
    #[serde(default = "default_chatgpt_models")]
    models: Vec<String>,
    #[serde(default)]
    client: Option<RawClient>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBroker {
    url: String,
    key_env: String,
}

/// providers.xai 的原样结构. 除 credentials 外都有默认值, 默认值取自 lyra::xai, 使配置与线路
/// 实现只有一处取值来源.
///
/// The as-written providers.xai. Every field except credentials has a default taken from
/// lyra::xai, so the configuration and the line implementation share one source of values.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawXai {
    credentials: String,
    #[serde(default = "default_xai_base_url")]
    base_url: String,
    #[serde(default = "default_xai_token_url")]
    token_url: String,
    #[serde(default = "default_xai_client_id")]
    client_id: String,
    #[serde(default = "default_xai_scope")]
    scope: String,
    #[serde(default = "default_xai_models")]
    models: Vec<String>,
    #[serde(default)]
    client: Option<RawClient>,
}

/// providers.commandcode 的原样结构. 全部字段都有默认值, 默认值取自 lyra::commandcode,
/// 使配置与线路实现共享同一处取值来源.
///
/// The as-written providers.commandcode. Every field has a default taken from
/// lyra::commandcode, so the configuration and the line implementation share one source of values.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCommandCode {
    #[serde(default = "default_commandcode_base_url")]
    base_url: String,
    #[serde(default = "default_commandcode_key_env")]
    key_env: String,
    #[serde(default = "default_commandcode_models")]
    models: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQoder {
    #[serde(default = "default_qoder_base_url")]
    base_url: String,
    #[serde(default = "default_qoder_token_env")]
    token_env: String,
    #[serde(default)]
    machine_file: Option<String>,
    #[serde(default = "default_max_concurrency")]
    max_concurrency: u64,
    #[serde(default)]
    models: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRoute {
    name: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    aliases: Vec<String>,
    model: String,
    upstream: String,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    base_url_env: Option<String>,
    #[serde(default)]
    key_env: Option<String>,
    #[serde(default)]
    upstreams: Option<Vec<RawUpstream>>,
    #[serde(default)]
    moonshot_search: bool,
    #[serde(default)]
    max_output_tokens: Option<i64>,
    #[serde(default)]
    reasoning_effort: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    base_url: String,
    #[serde(default)]
    base_url_env: String,
    key_env: String,
}

impl RawConfig {
    /// 校验结构并转换成运行期配置.
    ///
    /// Validates the structure and converts it into the runtime configuration.
    fn validate(self) -> Result<Config, String> {
        if self.max_concurrency > MAX_CONCURRENCY_LIMIT {
            return Err(format!(
                "max_concurrency must not exceed {MAX_CONCURRENCY_LIMIT}"
            ));
        }
        let max_concurrency = positive_count("max_concurrency", self.max_concurrency)?;
        let max_body_bytes = positive_count("max_body_bytes", self.max_body_bytes)?;
        let idle_secs = positive_value(
            "upstream_idle_timeout_secs",
            self.upstream_idle_timeout_secs,
        )?;
        let first_byte_secs = match self.upstream_first_byte_timeout_secs {
            Some(raw) => {
                let val = positive_value("upstream_first_byte_timeout_secs", raw)?;
                if val >= idle_secs {
                    return Err(
                        "upstream_first_byte_timeout_secs must be less than upstream_idle_timeout_secs"
                            .to_owned(),
                    );
                }
                val
            }
            None => {
                if idle_secs <= DEFAULT_UPSTREAM_FIRST_BYTE_TIMEOUT_SECS {
                    idle_secs
                } else {
                    DEFAULT_UPSTREAM_FIRST_BYTE_TIMEOUT_SECS
                }
            }
        };

        let timeout_cooldown_threshold = match self
            .providers
            .antigravity
            .as_ref()
            .and_then(|raw| raw.timeout_cooldown_threshold)
            .or(self.timeout_cooldown_threshold)
        {
            Some(raw) => positive_count("timeout_cooldown_threshold", raw)? as u32,
            None => DEFAULT_TIMEOUT_COOLDOWN_THRESHOLD as u32,
        };
        let timeout_cooldown_secs = match self
            .providers
            .antigravity
            .as_ref()
            .and_then(|raw| raw.timeout_cooldown_secs)
            .or(self.timeout_cooldown_secs)
        {
            Some(raw) => positive_value("timeout_cooldown_secs", raw)?,
            None => DEFAULT_TIMEOUT_COOLDOWN_SECS,
        };
        let timeout_cooldown_duration = Duration::from_secs(timeout_cooldown_secs);

        let antigravity = match self.providers.antigravity {
            Some(raw) => Some(AntigravityProvider {
                credentials: required_path("providers.antigravity.credentials", raw.credentials)?,
                base_url: raw.base_url,
                client_id: raw.client_id,
                client_secret: google_client_secret(raw.client_secret)?,
                client: client_config_of(raw.client, ClientConfig::default_antigravity()),
                timeout_cooldown_threshold,
                timeout_cooldown_duration,
            }),
            None => None,
        };
        let chatgpt = match self.providers.chatgpt {
            Some(raw) => {
                let source = match (raw.credentials, raw.broker) {
                    (Some(path), None) => {
                        let path = required_path("providers.chatgpt.credentials", path)?;
                        ChatGptSource::Credentials(path)
                    }
                    (None, Some(broker)) => {
                        if broker.url.is_empty() || broker.key_env.is_empty() {
                            return Err(
                                "providers.chatgpt.broker requires a nonempty url and key_env"
                                    .to_owned(),
                            );
                        }
                        validate_secure_url("providers.chatgpt.broker.url", &broker.url)?;
                        validate_key_env("providers.chatgpt.broker.key_env", &broker.key_env)?;
                        ChatGptSource::Broker {
                            url: broker.url,
                            key_env: broker.key_env,
                        }
                    }
                    (Some(_), Some(_)) | (None, None) => {
                        return Err(
                            "providers.chatgpt requires either credentials or broker".to_owned()
                        );
                    }
                };
                let broker_clients = match raw.broker_clients {
                    Some(path) => {
                        if !matches!(source, ChatGptSource::Credentials(_)) {
                            return Err(
                                "providers.chatgpt.broker_clients is only allowed with credentials"
                                    .to_owned(),
                            );
                        }
                        Some(required_path("providers.chatgpt.broker_clients", path)?)
                    }
                    None => None,
                };
                Some(ChatGptProvider {
                    source,
                    base_url: raw.base_url,
                    models: raw.models,
                    broker_clients,
                    client: client_config_of(raw.client, ClientConfig::default_chatgpt()),
                })
            }
            None => None,
        };
        let xai = match self.providers.xai {
            Some(raw) => Some(XaiProvider {
                credentials: required_path("providers.xai.credentials", raw.credentials)?,
                base_url: raw.base_url,
                token_url: raw.token_url,
                client_id: raw.client_id,
                scope: raw.scope,
                models: raw.models,
                client: client_config_of(raw.client, ClientConfig::default_xai()),
            }),
            None => None,
        };

        let commandcode = match self.providers.commandcode {
            Some(raw) => {
                validate_secure_url("providers.commandcode.base_url", &raw.base_url)?;
                validate_key_env("providers.commandcode.key_env", &raw.key_env)?;
                Some(CommandCodeProvider {
                    base_url: raw.base_url,
                    key_env: raw.key_env,
                    models: raw.models,
                })
            }
            None => None,
        };

        let moonshot_search = match self.moonshot_search {
            Some(raw) => {
                if raw.url.is_empty() || raw.key_env.is_empty() {
                    return Err("moonshot_search requires a nonempty url and key_env".to_owned());
                }
                Some(SearchServiceConfig {
                    url: raw.url,
                    key_env: raw.key_env,
                })
            }
            None => None,
        };

        let qoder = match self.providers.qoder {
            Some(raw) => {
                if raw.base_url.is_empty() {
                    return Err("providers.qoder requires a nonempty base_url".to_owned());
                }
                if raw.token_env.is_empty() {
                    return Err("providers.qoder requires a nonempty token_env".to_owned());
                }
                if raw.max_concurrency > MAX_CONCURRENCY_LIMIT {
                    return Err(format!(
                        "providers.qoder.max_concurrency must not exceed {MAX_CONCURRENCY_LIMIT}"
                    ));
                }
                let max_concurrency =
                    positive_count("providers.qoder.max_concurrency", raw.max_concurrency)?;
                let machine_file = match raw.machine_file {
                    Some(ref s) if s.trim().is_empty() => {
                        return Err(
                            "providers.qoder.machine_file must not be empty when specified"
                                .to_owned(),
                        );
                    }
                    Some(s) => Some(PathBuf::from(s)),
                    None => None,
                };
                Some(QoderProvider {
                    base_url: raw.base_url,
                    token_env: raw.token_env,
                    machine_file,
                    max_concurrency,
                    models: raw.models,
                })
            }
            None => None,
        };

        // 既没有路由也没有 ChatGPT 提供方时, 进程没有任何可服务的能力, 属于配置错误. 只有
        // ChatGPT 提供方能独立服务: 它带有 Responses 透传端点; xai 提供方只能经路由使用, 没有
        // 路由的 xai 配置同样无法服务, 仍按这条规则报错.
        //
        // With neither routes nor a ChatGPT provider the process has nothing to serve,
        // which is a configuration error. Only the ChatGPT provider can serve on its own through
        // the Responses passthrough; the xai provider is usable only through routes, so an xai
        // configuration without routes still fails under this rule.
        if self.routes.is_empty() && chatgpt.is_none() {
            return Err("aria requires at least one route or a chatgpt provider".to_owned());
        }

        let providers = Providers {
            search: moonshot_search.is_some(),
            antigravity: antigravity.is_some(),
            chatgpt: chatgpt.is_some(),
            xai: xai.is_some(),
            commandcode: commandcode.is_some(),
            qoder: qoder.is_some(),
        };
        let mut seen = BTreeSet::new();
        let mut routes = Vec::with_capacity(self.routes.len());
        for raw in self.routes {
            routes.push(validate_route(raw, &providers, &mut seen)?);
        }

        Ok(Config {
            listen: self.listen,
            clients: self.clients,
            max_concurrency,
            max_body_bytes,
            upstream_idle_timeout: Duration::from_secs(idle_secs),
            upstream_first_byte_timeout: Duration::from_secs(first_byte_secs),
            usage_dir: self.usage_dir.map(PathBuf::from),
            moonshot_search,
            antigravity,
            chatgpt,
            xai,
            commandcode,
            qoder,
            routes,
        })
    }
}

/// 路由校验需要知道的已配置服务. 合成一个值传入, 避免每增加一种提供方就给 validate_route
/// 多加一个布尔参数.
///
/// The configured services route validation needs to know about. They are passed as one value
/// so a new provider does not add yet another boolean parameter to validate_route.
struct Providers {
    search: bool,
    antigravity: bool,
    chatgpt: bool,
    xai: bool,
    commandcode: bool,
    qoder: bool,
}

/// 校验一条路由并把它转换成运行期描述.
///
/// Validates one route and converts it into the runtime description.
fn validate_route(
    raw: RawRoute,
    providers: &Providers,
    seen: &mut BTreeSet<String>,
) -> Result<RouteSpec, String> {
    if raw.name.is_empty() {
        return Err("route requires a nonempty name".to_owned());
    }
    if raw.model.is_empty() {
        return Err(format!("route {:?} requires a nonempty model", raw.name));
    }
    // 路由名与别名全局唯一: 同一个模型名指向两个上游时, 请求会落到哪一个是配置顺序决定的,
    // 这种不确定性必须在启动阶段拒绝.
    //
    // Route names and aliases are globally unique: when one model name points at two
    // upstreams the winner depends on configuration order, and that ambiguity must be
    // rejected at startup.
    for name in std::iter::once(&raw.name).chain(raw.aliases.iter()) {
        if name.is_empty() {
            return Err(format!("route {:?} has an empty alias", raw.name));
        }
        if !seen.insert(name.clone()) {
            return Err(format!("duplicate model name {name:?}"));
        }
    }
    if raw.moonshot_search && !providers.search {
        return Err(format!(
            "route {:?} requires the moonshot_search service",
            raw.name
        ));
    }

    if let Some(limit) = raw.max_output_tokens
        && limit <= 0
    {
        return Err(format!(
            "route {:?} has a negative or zero max_output_tokens",
            raw.name
        ));
    }

    let kind = match raw.upstream.as_str() {
        "messages" => {
            if raw.reasoning_effort.is_some() {
                return Err(format!(
                    "route {:?} is a messages route and cannot set reasoning_effort",
                    raw.name
                ));
            }
            if raw.upstreams.is_some()
                && (raw.base_url.is_some() || raw.base_url_env.is_some() || raw.key_env.is_some())
            {
                return Err(format!(
                    "route {:?} cannot combine upstreams with base_url, base_url_env or key_env",
                    raw.name
                ));
            }
            match raw.upstreams {
                Some(upstreams) => {
                    if upstreams.is_empty() {
                        return Err(format!(
                            "route {:?} requires at least one upstream",
                            raw.name
                        ));
                    }
                    let mut specs = Vec::with_capacity(upstreams.len());
                    for upstream in upstreams {
                        specs.push(validate_upstream(&raw.name, upstream)?);
                    }
                    RouteKind::Messages {
                        base_url: String::new(),
                        base_url_env: String::new(),
                        key_env: String::new(),
                        upstreams: specs,
                    }
                }
                None => {
                    let base_url = raw.base_url.unwrap_or_default();
                    let base_url_env = raw.base_url_env.unwrap_or_default();
                    let key_env = raw.key_env.unwrap_or_default();
                    if base_url.is_empty() || key_env.is_empty() {
                        return Err(format!(
                            "route {:?} requires base_url and key_env, or an upstreams list",
                            raw.name
                        ));
                    }
                    RouteKind::Messages {
                        base_url,
                        base_url_env,
                        key_env,
                        upstreams: Vec::new(),
                    }
                }
            }
        }
        "antigravity" => {
            if raw.reasoning_effort.is_some() {
                return Err(format!(
                    "route {:?} is an antigravity route and cannot set reasoning_effort",
                    raw.name
                ));
            }
            // antigravity 路由的上游地址与密钥来自 providers.antigravity, 因此这里出现
            // messages 专用的字段说明配置写错了类型, 必须报错而不是忽略.
            //
            // An antigravity route takes its upstream address and key from
            // providers.antigravity, so a messages-only field here means the type is wrong
            // and must fail instead of being ignored.
            for (field, present) in [
                ("base_url", raw.base_url.is_some()),
                ("base_url_env", raw.base_url_env.is_some()),
                ("key_env", raw.key_env.is_some()),
                ("upstreams", raw.upstreams.is_some()),
            ] {
                if present {
                    return Err(format!(
                        "route {:?} is an antigravity route and cannot set {field}",
                        raw.name
                    ));
                }
            }
            if !providers.antigravity {
                return Err(format!(
                    "route {:?} requires providers.antigravity",
                    raw.name
                ));
            }
            RouteKind::Antigravity
        }
        "chatgpt" => {
            for (field, present) in [
                ("base_url", raw.base_url.is_some()),
                ("base_url_env", raw.base_url_env.is_some()),
                ("key_env", raw.key_env.is_some()),
                ("upstreams", raw.upstreams.is_some()),
                ("max_output_tokens", raw.max_output_tokens.is_some()),
            ] {
                if present {
                    return Err(format!(
                        "route {:?} is a chatgpt route and cannot set {field}",
                        raw.name
                    ));
                }
            }
            if !providers.chatgpt {
                return Err(format!("route {:?} requires providers.chatgpt", raw.name));
            }
            let reasoning_effort = reasoning_effort_of(&raw.name, raw.reasoning_effort.as_deref())?;
            RouteKind::ChatGpt { reasoning_effort }
        }
        "xai" => {
            // xai 路由与 chatgpt 路由一样经 lyra 转换为 Responses 协议, 上游地址与凭据来自
            // providers.xai, 因此允许的字段相同. max_output_tokens 同样不允许: 请求转换不发送
            // max_tokens, 设置了也不会生效, 应当报错而不是静默忽略.
            //
            // An xai route, like a chatgpt route, is converted to the Responses protocol by lyra
            // and takes its upstream address and credentials from providers.xai, so the allowed
            // fields are the same. max_output_tokens is rejected as well: the request conversion
            // never sends max_tokens, so the option could not take effect and must fail rather
            // than be ignored.
            for (field, present) in [
                ("base_url", raw.base_url.is_some()),
                ("base_url_env", raw.base_url_env.is_some()),
                ("key_env", raw.key_env.is_some()),
                ("upstreams", raw.upstreams.is_some()),
                ("max_output_tokens", raw.max_output_tokens.is_some()),
            ] {
                if present {
                    return Err(format!(
                        "route {:?} is an xai route and cannot set {field}",
                        raw.name
                    ));
                }
            }
            if !providers.xai {
                return Err(format!("route {:?} requires providers.xai", raw.name));
            }
            let reasoning_effort = reasoning_effort_of(&raw.name, raw.reasoning_effort.as_deref())?;
            RouteKind::Xai { reasoning_effort }
        }
        "commandcode" => {
            // commandcode 路由与 xai 路由一样经 lyra 转换为 Responses 协议, 上游地址与凭据来自
            // providers.commandcode, 因此允许的字段相同, 禁止 base_url, base_url_env, key_env,
            // upstreams 与 max_output_tokens.
            //
            // A commandcode route is converted to the Responses protocol by lyra and takes its
            // upstream address and credentials from providers.commandcode, so base_url, base_url_env,
            // key_env, upstreams and max_output_tokens are rejected.
            for (field, present) in [
                ("base_url", raw.base_url.is_some()),
                ("base_url_env", raw.base_url_env.is_some()),
                ("key_env", raw.key_env.is_some()),
                ("upstreams", raw.upstreams.is_some()),
                ("max_output_tokens", raw.max_output_tokens.is_some()),
            ] {
                if present {
                    return Err(format!(
                        "route {:?} is a commandcode route and cannot set {field}",
                        raw.name
                    ));
                }
            }
            if !providers.commandcode {
                return Err(format!(
                    "route {:?} requires providers.commandcode",
                    raw.name
                ));
            }
            let reasoning_effort = reasoning_effort_of(&raw.name, raw.reasoning_effort.as_deref())?;
            RouteKind::CommandCode { reasoning_effort }
        }
        "qoder" => {
            if raw.reasoning_effort.is_some() {
                return Err(format!(
                    "route {:?} is a qoder route and cannot set reasoning_effort",
                    raw.name
                ));
            }
            for (field, present) in [
                ("base_url", raw.base_url.is_some()),
                ("base_url_env", raw.base_url_env.is_some()),
                ("key_env", raw.key_env.is_some()),
                ("upstreams", raw.upstreams.is_some()),
            ] {
                if present {
                    return Err(format!(
                        "route {:?} is a qoder route and cannot set {field}",
                        raw.name
                    ));
                }
            }
            if !providers.qoder {
                return Err(format!("route {:?} requires providers.qoder", raw.name));
            }
            RouteKind::Qoder
        }
        other => {
            return Err(format!(
                "route {:?} requires upstream \"messages\", \"antigravity\", \"chatgpt\", \"xai\", \"commandcode\" or \"qoder\", got {other:?}",
                raw.name
            ));
        }
    };

    Ok(RouteSpec {
        name: raw.name,
        display_name: raw.display_name,
        aliases: raw.aliases,
        model: raw.model,
        moonshot_search: raw.moonshot_search,
        max_output_tokens: raw.max_output_tokens,
        kind,
    })
}

/// 解析 chatgpt 与 xai 路由的默认思考强度档位: low, medium, high 或 xhigh, 缺省为 high.
///
/// Parses the default reasoning effort of a chatgpt or xai route: low, medium, high or xhigh,
/// defaulting to high.
fn reasoning_effort_of(route: &str, raw: Option<&str>) -> Result<String, String> {
    match raw {
        Some(effort @ ("low" | "medium" | "high" | "xhigh")) => Ok(effort.to_owned()),
        Some(other) => Err(format!(
            "route {route:?} has invalid reasoning_effort {other:?}"
        )),
        None => Ok("high".to_owned()),
    }
}

/// 校验多上游写法中的一条上游.
///
/// Validates one upstream of the multi-upstream form.
fn validate_upstream(route: &str, raw: RawUpstream) -> Result<UpstreamSpec, String> {
    if raw.base_url.is_empty() || raw.key_env.is_empty() {
        return Err(format!(
            "route {route:?} requires base_url and key_env on every upstream"
        ));
    }
    Ok(UpstreamSpec {
        base_url: raw.base_url,
        base_url_env: raw.base_url_env,
        key_env: raw.key_env,
    })
}

/// 校验正整数配置项并把它转换成 usize.
///
/// 零与超出 usize 的取值都会让限制失去意义, 因此在启动阶段拒绝, 而不是留到运行期.
///
/// Validates a positive integer option and converts it into usize.
///
/// Zero and values beyond usize make the limit meaningless, so they are rejected at
/// startup rather than at runtime.
fn positive_count(field: &str, value: u64) -> Result<usize, String> {
    let value = positive_value(field, value)?;
    usize::try_from(value).map_err(|_| format!("{field} is too large"))
}

/// 校验正整数配置项.
///
/// Validates a positive integer option.
fn positive_value(field: &str, value: u64) -> Result<u64, String> {
    if value == 0 {
        return Err(format!("{field} must be a positive integer"));
    }
    Ok(value)
}

/// 校验一个必填路径字段非空.
///
/// Validates that a required path field is not empty.
fn required_path(field: &str, value: String) -> Result<PathBuf, String> {
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(PathBuf::from(value))
}

/// 把原样的 client 配置按字段折叠到线路默认值上.
///
/// Folds the as-written client configuration onto the line defaults, field by field.
///
/// 缺省是逐字段的: 只写 version_url 时, 模板与后备版本仍取线路默认值, 因此 "client": {} 与
/// 不写等价, 见 architecture.md 3.8.
///
/// The defaulting is per field: writing only version_url keeps the line defaults for the
/// template and fallback, so "client": {} is equivalent to omitting it, as section 3.8 of
/// architecture.md states.
fn client_config_of(raw: Option<RawClient>, default: ClientConfig) -> ClientConfig {
    let Some(raw) = raw else {
        return default;
    };
    ClientConfig {
        user_agent_template: raw
            .user_agent_template
            .unwrap_or(default.user_agent_template),
        version_url: raw.version_url.unwrap_or(default.version_url),
        fallback_version: raw.fallback_version.unwrap_or(default.fallback_version),
    }
}

fn default_listen() -> String {
    DEFAULT_LISTEN.to_owned()
}

fn default_max_concurrency() -> u64 {
    DEFAULT_MAX_CONCURRENCY
}

fn default_max_body_bytes() -> u64 {
    DEFAULT_MAX_BODY_BYTES
}

fn default_upstream_idle_timeout_secs() -> u64 {
    DEFAULT_UPSTREAM_IDLE_TIMEOUT_SECS
}

fn default_antigravity_base_url() -> String {
    DEFAULT_ANTIGRAVITY_BASE_URL.to_owned()
}

fn default_google_client_id() -> String {
    DEFAULT_GOOGLE_CLIENT_ID.to_owned()
}

/// 解析 Antigravity 的 OAuth 客户端密钥: 配置取值优先, 其次环境变量, 都没有时失败.
///
/// 配置里的空字符串与缺失都视为没有提供, 因此误写的空值不会被当作有效密钥送到 Google.
///
/// Resolves the Antigravity OAuth client secret: the configured value first, then the environment
/// variable, and a failure when neither is present. An empty configured string counts as absent, so
/// a mistyped empty value is never sent to Google as a credential.
fn google_client_secret(configured: Option<String>) -> Result<String, String> {
    match configured {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => match std::env::var(GOOGLE_CLIENT_SECRET_ENV) {
            Ok(value) if !value.trim().is_empty() => Ok(value),
            _ => Err(format!(
                "providers.antigravity needs a client_secret: set it in the configuration or in the {GOOGLE_CLIENT_SECRET_ENV} environment variable"
            )),
        },
    }
}

fn default_chatgpt_base_url() -> String {
    DEFAULT_CHATGPT_BASE_URL.to_owned()
}

fn default_chatgpt_models() -> Vec<String> {
    lyra::chatgpt::CHATGPT_MODELS
        .iter()
        .map(|model| (*model).to_owned())
        .collect()
}

fn default_xai_base_url() -> String {
    lyra::xai::XAI_BASE_URL.to_owned()
}

fn default_xai_token_url() -> String {
    lyra::xai::XAI_TOKEN_URL.to_owned()
}

fn default_xai_client_id() -> String {
    lyra::xai::XAI_OAUTH_CLIENT_ID.to_owned()
}

fn default_xai_scope() -> String {
    lyra::xai::XAI_OAUTH_SCOPE.to_owned()
}

fn default_xai_models() -> Vec<String> {
    lyra::xai::XAI_MODELS
        .iter()
        .map(|model| (*model).to_owned())
        .collect()
}

fn default_commandcode_base_url() -> String {
    lyra::commandcode::COMMANDCODE_BASE_URL.to_owned()
}

fn default_commandcode_key_env() -> String {
    "COMMAND_CODE_API_KEY".to_owned()
}

fn default_commandcode_models() -> Vec<String> {
    lyra::commandcode::COMMANDCODE_MODELS
        .iter()
        .map(|model| (*model).to_owned())
        .collect()
}

fn default_qoder_base_url() -> String {
    DEFAULT_QODER_BASE_URL.to_owned()
}

fn default_qoder_token_env() -> String {
    DEFAULT_QODER_TOKEN_ENV.to_owned()
}

/// 校验一个会收到密钥的地址: 必须是 HTTPS 或回环 HTTP, 且不能包含用户信息, 查询串或片段.
///
/// 令牌分发地址与 Command Code 基址都会随请求收到密钥, 因此共用这一规则; field 是出错时报告的
/// 配置字段名.
///
/// Validates an address that receives a key: it must be HTTPS or loopback HTTP without userinfo,
/// query or fragment.
///
/// The broker URL and the Command Code base address both receive a key with every request, so
/// they share this rule; field is the configuration field reported on failure.
fn validate_secure_url(field: &str, raw: &str) -> Result<(), String> {
    let err = || format!("{field} requires HTTPS or loopback HTTP without credentials or query");
    if has_userinfo(raw) {
        return Err(err());
    }
    let Ok(url) = reqwest::Url::parse(raw) else {
        return Err(err());
    };
    let is_loopback = url
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|addr| addr.is_loopback());
    let scheme_ok = url.scheme() == "https" || (url.scheme() == "http" && is_loopback);
    if scheme_ok
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && !url.query().is_some_and(|q| !q.is_empty())
        && !url.fragment().is_some_and(|f| !f.is_empty())
    {
        Ok(())
    } else {
        Err(err())
    }
}

fn has_userinfo(raw: &str) -> bool {
    let after_scheme = raw.split_once("://").map_or(raw, |(_, rest)| rest);
    let end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    after_scheme[..end].contains('@')
}

/// 校验环境变量名称及取值: 变量必须存在, 非空且不含回车, 换行与空字符.
///
/// Validates the environment variable name and value: must exist, be non-empty and contain no
/// CR, LF or NUL.
fn validate_key_env(field: &str, var_name: &str) -> Result<(), String> {
    if var_name.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    match std::env::var(var_name) {
        Ok(val) if !val.is_empty() && !val.contains(['\r', '\n', '\0']) => Ok(()),
        _ => Err(format!(
            "{field} refers to environment variable {var_name:?}, which must be non-empty and must not contain CR, LF or NUL"
        )),
    }
}
