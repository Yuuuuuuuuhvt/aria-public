//! 各订阅线路的客户端标识与官方 CLI 版本号的跟进.
//!
//! 上游按 User-Agent 识别客户端, 标识落后于官方 CLI 会让这批请求显得陈旧. 每条线路都忠实
//! 各自官方 CLI 的标识形态: Codex 线路取官方 Codex CLI 的 codex_cli_rs, Antigravity 线路取
//! agy 实测的 antigravity/cli 长形态, xai 线路取官方 Grok CLI 的 grok-cli. 版本不再写死, 由
//! 后台任务定时获取官方 CLI 最新发布号, 每次构造请求时按模板渲染出 User-Agent. 获取失败沿用
//! 最近一次成功的值, 从未成功时使用配置的后备版本, 因此一次网络波动不会改变发出去的标识.
//!
//! The client identifier of the subscription routes and how it follows the official CLI
//! version.
//!
//! Upstream identifies the client by its User-Agent, and an identifier that lags the official
//! CLI makes the traffic look stale. Every line stays faithful to its official CLI's
//! identifier shape: the Codex line takes the official Codex CLI's codex_cli_rs, the
//! Antigravity line takes agy's measured antigravity/cli long form, and the xai line takes the
//! official Grok CLI's grok-cli. The version is no longer hard-coded; a background task fetches
//! the latest official CLI release on a schedule, and each request renders the User-Agent from
//! the template. A failed fetch keeps the last good value, and a never-fetched value uses the
//! configured fallback, so one network blip never changes what is sent.
//!
//! 版本清单的解析种类由清单地址决定: 以 .yml 或 .yaml 结尾按 YAML 取顶层 version: 行, 否则按
//! JSON 取顶层 version 字段, 字段缺失时回退 GitHub release 的 tag_name. 不引入 YAML 依赖.
//!
//! The manifest kind is decided by its address: a .yml or .yaml suffix is read as YAML taking
//! the top-level version: line, everything else as JSON taking the top-level version field,
//! falling back to the tag_name field of a GitHub release. No YAML dependency is added.
//!
//! 接口见 docs/architecture.md 3.8.
//!
//! See section 3.8 of docs/architecture.md for the specification.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::credential::read_limited;

/// 版本清单单次获取的整体超时.
///
/// The overall timeout of one manifest fetch.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// 获取响应体的读取上限, 与凭据刷新一致, 防止异常的清单无限读入内存.
///
/// The read limit of a fetch response body, matching credential refresh, so an odd manifest is
/// never read into memory without bound.
const MAX_MANIFEST_BYTES: usize = 1 << 20;

/// 版本清单的解析种类.
///
/// How a version manifest is parsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestKind {
    /// 从 JSON 顶层的 version 字段取版本, npm 的 @openai/codex 发布使用.
    ///
    /// The version field of the JSON top level, as used by npm's @openai/codex release.
    Json,
    /// 从以 version: 开头的行取版本, 供配置指向 YAML 清单的线路使用.
    ///
    /// The line beginning with version:, for lines whose manifest is YAML.
    Yaml,
}

/// 一条线路的客户端标识配置.
///
/// The client-identifier configuration of one route line.
///
/// 三个字段都可选, 缺省时按线路默认值补齐, 因此 providers 下写 "client": {} 与不写等价.
/// 模板不含 {version} 时视为固定标识, 不发起版本请求.
///
/// All three fields are optional and fall back to the line defaults, so writing "client": {}
/// under providers is equivalent to omitting it. A template without {version} is a fixed
/// identifier that issues no manifest request.
#[derive(Clone, Debug, PartialEq)]
pub struct ClientConfig {
    /// User-Agent 模板, {version} 处替换为当前版本.
    ///
    /// The User-Agent template; {version} is replaced with the current version.
    pub user_agent_template: String,
    /// 官方版本清单的地址.
    ///
    /// The address of the official version manifest.
    pub version_url: String,
    /// 从未成功获取时使用的版本.
    ///
    /// The version used when no fetch has ever succeeded.
    pub fallback_version: String,
}

impl ClientConfig {
    /// 按清单地址决定解析种类: 以 .yml 或 .yaml 结尾为 YAML, 其余为 JSON.
    ///
    /// Decides the manifest kind from its address: .yml or .yaml is YAML, the rest is JSON.
    pub fn manifest_kind(&self) -> ManifestKind {
        let lower = self.version_url.to_lowercase();
        if lower.ends_with(".yml") || lower.ends_with(".yaml") {
            ManifestKind::Yaml
        } else {
            ManifestKind::Json
        }
    }

    /// 把模板里的 {version} 替换成给定版本, 渲染出 User-Agent.
    ///
    /// Renders a User-Agent by replacing {version} in the template with the given version.
    pub fn user_agent(&self, version: &str) -> String {
        self.user_agent_template.replace("{version}", version)
    }

    /// 模板是否引用 {version}. 不含占位符时是固定标识, 获取版本清单没有意义.
    ///
    /// Whether the template references {version}. Without the placeholder it is a fixed
    /// identifier, and fetching a version manifest is pointless.
    pub fn has_version_placeholder(&self) -> bool {
        self.user_agent_template.contains("{version}")
    }

    /// Codex 线路的默认标识配置.
    ///
    /// The default client-identifier configuration of the Codex line.
    pub fn default_chatgpt() -> Self {
        Self {
            user_agent_template: "codex_cli_rs/{version}".to_owned(),
            version_url: "https://registry.npmjs.org/@openai/codex/latest".to_owned(),
            fallback_version: "0.160.1".to_owned(),
        }
    }

    /// Antigravity 线路的默认标识配置.
    ///
    /// 模板取自 agy 官方 CLI 的实测请求: 长形态的 antigravity/cli 标识. 版本清单是 agy 仓库
    /// 的 GitHub latest release, tag_name 字段给出三段数字.
    ///
    /// The default client-identifier configuration of the Antigravity line.
    ///
    /// The template comes from the measured requests of the agy official CLI: the long
    /// antigravity/cli identifier. The version manifest is the GitHub latest release of the agy
    /// repository, whose tag_name field gives a three-segment number.
    pub fn default_antigravity() -> Self {
        Self {
            user_agent_template: "antigravity/cli/{version} (aidev_client; os_type=darwin; arch=arm64; cl=989022288; auth_method=consumer)".to_owned(),
            version_url: "https://api.github.com/repos/google-antigravity/antigravity-cli/releases/latest".to_owned(),
            fallback_version: "1.2.17".to_owned(),
        }
    }

    /// xai 线路的默认标识配置.
    ///
    /// 模板取自官方 Grok CLI 的实际请求形态 grok-cli/{version}. 版本清单是 npm 上 @xai-official/grok
    /// 的 latest 发布, version 字段给出三段数字; 后备版本 1.0.50 是 2026-10-09 探针时的最新发布.
    /// 同一版本还写进 x-grok-client-version 请求头, 推理端点按它判断 CLI 是否过旧.
    ///
    /// The default client-identifier configuration of the xai line.
    ///
    /// The template follows the official Grok CLI's request form grok-cli/{version}. The version
    /// manifest is the latest npm release of @xai-official/grok, whose version field gives a
    /// three-segment number; the fallback 1.0.50 was the latest release at the 2026-10-09 probe.
    /// The same version also goes into the x-grok-client-version header, by which the inference
    /// endpoint decides whether the CLI is outdated.
    pub fn default_xai() -> Self {
        Self {
            user_agent_template: "grok-cli/{version}".to_owned(),
            version_url: "https://registry.npmjs.org/@xai-official/grok/latest".to_owned(),
            fallback_version: "1.0.50".to_owned(),
        }
    }
}

/// 版本号的来源, 供健康检查说明当前值从哪来.
///
/// Where a version came from, so the health check can state the source of the current value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionSource {
    /// 最近一次获取成功, 且尚未被一次失败的获取替换.
    ///
    /// A recent successful fetch that a later failed fetch has not replaced.
    Fetched,
    /// 获取曾成功, 但之后一次获取失败, 沿用旧值.
    ///
    /// A fetch had succeeded before, then a later one failed, keeping the old value.
    Cached,
    /// 从未成功获取, 使用配置的后备版本.
    ///
    /// No fetch has ever succeeded, so the configured fallback version is used.
    Fallback,
}

/// 版本与它来源的快照.
///
/// A snapshot of the version together with its source.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct VersionSnapshot {
    pub version: String,
    pub source: VersionSource,
}

/// 一条线路可共享的客户端标识.
///
/// The shareable client identifier of one route line.
///
/// 内部用一个 std::sync::Mutex 保护版本状态, 持锁期间不 await, 因此可以放在 Arc 里被请求路径
/// 与后台获取任务共享. 版本状态有三态: 从未成功 (用后备), 最近成功 (用抓取值), 成功后又失败
/// (沿用旧值).
///
/// A std::sync::Mutex guards the version state; no await is taken while holding it, so the value
/// can live in an Arc shared by the request path and the background fetch task. The state has
/// three forms: never fetched (use the fallback), a recent success (use the fetched value), and
/// a success followed by a failure (keep the old value).
pub struct ClientIdentity {
    config: ClientConfig,
    state: Mutex<State>,
}

struct State {
    /// 最近一次成功获取到的版本; 为 None 时从未成功过.
    ///
    /// The version of the last successful fetch; None when no fetch has ever succeeded.
    version: Option<String>,
    /// 是否曾发生过一次获取失败. 曾成功后失败才进入 Cached, 从未成功时失败仍是 Fallback.
    ///
    /// Whether a fetch failure has happened. Only a failure after a success enters Cached; a
    /// failure with no prior success stays Fallback.
    failed_since_success: bool,
}

impl ClientIdentity {
    /// 按配置构造标识. 初始状态为从未成功, 使用后备版本.
    ///
    /// Builds the identifier from a configuration. It starts as never-fetched, using the fallback.
    pub fn new(config: ClientConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: Mutex::new(State {
                version: None,
                failed_since_success: false,
            }),
        })
    }

    /// 当前配置.
    ///
    /// The current configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// 渲染出当前的 User-Agent.
    ///
    /// Renders the current User-Agent.
    pub fn user_agent(&self) -> String {
        self.config.user_agent(&self.snapshot().version)
    }

    /// 返回当前版本与来源的快照.
    ///
    /// Returns a snapshot of the current version and its source.
    pub fn snapshot(&self) -> VersionSnapshot {
        let state = self.lock();
        let (version, source) = match &state.version {
            Some(version) => (
                version.clone(),
                if state.failed_since_success {
                    VersionSource::Cached
                } else {
                    VersionSource::Fetched
                },
            ),
            None => (
                self.config.fallback_version.clone(),
                VersionSource::Fallback,
            ),
        };
        VersionSnapshot { version, source }
    }

    /// 用一次获取结果更新状态, 并返回更新后的快照.
    ///
    /// Applies one fetch result to the state and returns the updated snapshot.
    ///
    /// 成功时记录版本; 与当前值不同时写一行日志. 失败时若曾成功过则沿用旧值并标记 Cached,
    /// 否则保持后备版本.
    ///
    /// On success the version is recorded; a log line is written when it differs from the current
    /// value. On failure, if a fetch had ever succeeded the old value is kept and marked Cached,
    /// otherwise the fallback stays.
    pub async fn refresh(&self, client: &reqwest::Client) -> VersionSnapshot {
        // 固定模板不引用 {version}, 获取版本清单不会改变渲染结果, 因此不发请求.
        //
        // A fixed template does not reference {version}, so fetching a version manifest would not
        // change the rendered value; no request is issued.
        if !self.config.has_version_placeholder() {
            return self.snapshot();
        }
        let result = fetch_once(client, &self.config).await;
        let mut state = self.lock();
        let previous = state
            .version
            .clone()
            .unwrap_or_else(|| self.config.fallback_version.clone());
        match result {
            Ok(version) => {
                state.version = Some(version);
                state.failed_since_success = false;
            }
            Err(error) => {
                state.failed_since_success = true;
                aria_core::log!("client id: 版本获取失败, 沿用当前值: {error}");
            }
        }
        let next = state
            .version
            .clone()
            .unwrap_or_else(|| self.config.fallback_version.clone());
        // 只在有效版本真正变化时写日志; 首次获取失败时前后都是后备值, 不产生无谓的一行.
        //
        // Log only when the effective version actually changes; a first failed fetch has the
        // fallback before and after, so it produces no spurious line.
        if next != previous {
            aria_core::log!("client id: 版本 {} -> {}", previous, next);
        }
        let source = match &state.version {
            Some(_) => {
                if state.failed_since_success {
                    VersionSource::Cached
                } else {
                    VersionSource::Fetched
                }
            }
            None => VersionSource::Fallback,
        };
        VersionSnapshot {
            version: next,
            source,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// 发起一次版本获取并解析出三段数字版本.
///
/// Issues one manifest fetch and parses it into a three-segment version.
///
/// 获取版本清单是 Aria 自身的行为, 与官方 CLI 发往业务接口的请求不同, 因此用一个能说明来处
/// 的标识 aria, 不与任何官方形态混同.
///
/// Fetching the version manifest is Aria's own act, distinct from the official CLI's requests
/// to the business endpoints, so it uses an identifier that says where it comes from, aria,
/// rather than blending in with any official shape.
async fn fetch_once(client: &reqwest::Client, config: &ClientConfig) -> Result<String, String> {
    let kind = config.manifest_kind();
    let user_agent = "aria";
    let response = client
        .get(&config.version_url)
        .timeout(FETCH_TIMEOUT)
        .header(reqwest::header::USER_AGENT, user_agent)
        .send()
        .await
        .map_err(|error| format!("获取版本清单失败: {error}"))?;
    let status = response.status();
    let payload = read_limited(response, MAX_MANIFEST_BYTES).await;
    if status != reqwest::StatusCode::OK {
        let lossy = String::from_utf8_lossy(&payload[..payload.len().min(200)]);
        let snippet = lossy.trim().to_owned();
        return Err(format!("版本清单返回状态码 {status:?}: {snippet}"));
    }
    parse_manifest(&payload, kind)
}

/// 按种类从清单字节里取出版本并校验为三段数字.
///
/// Extracts the version from the manifest bytes by kind and checks it is a three-segment number.
fn parse_manifest(payload: &[u8], kind: ManifestKind) -> Result<String, String> {
    let raw = match kind {
        ManifestKind::Json => {
            let value: serde_json::Value = serde_json::from_slice(payload)
                .map_err(|error| format!("清单不是合法 JSON: {error}"))?;
            // npm 发布用 version 字段, GitHub release 用 tag_name, 两处取值相同则取前者.
            //
            // npm releases carry the version field, GitHub releases carry tag_name; when both
            // are present the version field wins.
            json_field(&value, "version")
                .or_else(|| json_field(&value, "tag_name"))
                .ok_or("JSON 清单缺少 version 或 tag_name 字段".to_owned())?
        }
        ManifestKind::Yaml => yaml_version(&String::from_utf8_lossy(payload))
            .ok_or("YAML 清单缺少 version: 行".to_owned())?,
    };
    validate_version(&raw)
}

/// 从 JSON 对象顶层取一个字符串字段, 取值不是字符串时返回 None.
///
/// Takes a top-level string field of a JSON object; a non-string value yields None.
fn json_field(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// 从 YAML 文本里取第一个顶层 version: 行.
///
/// Takes the first top-level version: line from a YAML text.
///
/// 只认行首的 version:, 缩进的 version: 属于子结构, 不采用. 取值去掉引号与行尾空白.
///
/// Only a version: at the start of a line counts; an indented version: belongs to a nested
/// structure and is not used. The value is stripped of quotes and trailing whitespace.
fn yaml_version(text: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("version:") {
            let value = rest.trim().trim_matches(|c| c == '"' || c == '\'');
            if !value.is_empty() {
                return Some(value.to_owned());
            }
        }
    }
    None
}

/// 校验版本是至少三段的点分数字.
///
/// Checks that a version is at least three dot-separated numeric segments.
///
/// 规则按至少三段判断: 每段纯数字, 段间一个点. Codex 的发布号是 0.160.1 四段, agy 的发布号
/// 是 1.2.17 三段, 两者都要通过. 带字母前缀, 连字符后缀或不足三段都算失败.
///
/// The rule is at least three segments: each segment is purely numeric, separated by a single
/// dot. Codex releases are four-segment like 0.160.1 and agy releases are three-segment like
/// 1.2.17, so both must pass. A letter prefix, a hyphen suffix or fewer than three segments
/// all fail.
pub fn validate_version(version: &str) -> Result<String, String> {
    let trimmed = version.trim();
    let mut segments = trimmed.split('.');
    let first = segments.next().unwrap_or("");
    let second = segments.next().unwrap_or("");
    let third = segments.next().unwrap_or("");
    let all_numeric = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if all_numeric(first) && all_numeric(second) && all_numeric(third) {
        Ok(trimmed.to_owned())
    } else {
        Err(format!("版本号 {trimmed:?} 不是至少三段的点分数字"))
    }
}
