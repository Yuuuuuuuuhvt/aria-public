//! Google 线路的凭据源: 会话固定, 刷新, 项目标识推导.
//!
//! 结构与 Pool 平行: 文件是运行真值, 取号按优先级分组轮转, 刷新结果原子写回. 额外的
//! 差异有两个: Google 凭据用邮箱标识账号, 因此会话固定以邮箱为键; 上游信封要求项目
//! 标识, 缺失时在刷新后用 loadCodeAssist 补齐.
//!
//! 刷新与推导请求都带 60 秒整体超时, 与 Go 版的客户端超时一致. 令牌地址与
//! loadCodeAssist 基址由构造参数传入, 使测试可以指向本机桩服务, 从而覆盖真实的请求
//! 构造与响应解析, 而不是用一个替身跳过整段逻辑.
//!
//! The Google route's credential source: session pinning, refresh and project id
//! discovery.
//!
//! Its structure parallels Pool: the file is the runtime source of truth, selection
//! rotates by priority group, and refresh results are written back atomically. Two
//! differences remain: Google credentials identify accounts by email, so session pinning
//! uses the email as its key, and the upstream envelope requires a project id, which is
//! filled in through loadCodeAssist after a refresh when missing.
//!
//! Both the refresh and the discovery request carry a 60-second overall timeout, matching
//! the Go client's timeout. The token URL and the loadCodeAssist base are constructor
//! parameters so tests can point them at a local stub and still cover the real request
//! construction and response parsing instead of skipping the whole path with a stand-in.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use http::{StatusCode, header};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;

use aria_core::jsonval;

use super::file::CredentialFile;
use super::source::{SessionScoped, Source};
use super::{
    AuthHeaders, Entry, Error, GO_HTTP_CLIENT_USER_AGENT, Status, enabled, expires_at, form_body,
    name_of, order_entries, priority_of, read_limited, truncate, unix_now, updated_at_text,
    upstream_error_suffix,
};
use crate::client_id::ClientIdentity;

/// 刷新访问令牌的端点.
///
/// The access token refresh endpoint.
pub const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// GOOGLE_CLIENT_ID 是 Antigravity 客户端使用的 OAuth 客户端编号.
///
/// 只有客户端编号是公开常量; 配对的客户端密钥属于 Antigravity 官方客户端, 不随本仓库分发,
/// 调用方通过 GoogleSource::new 的 client_secret 参数传入, 该参数来自配置或环境变量.
///
/// GOOGLE_CLIENT_ID is the OAuth client id used by the Antigravity client.
///
/// Only the client id is a public constant; the paired client secret belongs to the Antigravity
/// official client and is not distributed with this repository. Callers pass it through the
/// client_secret parameter of GoogleSource::new, sourced from configuration or the environment.
pub const GOOGLE_CLIENT_ID: &str =
    "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";

/// 提供 Antigravity OAuth 客户端密钥的环境变量名.
///
/// 这是缺少配置取值时的回退来源, 由需要的线路统一引用, 避免同一名称在多处各写一份.
///
/// The environment variable that carries the Antigravity OAuth client secret.
///
/// It is the fallback when no configured value exists and is referenced by every line that needs
/// it, so the name is written once.
pub const GOOGLE_CLIENT_SECRET_ENV: &str = "ARIA_ANTIGRAVITY_CLIENT_SECRET";

/// 从环境变量读取 Antigravity 的 OAuth 客户端密钥, 未设置或为空时给出清楚的错误.
///
/// Reads the Antigravity OAuth client secret from the environment, with a clear error when it is
/// unset or empty.
pub fn google_client_secret_from_env() -> Result<String, String> {
    match std::env::var(GOOGLE_CLIENT_SECRET_ENV) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(format!(
            "Google login needs a client secret: set the {GOOGLE_CLIENT_SECRET_ENV} environment variable"
        )),
    }
}

/// 内部接口的版本前缀.
///
/// The version prefix of the internal API.
pub const GOOGLE_API_VERSION: &str = "v1internal";

/// GOOGLE_REFRESH_MARGIN_SECONDS 是提前刷新的余量. Google 访问令牌有效期约一小时,
/// 官方客户端留 5 分钟, 这里与它一致. 与 ChatGPT 的 24 小时余量差异很大, 是因为两者
/// 的令牌有效期相差两个数量级.
///
/// GOOGLE_REFRESH_MARGIN_SECONDS is the early-refresh margin. A Google access token is
/// valid for about one hour and the official client leaves 5 minutes, which is matched
/// here. The large difference from ChatGPT's 24-hour margin comes from the token
/// lifetimes differing by two orders of magnitude.
pub const GOOGLE_REFRESH_MARGIN_SECONDS: i64 = 300;

/// 刷新与项目标识推导的整体超时.
///
/// The overall timeout of a refresh and of project id discovery.
pub const GOOGLE_REFRESH_TIMEOUT_SECONDS: u64 = 60;

/// maxSessionPins 是会话固定表的容量上限. 超出后按插入顺序淘汰最早的会话, 避免长期
/// 运行下这张表无界增长. 淘汰只影响该会话一次取号, 之后会重新固定.
///
/// MAX_SESSION_PINS is the capacity limit of the session pin table. Beyond it the oldest
/// session is evicted in insertion order, preventing unbounded growth over a long run.
/// Eviction only affects that session's next selection, after which it pins again.
pub const MAX_SESSION_PINS: usize = 4096;

/// 刷新与推导响应最多读取的字节数, 与 Go 的 io.LimitReader(body, 1<<20) 一致.
///
/// The maximum number of bytes read from a refresh or discovery response, matching Go's
/// io.LimitReader(body, 1<<20).
const MAX_GOOGLE_BODY_BYTES: usize = 1 << 20;

struct State {
    cursor: usize,
    file: CredentialFile,
    /// pins 记录每个会话当前固定的账号邮箱, pin_order 按插入顺序保存键以便淘汰.
    /// 会话固定让同一会话的连续请求落在同一账号上, 从而提高上游提示缓存的命中率.
    ///
    /// pins records the account email currently pinned for each session, and pin_order
    /// keeps the keys in insertion order for eviction. Pinning makes consecutive requests
    /// of one session land on the same account, raising the upstream prompt cache hit
    /// rate.
    pins: HashMap<String, String>,
    pin_order: VecDeque<String>,
}

/// GoogleSource 是 Google 线路的凭据源.
///
/// GoogleSource is the credential source for the Google route.
pub struct GoogleSource {
    client: reqwest::Client,
    client_id: String,
    client_secret: String,
    daily_base: String,
    token_url: String,
    /// 共享的 Antigravity 客户端标识, 由 3.8 的后台任务更新.
    ///
    /// The shared Antigravity client identifier, updated by the 3.8 background task.
    user_agent: Arc<ClientIdentity>,
    state: Mutex<State>,
}

impl GoogleSource {
    /// 构造凭据源. client 用于令牌刷新与项目标识推导; token_url 与 daily_base 由调用方
    /// 传入, 生产值分别是 GOOGLE_TOKEN_URL 与配置中的上游基址.
    ///
    /// Builds the credential source. The client is used for token refresh and project id
    /// discovery; token_url and daily_base come from the caller, with production values
    /// GOOGLE_TOKEN_URL and the upstream base from configuration.
    pub fn new(
        path: impl Into<PathBuf>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        daily_base: impl Into<String>,
        token_url: impl Into<String>,
        client: reqwest::Client,
        user_agent: Arc<ClientIdentity>,
    ) -> Self {
        Self {
            client,
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            daily_base: daily_base.into(),
            token_url: token_url.into(),
            user_agent,
            state: Mutex::new(State {
                cursor: 0,
                file: CredentialFile::new(
                    path,
                    "读取 Google 凭据文件失败",
                    Error::NoGoogleCredential,
                ),
                pins: HashMap::new(),
                pin_order: VecDeque::new(),
            }),
        }
    }

    /// freshEnough 判断访问令牌是否还在刷新余量之外. 与 Pool 的区别只在余量取值, 这里
    /// 按 Google 的一小时有效期使用 5 分钟.
    ///
    /// fresh_enough reports whether the access token is still beyond the refresh margin.
    /// It differs from Pool's only in the margin value: 5 minutes here, matching Google's
    /// one-hour validity.
    fn fresh_enough(&self, entry: &Entry) -> bool {
        let token = jsonval::as_str(entry.get("access_token")).unwrap_or("");
        if token.trim().is_empty() {
            return false;
        }
        expires_at(entry) > unix_now() + GOOGLE_REFRESH_MARGIN_SECONDS
    }

    /// 用刷新令牌换访问令牌, 必要时推导项目标识.
    ///
    /// 刷新请求只有四个字段, 没有 redirect_uri 也没有 scope, 这一点与官方客户端的实际
    /// 请求一致.
    ///
    /// Exchanges a refresh token for an access token and discovers the project id when
    /// needed. The refresh request has only four fields, with neither redirect_uri nor
    /// scope, matching the official client's actual request.
    async fn default_refresher(&self, entry: &Entry) -> Result<Entry, Error> {
        let refresh_token = jsonval::as_str(entry.get("refresh_token")).unwrap_or("");
        if refresh_token.trim().is_empty() {
            return Err(Error::message(format!(
                "凭据缺少 refresh_token: {}",
                name_of(entry)
            )));
        }
        let form = [
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.as_str()),
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ];
        let response = self
            .client
            .post(&self.token_url)
            .timeout(Duration::from_secs(GOOGLE_REFRESH_TIMEOUT_SECONDS))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            // 官方客户端在这一步刻意使用 Go 的默认标识, 因此与 Go 一样 fixed 该请求头.
            //
            // The official client deliberately uses Go's default identifier at this step,
            // so the header is pinned as in Go.
            .header(header::USER_AGENT, GO_HTTP_CLIENT_USER_AGENT)
            .body(form_body(&form))
            .send()
            .await
            .map_err(|error| Error::message(format!("刷新 Google 访问令牌失败: {error}")))?;
        let status = response.status();
        let payload = read_limited(response, MAX_GOOGLE_BODY_BYTES).await;
        if status != StatusCode::OK {
            return Err(Error::message(format!(
                "刷新 Google 访问令牌失败, 状态码 {}{}",
                status.as_u16(),
                upstream_error_suffix(&payload)
            )));
        }
        // Go 在这一步用 json.Unmarshal 解进固定结构体, 它拒绝尾随内容, 因此这里同样
        // 用严格解析.
        //
        // Go unmarshals into a fixed struct here, which rejects trailing content, so this
        // parse is strict as well.
        let parsed: GoogleTokenResponse = serde_json::from_slice(&payload)
            .map_err(|error| Error::message(format!("解析刷新响应失败: {error}")))?;
        let access_token = parsed.access_token.unwrap_or_default();
        if access_token.is_empty() {
            return Err(Error::message("刷新响应中没有 access_token"));
        }
        let mut updated = entry.clone();
        updated.insert(
            "access_token".to_owned(),
            Value::String(access_token.clone()),
        );
        // 只在响应确实带回新的刷新令牌时才覆盖, 否则会把可用的旧值抹掉.
        // Overwrite only when the response actually carries a new refresh token;
        // otherwise a usable old value would be erased.
        if let Some(rotated) = parsed.refresh_token.filter(|text| !text.is_empty()) {
            updated.insert("refresh_token".to_owned(), Value::String(rotated));
        }
        updated.insert(
            "expires_at".to_owned(),
            Value::from(unix_now().saturating_add(parsed.expires_in.unwrap_or(0))),
        );
        if let Some(token_type) = parsed.token_type.filter(|text| !text.is_empty()) {
            updated.insert("token_type".to_owned(), Value::String(token_type));
        }
        updated.insert("updated_at".to_owned(), Value::String(updated_at_text()));

        // 项目标识是上游信封的必填字段, 缺失时在这里补齐, 否则发出去的请求会被拒.
        // The project id is a required field of the upstream envelope and is filled in
        // here when missing; otherwise the outgoing request would be rejected.
        if project_id_of(&updated).is_empty() {
            let project_id = self.discover_project_id(&access_token).await?;
            updated.insert("project_id".to_owned(), Value::String(project_id));
        }
        Ok(updated)
    }

    /// 调用 loadCodeAssist 取得项目标识.
    ///
    /// agy 的实测抓取里, 刷新令牌后就是 loadCodeAssist 读回项目标识, 没有单独的兜底
    /// 创建步骤. 本流程沿用这一步: 只做读取, 不做创建. 真出现缺失时返回错误比静默
    /// 发一个会被拒绝的请求更容易排查.
    ///
    /// Calls loadCodeAssist to obtain the project id. In agy's measured capture, the
    /// loadCodeAssist read follows the token refresh with no separate create step. This
    /// flow keeps that shape: it only reads and never creates a fallback. Returning an
    /// error when it is genuinely missing is easier to diagnose than silently sending a
    /// request that will be rejected.
    async fn discover_project_id(&self, access_token: &str) -> Result<String, Error> {
        let body = json!({"metadata": {"ideType": "ANTIGRAVITY"}}).to_string();
        let endpoint = format!("{}/{}:loadCodeAssist", self.daily_base, GOOGLE_API_VERSION);
        // loadCodeAssist 使用共享标识, 与生成请求保持同一版本, 见 architecture.md 3.8.
        //
        // loadCodeAssist uses the shared identifier, the same version as generation requests,
        // see section 3.8 of architecture.md.
        let user_agent = self.user_agent.user_agent();
        let response = self
            .client
            .post(&endpoint)
            .timeout(Duration::from_secs(GOOGLE_REFRESH_TIMEOUT_SECONDS))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::USER_AGENT, user_agent.as_str())
            .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
            .body(body)
            .send()
            .await
            .map_err(|error| Error::message(format!("推导项目标识失败: {error}")))?;
        let status = response.status();
        let raw = read_limited(response, MAX_GOOGLE_BODY_BYTES).await;
        if status != StatusCode::OK {
            return Err(Error::message(format!(
                "推导项目标识失败, 状态码 {}{}",
                status.as_u16(),
                upstream_error_suffix(&raw)
            )));
        }
        let Value::Object(payload) = serde_json::from_slice(&raw)
            .map_err(|error| Error::message(format!("解析 loadCodeAssist 响应失败: {error}")))?
        else {
            return Err(Error::message(
                "解析 loadCodeAssist 响应失败: 顶层不是 JSON 对象",
            ));
        };
        match pick_project(&payload) {
            Some(project) => Ok(project),
            None => Err(Error::message("loadCodeAssist 响应中没有项目标识")),
        }
    }
}

impl Source for GoogleSource {
    /// 不按会话固定的调用方走这里. 按会话固定的调用方使用 snapshot_for, 让同一会话
    /// 固定到同一账号, 以复用上游提示缓存.
    ///
    /// Callers that do not pin by session use this. Callers that do use snapshot_for so
    /// one session pins to one account and reuses the upstream prompt cache.
    async fn snapshot(&self) -> Result<Vec<Entry>, Error> {
        let mut state = self.state.lock().await;
        let entries = state.file.load(false).await?;
        let ordered = order_entries(&entries, state.cursor, |_| false)?;
        state.cursor = state.cursor.wrapping_add(1);
        Ok(ordered)
    }

    /// 快路径不加锁, 与 Pool 相同. 需要刷新时在锁内重新读取完整文件, 避免并发刷新把
    /// 同一把刷新令牌用两次, 也避免写回时丢掉已停用的记录.
    ///
    /// The fast path takes no lock, as in Pool. When a refresh is needed the whole file is
    /// re-read under the lock, preventing two concurrent refreshes from using the same
    /// refresh token and preventing a write-back from dropping disabled records.
    async fn authorize(&self, entry: Entry, force_refresh: bool) -> Result<AuthHeaders, Error> {
        if !force_refresh && self.fresh_enough(&entry) {
            return google_headers_of(&entry);
        }
        let mut state = self.state.lock().await;
        let mut entries = state.file.load(true).await?;
        let Some(index) = entries
            .iter()
            .position(|item| same_google_identity(item, &entry))
        else {
            return Err(Error::message(format!(
                "账号已从凭据池移除: {}",
                name_of(&entry)
            )));
        };
        let current = entries[index].clone();
        if !force_refresh && self.fresh_enough(&current) {
            return google_headers_of(&current);
        }
        let refreshed = self.default_refresher(&current).await?;
        entries[index] = refreshed.clone();
        // 写回失败不让请求失败. 刷新已经成功, 令牌在内存里是可用的; 因为磁盘或权限
        // 问题把一个本来能成功的请求判为失败, 代价远大于少一次持久化. 后果只是下次
        // 请求会再刷新一次, 因此只记告警.
        //
        // A failed write-back does not fail the request. The refresh already succeeded
        // and the token is usable in memory; failing a request that could succeed
        // because of disk or permission problems costs far more than skipping one
        // persistence. The only consequence is one more refresh on the next request,
        // so only a warning is logged.
        if let Err(error) = state.file.store(&entries).await {
            aria_core::log!(
                "credential {}: 刷新结果写回失败, 本次仍使用新令牌: {}",
                name_of(&refreshed),
                error
            );
            return google_headers_of(&refreshed);
        }
        aria_core::log!("credential {}: access token refreshed", name_of(&refreshed));
        google_headers_of(&refreshed)
    }

    async fn status(&self) -> Result<Vec<Status>, Error> {
        let mut state = self.state.lock().await;
        let entries = state.file.load(true).await?;
        let now = unix_now();
        let mut statuses = Vec::with_capacity(entries.len());
        for entry in &entries {
            let priority = priority_of(entry)?;
            let email = email_of(entry);
            let expires = expires_at(entry);
            statuses.push(Status {
                name: name_of(entry),
                account_id_prefix: truncate(&email, 8),
                plan: entry.get("plan").cloned().unwrap_or(Value::Null),
                enabled: enabled(entry),
                priority,
                note: entry.get("note").cloned().unwrap_or(Value::Null),
                expires_at: expires,
                expires_in_seconds: expires.saturating_sub(now),
            });
        }
        Ok(statuses)
    }
}

impl SessionScoped for GoogleSource {
    /// 首次出现的会话固定到本轮起点账号并推进游标, 因此新会话仍会分散到不同账号;
    /// 之后该会话不再随游标移动. session_key 为空时退化为 snapshot 的行为.
    ///
    /// A session appearing for the first time pins the account at the current rotation
    /// start and advances the cursor, so new sessions still spread across accounts;
    /// afterwards that session no longer follows the cursor. An empty session_key
    /// degrades to snapshot's behavior.
    async fn snapshot_for(&self, session_key: &str) -> Result<Vec<Entry>, Error> {
        let mut state = self.state.lock().await;
        let entries = state.file.load(false).await?;
        if session_key.is_empty() {
            let ordered = order_entries(&entries, state.cursor, |_| false)?;
            state.cursor = state.cursor.wrapping_add(1);
            return Ok(ordered);
        }
        let pinned = state.pins.get(session_key).cloned().unwrap_or_default();
        let ordered = order_entries(&entries, state.cursor, |entry| {
            !pinned.is_empty() && email_of(entry) == pinned
        })?;
        if pinned.is_empty() && !ordered.is_empty() {
            let email = email_of(&ordered[0]);
            pin_locked(&mut state, session_key, email);
            state.cursor = state.cursor.wrapping_add(1);
        }
        Ok(ordered)
    }

    /// 在当前固定的账号不可用时调用, 例如上游返回 401, 403, 429 或 503. 只有一个可用
    /// 账号时没有下一个可换, 保持原固定位置.
    ///
    /// Called when the currently pinned account is unavailable, for example when upstream
    /// returns 401, 403, 429 or 503. With only one usable account there is no next account
    /// to move to, so the pin stays.
    async fn advance(&self, session_key: &str) {
        if session_key.is_empty() {
            return;
        }
        let mut state = self.state.lock().await;
        let Ok(entries) = state.file.load(false).await else {
            return;
        };
        let pinned = state.pins.get(session_key).cloned().unwrap_or_default();
        let Ok(ordered) = order_entries(&entries, state.cursor, |entry| {
            !pinned.is_empty() && email_of(entry) == pinned
        }) else {
            return;
        };
        if ordered.len() < 2 {
            return;
        }
        let next = email_of(&ordered[1]);
        pin_locked(&mut state, session_key, next);
    }
}

/// 更新会话固定, 并在超出容量时按插入顺序淘汰最早的会话. 已经存在的会话不会改变淘汰
/// 顺序, 与 Go 的实现一致.
///
/// Updates the session pin and evicts the oldest session in insertion order when the
/// capacity is exceeded. An existing session keeps its eviction position, matching the Go
/// implementation.
fn pin_locked(state: &mut State, session_key: &str, email: String) {
    if !state.pins.contains_key(session_key) {
        state.pin_order.push_back(session_key.to_owned());
        if state.pin_order.len() > MAX_SESSION_PINS
            && let Some(oldest) = state.pin_order.pop_front()
        {
            state.pins.remove(&oldest);
        }
    }
    state.pins.insert(session_key.to_owned(), email);
}

/// ProjectIDOf 返回条目的项目标识, 缺少时返回空串. 上游信封需要它.
///
/// Returns the entry's project id, or an empty string when missing. The upstream
/// envelope needs it.
pub fn project_id_of(entry: &Entry) -> String {
    jsonval::as_str(entry.get("project_id"))
        .unwrap_or("")
        .to_owned()
}

/// EmailOf 返回条目的邮箱. Google 凭据用邮箱标识账号, 会话固定以它为键.
///
/// Returns the entry's email. Google credentials identify accounts by email, and session
/// pinning uses it as the key.
pub fn email_of(entry: &Entry) -> String {
    jsonval::as_str(entry.get("email")).unwrap_or("").to_owned()
}

/// 判断两条记录是否指向同一个账号. Google 凭据用邮箱标识; 邮箱缺失时退回用名称比较,
/// 与 name_of 的规则一致.
///
/// Reports whether two records point to the same account. Google credentials are
/// identified by email; when the email is missing it falls back to comparing names,
/// matching name_of's rule.
fn same_google_identity(left: &Entry, right: &Entry) -> bool {
    let left_email = email_of(left);
    let right_email = email_of(right);
    if !left_email.is_empty() && !right_email.is_empty() {
        return left_email == right_email;
    }
    name_of(left) == name_of(right)
}

/// 按官方客户端与两个参考实现共有的顺序提取项目标识: 三个键, 值可以是字符串, 也可以是
/// 带 id 子字段的对象.
///
/// Extracts the project id in the order shared by the official client and two reference
/// implementations: three keys whose value may be a string or an object with an id
/// subfield.
fn pick_project(payload: &Map<String, Value>) -> Option<String> {
    for key in ["cloudaicompanionProject", "projectId", "project"] {
        match payload.get(key) {
            Some(Value::String(text)) if !text.is_empty() => return Some(text.clone()),
            Some(Value::Object(inner)) => {
                if let Some(Value::String(id)) = inner.get("id")
                    && !id.is_empty()
                {
                    return Some(id.clone());
                }
            }
            _ => {}
        }
    }
    None
}

/// googleHeadersOf 生成上游请求需要的鉴权头. Google 线路只有 Authorization 一项.
///
/// Builds the auth headers the upstream request needs. The Google route has only
/// Authorization.
fn google_headers_of(entry: &Entry) -> Result<AuthHeaders, Error> {
    let token = jsonval::as_str(entry.get("access_token")).unwrap_or("");
    if token.trim().is_empty() {
        return Err(Error::message(format!(
            "凭据没有可用的访问令牌: {}",
            name_of(entry)
        )));
    }
    Ok(vec![("Authorization", format!("Bearer {token}"))])
}

/// 刷新响应中使用的字段. 用 Option 接收是因为 Go 的字符串与整数在收到 null 时不报错,
/// 只保持零值, 而 null 在 serde 的裸类型上是错误.
///
/// The fields used from a refresh response. Option is used because Go's strings and
/// integers accept null without error and simply keep the zero value, whereas null is an
/// error for a bare type in serde.
#[derive(Debug, Default, Deserialize)]
struct GoogleTokenResponse {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    token_type: Option<String>,
}

#[cfg(test)]
mod tests;
