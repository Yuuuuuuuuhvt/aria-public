//! ChatGPT 账号池: 排序, 轮询, 刷新, 原子写回. 文件是运行真值.
//!
//! 刷新令牌可能随刷新轮换, 上游没有返回新值时才沿用旧值; 写回失败不让请求失败, 因为
//! 刷新本身已经成功, 令牌在内存里可用, 只记告警.
//!
//! 所有文件读写都在同一把状态锁内完成, 与改写前的 Python 实现的单锁语义一致. 需要
//! 刷新时在锁内重新读取完整文件, 这样写回时不会丢掉已停用的记录, 也不会出现两个并发
//! 请求用同一把刷新令牌把上游打两次.
//!
//! xai 线路的 SuperGrok 账号也是 OAuth 刷新令牌加原子写回的同一种模型, 因此复用本账号池.
//! 两条线路只在 OAuth 客户端参数上不同: 令牌端点, client_id, scope 与提前刷新的余量, 这些
//! 取值由构造函数传入, 取号, 刷新与写回的规则不变.
//!
//! The ChatGPT account pool: ordering, rotation, refresh and atomic write-back. The file
//! is the runtime source of truth.
//!
//! The refresh token may rotate with a refresh, so the old value is kept only when
//! upstream returns no new one; a failed write-back does not fail the request, because the
//! refresh itself succeeded and the token is usable in memory, so only a warning is
//! logged.
//!
//! All file reads and writes happen under one state lock, matching the single-lock
//! semantics of the Python implementation before the rewrite. When a refresh is needed the
//! whole file is re-read under the lock, so the write-back does not drop disabled records
//! and two concurrent requests cannot hit upstream twice with the same refresh token.
//!
//! The SuperGrok accounts of the xai line follow the same model of OAuth refresh tokens with
//! atomic write-back, so they reuse this pool. The two lines differ only in their OAuth client
//! parameters: the token endpoint, client_id, scope and the early-refresh margin, which the
//! constructor takes; selection, refresh and write-back rules stay the same.

use std::path::PathBuf;
use std::time::Duration;

use http::header;
use serde_json::Value;
use tokio::sync::Mutex;

use aria_core::jsonval;

use super::file::CredentialFile;
use super::source::Source;
use super::{
    AuthHeaders, Entry, Error, GO_HTTP_CLIENT_USER_AGENT, Status, enabled, expires_at, form_body,
    headers_of, name_of, order_entries, parse_json_value, priority_of, read_limited, truncate,
    unix_now, updated_at_text, upstream_error_suffix,
};

/// TokenURL 是刷新访问令牌的端点, 与改写前的 Python 实现一致.
///
/// TOKEN_URL is the access token refresh endpoint, matching the Python implementation
/// before the rewrite.
pub const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";

/// OAUTH_CLIENT_ID 与 OAUTH_SCOPE 沿用改写前的 Python 实现的取值. 它们来自 Codex 客户端
/// 的公开行为, 改动会导致刷新被上游拒绝.
///
/// OAUTH_CLIENT_ID and OAUTH_SCOPE reuse the values from the Python implementation before
/// the rewrite. They come from the public behavior of the Codex client, and changing them would
/// make upstream reject the refresh.
pub const OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const OAUTH_SCOPE: &str = "openid profile email offline_access";

/// REFRESH_MARGIN_SECONDS 是提前刷新的余量, 与 Go 版 config.RefreshMarginSeconds 的
/// 24 小时一致. 余量这么大是因为 ChatGPT 的访问令牌有效期以天计.
///
/// REFRESH_MARGIN_SECONDS is the early-refresh margin, matching Go's
/// config.RefreshMarginSeconds of 24 hours. The margin is large because a ChatGPT access
/// token lives for days.
pub const REFRESH_MARGIN_SECONDS: i64 = 24 * 3600;

/// REFRESH_TIMEOUT_SECONDS 是令牌刷新请求的整体超时, 与 Go 版 config 的 60 秒一致.
///
/// REFRESH_TIMEOUT_SECONDS is the overall timeout of a token refresh request, matching
/// Go's config value of 60 seconds.
pub const REFRESH_TIMEOUT_SECONDS: u64 = 60;

/// 刷新响应最多读取的字节数, 与 Go 的 io.LimitReader(body, 64<<10) 一致.
///
/// The maximum number of bytes read from a refresh response, matching Go's
/// io.LimitReader(body, 64<<10).
const MAX_REFRESH_BODY_BYTES: usize = 64 << 10;

struct State {
    cursor: usize,
    file: CredentialFile,
}

/// Pool 是凭据池.
///
/// Pool is the credential pool.
pub struct Pool {
    token_url: String,
    client_id: String,
    scope: String,
    /// 距到期不足这么多秒时提前刷新. 余量必须小于该线路访问令牌的有效期, 否则令牌永远
    /// 不够新鲜, 每次取号都会刷新一次并轮换刷新令牌.
    ///
    /// The token is refreshed early once fewer seconds than this remain. The margin must be
    /// shorter than the line's access token lifetime; otherwise the token is never fresh
    /// enough and every selection refreshes it and rotates the refresh token.
    refresh_margin_seconds: i64,
    client: reqwest::Client,
    state: Mutex<State>,
}

impl Pool {
    /// 用 ChatGPT 线路的 OAuth 客户端参数构造凭据池. token_url 单独传入而不是写死, 使
    /// 测试可以指向本机桩服务, 从而覆盖真实的刷新请求构造与响应解析; 生产调用方传入
    /// TOKEN_URL.
    ///
    /// Builds the credential pool with the OAuth client parameters of the ChatGPT line.
    /// token_url is a parameter rather than a hard-coded value so tests can point it at a
    /// local stub and still cover the real refresh request construction and response
    /// parsing; production callers pass TOKEN_URL.
    pub fn new(
        path: impl Into<PathBuf>,
        token_url: impl Into<String>,
        client: reqwest::Client,
    ) -> Self {
        Self::with_oauth(
            path,
            token_url,
            client,
            OAUTH_CLIENT_ID,
            OAUTH_SCOPE,
            REFRESH_MARGIN_SECONDS,
        )
    }

    /// 用给定线路的 OAuth 客户端参数构造凭据池, 供 xai 线路使用.
    ///
    /// client_id 与 scope 写进刷新表单, 取值必须与登录时官方客户端使用的参数一致, 否则
    /// 刷新会被上游拒绝. refresh_margin_seconds 按该线路访问令牌的有效期选择, 约束见字段
    /// 说明.
    ///
    /// Builds the credential pool with the OAuth client parameters of a given line, used by
    /// the xai line. client_id and scope go into the refresh form and must match the
    /// parameters the official client used at login, otherwise upstream rejects the refresh.
    /// refresh_margin_seconds is chosen from the line's access token lifetime; see the field
    /// for the constraint.
    pub fn with_oauth(
        path: impl Into<PathBuf>,
        token_url: impl Into<String>,
        client: reqwest::Client,
        client_id: impl Into<String>,
        scope: impl Into<String>,
        refresh_margin_seconds: i64,
    ) -> Self {
        Self {
            token_url: token_url.into(),
            client_id: client_id.into(),
            scope: scope.into(),
            refresh_margin_seconds,
            client,
            state: Mutex::new(State {
                cursor: 0,
                file: CredentialFile::new(path, "读取凭据文件失败", Error::NoEnabledCredential),
            }),
        }
    }

    /// 用刷新令牌换取新的访问令牌.
    ///
    /// 刷新令牌可能随刷新轮换, 上游没有返回新值时才沿用旧值. 返回的新条目复制自原
    /// 条目, 因此未知字段不会丢失.
    ///
    /// Exchanges a refresh token for a new access token. The refresh token may rotate with
    /// a refresh, so the old value is kept only when upstream returns no new one. The
    /// returned entry is copied from the original, so unknown fields are not lost.
    async fn refresh_token(&self, entry: &Entry) -> Result<Entry, Error> {
        let refresh_token = jsonval::as_str(entry.get("refresh_token"))
            .filter(|text| !text.is_empty())
            .ok_or_else(|| Error::message("凭据缺少 refresh_token"))?;
        // 字段按字典序排列, 与 Go 的 url.Values.Encode() 输出顺序一致.
        //
        // Fields are ordered by name, matching the order Go's url.Values.Encode() emits.
        let form = [
            ("client_id", self.client_id.as_str()),
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("scope", self.scope.as_str()),
        ];
        let response = self
            .client
            .post(&self.token_url)
            .timeout(Duration::from_secs(REFRESH_TIMEOUT_SECONDS))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::ACCEPT, "application/json")
            // Go 的客户端会补上这个标识, 上游据它区分调用方, 因此两个方向都显式固定.
            //
            // Go's client adds this identifier and upstream uses it to tell callers apart,
            // so it is pinned explicitly on both sides.
            .header(header::USER_AGENT, GO_HTTP_CLIENT_USER_AGENT)
            .body(form_body(&form))
            .send()
            .await
            .map_err(|error| Error::message(format!("令牌刷新失败: {error}")))?;
        let status = response.status();
        let body = read_limited(response, MAX_REFRESH_BODY_BYTES).await;
        if !status.is_success() {
            // pool.go 在这里带出响应体前 200 字节, Aria 只保留错误代码, 原因见
            // upstream_error_code.
            //
            // pool.go includes the first 200 bytes of the body here; Aria keeps only the
            // error code, for the reason given at upstream_error_code.
            return Err(Error::message(format!(
                "令牌刷新被拒绝 HTTP {}{}",
                status.as_u16(),
                upstream_error_suffix(&body)
            )));
        }
        let Value::Object(payload) = parse_json_value(&body)
            .map_err(|error| Error::message(format!("令牌刷新响应解析失败: {error}")))?
        else {
            return Err(Error::message("令牌刷新响应解析失败: 顶层不是 JSON 对象"));
        };
        let access_token = jsonval::as_str(payload.get("access_token"))
            .filter(|text| !text.is_empty())
            .ok_or_else(|| Error::message("令牌刷新未返回 access_token"))?;
        let mut refreshed = entry.clone();
        refreshed.insert(
            "access_token".to_owned(),
            Value::String(access_token.to_owned()),
        );
        if let Some(rotated) = jsonval::as_str(payload.get("refresh_token"))
            && !rotated.is_empty()
        {
            refreshed.insert(
                "refresh_token".to_owned(),
                Value::String(rotated.to_owned()),
            );
        }
        let mut expires_in = 3600_i64;
        if let Some(value) = jsonval::int_loose(payload.get("expires_in").unwrap_or(&Value::Null))
            && value > 0
        {
            expires_in = value;
        }
        // 上游报告的剩余秒数异常大时按饱和处理, 避免整数溢出; Go 在此处会回绕.
        //
        // An absurdly large remaining lifetime saturates instead of overflowing; Go wraps
        // here.
        refreshed.insert(
            "expires_at".to_owned(),
            Value::from(unix_now().saturating_add(expires_in)),
        );
        refreshed.insert("updated_at".to_owned(), Value::String(updated_at_text()));
        Ok(refreshed)
    }

    /// 返回该凭据刷新写回后的完整条目.
    ///
    /// 快路径不加锁: 令牌还在刷新余量之外时直接使用传入的条目. 需要刷新时在锁内重新
    /// 读取完整文件, 因此写回不会丢掉已停用的记录, 也不会出现两个并发请求用同一把刷新
    /// 令牌把上游打两次.
    ///
    /// Returns the complete entry after refreshing and writing back when needed.
    ///
    /// The fast path takes no lock: when the token is beyond the refresh margin it uses
    /// the passed entry directly. When a refresh is needed the whole file is re-read under
    /// the lock, so the write-back does not drop disabled records and two concurrent
    /// requests cannot hit upstream twice with the same refresh token.
    pub async fn authorize_entry(
        &self,
        entry: &Entry,
        force_refresh: bool,
    ) -> Result<Entry, Error> {
        if !force_refresh && fresh_enough(entry, self.refresh_margin_seconds) {
            return Ok(entry.clone());
        }
        let mut state = self.state.lock().await;
        let mut entries = state.file.load(true).await?;
        let Some(index) = entries
            .iter()
            .position(|item| jsonval::same_value(item.get("account_id"), entry.get("account_id")))
        else {
            return Err(Error::message(format!(
                "账号已从凭据池移除: {}",
                name_of(entry)
            )));
        };
        let current = entries[index].clone();
        if !force_refresh && fresh_enough(&current, self.refresh_margin_seconds) {
            return Ok(current);
        }
        let refreshed = self.refresh_token(&current).await?;
        entries[index] = refreshed.clone();
        // 写回失败不让请求失败. 刷新本身已经成功, 令牌在内存里可用, 因磁盘或权限
        // 问题把可成功的请求判为失败不划算, 只记告警.
        //
        // A failed write-back does not fail the request. The refresh itself succeeded
        // and the token is usable in memory, so failing a request that could succeed
        // because of disk or permission problems is a bad trade; only a warning is
        // logged.
        if let Err(error) = state.file.store(&entries).await {
            aria_core::log!(
                "credential {}: 刷新结果写回失败, 本次仍使用新令牌: {}",
                name_of(&refreshed),
                error
            );
            return Ok(refreshed);
        }
        aria_core::log!("credential {}: access token refreshed", name_of(&refreshed));
        Ok(refreshed)
    }

    /// 为分发端提供已刷新的账号条目列表.
    ///
    /// 处理时对目标账号执行与 Responses 透传相同的 authorize 规则, 刷新时写回凭据文件,
    /// 返回规格规定的字段, 排除 refresh_token 与其他非公开字段. 全部刷新失败时返回错误.
    ///
    /// Provides refreshed account entries for the broker endpoint.
    ///
    /// Each target account follows the same authorize rules as Responses passthrough,
    /// writing back to the credential file on refresh and returning only the fields specified
    /// by the spec while omitting refresh_token and other non-public fields. Fails if all
    /// target accounts fail to refresh.
    pub async fn broker_entries(
        &self,
        account: Option<&str>,
        force: bool,
    ) -> Result<Vec<Entry>, Error> {
        let entries = {
            let mut state = self.state.lock().await;
            state.file.load(false).await?
        };
        let targets: Vec<Entry> = match account {
            Some(account_id) => entries
                .into_iter()
                .filter(|item| {
                    jsonval::same_value(
                        item.get("account_id"),
                        Some(&Value::String(account_id.to_owned())),
                    )
                })
                .collect(),
            None => entries.into_iter().filter(enabled).collect(),
        };
        if targets.is_empty() {
            return Err(Error::message("未找到目标账号"));
        }
        let mut results = Vec::new();
        for target in targets {
            let target_name = name_of(&target);
            match self.authorize_entry(&target, force).await {
                Ok(authorized) => match sanitize_broker_entry(&authorized) {
                    Ok(clean) => results.push(clean),
                    Err(error) => {
                        aria_core::log!("credential {}: 分发条目校验失败: {}", target_name, error);
                    }
                },
                Err(error) => {
                    aria_core::log!(
                        "credential {}: 令牌刷新失败, 跳过分发: {}",
                        target_name,
                        error
                    );
                }
            }
        }
        if results.is_empty() {
            return Err(Error::message("全部目标账号刷新或获取失败"));
        }
        Ok(results)
    }
}

impl Source for Pool {
    /// 顺序规则与改写前的 Python 实现一致: 先按 priority 升序分组, 每组内部按游标
    /// 旋转起点, 游标在每次取号后自增一次. 这样同优先级的账号被均匀摊到各请求上, 而
    /// 备用账号不会因为全池轮转成为首选.
    ///
    /// The ordering rule matches the Python implementation before the rewrite: group by
    /// ascending priority, rotate the start within each group by the cursor, and advance
    /// the cursor once per selection. This spreads same-priority accounts evenly across
    /// requests and keeps a backup account from becoming the first choice through
    /// whole-pool rotation.
    async fn snapshot(&self) -> Result<Vec<Entry>, Error> {
        let mut state = self.state.lock().await;
        let entries = state.file.load(false).await?;
        let ordered = order_entries(&entries, state.cursor, |_| false)?;
        state.cursor = state.cursor.wrapping_add(1);
        Ok(ordered)
    }

    async fn authorize(&self, entry: Entry, force_refresh: bool) -> Result<AuthHeaders, Error> {
        let authorized = self.authorize_entry(&entry, force_refresh).await?;
        headers_of(&authorized)
    }

    async fn status(&self) -> Result<Vec<Status>, Error> {
        let mut state = self.state.lock().await;
        let entries = state.file.load(true).await?;
        let now = unix_now();
        let mut statuses = Vec::with_capacity(entries.len());
        for entry in &entries {
            let priority = priority_of(entry)?;
            let account_id = jsonval::as_str(entry.get("account_id")).unwrap_or("");
            let expires = expires_at(entry);
            statuses.push(Status {
                name: name_of(entry),
                account_id_prefix: truncate(account_id, 8),
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

/// freshEnough 判断令牌是否还在刷新余量之外.
///
/// fresh_enough reports whether the token is still beyond the refresh margin.
fn fresh_enough(entry: &Entry, margin_seconds: i64) -> bool {
    expires_at(entry).saturating_sub(unix_now()) >= margin_seconds
}

/// 过滤并校验分发端返回的凭据条目, 确保只包含规格规定的 7 个字段且不含 refresh_token.
///
/// Filters and validates a credential entry for the broker, ensuring it contains only
/// the seven fields required by the spec and never contains refresh_token.
pub(crate) fn sanitize_broker_entry(entry: &Entry) -> Result<Entry, Error> {
    let mut clean = Entry::new();
    let name = match jsonval::as_str(entry.get("name")) {
        Some(name) if !name.is_empty() => name.to_owned(),
        _ => name_of(entry),
    };
    clean.insert("name".to_owned(), Value::String(name));
    let account_id = entry
        .get("account_id")
        .cloned()
        .ok_or_else(|| Error::message("凭据缺少 account_id"))?;
    clean.insert("account_id".to_owned(), account_id);
    clean.insert(
        "plan".to_owned(),
        entry.get("plan").cloned().unwrap_or(Value::Null),
    );
    clean.insert("enabled".to_owned(), Value::Bool(enabled(entry)));
    clean.insert("priority".to_owned(), Value::from(priority_of(entry)?));
    let access_token = jsonval::as_str(entry.get("access_token"))
        .filter(|text| !text.is_empty())
        .ok_or_else(|| Error::message("凭据缺少 access_token"))?;
    clean.insert(
        "access_token".to_owned(),
        Value::String(access_token.to_owned()),
    );
    clean.insert("expires_at".to_owned(), Value::from(expires_at(entry)));
    Ok(clean)
}

#[cfg(test)]
mod tests;
