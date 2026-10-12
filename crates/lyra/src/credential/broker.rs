//! ChatGPT 令牌分发的索取端凭据源. 账号只保存在内存中, 启动时不联网, 第一次取号时向
//! 分发端索取全部账号. 令牌临近过期或收到 401 刷新重试时, 以账号标识与强制刷新参数重新
//! 向分发端索取该账号并更新内存缓存.
//!
//! Client-side credential source for ChatGPT token brokering. Accounts live in memory only,
//! startup requires no network calls, and the first selection fetches all accounts from the
//! broker. When a token nears expiration or triggers a 401 refresh retry, the account is
//! re-fetched from the broker with its identifier and a force flag to update the cache.

use std::time::Duration;

use aria_core::jsonval;
use http::header;
use serde_json::Value;
use tokio::sync::Mutex;

use super::source::Source;
use super::{
    AuthHeaders, Entry, Error, GO_HTTP_CLIENT_USER_AGENT, Status, enabled, expires_at, headers_of,
    name_of, order_entries, parse_json_value, priority_of, read_limited, truncate, unix_now,
    upstream_error_suffix,
};

/// 索取端请求分发端超时时间, 与规格定义的 60 秒一致.
///
/// The overall timeout of a broker request, matching the 60 seconds defined by the spec.
pub const BROKER_TIMEOUT_SECONDS: u64 = 60;

/// 索取端提前重新索取的余量, 与规格定义的 3600 秒一致.
///
/// The early re-fetch margin on the broker client, matching the 3600 seconds defined by the spec.
pub const BROKER_REFRESH_MARGIN_SECONDS: i64 = 3600;

/// 分发响应最多读取的字节数.
///
/// The maximum number of bytes read from a broker response.
const MAX_BROKER_RESPONSE_BYTES: usize = 1 << 20;

struct BrokerState {
    cursor: usize,
    entries: Option<Vec<Entry>>,
}

/// BrokerSource 是分发索取源.
///
/// BrokerSource is the credential source fetching from a broker.
pub struct BrokerSource {
    url: String,
    key: String,
    client: reqwest::Client,
    state: Mutex<BrokerState>,
}

impl BrokerSource {
    /// 构造分发索取源.
    ///
    /// Builds the broker credential source.
    pub fn new(url: impl Into<String>, key: impl Into<String>, client: reqwest::Client) -> Self {
        Self {
            url: url.into(),
            key: key.into(),
            client,
            state: Mutex::new(BrokerState {
                cursor: 0,
                entries: None,
            }),
        }
    }

    /// 目标端点地址.
    ///
    /// The target endpoint URL.
    fn endpoint_url(&self) -> String {
        let trimmed = self.url.trim_end_matches('/');
        if trimmed.ends_with("/v1/broker/chatgpt") {
            trimmed.to_owned()
        } else {
            format!("{trimmed}/v1/broker/chatgpt")
        }
    }

    /// 向分发端索取账号.
    ///
    /// Fetches accounts from the broker.
    async fn fetch_from_broker(
        &self,
        account: Option<&str>,
        force: bool,
    ) -> Result<Vec<Entry>, Error> {
        let mut url = reqwest::Url::parse(&self.endpoint_url())
            .map_err(|error| Error::message(format!("分发地址无效: {error}")))?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(account_id) = account {
                query.append_pair("account", account_id);
            }
            if force {
                query.append_pair("force", "1");
            }
        }
        let response = self
            .client
            .get(url)
            .timeout(Duration::from_secs(BROKER_TIMEOUT_SECONDS))
            .header(header::AUTHORIZATION, format!("Bearer {}", self.key))
            .header(header::ACCEPT, "application/json")
            .header(header::USER_AGENT, GO_HTTP_CLIENT_USER_AGENT)
            .send()
            .await
            .map_err(|error| Error::message(format!("分发端请求失败: {error}")))?;
        let status = response.status();
        let body = read_limited(response, MAX_BROKER_RESPONSE_BYTES).await;
        if !status.is_success() {
            return Err(Error::message(format!(
                "分发端返回错误 HTTP {}{}",
                status.as_u16(),
                upstream_error_suffix(&body)
            )));
        }
        let Value::Object(payload) = parse_json_value(&body)
            .map_err(|error| Error::message(format!("分发端响应解析失败: {error}")))?
        else {
            return Err(Error::message("分发端响应顶层不是 JSON 对象"));
        };
        let Some(Value::Array(raw_credentials)) = payload.get("credentials") else {
            return Err(Error::message("分发端响应缺少 credentials 数组"));
        };
        let mut credentials = Vec::with_capacity(raw_credentials.len());
        for raw in raw_credentials {
            let Value::Object(entry) = raw else {
                return Err(Error::message("分发凭据条目不是 JSON 对象"));
            };
            // 校验规格要求的必要字段
            //
            // Validate required fields per specification
            let access_token = jsonval::as_str(entry.get("access_token"))
                .filter(|text| !text.is_empty())
                .ok_or_else(|| Error::message("分发凭据缺少 access_token"))?;
            let _ = access_token;
            let account_id = entry
                .get("account_id")
                .ok_or_else(|| Error::message("分发凭据缺少 account_id"))?;
            let _ = account_id;
            if entry.get("expires_at").is_none() {
                return Err(Error::message("分发凭据缺少 expires_at"));
            }
            credentials.push(entry.clone());
        }
        Ok(credentials)
    }
}

impl Source for BrokerSource {
    /// 第一次取号时向分发端索取全部账号并存入内存缓存, 之后复用 Pool 的优先级与游标排序规则.
    ///
    /// Fetches all accounts from the broker on first selection into memory, then reuses
    /// Pool's priority and cursor ordering.
    async fn snapshot(&self) -> Result<Vec<Entry>, Error> {
        let mut state = self.state.lock().await;
        if state.entries.is_none() {
            let fetched = self.fetch_from_broker(None, false).await?;
            if fetched.is_empty() {
                return Err(Error::NoEnabledCredential);
            }
            state.entries = Some(fetched);
        }
        let Some(entries) = state.entries.as_ref() else {
            return Err(Error::NoEnabledCredential);
        };
        let ordered = order_entries(entries, state.cursor, |_| false)?;
        state.cursor = state.cursor.wrapping_add(1);
        Ok(ordered)
    }

    /// 距到期不少于 3600 秒且不是强制刷新时直接返回认证头, 否则向分发端索取该账号并更新缓存.
    ///
    /// Returns auth headers directly when at least 3600 seconds remain before expiration and
    /// not a forced refresh; otherwise re-fetches from the broker and updates the cache.
    async fn authorize(&self, entry: Entry, force_refresh: bool) -> Result<AuthHeaders, Error> {
        if !force_refresh && fresh_enough_for_broker(&entry) {
            return headers_of(&entry);
        }
        let mut state = self.state.lock().await;
        let account_id = entry
            .get("account_id")
            .ok_or_else(|| Error::message("凭据缺少 account_id"))?;
        let account_id_str = jsonval::as_str(Some(account_id)).unwrap_or("");
        if !force_refresh
            && let Some(entries) = &state.entries
            && let Some(cached) = entries
                .iter()
                .find(|item| jsonval::same_value(item.get("account_id"), Some(account_id)))
            && fresh_enough_for_broker(cached)
        {
            return headers_of(cached);
        }
        let fetched = self
            .fetch_from_broker(Some(account_id_str), force_refresh)
            .await?;
        let updated = fetched
            .into_iter()
            .next()
            .ok_or_else(|| Error::message("分发端未返回账号"))?;
        if let Some(entries) = &mut state.entries {
            if let Some(index) = entries
                .iter()
                .position(|item| jsonval::same_value(item.get("account_id"), Some(account_id)))
            {
                entries[index] = updated.clone();
            } else {
                entries.push(updated.clone());
            }
        } else {
            state.entries = Some(vec![updated.clone()]);
        }
        headers_of(&updated)
    }

    /// 返回内存缓存中的账号状态. 账号列表只在内存中保存, 不读写任何文件.
    ///
    /// Returns the account statuses from the memory cache. Accounts live in memory only
    /// without reading or writing any file.
    async fn status(&self) -> Result<Vec<Status>, Error> {
        let state = self.state.lock().await;
        let entries = state.entries.as_deref().unwrap_or(&[]);
        let now = unix_now();
        let mut statuses = Vec::with_capacity(entries.len());
        for entry in entries {
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

/// fresh_enough_for_broker 判断令牌距到期是否不少于 3600 秒.
///
/// fresh_enough_for_broker reports whether the token has at least 3600 seconds before expiry.
fn fresh_enough_for_broker(entry: &Entry) -> bool {
    expires_at(entry).saturating_sub(unix_now()) >= BROKER_REFRESH_MARGIN_SECONDS
}
