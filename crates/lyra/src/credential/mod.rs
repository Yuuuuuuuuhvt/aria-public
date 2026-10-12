//! 账号凭据池. pool 管理 ChatGPT 账号, 也以 xai 线路的 OAuth 参数管理 SuperGrok 账号,
//! google 管理 Antigravity 使用的 Google 账号, file 提供各线路共用的文件格式与缓存, source
//! 约定调用方依赖的取号与鉴权接口.
//!
//! 两条线路的凭据语义不同: ChatGPT 用账号令牌, 续期走 auth.openai.com; Google 用
//! OAuth 刷新令牌, 续期走 oauth2.googleapis.com, 并且额外需要一个项目标识. 因此共用
//! 的只有文件格式, 取值规则与取号顺序, 具体刷新各自实现.
//!
//! 本模块的行为与 Go 版 Lyra 的 internal/credential 逐项对齐, 需要特别保持一致的有
//! 四处:
//!
//!  1. 条目用 JSON 映射承载而不是固定结构体. 刷新后要把整条记录写回, note, source,
//!     updated_at 等字段必须原样保留, 固定结构体会静默丢弃未知字段.
//!  2. priority 必须是整数字面量. 3.0 这类小数字面量必须被拒绝, 因此用
//!     jsonval::int_strict 按字面量判断.
//!  3. enabled 按真值判断, 字符串 "false" 仍算启用.
//!  4. 每个优先级组内部按请求轮转, 组间按 priority 升序, 游标每次取号自增一次.
//!
//! Account credential pools: pool manages ChatGPT accounts and, with the OAuth parameters of
//! the xai line, SuperGrok accounts; google manages the Google accounts used by Antigravity,
//! file provides the file format and cache shared by the lines, and source fixes the
//! selection and authorization interface callers depend on.
//!
//! The two routes have different credential semantics: ChatGPT uses account tokens
//! renewed through auth.openai.com, while Google uses OAuth refresh tokens renewed
//! through oauth2.googleapis.com and additionally needs a project id. Only the file
//! format, the value rules and the selection order are shared; each side implements its
//! own refresh.
//!
//! This module aligns item by item with Go Lyra's internal/credential; four points must
//! stay consistent in particular:
//!
//!  1. Entries are carried as JSON maps rather than fixed structs. After a refresh the
//!     whole record is written back, so fields such as note, source and updated_at must
//!     survive unchanged; a fixed struct would silently drop unknown fields.
//!  2. priority must be an integer literal. Decimal literals such as 3.0 must be
//!     rejected, so jsonval::int_strict judges by literal form.
//!  3. enabled is judged by truthiness, and the string "false" still counts as enabled.
//!  4. Within each priority group accounts rotate per request, groups are ordered by
//!     ascending priority, and the cursor advances once per selection.

use std::collections::BTreeMap;
use std::fmt;

use serde::Serialize;
use serde_json::{Map, Value};

use aria_core::jsonval;

pub mod broker;
pub mod file;
pub mod google;
pub mod pool;
pub mod source;

#[cfg(test)]
mod stub;

pub use broker::BrokerSource;
pub use google::{GOOGLE_REFRESH_MARGIN_SECONDS, GoogleSource, MAX_SESSION_PINS};
pub use pool::{OAUTH_CLIENT_ID, OAUTH_SCOPE, Pool, REFRESH_MARGIN_SECONDS, TOKEN_URL};
pub use source::{SessionScoped, Source};

/// 一条凭据记录. 用 JSON 映射而不是固定结构体, 使刷新写回时不丢失未知字段, 数字也
/// 保持原始字面量: 整数不会因为经过浮点结构而变成小数.
///
/// One credential record. A JSON map rather than a fixed struct keeps unknown fields
/// intact when a refresh is written back, and numbers keep their original literal form:
/// an integer never turns into a decimal by passing through a floating-point struct.
pub type Entry = Map<String, Value>;

/// 需要设置的认证请求头. 用列表而不是映射, 使设置顺序固定且可以被测试直接断言.
///
/// The auth headers to set. A list rather than a map keeps the order fixed and directly
/// assertable in tests.
pub type AuthHeaders = Vec<(&'static str, String)>;

/// 凭据池操作失败的原因.
///
/// Why a credential pool operation failed.
#[derive(Debug, Clone)]
pub enum Error {
    /// 对应 ErrNoEnabledCredential: 池中没有启用的账号.
    NoEnabledCredential,
    /// 对应 ErrNoGoogleCredential: Google 凭据池中没有可用的账号.
    NoGoogleCredential,
    /// 其余失败. 消息面向运维, 不含令牌内容.
    Message(String),
}

impl Error {
    /// 构造一条面向运维的失败消息.
    ///
    /// Builds an operator-facing failure message.
    pub fn message(text: impl Into<String>) -> Self {
        Self::Message(text.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEnabledCredential => formatter.write_str("凭据池中没有启用的账号"),
            Self::NoGoogleCredential => formatter.write_str("Google 凭据池中没有可用的账号"),
            Self::Message(text) => formatter.write_str(text),
        }
    }
}

impl std::error::Error for Error {}

/// 健康检查里单条凭据的公开状态, 不含任何令牌内容.
///
/// The public status of one credential in the health check, containing no token material.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Status {
    pub name: String,
    pub account_id_prefix: String,
    pub plan: Value,
    pub enabled: bool,
    pub priority: i64,
    pub note: Value,
    pub expires_at: i64,
    pub expires_in_seconds: i64,
}

/// 返回条目的展示名, 与 Go 的 NameOf 规则一致: 先取 name, 为空时取 account_id 的
/// 前 8 个字符, 连 account_id 都缺失时返回 unknown.
///
/// Returns the entry's display name, matching Go's NameOf: take name first, then the
/// first 8 characters of account_id, and unknown when even account_id is missing.
pub fn name_of(entry: &Entry) -> String {
    if let Some(name) = jsonval::as_str(entry.get("name"))
        && !name.is_empty()
    {
        return name.to_owned();
    }
    match entry.get("account_id") {
        None => "unknown".to_owned(),
        Some(value) => truncate(&scalar_text(value), 8),
    }
}

/// 返回条目的优先级, 缺失时为 0.
///
/// 与 Go 一样拒绝非整数字面量, 而不是静默取默认值: 配置写错时应当立刻失败, 不能悄悄
/// 改变账号的主备关系.
///
/// Returns the entry's priority, or 0 when missing. Like Go it rejects non-integer
/// literals rather than silently defaulting: a miswritten configuration should fail
/// immediately and must not quietly change the primary/backup relationship of accounts.
pub fn priority_of(entry: &Entry) -> Result<i64, Error> {
    match entry.get("priority") {
        None | Some(Value::Null) => Ok(0),
        Some(value) => jsonval::int_strict(value)
            .ok_or_else(|| Error::message("credential priority must be an integer")),
    }
}

/// 复现改写前的 Python 实现的 entry.get("enabled", True).
///
/// Reproduces the entry.get("enabled", True) expression of the Python implementation
/// before the rewrite.
pub fn enabled(entry: &Entry) -> bool {
    match entry.get("enabled") {
        None => true,
        Some(value) => jsonval::truthy(value),
    }
}

/// 读取到期时刻. 取值不是数字时按 0 处理, 结果是触发一次刷新尝试, 比直接失败更接近
/// 可用的语义.
///
/// Reads the expiry instant. A non-numeric value is treated as 0, which triggers one
/// refresh attempt; that is closer to a usable semantics than failing outright.
pub fn expires_at(entry: &Entry) -> i64 {
    match entry.get("expires_at") {
        None => 0,
        Some(value) => jsonval::int_loose(value).unwrap_or(0),
    }
}

/// 截断字符串到给定字节数. Go 按字节截断, 可能切断多字节字符; Go 的 JSON 编码会把
/// 非法字节写成 U+FFFD, 因此这里用有损转换得到同样的文本, 同时避免在 Rust 中按字节
/// 切片时切到字符边界而 panic.
///
/// Shortens a string to the given number of bytes. Go truncates by bytes and may cut a
/// multi-byte character; its JSON encoder writes the invalid bytes as U+FFFD, so a lossy
/// conversion here yields the same text while avoiding a panic when a Rust byte slice
/// would cut inside a character.
pub fn truncate(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }
    String::from_utf8_lossy(&value.as_bytes()[..limit]).into_owned()
}

/// 从上游的错误响应体中取出一个可以安全展示的错误代码, 取不到时返回 None.
///
/// 凭据刷新与令牌分发的错误信息会写进日志, 并经 relay 返回给客户端. Go 版在这里带出
/// 响应体前 200 字节, 但上游的错误正文可能回显令牌或请求内容, 违反审查标准 4.1. 因此
/// 只保留排查最需要的错误代码: 依次尝试顶层 error 为字符串时的值, error.code 与
/// error.type, 采用第一个通过白名单的取值. error.message 这类自由文本一律不取. 白名单
/// 只允许 ASCII 字母, 数字, 下划线, 点与连字符, 长度 1 到 64, 使取值无法携带令牌之外的
/// 任意文本, 也无法注入换行伪造日志.
///
/// Extracts an error code that is safe to show from an upstream error body, or None.
///
/// Credential refresh and token broker errors are logged and returned to clients through
/// relay. The Go version included the first 200 bytes of the body here, but an upstream
/// error body may echo tokens or request content, which violates review standard 4.1. Only
/// the error code most useful for diagnosis is kept: try a string top-level error, then
/// error.code and error.type, and take the first value passing the whitelist. Free text such
/// as error.message is never taken. The whitelist admits only ASCII letters, digits,
/// underscores, dots and hyphens with a length of 1 to 64, so the value cannot carry
/// arbitrary text, nor inject a newline to forge log lines.
pub(crate) fn upstream_error_code(body: &[u8]) -> Option<String> {
    let Ok(Value::Object(payload)) = parse_json_value(body) else {
        return None;
    };
    let error = payload.get("error")?;
    let candidates = [
        error.as_str(),
        error.get("code").and_then(Value::as_str),
        error.get("type").and_then(Value::as_str),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|code| {
            (1..=64).contains(&code.len())
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        })
        .map(ToOwned::to_owned)
}

/// 错误信息中状态码之后的诊断后缀: 取到错误代码时为 ": 代码", 否则为空串, 使几处失败
/// 消息保持统一的形式.
///
/// The diagnostic suffix after the status code in an error message: ": code" when an error
/// code is found and empty otherwise, keeping the failure messages in one form.
pub(crate) fn upstream_error_suffix(body: &[u8]) -> String {
    upstream_error_code(body)
        .map(|code| format!(": {code}"))
        .unwrap_or_default()
}

/// 按 Go 的 fmt.Sprint 文本规则输出标量. 与 jsonval::same_value 的取值语义一致,
/// 因此名称比较与账号比较不会出现两套规则.
///
/// Renders a scalar the way Go's fmt.Sprint does. The rule matches jsonval::same_value's
/// value semantics, so name comparison and account comparison never use two rule sets.
fn scalar_text(value: &Value) -> String {
    match value {
        Value::Null => "<nil>".to_owned(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// 按 priority 升序分组, 每组内部按游标旋转起点. is_preferred 命中的条目排在所属组
/// 的组首, 供会话固定使用; 没有固定要求时传入恒假的判定.
///
/// 规则与改写前的 Python 实现一致: 同优先级的账号被均匀摊到各请求上, 而备用账号不会
/// 因为全池轮转成为首选.
///
/// Groups by ascending priority and rotates each group's start by the cursor. The entry
/// matched by is_preferred goes first in its group, which session pinning uses; pass a
/// predicate that is always false when nothing is pinned.
///
/// The rule matches the Python implementation before the rewrite: same-priority accounts
/// spread evenly across requests, and a backup account never becomes the first choice
/// through whole-pool rotation.
pub(crate) fn order_entries<F: Fn(&Entry) -> bool>(
    entries: &[Entry],
    cursor: usize,
    is_preferred: F,
) -> Result<Vec<Entry>, Error> {
    let mut groups: BTreeMap<i64, Vec<Entry>> = BTreeMap::new();
    for entry in entries {
        let priority = priority_of(entry)?;
        groups.entry(priority).or_default().push(entry.clone());
    }
    let mut ordered = Vec::with_capacity(entries.len());
    for group in groups.values() {
        // 每个分组至少有一条记录, 因此取模不会除以零.
        //
        // Every group holds at least one entry, so the modulus cannot divide by zero.
        let mut start = cursor % group.len();
        if let Some(index) = group.iter().position(&is_preferred) {
            start = index;
        }
        ordered.extend(group[start..].iter().cloned());
        ordered.extend(group[..start].iter().cloned());
    }
    Ok(ordered)
}

/// 编码 application/x-www-form-urlencoded 请求体. 工作区没有启用 reqwest 的 form
/// 功能, 因此用 url 的表单序列化器, 它按插入顺序输出字段; Go 的 url.Values.Encode 会
/// 排序键, 两者解码后完全等价.
///
/// Encodes an application/x-www-form-urlencoded body. The workspace does not enable
/// reqwest's form feature, so the url crate's form serializer is used; it emits fields in
/// insertion order, while Go's url.Values.Encode sorts keys. Both decode to the same
/// values.
pub(crate) fn form_body(fields: &[(&str, &str)]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in fields {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

/// 读取响应体, 最多读取 limit 字节, 与 Go 的 io.ReadAll(io.LimitReader(...)) 相同.
/// 上游返回超长响应时不会把整段内容读进内存.
///
/// 读取错误被忽略: Go 的三个调用点都丢弃 ReadAll 的错误并继续解析已经读到的内容, 因此
/// 连接在响应收尾时报错不会让刷新失败, 也不会丢掉已经读到的正文.
///
/// Reads a response body up to limit bytes, matching Go's
/// io.ReadAll(io.LimitReader(...)). An oversized upstream response is never fully read
/// into memory.
///
/// Read errors are ignored: all three Go call sites discard ReadAll's error and keep
/// parsing what was read, so a connection failing at the tail neither fails the refresh nor
/// loses the body already read.
pub(crate) async fn read_limited(mut response: reqwest::Response, limit: usize) -> Vec<u8> {
    let mut body = Vec::new();
    while body.len() < limit {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = limit - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) | Err(_) => break,
        }
    }
    body
}

/// Go 的 net/http 在调用方没有设置时才补上的 User-Agent. 两条线路的令牌刷新请求在 Go 版
/// 里都没有设置自定义标识, 上游看到的是这个默认值, google.go 的注释也写明这一步刻意
/// 保持 Go 的默认标识; reqwest 与 hyper 都不补默认头, 因此显式设置, 使刷新请求与 Go 版
/// 在请求头上一致. loadCodeAssist 请求使用官方客户端标识, 不受此常量影响.
///
/// The User-Agent Go's net/http adds only when the caller sets none. Both routes' token
/// refresh requests set no custom identifier in the Go version, so upstream sees this
/// default, and the comment in google.go states that this step deliberately keeps Go's
/// default identifier; neither reqwest nor hyper adds one, so it is set explicitly to keep
/// the refresh requests equal to the Go version in request headers. The loadCodeAssist
/// request uses the official client identifier and is unaffected.
pub const GO_HTTP_CLIENT_USER_AGENT: &str = "Go-http-client/1.1";

/// 解析第一个 JSON 值并忽略其后的字节. 与 Go 的 json.Decoder.Decode 一致: 它读取一个
/// 值就返回, 不检查是否还有多余内容, 凭据文件与令牌响应因此都不会因为尾随空白之外的
/// 多余字节而被拒绝.
///
/// Parses the first JSON value and ignores the bytes after it. This matches Go's
/// json.Decoder.Decode, which returns after reading one value and never checks for
/// trailing content, so neither a credential file nor a token response is rejected for
/// extra bytes beyond trailing whitespace.
pub(crate) fn parse_json_value(data: &[u8]) -> Result<Value, String> {
    match serde_json::Deserializer::from_slice(data)
        .into_iter::<Value>()
        .next()
    {
        Some(Ok(value)) => Ok(value),
        Some(Err(error)) => Err(error.to_string()),
        None => Err("unexpected end of JSON input".to_owned()),
    }
}

/// 当前的 Unix 秒. 两处刷新都用它计算到期时刻.
///
/// The current Unix second. Both refresh paths use it to compute the expiry instant.
pub(crate) fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// 刷新写回时使用的本地时间文本, 格式与 Go 的 2006-01-02T15:04:05-0700 相同.
///
/// The local-time text used when writing a refresh back, in Go's
/// 2006-01-02T15:04:05-0700 format.
pub(crate) fn updated_at_text() -> String {
    chrono::Local::now()
        .format("%Y-%m-%dT%H:%M:%S%z")
        .to_string()
}

/// headersOf 生成上游需要的两个鉴权头.
///
/// headers_of builds the two auth headers upstream needs.
pub(crate) fn headers_of(entry: &Entry) -> Result<AuthHeaders, Error> {
    let mut headers = bearer_headers_of(entry)?;
    let account_id = jsonval::as_str(entry.get("account_id")).unwrap_or("");
    headers.push(("chatgpt-account-id", account_id.to_owned()));
    Ok(headers)
}

/// 只生成 Authorization 一个鉴权头, 供只凭访问令牌识别账号的 xai 线路使用.
///
/// headers_of 额外生成的 chatgpt-account-id 是 ChatGPT 后端专用的请求头. 官方 Grok CLI 不发送
/// 它, 发往 cli-chat-proxy.grok.com 会让请求偏离官方客户端的形态, 并暴露请求来自 Codex 工具链.
///
/// Builds only the Authorization header, for the xai line whose upstream identifies the account
/// by the access token alone. The chatgpt-account-id header that headers_of adds is specific to
/// the ChatGPT backend; the official Grok CLI never sends it, and sending it to
/// cli-chat-proxy.grok.com would make the request deviate from the official client and reveal
/// that it comes from Codex tooling.
pub(crate) fn bearer_headers_of(entry: &Entry) -> Result<AuthHeaders, Error> {
    let access_token = jsonval::as_str(entry.get("access_token"))
        .filter(|text| !text.is_empty())
        .ok_or_else(|| Error::message(format!("凭据 {} 缺少 access_token", name_of(entry))))?;
    Ok(vec![("Authorization", format!("Bearer {access_token}"))])
}
