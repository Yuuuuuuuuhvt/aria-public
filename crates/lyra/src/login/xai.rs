//! xai 的浏览器登录, 流程与官方 Grok CLI 的浏览器登录相同.
//!
//! 这是 Aria 新增的命令, 行为规格见 docs/architecture.md 6.3. 与 ChatGPT 登录共用回调接收,
//! 候选文件校验与发布三部分, 区别在授权地址参数, 回调地址与身份来源: 授权地址比 ChatGPT 多出
//! nonce 与 referrer; 回调地址是官方 CLI 注册的 127.0.0.1 回环形态; Grok 的访问令牌载荷只有
//! sub 而没有邮箱, 因此令牌交换之后调用 userinfo 取得 sub 与 email.
//!
//! 每台设备各自登录一次即可取得独立的 refresh token 族, 不与其他设备的令牌轮换冲突.
//!
//! Browser login for xai, following the same flow as the official Grok CLI browser login.
//!
//! This command is new in Aria; its behaviour is specified in docs/architecture.md 6.3. It shares
//! the callback receiver, candidate validation and publication with the ChatGPT login and differs
//! in the authorization parameters, the callback address and the identity source: the
//! authorization URL adds nonce and referrer to the ChatGPT set; the callback address is the
//! 127.0.0.1 loopback form registered by the official CLI; and the Grok access token payload holds
//! only sub without an email, so userinfo is called after the code exchange to obtain sub and
//! email.
//!
//! Each device logs in once to obtain an independent refresh token family, which does not conflict
//! with the token rotation of other devices.

use std::path::PathBuf;

use http::header;
use serde::Deserialize;
use serde_json::{Map, Value};

pub use super::LoginOptions;
use super::callback::CallbackServer;
use super::candidate::{self, write_candidate};

use super::REQUEST_TIMEOUT;
use super::{Token, do_json, hostname, login_client, pkce, random_token};
use crate::credential::{GO_HTTP_CLIENT_USER_AGENT, form_body, unix_now, updated_at_text};
use crate::xai::{XAI_OAUTH_CLIENT_ID, XAI_OAUTH_SCOPE, XAI_TOKEN_URL};

const AUTH_ENDPOINT: &str = "https://auth.x.ai/oauth2/authorize";
const USERINFO_ENDPOINT: &str = "https://auth.x.ai/oauth2/userinfo";

/// 回调路径. 官方 CLI 注册的回调地址是 http://127.0.0.1/callback, 端口在登录时随机绑定; 授权服务
/// 按 RFC 8252 对回环回调不比较端口, 但路径必须一致, 因此这里取 /callback.
///
/// The callback path. The official CLI registers http://127.0.0.1/callback and binds a random port
/// at sign-in; following RFC 8252 the authorization service ignores the port of a loopback
/// callback but the path must match, hence /callback.
const CALLBACK_PATH: &str = "/callback";

/// 授权地址的附加参数, 与官方 CLI 相同.
///
/// The extra authorization parameter, the same as the official CLI's.
const REFERRER: &str = "grok-build";

/// 候选条目的 plan 字段. 只用于健康检查与登录摘要的展示, 账号池不读取它.
///
/// The plan field of the candidate entry. It is only shown by the health check and the login
/// summary; the pool never reads it.
const PLAN: &str = "superGrok";

/// 候选文件的暂存文件名前缀.
///
/// The temporary file name prefix of a candidate.
const TEMPORARY_PREFIX: &str = ".xai-candidate-";

/// 一次登录使用的端点与客户端标识.
///
/// 生产取值来自官方 Grok CLI; 测试把端点指向本机桩服务, 因此端点地址不是 LoginOptions 的一部分.
///
/// The endpoints and client identifier of one login. The production values come from the official
/// Grok CLI; tests point the endpoints at a local stub, so endpoint addresses are not part of
/// LoginOptions.
#[derive(Debug, Clone)]
pub(crate) struct Endpoints {
    pub(crate) auth: String,
    pub(crate) token: String,
    pub(crate) userinfo: String,
    pub(crate) client_id: String,
}

impl Endpoints {
    /// 生产端点.
    ///
    /// The production endpoints.
    pub(crate) fn production() -> Endpoints {
        Endpoints {
            auth: AUTH_ENDPOINT.to_owned(),
            token: XAI_TOKEN_URL.to_owned(),
            userinfo: USERINFO_ENDPOINT.to_owned(),
            client_id: XAI_OAUTH_CLIENT_ID.to_owned(),
        }
    }
}

/// 从 userinfo 读到的账号身份. sub 是 xAI 的用户标识, 作为账号池的 account_id.
///
/// The account identity read from userinfo. sub is the xAI user identifier and becomes the pool's
/// account_id.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct Identity {
    #[serde(default)]
    pub(crate) sub: String,
    #[serde(default)]
    pub(crate) email: String,
}

/// 一次登录在回调之后的产物.
///
/// The outcome of one login after the callback.
pub(crate) struct Published {
    pub(crate) account_id: String,
    pub(crate) email: String,
    pub(crate) path: PathBuf,
}

/// 完成一次 SuperGrok 账号授权, 写出独立候选文件.
///
/// Completes one SuperGrok account authorization and writes a standalone candidate file.
pub async fn run(options: LoginOptions) -> Result<(), String> {
    run_with(options, Endpoints::production()).await
}

/// 用指定端点完成一次授权.
///
/// Completes one authorization with the given endpoints.
pub(crate) async fn run_with(options: LoginOptions, endpoints: Endpoints) -> Result<(), String> {
    // 在开始监听或授权前拒绝不安全的输出路径, 避免取得令牌后才发现无法保存.
    //
    // Reject unsafe output paths before opening a listener or starting authorization, so an
    // unsavable output is found before tokens are obtained.
    let out = candidate::candidate_path(&options.out, &options.protected).await?;
    if options.port == 0 || options.timeout.is_zero() {
        return Err("回调端口必须为 1 至 65535，等待时间必须为正值".to_owned());
    }

    // state 绑定浏览器回调, PKCE 绑定令牌交换, nonce 与官方 CLI 一样随授权请求发出. 身份取自
    // TLS 连接上的 userinfo 而不是 id_token, 因此不需要回读 nonce.
    //
    // State binds the browser callback and PKCE binds the code exchange; nonce is sent with the
    // authorization request as the official CLI does. The identity comes from userinfo over TLS
    // rather than from an id_token, so nonce never needs to be read back.
    let state = random_token(32)?;
    let nonce = random_token(32)?;
    let (verifier, challenge) = pkce()?;
    let redirect_uri = format!("http://127.0.0.1:{}{CALLBACK_PATH}", options.port);
    let server = CallbackServer::start(options.port, CALLBACK_PATH, state.clone()).await?;
    println!("请在浏览器里打开下面的链接, 用目标 SuperGrok 账号登录并同意授权:");
    println!();
    println!(
        "{}",
        auth_url(&endpoints, &redirect_uri, &state, &nonce, &challenge)
    );
    println!();
    println!(
        "回调监听在 127.0.0.1:{}, 最长等待 {:?}。",
        options.port, options.timeout
    );
    let code = server.wait(options.timeout).await?;

    let published =
        finish_login(&options, &endpoints, &code, &verifier, &redirect_uri, &out).await?;

    println!();
    println!("候选账号已保存，尚未导入生产池:");
    println!("  邮箱        {}", published.email);
    println!("  账号标识    {}", published.account_id);
    println!("  套餐        {PLAN}");
    println!("  凭据文件    {}", published.path.display());
    Ok(())
}

/// 完成回调之后的部分: 交换令牌, 读取 userinfo 身份, 组装并发布候选文件.
///
/// 与等待回调分开, 使这一段可以在回环桩服务上完整验证.
///
/// Completes the part after the callback: exchange the token, read the userinfo identity, assemble
/// and publish the candidate file. It is separate from waiting for the callback so this part can be
/// fully verified against loopback stubs.
pub(crate) async fn finish_login(
    options: &LoginOptions,
    endpoints: &Endpoints,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    out: &std::path::Path,
) -> Result<Published, String> {
    let client = login_client()?;
    let token = exchange_code(&client, endpoints, code, verifier, redirect_uri).await?;
    let identity = fetch_identity(&client, endpoints, &token.access_token).await?;
    let entry = credential_entry(options, &token, &identity);
    let path = write_candidate(out, entry, &options.protected, TEMPORARY_PREFIX).await?;
    Ok(Published {
        account_id: identity.sub,
        email: identity.email,
        path,
    })
}

/// 构造浏览器授权地址. 参数按键排序, 与授权服务读取参数的方式无关, 但链接文本因此可以直接
/// 对照规格里的参数清单.
///
/// Builds the browser authorization URL. The parameters are sorted by key, which does not affect how
/// the authorization service reads them but lets the link text be compared directly against the
/// parameter list in the specification.
fn auth_url(
    endpoints: &Endpoints,
    redirect_uri: &str,
    state: &str,
    nonce: &str,
    challenge: &str,
) -> String {
    let fields = [
        ("client_id", endpoints.client_id.as_str()),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256"),
        ("nonce", nonce),
        ("redirect_uri", redirect_uri),
        ("referrer", REFERRER),
        ("response_type", "code"),
        ("scope", XAI_OAUTH_SCOPE),
        ("state", state),
    ];
    format!("{}?{}", endpoints.auth, form_body(&fields))
}

/// 用授权码换取令牌.
///
/// 表单与 ChatGPT 登录相同, 没有 client_secret. 请求标识与账号池刷新同一端点时使用的
/// GO_HTTP_CLIENT_USER_AGENT 相同, 使 auth.x.ai 只看到一种客户端标识.
///
/// Exchanges the authorization code for tokens. The form matches the ChatGPT login and carries no
/// client_secret. The request identifier equals the GO_HTTP_CLIENT_USER_AGENT the pool uses when
/// refreshing at the same endpoint, so auth.x.ai sees only one client identifier.
pub(crate) async fn exchange_code(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> Result<Token, String> {
    let form = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("client_id", endpoints.client_id.as_str()),
        ("code_verifier", verifier),
    ];
    let request = client
        .post(&endpoints.token)
        .timeout(REQUEST_TIMEOUT)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(header::ACCEPT, "application/json")
        .header(header::USER_AGENT, GO_HTTP_CLIENT_USER_AGENT)
        .body(form_body(&form))
        .build()
        .map_err(|error| format!("构造令牌请求失败: {error}"))?;
    let payload = do_json(client, request)
        .await
        .map_err(|error| format!("换取令牌失败: {error}"))?;
    let token: Token =
        serde_json::from_slice(&payload).map_err(|error| format!("解析令牌响应失败: {error}"))?;
    if token.access_token.is_empty() || token.refresh_token.is_empty() {
        return Err("令牌响应缺少必要字段".to_owned());
    }
    Ok(token)
}

/// 用访问令牌读取账号身份.
///
/// sub 缺失时直接失败: 账号池的取号与写回都以 account_id 为键, 缺了它候选条目不可用. 邮箱只用于
/// 命名与对账, 允许为空, 此时账号池按 account_id 前缀展示账号.
///
/// Reads the account identity with the access token. A missing sub fails directly: the pool keys
/// selection and write-back on account_id and the candidate would be unusable without it. The email
/// is only used for naming and reconciliation and may be empty, in which case the pool shows the
/// account by its account_id prefix.
pub(crate) async fn fetch_identity(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    access_token: &str,
) -> Result<Identity, String> {
    let request = client
        .get(&endpoints.userinfo)
        .timeout(REQUEST_TIMEOUT)
        .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
        .header(header::ACCEPT, "application/json")
        .header(header::USER_AGENT, GO_HTTP_CLIENT_USER_AGENT)
        .build()
        .map_err(|error| format!("构造用户信息请求失败: {error}"))?;
    let payload = do_json(client, request)
        .await
        .map_err(|error| format!("读取用户信息失败: {error}"))?;
    let identity: Identity =
        serde_json::from_slice(&payload).map_err(|error| format!("解析用户信息失败: {error}"))?;
    if identity.sub.is_empty() {
        return Err("用户信息缺少 sub".to_owned());
    }
    Ok(identity)
}

/// 组装候选凭据条目, 字段与 ChatGPT 登录的候选条目相同: name 取邮箱, account_id 取 sub.
///
/// Assembles the candidate credential entry with the same fields as the ChatGPT login's candidate:
/// name takes the email and account_id takes sub.
fn credential_entry(
    options: &LoginOptions,
    token: &Token,
    identity: &Identity,
) -> Map<String, Value> {
    let mut entry = Map::new();
    entry.insert("name".to_owned(), Value::String(identity.email.clone()));
    entry.insert("account_id".to_owned(), Value::String(identity.sub.clone()));
    entry.insert("plan".to_owned(), Value::String(PLAN.to_owned()));
    entry.insert(
        "access_token".to_owned(),
        Value::String(token.access_token.clone()),
    );
    entry.insert(
        "refresh_token".to_owned(),
        Value::String(token.refresh_token.clone()),
    );
    // 上游报告的剩余秒数异常大时按饱和处理, 避免整数溢出.
    //
    // An absurdly large remaining lifetime saturates instead of overflowing.
    entry.insert(
        "expires_at".to_owned(),
        Value::from(unix_now().saturating_add(token.expires_in)),
    );
    entry.insert("enabled".to_owned(), Value::Bool(true));
    entry.insert("priority".to_owned(), Value::from(options.priority));
    entry.insert(
        "source".to_owned(),
        Value::String(format!("xai-login on {}", hostname())),
    );
    entry.insert("updated_at".to_owned(), Value::String(updated_at_text()));
    if let Some(note) = &options.note {
        entry.insert("note".to_owned(), Value::String(note.clone()));
    }
    entry
}
