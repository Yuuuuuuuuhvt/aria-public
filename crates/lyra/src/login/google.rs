//! Google Antigravity 的浏览器登录, 逐项移植 Go 版 cmd/agy-login.
//!
//! 授权参数取自 Antigravity 官方客户端 1.2.12 的实际请求: 申请七项 scope, 使用 PKCE 的 S256
//! 方式, 回调地址为 http://localhost:51121/oauth-callback. 与 Go 版一致的三处关键行为是:
//! 在监听之前先校验输出路径, 回调只监听 127.0.0.1 并校验 state, 候选文件原子发布且不覆盖既有
//! 文件.
//!
//! 与 Go 版有意的差异只有一处: 受保护路径改为由调用方从配置里取出 (见 docs/architecture.md
//! 6.1), 原实现对 /etc/codex-relay 与 /etc/gemini-relay 两个固定目录和环境变量的保护被删除.
//!
//! Browser login for Google Antigravity, ported item by item from Go's cmd/agy-login.
//!
//! The authorization parameters come from the actual requests of the Antigravity official client
//! 1.2.12: seven scopes, PKCE with S256, and the callback address
//! http://localhost:51121/oauth-callback. Three behaviours match Go: the output path is validated
//! before listening, the callback listens on 127.0.0.1 only and checks state, and the candidate
//! file is published atomically without overwriting an existing file.
//!
//! One deliberate difference from Go: the protected paths come from the caller's configuration (see
//! docs/architecture.md 6.1), and the original protection of the fixed /etc/codex-relay and
//! /etc/gemini-relay directories and of environment variables is removed.

use http::header;
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub use super::LoginOptions;
use super::callback::CallbackServer;
use super::candidate::{self, write_candidate};

use super::pkce;
use super::{REQUEST_TIMEOUT, Token, do_json, hostname, login_client, random_token};
use crate::credential::google::{GOOGLE_CLIENT_ID, google_client_secret_from_env};
use crate::credential::{form_body, unix_now, updated_at_text};

/// 官方客户端申请的七项 scope. aicode 是 Antigravity 专有授权, 缺了它同样会拿到令牌, 但不代表
/// 服务端会认可.
///
/// The seven scopes the official client requests. aicode is an Antigravity-specific authorization;
/// without it a token is still issued, but that does not mean the server accepts it.
const SCOPES: [&str; 7] = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/cclog",
    "https://www.googleapis.com/auth/experimentsandconfigs",
    "https://www.googleapis.com/auth/aicode",
    "openid",
];

const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
const USERINFO_ENDPOINT: &str = "https://www.googleapis.com/oauth2/v2/userinfo";
const UPSTREAM_BASE: &str = "https://daily-cloudcode-pa.googleapis.com";
const CALLBACK_PATH: &str = "/oauth-callback";

/// 候选文件的暂存文件名前缀.
///
/// The temporary file name prefix of a candidate.
const TEMPORARY_PREFIX: &str = ".agy-candidate-";

/// 一次登录使用的端点与客户端标识.
///
/// 生产取值来自 Google 与 Antigravity 官方客户端; 测试把端点指向本机桩服务, 因此端点地址不是
/// LoginOptions 的一部分, 公开签名保持不变.
///
/// client_secret 没有内置默认值: 它属于 Antigravity 官方客户端, 由调用方在登录开始时从环境变量
/// 读出后传入, 缺失时登录在发出请求前失败.
///
/// The endpoints and client identifiers of one login. The production values come from Google and the
/// Antigravity official client; tests point the endpoints at a local stub, so endpoint addresses are
/// not part of LoginOptions and the public signature stays unchanged.
///
/// client_secret has no built-in default: it belongs to the Antigravity official client and is read
/// by the caller from the environment at the start of a login, so the login fails before any request
/// when it is absent.
#[derive(Debug, Clone)]
pub(crate) struct Endpoints {
    pub(crate) auth: String,
    pub(crate) token: String,
    pub(crate) userinfo: String,
    pub(crate) load_code_assist: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: Option<String>,
}

impl Endpoints {
    /// 生产端点, 客户端密钥留空待调用方填入.
    ///
    /// The production endpoints, with the client secret left for the caller to fill in.
    pub(crate) fn production() -> Endpoints {
        Endpoints {
            auth: AUTH_ENDPOINT.to_owned(),
            token: TOKEN_ENDPOINT.to_owned(),
            userinfo: USERINFO_ENDPOINT.to_owned(),
            load_code_assist: format!("{UPSTREAM_BASE}/v1internal:loadCodeAssist"),
            client_id: GOOGLE_CLIENT_ID.to_owned(),
            client_secret: None,
        }
    }
}

/// 完成一次 Google Antigravity 的账号授权, 写出独立候选文件.
///
/// 客户端密钥从环境变量 ARIA_ANTIGRAVITY_CLIENT_SECRET 读取, 缺失时直接失败, 不静默使用空值.
///
/// Completes one Google Antigravity account authorization and writes a standalone candidate file.
///
/// The client secret is read from the ARIA_ANTIGRAVITY_CLIENT_SECRET environment variable; when it
/// is absent the login fails instead of silently using an empty value.
pub async fn run(options: LoginOptions) -> Result<(), String> {
    let secret = google_client_secret_from_env()?;
    let mut endpoints = Endpoints::production();
    endpoints.client_secret = Some(secret);
    run_with(options, endpoints).await
}

/// 用指定端点完成一次授权.
///
/// Completes one authorization with the given endpoints.
pub(crate) async fn run_with(options: LoginOptions, endpoints: Endpoints) -> Result<(), String> {
    // client 字段克隆出来: 它只有三个字符串, 克隆开销很小, 之后整体借用 options 不会触发部分
    // 移动.
    //
    // The client field is cloned out: it holds only three strings, so cloning is cheap, and
    // borrowing the whole options later does not hit a partial move.
    let client_config = options.client.clone();
    // 在开始监听或授权前拒绝不安全的输出路径, 避免取得令牌后才发现无法保存.
    //
    // Reject unsafe output paths before opening a listener or starting authorization, so an
    // unsavable output is found before tokens are obtained.
    let out = candidate::candidate_path(&options.out, &options.protected).await?;
    if options.port == 0 || options.timeout.is_zero() {
        return Err("回调端口必须为 1 至 65535，等待时间必须为正值".to_owned());
    }

    // 登录是建号的一次性动作, 不做周期性获取: 这里用标识渲染后备版本, 与服务进程共享同一套
    // 渲染逻辑. 周期性获取版本是 serve 进程的职责, 见 architecture.md 3.8 第 4 条. 因此登录
    // 的网络面保持原有三项: 令牌交换, 取用户信息, 账号初始化, 不多发一次版本清单请求.
    //
    // Login is a one-shot account-creation action and does not fetch periodically: the identifier
    // here renders the fallback version, sharing the same rendering logic as the serve process.
    // Periodic version fetching is the serve process's job, see section 3.8 rule 4 of
    // architecture.md. Login's network surface therefore stays the original three requests: token
    // exchange, user info and account initialization, with no extra version-manifest request.
    let identity = crate::client_id::ClientIdentity::new(client_config);

    // 通过 state 与 PKCE 把浏览器回调及令牌交换绑定到本次授权尝试.
    //
    // State and PKCE bind the browser callback and the token exchange to this authorization
    // attempt.
    let state = random_token(32)?;
    let (verifier, challenge) = pkce()?;
    let redirect_uri = format!("http://localhost:{}{CALLBACK_PATH}", options.port);

    // 先启动回环回调接收端, 再显示浏览器授权地址, 确保回调有接收方.
    //
    // Start the loopback receiver before exposing the browser authorization URL, so the callback
    // has a receiver.
    let server = CallbackServer::start(options.port, CALLBACK_PATH, state.clone()).await?;
    let scope = SCOPES.join(" ");
    println!("请在浏览器里打开下面的链接, 用目标账号登录并同意全部授权:");
    println!();
    println!(
        "{}",
        auth_url(&endpoints, &redirect_uri, &state, &challenge, &scope)
    );
    println!();
    println!(
        "回调监听在 127.0.0.1:{}, 最长等待 {:?}。",
        options.port, options.timeout
    );
    let code = server.wait(options.timeout).await?;

    let published = finish_login(
        &options,
        &endpoints,
        &code,
        &verifier,
        &redirect_uri,
        &out,
        &identity,
    )
    .await?;

    println!();
    println!("候选账号已保存，尚未导入生产池:");
    println!(
        "  邮箱        {}",
        if published.email.is_empty() {
            "未取得"
        } else {
            &published.email
        }
    );
    println!("  项目标识    {}", published.project_id);
    println!("  档位        {}", published.tier);
    println!(
        "  访问令牌    已取得, 长度 {}",
        published.token.access_token.len()
    );
    println!(
        "  刷新令牌    已取得, 长度 {}",
        published.token.refresh_token.len()
    );
    println!("  凭据文件    {}", published.path.display());
    Ok(())
}

/// 一次登录在回调之后的产物.
///
/// The outcome of one login after the callback.
pub(crate) struct Published {
    pub(crate) email: String,
    pub(crate) project_id: String,
    pub(crate) tier: String,
    pub(crate) token: Token,
    pub(crate) path: std::path::PathBuf,
}

/// 完成回调之后的部分: 交换令牌, 读取身份与项目信息, 组装并发布候选文件.
///
/// 与等待回调分开, 使这一段可以在回环桩服务上完整验证, 而不必驱动浏览器流程.
///
/// Completes the part after the callback: exchange the token, read the identity and project
/// information, assemble and publish the candidate file. It is separate from waiting for the
/// callback so this part can be fully verified against loopback stubs without driving a browser.
pub(crate) async fn finish_login(
    options: &LoginOptions,
    endpoints: &Endpoints,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    out: &std::path::Path,
    identity: &crate::client_id::ClientIdentity,
) -> Result<Published, String> {
    let client = login_client()?;
    let token = exchange_code(&client, endpoints, code, verifier, redirect_uri).await?;
    let email = match fetch_email(&client, endpoints, &token.access_token).await {
        Ok(email) => email,
        Err(error) => {
            // 邮箱只用于命名与对账, 取不到不应阻断登录.
            //
            // The email is used only for naming and reconciliation; failing to read it must not
            // block the login.
            eprintln!("提示: 未能读取邮箱: {error}");
            String::new()
        }
    };
    let (project_id, tier) =
        fetch_project(&client, endpoints, &token.access_token, identity).await?;
    let entry = credential_entry(options, &token, &email, &project_id, &tier);
    let path = write_candidate(out, entry, &options.protected, TEMPORARY_PREFIX).await?;
    Ok(Published {
        email,
        project_id,
        tier,
        token,
        path,
    })
}

/// 组装候选凭据条目.
///
/// Assembles the candidate credential entry.
fn credential_entry(
    options: &LoginOptions,
    token: &Token,
    email: &str,
    project_id: &str,
    tier: &str,
) -> Map<String, Value> {
    let mut entry = Map::new();
    entry.insert(
        "name".to_owned(),
        Value::String(if email.is_empty() {
            "antigravity".to_owned()
        } else {
            email.to_owned()
        }),
    );
    entry.insert("email".to_owned(), Value::String(email.to_owned()));
    entry.insert(
        "project_id".to_owned(),
        Value::String(project_id.to_owned()),
    );
    entry.insert("tier_id".to_owned(), Value::String(tier.to_owned()));
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
    entry.insert(
        "token_type".to_owned(),
        Value::String(token.token_type.clone()),
    );
    entry.insert("enabled".to_owned(), Value::Bool(true));
    entry.insert("priority".to_owned(), Value::from(options.priority));
    entry.insert(
        "source".to_owned(),
        Value::String(format!("agy-login on {}", hostname())),
    );
    entry.insert("updated_at".to_owned(), Value::String(updated_at_text()));
    if let Some(note) = &options.note {
        entry.insert("note".to_owned(), Value::String(note.clone()));
    }
    entry
}

/// 构造浏览器授权地址. 参数顺序与 Go 的 url.Values.Encode 一致 (按键排序), 使链接文本可以直接
/// 对照; 取值以空格连接的 scope 与官方客户端相同.
///
/// Builds the browser authorization URL. The parameter order matches Go's url.Values.Encode (sorted
/// by key) so the link text can be compared directly; the space-joined scope matches the official
/// client.
fn auth_url(
    endpoints: &Endpoints,
    redirect_uri: &str,
    state: &str,
    challenge: &str,
    scope: &str,
) -> String {
    let fields = [
        ("access_type", "offline"),
        ("client_id", endpoints.client_id.as_str()),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256"),
        ("prompt", "consent"),
        ("redirect_uri", redirect_uri),
        ("response_type", "code"),
        ("scope", scope),
        ("state", state),
    ];
    format!("{}?{}", endpoints.auth, form_body(&fields))
}

/// 用授权码换取访问令牌与刷新令牌.
///
/// Exchanges the authorization code for an access token and a refresh token.
pub(crate) async fn exchange_code(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> Result<Token, String> {
    // 客户端密钥缺失属于调用方装配错误, 在构造请求前失败, 不把空值发给 Google.
    //
    // A missing client secret is a caller-assembly error and fails before the request is built, so
    // an empty value is never sent to Google.
    let client_secret = endpoints
        .client_secret
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "缺少 Google OAuth 客户端密钥".to_owned())?;
    let form = [
        ("code", code),
        ("client_id", endpoints.client_id.as_str()),
        ("client_secret", client_secret),
        ("code_verifier", verifier),
        ("redirect_uri", redirect_uri),
        ("grant_type", "authorization_code"),
    ];
    let request = client
        .post(&endpoints.token)
        .timeout(REQUEST_TIMEOUT)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
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

/// 读取账号邮箱.
///
/// agy 的取用户信息请求带的是 Go 的默认标识, 不是官方客户端标识, 这里对齐, 见 architecture.md 3.8.
///
/// Reads the account email.
///
/// agy's user-info request carries Go's default identifier, not the official client identifier,
/// which is matched here, see section 3.8 of architecture.md.
pub(crate) async fn fetch_email(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    access_token: &str,
) -> Result<String, String> {
    let request = client
        .get(&endpoints.userinfo)
        .timeout(REQUEST_TIMEOUT)
        .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
        .header(
            header::USER_AGENT,
            crate::credential::GO_HTTP_CLIENT_USER_AGENT,
        )
        .build()
        .map_err(|error| format!("构造用户信息请求失败: {error}"))?;
    let payload = do_json(client, request).await?;
    #[derive(Deserialize)]
    struct UserInfo {
        #[serde(default)]
        email: String,
    }
    let info: UserInfo =
        serde_json::from_slice(&payload).map_err(|error| format!("解析用户信息失败: {error}"))?;
    Ok(info.email)
}

/// 调用 loadCodeAssist 取得项目标识与档位.
///
/// agy 在登录阶段就调用 loadCodeAssist 取得项目标识; 本命令沿用这一步, 拿不到项目标识直接失败,
/// 因为后续所有请求都需要它.
///
/// Calls loadCodeAssist to obtain the project id and tier. agy obtains the project id at the
/// login stage; this command keeps that step and fails without the project id, because every
/// later request needs it.
pub(crate) async fn fetch_project(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    access_token: &str,
    identity: &crate::client_id::ClientIdentity,
) -> Result<(String, String), String> {
    let body = json!({"metadata": {"ideType": "ANTIGRAVITY"}}).to_string();
    // 账号初始化与生成请求共用基础标识, 见 architecture.md 3.8.
    //
    // Account initialization shares the base identifier with generation requests, see section 3.8
    // of architecture.md.
    let user_agent = identity.user_agent();
    let request = client
        .post(&endpoints.load_code_assist)
        .timeout(REQUEST_TIMEOUT)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::USER_AGENT, user_agent.as_str())
        .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
        .body(body)
        .build()
        .map_err(|error| format!("构造 loadCodeAssist 请求失败: {error}"))?;
    let payload = do_json(client, request)
        .await
        .map_err(|error| format!("调用 loadCodeAssist 失败: {error}"))?;
    let Value::Object(parsed) = serde_json::from_slice::<Value>(&payload)
        .map_err(|error| format!("解析 loadCodeAssist 响应失败: {error}"))?
    else {
        return Err("解析 loadCodeAssist 响应失败: 顶层不是 JSON 对象".to_owned());
    };

    // 项目标识可能出现在三个字段之一, 取值可能是字符串或带 id 的对象.
    //
    // The project id may appear in one of three fields and its value may be a string or an object
    // carrying an id.
    let mut project_id = String::new();
    for key in ["cloudaicompanionProject", "projectId", "project"] {
        match parsed.get(key) {
            Some(Value::String(value)) if !value.is_empty() => project_id = value.clone(),
            Some(Value::Object(inner)) => {
                if let Some(value) = inner.get("id").and_then(Value::as_str)
                    && !value.is_empty()
                {
                    project_id = value.to_owned();
                }
            }
            _ => {}
        }
        if !project_id.is_empty() {
            break;
        }
    }
    if project_id.is_empty() {
        return Err("响应中没有项目标识".to_owned());
    }
    let tier = parsed
        .get("currentTier")
        .and_then(Value::as_object)
        .and_then(|current| current.get("id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    Ok((project_id, tier))
}
