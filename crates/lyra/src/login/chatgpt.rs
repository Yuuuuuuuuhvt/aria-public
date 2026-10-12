//! ChatGPT 的浏览器登录, 流程与 Codex CLI 的浏览器登录相同.
//!
//! 这是 Aria 新增的命令, Go 版没有对应实现, 行为规格见 docs/architecture.md 6.2. 与 Google
//! 登录共用回调接收, 候选文件校验与发布三部分, 区别只在授权地址参数, 令牌端点, 身份来源 (从
//! id_token 载荷读取声明) 与候选条目字段.
//!
//! 每台设备各自登录一次即可取得独立的 refresh token 族, 不与其他设备的令牌轮换冲突.
//!
//! Browser login for ChatGPT, following the same flow as the Codex CLI browser login.
//!
//! This command is new in Aria and has no counterpart in Go; its behaviour is specified in
//! docs/architecture.md 6.2. It shares the callback receiver, candidate validation and publication
//! with the Google login, and differs only in the authorization parameters, the token endpoint, the
//! identity source (claims read from the id_token payload) and the candidate entry fields.
//!
//! Each device logs in once to obtain an independent refresh token family, which does not conflict
//! with the token rotation of other devices.

use std::path::PathBuf;

use base64::Engine;
use http::header;
use serde_json::{Map, Value};

pub use super::LoginOptions;
use super::callback::CallbackServer;
use super::candidate::{self, write_candidate};

use super::REQUEST_TIMEOUT;
use super::{do_json, hostname, login_client, pkce, random_token};
use crate::credential::pool::OAUTH_CLIENT_ID;
use crate::credential::{form_body, unix_now, updated_at_text};

const AUTH_ENDPOINT: &str = "https://auth.openai.com/oauth/authorize";
const TOKEN_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const CALLBACK_PATH: &str = "/auth/callback";

/// 与 Codex CLI 相同的 scope.
///
/// The same scopes as the Codex CLI.
const SCOPE: &str = "openid profile email offline_access";

/// id_token 里承载账号信息的自定义声明.
///
/// The custom claim in the id_token that carries the account information.
const AUTH_CLAIM: &str = "https://api.openai.com/auth";

/// 候选文件的暂存文件名前缀.
///
/// The temporary file name prefix of a candidate.
const TEMPORARY_PREFIX: &str = ".chatgpt-candidate-";

/// 一次登录使用的端点与客户端标识.
///
/// 生产取值来自 Codex CLI; 测试把端点指向本机桩服务, 因此端点地址不是 LoginOptions 的一部分.
///
/// The endpoints and client identifier of one login. The production values come from the Codex CLI;
/// tests point the endpoints at a local stub, so endpoint addresses are not part of LoginOptions.
#[derive(Debug, Clone)]
pub(crate) struct Endpoints {
    pub(crate) auth: String,
    pub(crate) token: String,
    pub(crate) client_id: String,
}

impl Endpoints {
    /// 生产端点.
    ///
    /// The production endpoints.
    pub(crate) fn production() -> Endpoints {
        Endpoints {
            auth: AUTH_ENDPOINT.to_owned(),
            token: TOKEN_ENDPOINT.to_owned(),
            client_id: OAUTH_CLIENT_ID.to_owned(),
        }
    }
}

/// 从 id_token 载荷里取出的账号信息.
///
/// The account information taken from the id_token payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Claims {
    pub(crate) account_id: String,
    pub(crate) plan: String,
    pub(crate) email: String,
}

/// 一次登录在回调之后的产物.
///
/// The outcome of one login after the callback.
pub(crate) struct Published {
    pub(crate) account_id: String,
    pub(crate) plan: String,
    pub(crate) email: String,
    pub(crate) path: PathBuf,
}

/// 完成一次 ChatGPT 账号授权, 写出独立候选文件.
///
/// Completes one ChatGPT account authorization and writes a standalone candidate file.
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

    let state = random_token(32)?;
    let (verifier, challenge) = pkce()?;
    let redirect_uri = format!("http://localhost:{}{CALLBACK_PATH}", options.port);
    let server = CallbackServer::start(options.port, CALLBACK_PATH, state.clone()).await?;
    println!("请在浏览器里打开下面的链接, 用目标 ChatGPT 账号登录并同意授权:");
    println!();
    println!(
        "{}",
        auth_url(&endpoints, &redirect_uri, &state, &challenge)
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
    println!("  套餐        {}", published.plan);
    println!("  凭据文件    {}", published.path.display());
    Ok(())
}

/// 完成回调之后的部分: 交换令牌, 解析 id_token 声明, 组装并发布候选文件.
///
/// 与等待回调分开, 使这一段可以在回环桩服务上完整验证.
///
/// Completes the part after the callback: exchange the token, parse the id_token claims, assemble
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
    let claims = id_token_claims(&token.id_token)?;
    let entry = credential_entry(options, &token, &claims);
    let path = write_candidate(out, entry, &options.protected, TEMPORARY_PREFIX).await?;
    Ok(Published {
        account_id: claims.account_id,
        plan: claims.plan,
        email: claims.email,
        path,
    })
}

/// 构造浏览器授权地址. 参数按键排序, 与授权服务读取参数的方式无关, 但链接文本因此可以直接
/// 对照规格里的参数清单.
///
/// Builds the browser authorization URL. The parameters are sorted by key, which does not affect how
/// the authorization service reads them but lets the link text be compared directly against the
/// parameter list in the specification.
fn auth_url(endpoints: &Endpoints, redirect_uri: &str, state: &str, challenge: &str) -> String {
    let fields = [
        ("client_id", endpoints.client_id.as_str()),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256"),
        ("codex_cli_simplified_flow", "true"),
        ("id_token_add_organizations", "true"),
        ("redirect_uri", redirect_uri),
        ("response_type", "code"),
        ("scope", SCOPE),
        ("state", state),
    ];
    format!("{}?{}", endpoints.auth, form_body(&fields))
}

/// 用授权码换取令牌.
///
/// 请求体与 Google 登录不同: 没有 client_secret, 但包含 redirect_uri, 这一点与 Codex CLI 的
/// 浏览器登录一致.
///
/// Exchanges the authorization code for tokens. The form differs from the Google login: there is no
/// client_secret but there is a redirect_uri, matching the Codex CLI browser login.
pub(crate) async fn exchange_code(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> Result<super::Token, String> {
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
        .body(form_body(&form))
        .build()
        .map_err(|error| format!("构造令牌请求失败: {error}"))?;
    let payload = do_json(client, request)
        .await
        .map_err(|error| format!("换取令牌失败: {error}"))?;
    let token: super::Token =
        serde_json::from_slice(&payload).map_err(|error| format!("解析令牌响应失败: {error}"))?;
    if token.access_token.is_empty() || token.refresh_token.is_empty() {
        return Err("令牌响应缺少必要字段".to_owned());
    }
    Ok(token)
}

/// 解析 id_token 的载荷声明.
///
/// 只解码载荷, 不校验签名: 令牌来自刚建立的 TLS 连接, 与 Codex CLI 的处理相同. 账号标识缺失时
/// 直接失败, 因为账号池的取号与写回都以它为键, 缺了它候选条目不可用.
///
/// Parses the claims of the id_token payload. Only the payload is decoded and the signature is not
/// verified: the token comes from the TLS connection just established, matching how the Codex CLI
/// handles it. A missing account id fails directly, because the pool keys selection and write-back
/// on it and the candidate would be unusable without it.
pub(crate) fn id_token_claims(id_token: &str) -> Result<Claims, String> {
    let payload = id_token
        .split('.')
        .nth(1)
        .ok_or_else(|| "id_token 不是 JWT".to_owned())?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|error| format!("id_token 载荷不是 base64url: {error}"))?;
    let value: Value = serde_json::from_slice(&decoded)
        .map_err(|error| format!("id_token 载荷不是 JSON: {error}"))?;
    let auth = value.get(AUTH_CLAIM);
    let text = |key: &str| {
        auth.and_then(|auth| auth.get(key))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let claims = Claims {
        account_id: text("chatgpt_account_id"),
        plan: text("chatgpt_plan_type"),
        email: value
            .get("email")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    };
    if claims.account_id.is_empty() {
        return Err("id_token 缺少 chatgpt_account_id".to_owned());
    }
    Ok(claims)
}

/// 组装候选凭据条目.
///
/// 字段顺序与规格列出的顺序一致, name 取邮箱: 账号池按 name 展示账号, 与 Google 登录的取值方式
/// 保持一致.
///
/// Assembles the candidate credential entry. The field order follows the specification and name
/// takes the email: the pool shows accounts by name, matching how the Google login fills it.
fn credential_entry(
    options: &LoginOptions,
    token: &super::Token,
    claims: &Claims,
) -> Map<String, Value> {
    let mut entry = Map::new();
    entry.insert("name".to_owned(), Value::String(claims.email.clone()));
    entry.insert(
        "account_id".to_owned(),
        Value::String(claims.account_id.clone()),
    );
    entry.insert("plan".to_owned(), Value::String(claims.plan.clone()));
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
        Value::String(format!("chatgpt-login on {}", hostname())),
    );
    entry.insert("updated_at".to_owned(), Value::String(updated_at_text()));
    if let Some(note) = &options.note {
        entry.insert("note".to_owned(), Value::String(note.clone()));
    }
    entry
}
