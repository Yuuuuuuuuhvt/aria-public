//! 浏览器 OAuth 登录命令, 输出独立候选凭据文件, 从不写入正在使用的账号池.
//!
//! 三个命令共享同一条发布路径: 先校验输出路径, 再监听回环端口等待浏览器回调, 用授权码换取
//! 令牌, 最后把候选文件原子发布到目标路径. 目标路径已存在时发布必定失败, 因此不会覆盖既有
//! 凭据; --config 中各提供方的凭据路径都属于受保护路径, 不能作为输出.
//!
//! Browser OAuth login commands. They write a separate candidate credential file and never write
//! into an account pool in use.
//!
//! All three commands share one publication path: validate the output path, listen on a loopback
//! port for the browser callback, exchange the authorization code for tokens, and finally publish
//! the candidate file atomically. Publishing always fails when the target exists, so no existing
//! credential is overwritten; the credential paths of every provider in --config are protected and
//! cannot be used as output.

pub mod callback;
pub mod candidate;
pub mod chatgpt;
pub mod google;
pub mod xai;

use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use sha2::Digest;

use crate::credential::read_limited;

/// 单次登录请求的总超时. 登录只在用户交互后短暂发生, 因此与 Go 版一样用 60 秒总超时.
///
/// The total timeout of one login request. Login happens briefly after a user interaction, so like
/// Go it uses a 60-second total timeout.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// 登录响应体的读取上限, 与 Go 版 doJSON 的 1<<20 相同.
///
/// The read limit of a login response body, matching the 1<<20 of Go's doJSON.
const MAX_RESPONSE_BYTES: usize = 1 << 20;

/// 令牌响应. 三条登录命令共用: Google 与 xai 用它取访问与刷新令牌, ChatGPT 还从中取出
/// id_token; xai 的身份来自 userinfo, 不依赖 id_token, 该字段缺省为空.
///
/// The token response, shared by all three login commands: Google and xai take the access and
/// refresh tokens from it and ChatGPT additionally reads the id_token; xai takes its identity from
/// userinfo instead, so that field defaults to empty for it.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct Token {
    #[serde(default)]
    pub(crate) access_token: String,
    #[serde(default)]
    pub(crate) refresh_token: String,
    #[serde(default)]
    pub(crate) expires_in: i64,
    #[serde(default)]
    pub(crate) token_type: String,
    #[serde(default)]
    pub(crate) id_token: String,
}

/// 登录命令的公共参数.
///
/// protected 是不能作为输出的账号池路径, 由调用方从 --config 中各提供方的 credentials
/// 取得; 它们都可能不存在, 校验按路径解析结果比较, 不要求文件已经存在. client 是该线路的
/// 客户端标识配置, 见 architecture.md 3.8; 调用方从 --config 的提供方取, 缺省时用线路默认值.
///
/// The parameters shared by the login commands. protected lists the account pool paths that cannot
/// be used as output, supplied by the caller from the credentials entries of every provider in
/// --config; none needs to exist yet, because the check compares resolved paths. client is the
/// line's identifier configuration, see section 3.8 of architecture.md; the caller takes it from
/// the provider in --config and uses the line defaults when the provider is absent.
pub struct LoginOptions {
    pub out: PathBuf,
    pub port: u16,
    pub timeout: Duration,
    pub priority: i64,
    pub note: Option<String>,
    pub protected: Vec<PathBuf>,
    pub client: crate::client_id::ClientConfig,
}
/// 发送请求并返回 2xx 的响应体, 最多读取 1 MiB.
///
/// 错误信息只带状态码, 不带上游响应正文: 令牌响应本身就可能含令牌, 写进日志等于把刚拿到的凭据
/// 泄露出去.
///
/// Sends a request and returns the body of a 2xx response, reading at most 1 MiB. Error messages
/// carry only the status and never the upstream body: a token response itself may contain tokens,
/// and logging it would leak credentials that were just obtained.
pub(crate) async fn do_json(
    client: &reqwest::Client,
    request: reqwest::Request,
) -> Result<Vec<u8>, String> {
    let response = client
        .execute(request)
        .await
        .map_err(|error| format!("请求失败: {}", transport_error(&error)))?;
    let status = response.status();
    // read_limited 读取失败时返回已经读到的部分, 与凭据刷新的其他调用方一致; 截断的响应随后
    // 会以解析失败或状态码不符的形式报错.
    //
    // read_limited returns whatever it managed to read when the read fails, matching the other
    // callers of credential refreshes; a truncated response then fails as a parse error or a
    // mismatched status instead.
    let payload = read_limited(response, MAX_RESPONSE_BYTES).await;
    if !status.is_success() {
        return Err(format!("状态码 {}", status.as_u16()));
    }
    Ok(payload)
}

/// 把传输层错误描述成不含地址的说明.
///
/// reqwest 的错误文本带有完整请求地址, 登录端点虽然不含密钥, 但保持与中继一致的口径更便于审查.
///
/// Describes a transport error without the address. Login endpoints carry no secrets, but keeping
/// the same rule as the relay makes review simpler.
fn transport_error(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "请求超时".to_owned()
    } else if error.is_connect() {
        "连接失败".to_owned()
    } else {
        "请求失败".to_owned()
    }
}

/// 构造登录使用的客户端: 复用上游客户端的构造方式, 代理只取自进程环境, 回环地址直连.
///
/// Builds the client used by login, reusing the upstream client construction: the proxy comes from
/// the process environment only and loopback addresses connect directly.
pub(crate) fn login_client() -> Result<reqwest::Client, String> {
    aria_core::upstream::build_client(&aria_core::upstream::ClientOptions {
        http2: false,
        connect_timeout: REQUEST_TIMEOUT,
        idle_timeout: REQUEST_TIMEOUT,
    })
}

/// 返回本机名, 用于候选条目的 source 字段.
///
/// Go 版取 os.Hostname, Rust 标准库没有对应接口, 而工作区禁用 unsafe, 因此这里取进程环境里的
/// HOSTNAME; 未设置时记 unknown, 与 Go 版取主机名失败时的取值一致. 该字段只用于对账, 不影响
/// 登录结果.
///
/// Returns the machine name for the candidate's source field. Go uses os.Hostname; the Rust standard
/// library has no equivalent and the workspace forbids unsafe, so this takes HOSTNAME from the
/// process environment and records unknown when it is unset, matching the value Go uses when the
/// hostname cannot be read. The field is only used for reconciliation and does not affect the login.
pub(crate) fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// 生成本次授权使用的 state.
///
/// Generates the state of this authorization attempt.
pub(crate) fn random_token(size: usize) -> Result<String, String> {
    let mut buffer = vec![0u8; size];
    rand::TryRng::try_fill_bytes(&mut rand::rngs::SysRng, &mut buffer)
        .map_err(|_| "随机源不可用".to_owned())?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&buffer))
}

/// 生成 PKCE 的 verifier 与 challenge.
///
/// Generates the PKCE verifier and challenge.
pub(crate) fn pkce() -> Result<(String, String), String> {
    let verifier = random_token(64)?;
    let digest = sha2::Sha256::digest(verifier.as_bytes());
    Ok((
        verifier,
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest),
    ))
}
