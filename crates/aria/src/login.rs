//! login 子命令: 提供方选择与浏览器登录的调用.
//!
//! 参数结构与浏览器授权流程都由 lyra 提供, 见 docs/architecture.md 9.3; aria 只负责把命令行
//! 参数与 --config 中各提供方的受保护路径交给它, 并决定每个提供方的默认回调端口.
//!
//! The login subcommand: provider selection and the call into the browser login.
//!
//! Lyra owns the parameters and the browser authorization flow, described in section 9.3 of
//! docs/architecture.md; aria only hands it the command line arguments and the protected paths of
//! every provider from --config, and decides the default callback port of each provider.

use lyra::login::LoginOptions;

/// 登录的提供方.
///
/// The provider to log in to.
#[derive(Clone, Copy)]
pub enum LoginProvider {
    /// Google 账号池使用的浏览器登录.
    ///
    /// The browser login used by the Google account pool.
    Google,
    /// ChatGPT 账号池使用的浏览器登录.
    ///
    /// The browser login used by the ChatGPT account pool.
    ChatGpt,
    /// SuperGrok 账号池使用的浏览器登录.
    ///
    /// The browser login used by the SuperGrok account pool.
    Xai,
}

impl LoginProvider {
    /// 返回该提供方的默认回调端口.
    ///
    /// Google 与 ChatGPT 的端口来自各自公开客户端的回调注册: Google 侧是 Antigravity 客户端使用
    /// 的端口, ChatGPT 侧是 Codex CLI 使用的端口. 官方 Grok CLI 每次登录绑定随机端口, 授权服务
    /// 对回环回调不比较端口, 因此 xai 取社区实现常用的固定端口 56121, 便于在防火墙或端口转发中
    /// 预先放行. 三个取值与 architecture 第 4 节的默认值相同.
    ///
    /// Returns the default callback port of this provider.
    ///
    /// The Google and ChatGPT ports come from the callback registration of the respective public
    /// client: the port used by the Antigravity client for Google and the one used by the Codex CLI
    /// for ChatGPT. The official Grok CLI binds a random port per login and the authorization
    /// service ignores the port of a loopback callback, so xai takes the fixed port 56121 common in
    /// community implementations, which can be allowed in advance in a firewall or port forward. All
    /// three match the defaults in section 4 of architecture.md.
    pub fn default_port(self) -> u16 {
        match self {
            LoginProvider::Google => 51121,
            LoginProvider::ChatGpt => 1455,
            LoginProvider::Xai => 56121,
        }
    }
}

/// 执行一次浏览器登录.
///
/// 授权地址, state 与 PKCE 校验, 令牌交换, 受保护路径判定与候选文件的原子发布都在
/// lyra::login 内完成; 这里不包装它的结果, 使各提供方的行为与独立运行 lyra 时完全一致.
///
/// Runs one browser login.
///
/// The authorization URL, the state and PKCE checks, the token exchange, the protected path
/// decision and the atomic publication of the candidate file all happen inside lyra::login; its
/// result is passed through unwrapped, so every provider behaves exactly as when lyra runs on its
/// own.
pub async fn run(provider: LoginProvider, options: LoginOptions) -> Result<(), String> {
    match provider {
        LoginProvider::Google => lyra::login::google::run(options).await,
        LoginProvider::ChatGpt => lyra::login::chatgpt::run(options).await,
        LoginProvider::Xai => lyra::login::xai::run(options).await,
    }
}
