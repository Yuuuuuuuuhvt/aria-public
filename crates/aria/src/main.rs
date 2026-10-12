//! aria 可执行文件入口. 子命令与配置格式见 docs/architecture.md.
//!
//! 命令行解析只用标准库, 不引入 clap: 参数数量少且固定, 手写解析可以精确控制未知参数, 缺少
//! 取值与重复参数这三类错误的退出码与信息.
//!
//! Entry point of the aria executable. Subcommands and the configuration format are
//! described in docs/architecture.md.
//!
//! Command line parsing uses only the standard library instead of clap: there are few fixed
//! arguments, and hand-written parsing controls exactly the exit codes and messages of the
//! three error classes: an unknown argument, a missing value and a repeated argument.

mod config;
mod dispatch;
mod login;
mod search_turn;
mod server;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use aria_core::auth::KeySet;

use lyra::login::LoginOptions;

use crate::config::{Config, RouteKind};
use crate::dispatch::AppState;
use crate::login::LoginProvider;

/// 命令行用法说明.
///
/// The command line usage text.
const USAGE: &str = "\
用法:
  aria serve --config FILE [--clients FILE] [--listen ADDR]
  aria check --config FILE [--clients FILE]
  aria login google --out FILE [--port 51121] [--timeout-secs 900] [--priority N] [--note TEXT] [--config FILE]
  aria login chatgpt --out FILE [--port 1455] [--timeout-secs 900] [--priority N] [--note TEXT] [--config FILE]
  aria login xai --out FILE [--port 56121] [--timeout-secs 900] [--priority N] [--note TEXT] [--config FILE]
  aria version";

/// 用法错误与运行失败的区分.
///
/// 用法错误以 2 退出, 配置或运行期失败以 1 退出, 与 architecture 第 4 节和第 5.10 节一致.
///
/// The distinction between a usage error and a runtime failure.
///
/// A usage error exits with 2 and a configuration or runtime failure with 1, matching
/// sections 4 and 5.10 of architecture.md.
enum Failure {
    /// 未知参数, 缺少取值或重复参数.
    ///
    /// An unknown argument, a missing value or a repeated argument.
    Usage(String),
    /// 配置, 环境变量或运行期失败.
    ///
    /// A configuration, environment or runtime failure.
    Run(String),
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Usage(message)) => {
            eprintln!("{message}");
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
        Err(Failure::Run(message)) => {
            eprintln!("{message}");
            ExitCode::from(1)
        }
    }
}

/// 解析命令行并执行对应子命令.
///
/// Parses the command line and runs the selected subcommand.
fn run() -> Result<(), Failure> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        return Err(Failure::Usage("缺少子命令".to_owned()));
    };
    let rest = &args[1..];
    match command.as_str() {
        "serve" => serve(parse_serve(rest)?),
        "check" => check(parse_check(rest)?),
        "login" => login(rest),
        "version" => {
            if rest.is_empty() {
                print_version();
                Ok(())
            } else {
                Err(Failure::Usage("version 不接受参数".to_owned()))
            }
        }
        other => Err(Failure::Usage(format!("未知子命令 {other:?}"))),
    }
}

/// serve 子命令的参数.
///
/// The arguments of the serve subcommand.
struct ServeArgs {
    config: PathBuf,
    clients: Option<PathBuf>,
    listen: Option<String>,
}

/// check 子命令的参数.
///
/// The arguments of the check subcommand.
struct CheckArgs {
    config: PathBuf,
    clients: Option<PathBuf>,
}

/// 解析 serve 的参数.
///
/// Parses the serve arguments.
fn parse_serve(args: &[String]) -> Result<ServeArgs, Failure> {
    let mut flags = Flags::parse(args, &["config", "clients", "listen"])?;
    Ok(ServeArgs {
        config: PathBuf::from(flags.required("config")?),
        clients: flags.take("clients").map(PathBuf::from),
        listen: flags.take("listen"),
    })
}

/// 解析 check 的参数.
///
/// Parses the check arguments.
fn parse_check(args: &[String]) -> Result<CheckArgs, Failure> {
    let mut flags = Flags::parse(args, &["config", "clients"])?;
    Ok(CheckArgs {
        config: PathBuf::from(flags.required("config")?),
        clients: flags.take("clients").map(PathBuf::from),
    })
}

/// 启动服务.
///
/// Starts the service.
fn serve(args: ServeArgs) -> Result<(), Failure> {
    let config = load_config(&args.config)?;
    let (clients, _) = load_clients(&config, args.clients)?;
    let broker_clients = load_broker_clients(&config)?;
    let listen = args.listen.unwrap_or_else(|| config.listen.clone());
    let state = Arc::new(AppState::new(&config, clients, broker_clients).map_err(Failure::Run)?);
    let runtime = runtime()?;
    // 后台任务在运行时内启动: 启动时立即获取一次版本, 之后每 3 小时一次, 见 architecture 3.8.
    //
    // The background task starts inside the runtime: one immediate version fetch at startup,
    // then every 3 hours, see section 3.8 of architecture.
    runtime
        .block_on(async move {
            state.start_identity_refresh();
            server::run(state, &listen).await
        })
        .map_err(Failure::Run)
}

/// 只做配置与环境变量校验, 输出路由摘要.
///
/// 与 serve 共用同一套构造流程, 因此 check 通过意味着 serve 至少在配置, 环境变量, 上游地址与
/// 提供方构造上不会失败.
///
/// Validates the configuration and environment only and prints the route summary.
///
/// It shares the construction path with serve, so a passing check means serve will not fail
/// at configuration, environment, upstream address or provider construction.
fn check(args: CheckArgs) -> Result<(), Failure> {
    let config = load_config(&args.config)?;
    let (clients, clients_path) = load_clients(&config, args.clients)?;
    let broker_clients = load_broker_clients(&config)?;
    let _state = AppState::new(&config, clients, broker_clients).map_err(Failure::Run)?;
    print_summary(&config, clients_path.as_path());
    Ok(())
}

/// 执行 login 子命令.
///
/// Runs the login subcommand.
fn login(args: &[String]) -> Result<(), Failure> {
    let Some(name) = args.first() else {
        return Err(Failure::Usage("login 缺少提供方".to_owned()));
    };
    let provider = match name.as_str() {
        "google" => LoginProvider::Google,
        "chatgpt" => LoginProvider::ChatGpt,
        "xai" => LoginProvider::Xai,
        other => return Err(Failure::Usage(format!("未知的登录提供方 {other:?}"))),
    };
    let mut flags = Flags::parse(
        &args[1..],
        &["out", "port", "timeout-secs", "priority", "note", "config"],
    )?;
    let out = PathBuf::from(flags.required("out")?);
    let port = match flags.take("port") {
        Some(value) => parse_u16("--port", &value)?,
        None => provider.default_port(),
    };
    let timeout_secs = match flags.take("timeout-secs") {
        Some(value) => parse_u64("--timeout-secs", &value)?,
        None => 900,
    };
    let priority = match flags.take("priority") {
        Some(value) => parse_i64("--priority", &value)?,
        None => 0,
    };
    let note = flags.take("note");
    // 受保护路径只做结构性配置解析, 因此不需要环境变量中的上游密钥; 这些路径来自账号池,
    // 登录输出绝不能覆盖它们. 同一份配置还提供本线路的客户端标识, 见 architecture 3.8.
    //
    // The protected paths need only the structural configuration parse and therefore no
    // upstream key from the environment; these paths belong to account pools that a login
    // output must never overwrite. The same configuration also supplies this line's client
    // identifier, see section 3.8 of architecture.
    let (protected, client) = match flags.take("config") {
        Some(path) => {
            let config = load_config(Path::new(&path))?;
            (
                protected_paths(&config),
                login_client_config(&config, provider),
            )
        }
        // 没有 --config 时提供方配置未知, 用本线路的默认标识; 登录阶段的尽力获取失败也只是
        // 退回后备值, 不影响登录.
        //
        // Without --config the provider configuration is unknown, so the line's default
        // identifier is used; a failed best-effort fetch during login only falls back to the
        // fallback value and does not affect the login.
        None => (
            Vec::new(),
            match provider {
                LoginProvider::Google => lyra::client_id::ClientConfig::default_antigravity(),
                LoginProvider::ChatGpt => lyra::client_id::ClientConfig::default_chatgpt(),
                LoginProvider::Xai => lyra::client_id::ClientConfig::default_xai(),
            },
        ),
    };
    let options = LoginOptions {
        out,
        port,
        timeout: Duration::from_secs(timeout_secs),
        priority,
        note,
        protected,
        client,
    };
    let runtime = runtime()?;
    runtime
        .block_on(login::run(provider, options))
        .map_err(Failure::Run)
}

/// 输出版本号与编译期记录的提交号.
///
/// Prints the version and the commit recorded at build time.
fn print_version() {
    println!("aria {}", env!("CARGO_PKG_VERSION"));
    println!(
        "commit {}",
        option_env!("ARIA_GIT_SHA").unwrap_or("unknown")
    );
}

/// 读取并校验配置文件.
///
/// Reads and validates the configuration file.
fn load_config(path: &Path) -> Result<Config, Failure> {
    config::load(path).map_err(Failure::Run)
}

/// 载入客户端密钥文件, 并拒绝没有启用密钥的配置.
///
/// 没有启用的密钥时任何人都能消耗上游额度, 因此与 Lyra 一样拒绝启动.
///
/// Loads the client key file and rejects a configuration without enabled keys.
///
/// With no enabled key anyone could spend upstream quota, so startup is refused as in Lyra.
fn load_clients(
    config: &Config,
    from_command_line: Option<PathBuf>,
) -> Result<(KeySet, PathBuf), Failure> {
    let path = match (from_command_line, config.clients.clone()) {
        (Some(path), _) => path,
        (None, Some(path)) => PathBuf::from(path),
        (None, None) => {
            return Err(Failure::Run(
                "配置缺少 clients, 命令行也未提供 --clients".to_owned(),
            ));
        }
    };
    let clients = KeySet::load(&path).map_err(Failure::Run)?;
    if clients.is_empty() {
        return Err(Failure::Run(format!(
            "no enabled client key in {}",
            path.display()
        )));
    }
    Ok((clients, path))
}

fn load_broker_clients(config: &Config) -> Result<Option<KeySet>, Failure> {
    let Some(provider) = &config.chatgpt else {
        return Ok(None);
    };
    let Some(path) = &provider.broker_clients else {
        return Ok(None);
    };
    let clients = KeySet::load(path).map_err(Failure::Run)?;
    if clients.is_empty() {
        return Err(Failure::Run(format!(
            "no enabled client key in {}",
            path.display()
        )));
    }
    Ok(Some(clients))
}

/// 返回 --config 中各提供方的账号池路径.
///
/// Returns the account pool paths of every provider in --config.
fn protected_paths(config: &Config) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(provider) = &config.antigravity {
        paths.push(provider.credentials.clone());
    }
    if let Some(provider) = &config.chatgpt
        && let config::ChatGptSource::Credentials(credentials) = &provider.source
    {
        paths.push(credentials.clone());
    }
    if let Some(provider) = &config.xai {
        paths.push(provider.credentials.clone());
    }
    paths
}

/// 返回 login 命令要用的客户端标识配置: 取 --config 中本线路提供方的配置, 提供方缺失时用线路
/// 默认值.
///
/// Returns the client-identifier configuration for the login command: the provider of this line
/// in --config, or the line defaults when the provider is absent.
fn login_client_config(config: &Config, provider: LoginProvider) -> lyra::client_id::ClientConfig {
    match provider {
        LoginProvider::Google => config
            .antigravity
            .as_ref()
            .map(|p| p.client.clone())
            .unwrap_or_else(lyra::client_id::ClientConfig::default_antigravity),
        LoginProvider::ChatGpt => config
            .chatgpt
            .as_ref()
            .map(|p| p.client.clone())
            .unwrap_or_else(lyra::client_id::ClientConfig::default_chatgpt),
        LoginProvider::Xai => config
            .xai
            .as_ref()
            .map(|p| p.client.clone())
            .unwrap_or_else(lyra::client_id::ClientConfig::default_xai),
    }
}

/// 输出 check 的路由摘要.
///
/// 只输出路径与计数, 不输出上游地址: 地址可能带有凭据, 而 check 的输出会被复制到工单与
/// 聊天记录中.
///
/// Prints the route summary of check.
///
/// Only paths and counts are printed, never an upstream address: an address may carry
/// credentials and the output of check ends up in tickets and chat logs.
fn print_summary(config: &Config, clients_path: &Path) {
    println!("listen {}", config.listen);
    println!("clients {}", clients_path.display());
    match &config.usage_dir {
        Some(dir) => println!("usage {}", dir.display()),
        None => println!("usage disabled"),
    }
    if let Some(provider) = &config.antigravity {
        println!("provider antigravity {}", provider.credentials.display());
        println!(
            "client antigravity template={} fallback={}",
            provider.client.user_agent_template, provider.client.fallback_version
        );
    }
    if let Some(provider) = &config.chatgpt {
        match &provider.source {
            config::ChatGptSource::Credentials(credentials) => {
                println!(
                    "provider chatgpt credentials {} models={}",
                    credentials.display(),
                    provider.models.len()
                );
            }
            config::ChatGptSource::Broker { .. } => {
                println!("provider chatgpt broker models={}", provider.models.len());
            }
        }
        println!(
            "client chatgpt template={} fallback={}",
            provider.client.user_agent_template, provider.client.fallback_version
        );
    }
    if let Some(provider) = &config.xai {
        println!(
            "provider xai credentials {} models={}",
            provider.credentials.display(),
            provider.models.len()
        );
        println!(
            "client xai template={} fallback={}",
            provider.client.user_agent_template, provider.client.fallback_version
        );
    }
    if let Some(provider) = &config.commandcode {
        println!(
            "provider commandcode {} key_env={} models={}",
            provider.base_url,
            provider.key_env,
            provider.models.len()
        );
    }
    if let Some(provider) = &config.qoder {
        println!(
            "provider qoder {} token_env={} max_concurrency={} models={}",
            provider.base_url,
            provider.token_env,
            provider.max_concurrency,
            provider.models.len()
        );
    }
    for route in &config.routes {
        match &route.kind {
            RouteKind::Messages { upstreams, .. } => {
                let count = if upstreams.is_empty() {
                    1
                } else {
                    upstreams.len()
                };
                println!(
                    "route messages {} -> {} upstreams={count}",
                    route.name, route.model
                );
            }
            RouteKind::Antigravity => {
                println!("route antigravity {} -> {}", route.name, route.model);
            }
            RouteKind::ChatGpt { reasoning_effort } => {
                println!(
                    "route chatgpt {} -> {} effort={reasoning_effort}",
                    route.name, route.model
                );
            }
            RouteKind::Xai { reasoning_effort } => {
                println!(
                    "route xai {} -> {} effort={reasoning_effort}",
                    route.name, route.model
                );
            }
            RouteKind::CommandCode { reasoning_effort } => {
                println!(
                    "route commandcode {} -> {} effort={reasoning_effort}",
                    route.name, route.model
                );
            }
            RouteKind::Qoder => {
                println!("route qoder {} -> {}", route.name, route.model);
            }
        }
    }
}

/// 构造 serve 与 login 使用的多线程运行时.
///
/// Builds the multi-threaded runtime used by serve and login.
fn runtime() -> Result<tokio::runtime::Runtime, Failure> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| Failure::Run(format!("构造 Tokio 运行时失败: {error}")))
}

/// 一组已经解析的命令行选项.
///
/// 只接受 "--名称 取值" 形式: 未知参数, 缺少取值与重复参数都在这里报错, 因此各子命令不必
/// 重复这三类检查.
///
/// A parsed set of command line options.
///
/// Only the "--name value" form is accepted: an unknown argument, a missing value and a
/// repeated argument all fail here, so no subcommand repeats those three checks.
struct Flags {
    values: BTreeMap<String, String>,
}

impl Flags {
    /// 解析参数列表, allowed 是允许出现的选项名, 不带前缀.
    ///
    /// Parses the argument list; allowed lists the option names that may appear, without
    /// their prefix.
    fn parse(args: &[String], allowed: &[&str]) -> Result<Flags, Failure> {
        let mut values = BTreeMap::new();
        let mut index = 0;
        while index < args.len() {
            let name = &args[index];
            let Some(key) = name.strip_prefix("--") else {
                return Err(Failure::Usage(format!("未知参数 {name:?}")));
            };
            if !allowed.contains(&key) {
                return Err(Failure::Usage(format!("未知参数 {name:?}")));
            }
            let Some(value) = args.get(index + 1) else {
                return Err(Failure::Usage(format!("参数 {name} 缺少取值")));
            };
            if values.insert(key.to_owned(), value.clone()).is_some() {
                return Err(Failure::Usage(format!("参数 {name} 重复出现")));
            }
            index += 2;
        }
        Ok(Flags { values })
    }

    /// 取出一个选项的取值, 名称不带前缀.
    ///
    /// Takes the value of one option; the name carries no prefix.
    fn take(&mut self, name: &str) -> Option<String> {
        self.values.remove(name)
    }

    /// 取出一个必填选项的取值, 名称不带前缀.
    ///
    /// Takes the value of a required option; the name carries no prefix.
    fn required(&mut self, name: &str) -> Result<String, Failure> {
        self.take(name)
            .ok_or_else(|| Failure::Usage(format!("缺少必填参数 --{name}")))
    }
}

/// 解析 u16 取值.
///
/// Parses a u16 value.
fn parse_u16(name: &str, value: &str) -> Result<u16, Failure> {
    value
        .parse()
        .map_err(|_| Failure::Usage(format!("参数 {name} 的取值 {value:?} 不是有效的端口")))
}

/// 解析 u64 取值.
///
/// Parses a u64 value.
fn parse_u64(name: &str, value: &str) -> Result<u64, Failure> {
    value
        .parse()
        .map_err(|_| Failure::Usage(format!("参数 {name} 的取值 {value:?} 不是有效的秒数")))
}

/// 解析 i64 取值.
///
/// Parses an i64 value.
fn parse_i64(name: &str, value: &str) -> Result<i64, Failure> {
    value
        .parse()
        .map_err(|_| Failure::Usage(format!("参数 {name} 的取值 {value:?} 不是有效的整数")))
}
