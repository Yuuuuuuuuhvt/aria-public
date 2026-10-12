//! 上游 HTTP 客户端的构造.
//!
//! 代理只来自进程环境中的 HTTPS_PROXY 与 NO_PROXY, 支持 http, https 与 socks5 代理
//! 地址. 系统代理读取必须关闭, 否则 macOS 的系统配置或 HTTP_PROXY 会在部署机器上
//! 引入未声明的出口. 回环地址的上游从不经过代理, 与 Go 版 http.ProxyFromEnvironment
//! 对 localhost 与 127.0.0.0/8 的处理一致.
//!
//! Construction of the upstream HTTP client.
//!
//! The proxy comes only from HTTPS_PROXY and NO_PROXY in the process environment, and
//! supports http, https and socks5 proxy addresses. System proxy discovery must be
//! disabled, or the macOS system configuration or HTTP_PROXY would introduce an
//! undeclared egress path on a deployment machine. Loopback upstreams never go through a
//! proxy, matching how Go's http.ProxyFromEnvironment treats localhost and 127.0.0.0/8.

use std::time::Duration;

use reqwest::{Client, ClientBuilder, NoProxy, Proxy};

/// 构造上游客户端所需的参数. 两个提供方与 messages 路由使用不同的取值, 因此参数由
/// 调用方给出, 而不是在这里写死.
///
/// Parameters for building an upstream client. The two providers and the messages routes
/// use different values, so the caller supplies them instead of this module hard-coding
/// them.
pub struct ClientOptions {
    /// 是否允许 HTTP/2. 为假时只使用 HTTP/1.1.
    ///
    /// Whether HTTP/2 is allowed. When false, only HTTP/1.1 is used.
    pub http2: bool,
    /// TCP 连接超时.
    ///
    /// The TCP connect timeout.
    pub connect_timeout: Duration,
    /// 等待响应头与响应体任意字节的最长空闲时间.
    ///
    /// The longest idle time while waiting for response headers or body bytes.
    pub idle_timeout: Duration,
}

/// 回环地址的排除列表. Go 的代理判定对 localhost 与回环地址直接返回直连, reqwest
/// 不做这件事, 因此这里显式加入.
///
/// The loopback exclusion list. Go's proxy decision returns a direct connection for
/// localhost and loopback addresses; reqwest does not, so they are added explicitly.
const LOOPBACK_NO_PROXY: &str = "127.0.0.1,::1,localhost";

/// 按给定选项构造上游客户端, 代理取自当前进程环境.
///
/// Builds an upstream client with the given options; the proxy is read from the current
/// process environment.
pub fn build_client(options: &ClientOptions) -> Result<Client, String> {
    build_client_with(options, &|name| std::env::var(name).ok())
}

/// 按给定选项与取值函数构造上游客户端.
///
/// idle_timeout 通过 reqwest 的 read_timeout 实现. reqwest 把同一个时长用在两处: 它
/// 与等待响应头的 future 一起被轮询, 因此覆盖等待响应头的时间; 响应体读取时它按每次
/// 读取重置. 这正好等价于 Go 版的 ResponseHeaderTimeout 加空闲看门狗: 度量的是空闲
/// 时间, 而不是给整段生成设置总时长上限.
///
/// 不启用自动解压. 当前依赖特性没有编译进 gzip, brotli, deflate 与 zstd 支持, 上游
/// 请求也不带 Accept-Encoding, 因此不存在透明解压引入的缓冲.
///
/// Builds an upstream client with the given options and value getter.
///
/// idle_timeout is implemented with reqwest's read_timeout. reqwest uses that duration in
/// two places: it is polled alongside the future that waits for response headers, so it
/// covers the header wait, and it resets on every successful body read. That is exactly
/// equivalent to Go's ResponseHeaderTimeout plus the inactivity watchdog: it measures
/// idle time rather than imposing a total generation duration.
///
/// Automatic decompression is off. The current dependency features do not compile in
/// gzip, brotli, deflate or zstd support, and upstream requests carry no Accept-Encoding,
/// so there is no buffering from transparent decompression.
fn build_client_with(
    options: &ClientOptions,
    getenv: &dyn Fn(&str) -> Option<String>,
) -> Result<Client, String> {
    let mut builder = ClientBuilder::new()
        // 先关闭默认的系统代理读取, 再显式添加进程环境中的代理.
        //
        // Disable the default system proxy lookup first, then add the process
        // environment proxy explicitly.
        .no_proxy()
        .connect_timeout(options.connect_timeout)
        .read_timeout(options.idle_timeout)
        .pool_max_idle_per_host(16)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(30));
    if !options.http2 {
        builder = builder.http1_only();
    }
    if let Some(proxy) = proxy_settings(getenv)? {
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map_err(|error| format!("构造上游 HTTP 客户端失败: {error}"))
}

/// 从取值函数读取代理配置. 取值顺序与 Go 的 getEnvAny 相同: 先看大写名称, 再看小写
/// 名称, 取第一个非空值.
///
/// 取值函数使本函数不依赖进程环境, 因此测试可以覆盖各种组合而不修改全局状态.
///
/// 代理地址可能带有凭据, 解析失败时不回显地址本身.
///
/// Reads the proxy configuration through the value getter. The order matches Go's
/// getEnvAny: the upper-case name first, then the lower-case one, taking the first
/// non-empty value.
///
/// The getter keeps this function independent of the process environment, so tests cover
/// every combination without mutating global state.
///
/// A proxy address may carry credentials, so a parse failure never echoes the address.
fn proxy_settings(getenv: &dyn Fn(&str) -> Option<String>) -> Result<Option<Proxy>, String> {
    let Some(address) = env_value(getenv, &["HTTPS_PROXY", "https_proxy"]) else {
        return Ok(None);
    };
    let exclusions = match env_value(getenv, &["NO_PROXY", "no_proxy"]) {
        Some(list) if !list.trim().is_empty() => format!("{list},{LOOPBACK_NO_PROXY}"),
        _ => LOOPBACK_NO_PROXY.to_owned(),
    };
    let proxy = Proxy::all(&address)
        .map_err(|_| "HTTPS_PROXY 不是有效的代理地址".to_owned())?
        .no_proxy(NoProxy::from_string(&exclusions));
    Ok(Some(proxy))
}

/// 返回第一个非空的取值.
///
/// Returns the first non-empty value.
fn env_value(getenv: &dyn Fn(&str) -> Option<String>, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| getenv(name).filter(|value| !value.is_empty()))
}
