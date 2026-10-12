//! messages 路由的请求改写与流式转发.
//!
//! 上游已经实现 Anthropic Messages 协议, 因此网关只改写模型名并原样转发其余字段,
//! 再把上游响应按块流式送回客户端. 背压来自拉取式读取: 客户端读得慢, 上游连接就
//! 不会继续被读取; 客户端断开后响应体被丢弃, 上游请求随之取消.
//!
//! Request rewriting and streaming forwarding for messages routes.
//!
//! The upstream already implements the Anthropic Messages protocol, so the gateway only
//! rewrites the model name, forwards every other field unchanged, and streams the
//! upstream response back to the client chunk by chunk. Backpressure comes from
//! pull-based reads: a slow client stops the upstream connection from being read, and a
//! disconnected client drops the response body, which cancels the upstream request.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use aria_core::body::{Body, BoxError};
use aria_core::errors;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode, header};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use serde::Serialize;
use serde_json::value::RawValue;
use url::Url;

use crate::unavailable::{RouteUnavailable, RouteUnavailableReason, quota_reason, reset_delay};

/// 保留原始取值的请求或响应字段表.
///
/// 每个取值保存为原始 JSON 字节, 因此提供方的未知字段, 大整数与不透明签名在往返
/// 过程中保持不变, 不会经过会丢失字段或改变数值精度的中间结构. 这里使用标准库的
/// BTreeMap 而不是 serde_json::Map: 后者只为 Map<String, Value> 实现序列化, 无法直接
/// 编码保存原始值的映射. BTreeMap 按字典序输出键, 与 Go 版 json.Marshal 对 map 的
/// 处理相同.
///
/// A request or response field map that keeps raw values.
///
/// Every value is stored as raw JSON bytes, so provider-specific unknown fields, large
/// integers and opaque signatures survive the round trip unchanged without passing
/// through an intermediate structure that could drop fields or change numeric precision.
/// The standard BTreeMap is used instead of serde_json::Map because the latter only
/// implements serialization for Map<String, Value> and cannot encode a map of raw values.
/// BTreeMap emits keys in lexicographic order, matching how Go's json.Marshal handles a
/// map.
pub type RawObject = BTreeMap<String, Box<RawValue>>;

/// 一条 messages 路由的配置. 空字符串表示该可选字段没有配置, 与 Go 版 Route 的
/// omitempty 语义相同.
///
/// The configuration of one messages route. An empty string means the optional field is
/// absent, matching the omitempty semantics of the Go Route.
pub struct MessagesRoute {
    /// 路由名, 也是可请求的模型名.
    ///
    /// The route name, which is also a requestable model name.
    pub name: String,
    /// 模型列表中的展示名.
    ///
    /// The display name shown in the model list.
    pub display_name: String,
    /// 其他可请求的模型名.
    ///
    /// Further requestable model names.
    pub aliases: Vec<String>,
    /// 发给上游的模型名.
    ///
    /// The model name sent upstream.
    pub model: String,
    /// 上游 Messages 基址, 网关追加 /v1/messages.
    ///
    /// The upstream Messages base URL; the gateway appends /v1/messages.
    pub base_url: String,
    /// 环境变量非空时覆盖 base_url.
    ///
    /// Overrides base_url when the environment variable is non-empty.
    pub base_url_env: String,
    /// 上游密钥所在的环境变量.
    ///
    /// The environment variable holding the upstream key.
    pub key_env: String,
    /// 多上游写法的上游列表.
    ///
    /// 同一服务商可能有多把密钥或多个渠道, 用户要求轮换使用. upstreams 与
    /// base_url, base_url_env, key_env 三个字段是两种互斥的写法, 只能选一种;
    /// 列表为空表示使用单上游写法.
    ///
    /// The upstream list of the multi-upstream form.
    ///
    /// One provider may offer several keys or channels and the user asked for rotation.
    /// upstreams and the base_url, base_url_env and key_env fields are two mutually
    /// exclusive forms; an empty list selects the single-upstream form.
    pub upstreams: Vec<MessagesUpstream>,
    /// 为真时独立网页搜索走 Kimi 搜索服务.
    ///
    /// When true, standalone web search goes to the Kimi search service.
    pub moonshot_search: bool,
}

/// 多上游写法中的一条上游.
///
/// One upstream of the multi-upstream form.
pub struct MessagesUpstream {
    /// 上游 Messages 基址, 网关追加 /v1/messages.
    ///
    /// The upstream Messages base URL; the gateway appends /v1/messages.
    pub base_url: String,
    /// 环境变量非空时覆盖 base_url.
    ///
    /// Overrides base_url when the environment variable is non-empty.
    pub base_url_env: String,
    /// 上游密钥所在的环境变量.
    ///
    /// The environment variable holding the upstream key.
    pub key_env: String,
}

/// 一条已经解析完成的路由: 上游模型与一条或多条上游.
///
/// 轮询游标按请求前进, 使多把密钥的用量大致均衡. 游标用原子整数表达, 因为 forward
/// 只持有共享引用; 递增按定义回绕, 取模后始终落在有效范围内.
///
/// One resolved route: the upstream model and one or more upstreams.
///
/// The round-robin cursor advances once per request so several keys see roughly equal
/// use. It is an atomic integer because forward only holds a shared reference; the
/// increment wraps by definition and the modulo always yields a valid index.
pub struct MessagesDestination {
    name: String,
    model: String,
    upstreams: Vec<Upstream>,
    cursor: AtomicUsize,
    client: reqwest::Client,
    moonshot_search: bool,
    /// 路由级不可用窗口, 见 unavailable.rs.
    ///
    /// The route-level unavailable window, see unavailable.rs.
    unavailable: RouteUnavailable,
}

/// 路由级不可用状态, 供健康检查展示.
///
/// Route-level unavailability, shown in the health check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteUnavailableStatus {
    /// 不可用原因: quota_exhausted 或 rate_limited.
    ///
    /// The reason: quota_exhausted or rate_limited.
    pub reason: &'static str,
    pub remaining_seconds: u64,
}

/// 一条已经解析完成的上游: 基址与凭据.
///
/// One resolved upstream: base URL and credential.
struct Upstream {
    base: Url,
    /// 已经构造好的上游认证头. 密钥只在这里出现, 不进入日志或错误信息.
    ///
    /// The prepared upstream authentication headers. The key appears only here and never
    /// enters a log line or an error message.
    authorization: HeaderValue,
    api_key: HeaderValue,
}

/// 表示上游在生成开始前明确拒绝的状态码.
///
/// 这些状态码下换上游重新发送同一请求体不违反不重放生成请求的原则; 连接失败, 超时
/// 和其他状态码都不换上游.
///
/// The status codes that mean the upstream refused before generation started.
///
/// Retrying the same request body on another upstream under these codes does not violate
/// the no-implicit-replay rule; connection failures, timeouts and every other status code
/// do not switch upstream.
fn switchable(status: StatusCode) -> bool {
    matches!(status.as_u16(), 401 | 402 | 403 | 429)
}

/// 换上游前最多读取并丢弃的响应体字节数.
///
/// Lyra 的 drainAndClose 用 io.LimitReader 把丢弃读取限制在 1 MiB 后关闭响应体, 这里取
/// 同样的上限, 与 Go 版一致. 上限防止某个上游用持续快速输出的拒绝响应长期占用一个并发
/// 槽位与一条上游连接; 超过上限时不再读取, 直接丢弃响应, 连接随之关闭而不复用.
///
/// The maximum number of response body bytes read and discarded before switching
/// upstream.
///
/// Lyra's drainAndClose bounds the discard read with io.LimitReader at 1 MiB and then
/// closes the body, and the same limit is used here for parity with the Go version. The
/// limit keeps an upstream that answers a rejection with a long, fast body from holding a
/// concurrency slot and an upstream connection; once it is exceeded the response is
/// dropped instead, which closes the connection rather than reusing it.
const DRAIN_LIMIT: usize = 1 << 20;

/// 逐跳头, 与 Go 版 httputil.ReverseProxy 的 hopHeaders 列表相同.
///
/// Hop-by-hop headers, matching the hopHeaders list of Go's httputil.ReverseProxy.
const HOP_BY_HOP: [&str; 9] = [
    "connection",
    "proxy-connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// 客户端提供的转发身份标识. 设置 Rewrite 的 ReverseProxy 会剥掉它们, 网关既不转发
/// 调用方伪造的值, 也不添加自己的值.
///
/// Client-supplied forwarding identity. A ReverseProxy with Rewrite strips them; the
/// gateway neither forwards forged values nor adds its own.
const FORWARDING: [&str; 4] = [
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
];

/// 由调用方提供, 因此网关必须替换或删除的请求头.
///
/// content-type 在替换集合里而不是只被覆盖: reqwest 的 RequestBuilder::header 是追加
/// 语义, 不先跳过客户端的取值就会让上游收到两个 Content-Type, 而 Go 版用
/// Header.Set("Content-Type", ...) 替换, 上游只收到一个取值.
///
/// Request headers the caller may supply, so the gateway must replace or drop them.
///
/// content-type belongs in the replaced set rather than merely being overwritten:
/// reqwest's RequestBuilder::header appends, so without skipping the client value the
/// upstream would receive two Content-Type values, while the Go version replaces with
/// Header.Set("Content-Type", ...) and sends exactly one.
const REPLACED: [&str; 7] = [
    "authorization",
    "x-api-key",
    "cookie",
    "accept-encoding",
    "host",
    "content-length",
    "content-type",
];

impl MessagesDestination {
    /// 校验一条路由并构建其目标.
    ///
    /// 校验规则同 Go 版 destination.go 的 build: 基址必须是 HTTPS, 只有回环地址允许
    /// HTTP, 不能包含用户信息, 查询串或片段; 名称, 模型与密钥都必须非空, 密钥不能
    /// 含有回车, 换行与空字符.
    ///
    /// 密钥还会在启动阶段检查能否放入 HTTP 头. Go 版把这类密钥留到请求阶段, 由
    /// net/http 以请求失败告终; 这里提前到启动阶段拒绝, 两种方式都不会让请求成功.
    ///
    /// Validates one route and builds its destination.
    ///
    /// The rules match build in the Go destination.go: the base URL must be HTTPS, HTTP
    /// is allowed only for loopback, and it must carry no credentials, query string or
    /// fragment; name, model and key must be non-empty, and the key must contain no
    /// carriage return, newline or NUL.
    ///
    /// The key is also checked here for being usable in an HTTP header. Go leaves such a
    /// key to request time, where net/http fails the request; rejecting it at startup is
    /// earlier, and neither version lets such a request succeed.
    pub fn new(
        route: &MessagesRoute,
        client: reqwest::Client,
        getenv: &dyn Fn(&str) -> Option<String>,
    ) -> Result<MessagesDestination, String> {
        if route.name.is_empty() || route.model.is_empty() {
            return Err(key_error(&route.name));
        }
        let upstreams = if route.upstreams.is_empty() {
            vec![build_upstream(
                &route.name,
                &route.base_url,
                &route.base_url_env,
                &route.key_env,
                getenv,
            )?]
        } else {
            // 多上游写法与单上游写法只能选一种, 同时出现时按配置错误拒绝, 而不是
            // 静默忽略其中一组.
            //
            // The multi-upstream and single-upstream forms are mutually exclusive; both
            // present is a configuration error rather than one silently winning.
            if !route.base_url.is_empty()
                || !route.base_url_env.is_empty()
                || !route.key_env.is_empty()
            {
                return Err(format!(
                    "route {:?} cannot mix upstreams with base_url, base_url_env or key_env",
                    route.name
                ));
            }
            let mut upstreams = Vec::with_capacity(route.upstreams.len());
            for upstream in &route.upstreams {
                upstreams.push(build_upstream(
                    &route.name,
                    &upstream.base_url,
                    &upstream.base_url_env,
                    &upstream.key_env,
                    getenv,
                )?);
            }
            upstreams
        };
        Ok(MessagesDestination {
            name: route.name.clone(),
            model: route.model.clone(),
            upstreams,
            cursor: AtomicUsize::new(0),
            client,
            moonshot_search: route.moonshot_search,
            unavailable: RouteUnavailable::new(),
        })
    }

    /// 返回该路由发往上游的模型名.
    ///
    /// Returns the model name this route sends upstream.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// 返回路由名, 供搜索轮次的日志与错误说明使用.
    ///
    /// Returns the route name, used by the search rounds for log lines and error descriptions.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// 报告该路由是否声明了 Kimi 搜索.
    ///
    /// Reports whether this route declares Kimi search.
    pub fn moonshot_search(&self) -> bool {
        self.moonshot_search
    }

    /// 当前的路由级不可用窗口: 原因与剩余时长; 没有窗口或窗口已过期时为 None.
    ///
    /// 搜索轮次与常规生成共用这个窗口, 因此单独提供读取入口.
    ///
    /// The current route-level unavailable window: the reason and the remaining time, or None when
    /// there is none or it has expired.
    ///
    /// Search rounds share this window with ordinary generation, so reading it has its own entry
    /// point.
    pub(crate) fn unavailable_window(&self) -> Option<(RouteUnavailableReason, Duration)> {
        self.unavailable.window(Instant::now())
    }

    /// 记录一次成功, 清除路由级不可用窗口与连续计数.
    ///
    /// 与常规生成共用同一份状态: 一次成功说明路由可以正常服务, 之前的拒绝都不再成立.
    ///
    /// Records one success, clearing the route-level window and the consecutive count.
    ///
    /// The state is shared with ordinary generation: one success proves the route serves requests
    /// again, so the earlier rejections no longer hold.
    pub(crate) fn record_success(&self) {
        self.unavailable.record_success();
    }

    /// 路由级不可用状态: 原因与剩余秒数; 没有窗口或窗口已过期时为 None.
    ///
    /// The route-level unavailability: the reason and the remaining seconds, or None when there is
    /// no window or it has expired.
    pub fn unavailable_status(&self) -> Option<RouteUnavailableStatus> {
        self.unavailable
            .window(Instant::now())
            .map(|(reason, remaining)| RouteUnavailableStatus {
                reason: reason.as_str(),
                remaining_seconds: remaining.as_secs(),
            })
    }

    /// 转发一次请求并原样流式送回上游响应.
    ///
    /// body 是已经改写好模型名的请求体; path 为 /v1/messages 或
    /// /v1/messages/count_tokens. 连接上游失败时返回 502 并记录一行日志, 含失败阶段与错误
    /// 原因, 不记录可能带凭据的地址; 连接建立阶段的失败在同一上游上重试一次, 见 send_attempt.
    ///
    /// 路由处于不可用窗口时直接返回带恢复时间的 503, 不转发上游; 一次配额类拒绝 (429 或
    /// 命中用量窗口文本的 403) 会记录窗口, 窗口内的后续请求因此不再触达上游.
    ///
    /// Forwards one request and streams the upstream response back unchanged.
    ///
    /// body is the request body with the model name already rewritten; path is
    /// /v1/messages or /v1/messages/count_tokens. A failed upstream connection returns
    /// 502 together with one log line carrying the failure stage and the cause, and never
    /// logs an address that may carry credentials; a failure in the connection
    /// establishment stage is retried once on the same upstream, see send_attempt.
    ///
    /// While the route is inside an unavailable window a 503 carrying the recovery time is
    /// returned without forwarding; one quota-class rejection (a 429, or a 403 whose text states an
    /// exhausted usage window) records that window, so later requests inside it never reach
    /// upstream.
    pub async fn forward(&self, headers: &HeaderMap, path: &str, body: Bytes) -> Response<Body> {
        if let Some(response) = self.rejection(Instant::now()) {
            return response;
        }
        let sent = self
            .send_with_rotation(|index| self.send_attempt(index, path, headers, body.clone()))
            .await;
        match sent {
            Ok(upstream) => self.inspect(upstream).await,
            // 连接失败与超时都不换上游, 与 architecture 3.4 第 2 条相同.
            //
            // Neither a connection failure nor a timeout switches upstream, matching the
            // second rule of architecture 3.4.
            Err(error) => {
                aria_core::log!(
                    "messages route={} stage={} error={}",
                    self.name,
                    send_failure_stage(&error),
                    describe_failure(&error)
                );
                errors::anthropic_error(
                    StatusCode::BAD_GATEWAY,
                    Some("api_error"),
                    "upstream connection failed",
                )
            }
        }
    }

    /// 发送一次上游请求; 连接建立阶段的失败在同一上游上重试一次.
    ///
    /// 失败按 reqwest 的分类判断: 连接建立阶段 (TCP, DNS, TLS 或代理握手) 的失败发生在任何
    /// 请求字节送达上游之前, 上游没有计费也没有生成内容, 因此重试不会造成重复计费, 也不会让
    /// 客户端收到重复输出. 连接超时与其余发送失败都不重试: 超时可能只是说明上游仍在生成,
    /// 请求已经发出之后的断开无法证明上游没有收到请求, 两者都保持 3.4 节的不重放规则. 重试
    /// 仍用同一个上游, 不改变轮换顺序.
    ///
    /// Sends one upstream request, retrying once on the same upstream when only connection
    /// establishment failed.
    ///
    /// The failure is classified by reqwest: a connection-establishment failure (TCP, DNS,
    /// TLS or a proxy handshake) happens before any request byte reaches the upstream, which
    /// has therefore neither billed nor produced output, so the retry cannot double-bill or
    /// repeat output. Connect timeouts and every other send failure are not retried: a
    /// timeout may only mean the upstream is still generating, and a break after the request
    /// went out cannot prove the upstream never received it; both keep the no-replay rule of
    /// section 3.4. The retry stays on the same upstream and does not change the rotation
    /// order.
    async fn send_attempt(
        &self,
        index: usize,
        path: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let sent = self
            .request(index, path, headers, body.clone())
            .send()
            .await;
        match sent {
            Err(error) if retryable_connect_failure(&error) => {
                aria_core::log!(
                    "messages route={} upstream={} stage=connect retrying once: {}",
                    self.name,
                    index + 1,
                    describe_failure(&error)
                );
                self.request(index, path, headers, body).send().await
            }
            other => other,
        }
    }

    /// 路由处于不可用窗口时构造直接拒绝的响应.
    ///
    /// 状态码取 503 并在 Retry-After 中给出剩余秒数, 报文说明原因与恢复时间, 与账号池的
    /// 容量预算耗尽路径一致; 日志只包含路由名, 原因与剩余秒数, 不包含响应体.
    ///
    /// Builds the direct rejection returned while the route is inside an unavailable window.
    ///
    /// The status is 503 with the remaining seconds in Retry-After, and the payload states the
    /// reason and the recovery time, following the exhausted capacity budget path of the account
    /// pool; the log line carries only the route name, the reason and the remaining seconds, never
    /// a response body.
    fn rejection(&self, now: Instant) -> Option<Response<Body>> {
        let (reason, remaining) = self.unavailable.window(now)?;
        let seconds = remaining.as_secs().max(1);
        aria_core::log!(
            "messages route={} unavailable ({}), request rejected, {} seconds remaining",
            self.name,
            reason.as_str(),
            seconds
        );
        let message = reason.message(&self.name, seconds);
        let mut response =
            errors::anthropic_error(StatusCode::SERVICE_UNAVAILABLE, Some("api_error"), &message);
        if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        Some(response)
    }

    /// 检查一次上游响应, 再交给客户端.
    ///
    /// 非成功状态都走先读前缀再补回的路径: 403 与 429 据此判断是否属于配额类, 其余状态据此
    /// 记录状态码与截断后的错误响应体; 成功响应原样流式转发, 成功时清除窗口与连续计数.
    ///
    /// Inspects one upstream response before handing it to the client.
    ///
    /// Every non-success status takes the read-prefix-and-restore path: a 403 or 429 is
    /// classified as quota-class from the prefix, and the remaining statuses log their status
    /// with the truncated error body; a success streams through unchanged and clears the
    /// window and the consecutive count.
    async fn inspect(&self, mut upstream: reqwest::Response) -> Response<Body> {
        let status = upstream.status();
        if matches!(status.as_u16(), 403 | 429) {
            let prefix = self.observe_rejection(&mut upstream, status).await;
            return buffered_prefix_response(upstream, prefix, self.name.clone());
        }
        if !status.is_success() {
            let prefix = read_error_prefix(&mut upstream, MAX_ERROR_BODY_BYTES).await;
            self.log_error_body(status, &prefix);
            return buffered_prefix_response(upstream, prefix, self.name.clone());
        }
        self.unavailable.record_success();
        stream_response(upstream, self.name.clone())
    }

    /// 记录一次非成功上游响应的状态码与截断后的错误响应体.
    ///
    /// 上游错误体通常只有一两百字节且写明原因, 不记录的话只能看到状态码, 无法判断是限流,
    /// 容量还是参数问题. 正文去掉回车与换行并按 MAX_LOGGED_ERROR_BYTES 截断, 与搜索轮次的
    /// 失败原因同一规则; 不记录密钥与请求内容.
    ///
    /// Logs the status and the truncated error body of one non-success upstream response.
    ///
    /// An upstream error body is usually one or two hundred bytes and states the reason;
    /// without it only the status code would be visible, which cannot tell rate limiting,
    /// capacity and parameter problems apart. The text drops carriage returns and newlines and
    /// is truncated to MAX_LOGGED_ERROR_BYTES, the same rule as the search rounds' failure
    /// reason; no key and no request content is recorded.
    fn log_error_body(&self, status: StatusCode, body: &[u8]) {
        aria_core::log!(
            "messages route={} upstream={} error body={}",
            self.name,
            status.as_u16(),
            aria_core::log::single_line(&String::from_utf8_lossy(body), MAX_LOGGED_ERROR_BYTES)
        );
    }

    /// 读取拒绝响应体的前缀, 记录状态码与截断后的错误体, 判断是否为配额类拒绝并记录窗口,
    /// 返回已读前缀.
    ///
    /// 调用方必须把前缀交回客户端 (或按自身契约丢弃), 否则客户端会拿到被截断的报文.
    ///
    /// Reads the prefix of a rejection body, logs the status with the truncated error body,
    /// decides whether the rejection is quota-class and records the window, and returns the
    /// bytes read.
    ///
    /// The caller must hand the prefix back to the client, or drop it according to its own
    /// contract, or the client would receive a truncated message.
    pub(crate) async fn observe_rejection(
        &self,
        upstream: &mut reqwest::Response,
        status: StatusCode,
    ) -> Bytes {
        let prefix = read_error_prefix(upstream, MAX_ERROR_BODY_BYTES).await;
        self.log_error_body(status, &prefix);
        let Some(reason) = quota_reason(status, &prefix) else {
            return prefix;
        };
        let parsed = reset_delay(upstream.headers(), &prefix);
        let duration = self.unavailable.record(reason, parsed, Instant::now());
        let source = if parsed.is_some() {
            "upstream reset time"
        } else {
            "backoff"
        };
        aria_core::log!(
            "messages route={} status={} quota rejection, {} unavailable for {} seconds ({})",
            self.name,
            status.as_u16(),
            reason.as_str(),
            duration.as_secs(),
            source
        );
        prefix
    }

    /// 按轮询顺序发送一次请求.
    ///
    /// 从游标指向的上游开始, 上游在生成开始前明确拒绝时换下一个上游重新发送同一请求
    /// 体, 直到全部上游都试过; 最后一个上游的响应原样返回. 连接失败立即返回错误, 不换
    /// 上游. 常规生成, count_tokens 与受限搜索轮次共用本方法, 因此三者的轮换规则一致.
    ///
    /// Sends one request in round-robin order.
    ///
    /// It starts at the upstream the cursor points to and, when an upstream refuses before
    /// generation starts, switches to the next one with the same request body until every
    /// upstream has been tried; the last upstream's response is returned as it is. A
    /// connection failure returns immediately without switching. Ordinary generation,
    /// count_tokens and the bounded search turn share this method, so all three rotate
    /// identically.
    pub(crate) async fn send_with_rotation<F, Fut>(
        &self,
        send: F,
    ) -> Result<reqwest::Response, reqwest::Error>
    where
        F: Fn(usize) -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        let count = self.upstreams.len();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % count;
        let mut offset = 0;
        loop {
            let index = (start + offset) % count;
            let response = send(index).await?;
            if switchable(response.status()) && offset + 1 < count {
                self.switch_upstream(index, response).await;
                offset += 1;
                continue;
            }
            return Ok(response);
        }
    }

    /// 记录一次换上游并读取丢弃响应体.
    ///
    /// 日志只包含路由名, 上游序号与状态码, 不包含密钥与地址. 响应体逐块丢弃, 因此
    /// 内存占用与响应体大小无关, 连接也可以复用.
    ///
    /// Records one upstream switch and discards the response body.
    ///
    /// The log line carries only the route name, the upstream number and the status code,
    /// never a key or an address. The body is discarded chunk by chunk, so memory use does
    /// not depend on the body size and the connection can be reused.
    async fn switch_upstream(&self, index: usize, mut response: reqwest::Response) {
        aria_core::log!(
            "messages route={} upstream={} status={} retrying the next upstream",
            self.name,
            index + 1,
            response.status().as_u16()
        );
        let mut drained = 0;
        while drained < DRAIN_LIMIT {
            match response.chunk().await {
                Ok(Some(chunk)) => drained += chunk.len(),
                _ => break,
            }
        }
    }

    /// 构造一次上游请求, 常规生成与受限搜索轮次共用它, 因此两者使用相同的传输层,
    /// 上游地址与改写策略.
    ///
    /// Builds one upstream request. Ordinary generation and the bounded search turn share
    /// it, so both use the same transport, target and rewrite policy.
    pub(crate) fn request(
        &self,
        index: usize,
        path: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> reqwest::RequestBuilder {
        let upstream = &self.upstreams[index];
        let listed = connection_listed(headers);
        let mut request = self.client.post(upstream.target(path)).body(body);
        for (name, value) in headers {
            if is_dropped_request_header(name, &listed) {
                continue;
            }
            request = request.header(name.clone(), value.clone());
        }
        request
            .header(header::AUTHORIZATION, upstream.authorization.clone())
            .header("x-api-key", upstream.api_key.clone())
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )
    }
}

impl Upstream {
    /// 把基址的路径与请求路径拼接为上游地址, 两者之间只保留一个斜杠, 与 Go 版
    /// ReverseProxy 的拼接规则相同.
    ///
    /// path 可以带上客户端请求的查询串, 例如 /v1/messages?beta=true, 因为 Go 版在目标
    /// 基址没有查询串时会保留客户端的查询串.
    ///
    /// Joins the base path with the request path using exactly one slash between them,
    /// matching the join rule of the Go ReverseProxy.
    ///
    /// path may carry the client's query string, for example /v1/messages?beta=true,
    /// because the Go version keeps the client query when the target base URL has none.
    fn target(&self, path: &str) -> Url {
        let (path, query) = match path.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (path, None),
        };
        let mut url = self.base.clone();
        let base_path = url.path().trim_end_matches('/');
        url.set_path(&format!("{base_path}{path}"));
        if let Some(query) = query {
            url.set_query(Some(query));
        }
        url
    }
}

/// 校验并构造一条上游.
///
/// 校验规则同 Go 版 destination.go 的 build: 基址必须是 HTTPS, 只有回环地址允许
/// HTTP, 不能包含用户信息, 查询串或片段; 密钥必须非空且不含回车, 换行与空字符.
///
/// 密钥还会在启动阶段检查能否放入 HTTP 头. Go 版把这类密钥留到请求阶段, 由 net/http
/// 以请求失败告终; 这里提前到启动阶段拒绝, 两种方式都不会让请求成功.
///
/// Validates and builds one upstream.
///
/// The rules match build in the Go destination.go: the base URL must be HTTPS, HTTP is
/// allowed only for loopback, and it must carry no credentials, query string or fragment;
/// the key must be non-empty and contain no carriage return, newline or NUL.
///
/// The key is also checked here for being usable in an HTTP header. Go leaves such a key
/// to request time, where net/http fails the request; rejecting it at startup is earlier,
/// and neither version lets such a request succeed.
fn build_upstream(
    route_name: &str,
    base_url: &str,
    base_url_env: &str,
    key_env: &str,
    getenv: &dyn Fn(&str) -> Option<String>,
) -> Result<Upstream, String> {
    let base_url = if base_url_env.is_empty() {
        base_url.to_owned()
    } else {
        getenv(base_url_env)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| base_url.to_owned())
    };
    let base = parse_base_url(&base_url).ok_or_else(|| base_url_error(route_name))?;
    let key = getenv(key_env).unwrap_or_default();
    if key_env.is_empty() || key.is_empty() || key.contains(['\r', '\n', '\0']) {
        return Err(key_error(route_name));
    }
    let header_error = || format!("route {route_name:?} requires a key valid in an HTTP header");
    let authorization =
        HeaderValue::from_bytes(format!("Bearer {key}").as_bytes()).map_err(|_| header_error())?;
    let api_key = HeaderValue::from_bytes(key.as_bytes()).map_err(|_| header_error())?;
    Ok(Upstream {
        base,
        authorization,
        api_key,
    })
}

/// 生成名称, 模型或密钥校验失败的错误信息, 与 Go 版 build 的信息相同.
///
/// Builds the error message for a failed name, model or key check, unchanged from the Go
/// build.
fn key_error(route_name: &str) -> String {
    format!("route {route_name:?} requires a name, model and valid key environment variable")
}

/// 解析已经读取的 Messages 请求体.
///
/// 返回字段表与模型名. 请求体必须是含非空字符串 model 字段的 JSON 对象, 否则返回
/// None, 由调用方返回 400. 各字段保存为原始 JSON, 因此提供方的未知字段, 大整数与
/// 不透明签名在往返过程中保持不变.
///
/// Parses an already-read Messages request body.
///
/// Returns the field map and the model name. The body must be a JSON object with a
/// non-empty string model field, otherwise this returns None and the caller answers 400.
/// Every field is kept as raw JSON, so provider-specific unknown fields, large integers
/// and opaque signatures survive the round trip unchanged.
pub fn parse_messages_request(body: &[u8]) -> Option<(RawObject, String)> {
    let fields: RawObject = serde_json::from_slice(body).ok()?;
    let model = fields
        .get("model")
        .and_then(|raw| serde_json::from_str::<String>(raw.get()).ok())?;
    if model.is_empty() {
        return None;
    }
    Some((fields, model))
}

/// 只改写 model 字段并重新编码请求体.
///
/// 其余字段以原始 JSON 保留, 不经过会丢失字段或改变数值精度的中间结构; 键顺序按
/// 字典序重排, 与 Go 版 json.Marshal 对 map 的处理相同. 调用方在请求的模型名与路由
/// 的模型名相同时不应调用本函数, 否则会丢失客户端原始字节的键顺序与空白.
///
/// Rewrites only the model field and re-encodes the request body.
///
/// Every other field is kept as raw JSON without passing through an intermediate
/// structure that could drop fields or change numeric precision; keys are reordered
/// lexicographically, matching how Go's json.Marshal handles a map. The caller must not
/// call this when the requested model already equals the route model, or the client's
/// original key order and whitespace would be lost.
pub fn rewrite_model(fields: &mut RawObject, model: &str) -> Result<Bytes, String> {
    let encoded = serde_json::to_string(model).map_err(|_| "invalid request body".to_owned())?;
    let value = RawValue::from_string(encoded).map_err(|_| "invalid request body".to_owned())?;
    fields.insert("model".to_owned(), value);
    let body = serde_json::to_vec(fields).map_err(|_| "invalid request body".to_owned())?;
    Ok(Bytes::from(body))
}

/// 把请求的输出上限压到路由允许的值以内, 并同步压缩思考预算.
///
/// 独立于上限: 任何一轮请求中 thinking.budget_tokens 存在且不小于该轮最终的 max_tokens 时,
/// 改写为 max_tokens 减 1, 结果低于 1024 时仍然保留, 交由上游判断.
///
/// Lowers the request's output ceiling to the route's allowance and compresses the thinking
/// budget with it. Independent of the ceiling: whenever thinking.budget_tokens is not below
/// the final max_tokens, it is lowered to max_tokens minus one.
pub fn clamp_output_ceiling(fields: &mut RawObject, limit: Option<i64>) -> Result<bool, String> {
    let mut modified = false;
    let current_max_tokens = fields
        .get("max_tokens")
        .and_then(|raw| serde_json::from_str::<i64>(raw.get()).ok());

    let effective_max_tokens = match (current_max_tokens, limit) {
        (Some(max_tokens), Some(lim)) if lim > 0 && max_tokens > lim => {
            let encoded =
                serde_json::to_string(&lim).map_err(|_| "invalid request body".to_owned())?;
            let value =
                RawValue::from_string(encoded).map_err(|_| "invalid request body".to_owned())?;
            fields.insert("max_tokens".to_owned(), value);
            modified = true;
            lim
        }
        (Some(max_tokens), _) => max_tokens,
        (None, _) => return Ok(false),
    };

    if cap_thinking_budget(fields, effective_max_tokens)? {
        modified = true;
    }
    Ok(modified)
}

/// 让 thinking.budget_tokens 保持小于 max_tokens.
///
/// Anthropic 要求思考预算小于 max_tokens, 网关压低上限或者搜索编排的后续轮次改用剩余额度时,
/// 预算必须同步下降, 否则上游会以预算过大拒绝请求. 没有 thinking 或预算已经足够小时不改写.
///
/// Keeps thinking.budget_tokens below max_tokens. Anthropic requires the thinking budget to
/// stay below max_tokens, so whenever the gateway lowers the ceiling, or a later search round
/// switches to the remaining budget, the budget must drop with it.
pub fn cap_thinking_budget(fields: &mut RawObject, max_tokens: i64) -> Result<bool, String> {
    let Some(thinking_raw) = fields.get("thinking") else {
        return Ok(false);
    };
    let Ok(mut thinking) = serde_json::from_str::<RawObject>(thinking_raw.get()) else {
        return Ok(false);
    };
    let Some(budget_raw) = thinking.get("budget_tokens") else {
        return Ok(false);
    };
    let Ok(budget) = serde_json::from_str::<i64>(budget_raw.get()) else {
        return Ok(false);
    };
    if budget < max_tokens {
        return Ok(false);
    }
    // max_tokens 来自客户端, 取 i64 最小值时减 1 会溢出; 这样的请求本身无效, 不改写, 交由上游拒绝.
    //
    // max_tokens comes from the client and subtracting one from i64::MIN overflows; such a
    // request is invalid anyway, so it is left unchanged for the upstream to reject.
    let Some(target) = max_tokens.checked_sub(1) else {
        return Ok(false);
    };
    let encoded_target =
        serde_json::to_string(&target).map_err(|_| "invalid request body".to_owned())?;
    let value_target =
        RawValue::from_string(encoded_target).map_err(|_| "invalid request body".to_owned())?;
    thinking.insert("budget_tokens".to_owned(), value_target);

    let encoded_thinking =
        serde_json::to_string(&thinking).map_err(|_| "invalid request body".to_owned())?;
    let value_thinking =
        RawValue::from_string(encoded_thinking).map_err(|_| "invalid request body".to_owned())?;
    fields.insert("thinking".to_owned(), value_thinking);
    Ok(true)
}

/// 解析并校验上游基址: 必须是 HTTPS, 或回环地址上的 HTTP, 且不能包含用户信息, 查询
/// 串或片段. 校验失败时返回 None.
///
/// 用户信息按原始字符串判断, 因为 url 解析器会丢弃空的用户信息: https://@example.com
/// 解析后与 https://example.com 完全相同, 而 Go 版用 base.User != nil 仍然拒绝它.
///
/// Parses and validates an upstream base URL: HTTPS, or HTTP on a loopback address, with
/// no credentials, query string or fragment. It returns None when the check fails.
///
/// User information is judged from the raw string because the url parser drops empty user
/// information: https://@example.com parses exactly like https://example.com, while the Go
/// version still rejects it through base.User != nil.
pub(crate) fn parse_base_url(raw: &str) -> Option<Url> {
    if has_userinfo(raw) {
        return None;
    }
    let url = Url::parse(raw).ok()?;
    let scheme_allowed =
        url.scheme() == "https" || (url.scheme() == "http" && is_loopback_host(&url));
    (scheme_allowed
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && !url.query().is_some_and(|query| !query.is_empty())
        && !url.fragment().is_some_and(|fragment| !fragment.is_empty()))
    .then_some(url)
}

/// 报告地址字符串的授权部分是否携带用户信息标记, 包括空的用户信息.
///
/// Reports whether the authority of a URL string carries a user information marker,
/// including empty user information.
fn has_userinfo(raw: &str) -> bool {
    let after_scheme = raw.split_once("://").map_or(raw, |(_, rest)| rest);
    let end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    after_scheme[..end].contains('@')
}

/// 报告主机是否为回环地址. Go 版用 net.ParseIP(...).IsLoopback() 判断, 域名
/// localhost 不算回环, 这里保持一致.
///
/// Reports whether the host is a loopback address. The Go version uses
/// net.ParseIP(...).IsLoopback(), which does not treat the localhost domain name as
/// loopback; this keeps that behavior.
pub(crate) fn is_loopback_host(url: &Url) -> bool {
    url.host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback())
}

/// 生成基址校验失败的错误信息.
///
/// Builds the error message for a failed base URL check.
fn base_url_error(route_name: &str) -> String {
    format!("route {route_name:?} requires HTTPS or loopback HTTP without credentials or query")
}

/// 读取 Connection 头列出的连接级请求头名.
///
/// Reads the connection-scoped header names listed by the Connection header.
fn connection_listed(headers: &HeaderMap) -> Vec<HeaderName> {
    let mut listed = Vec::new();
    for value in headers.get_all(header::CONNECTION).iter() {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for token in value.split(',') {
            if let Ok(name) = HeaderName::from_bytes(token.trim().as_bytes()) {
                listed.push(name);
            }
        }
    }
    listed
}

/// 判断一个客户端请求头是否必须被丢弃.
///
/// 调用方只能选择已经配置好的路由, 无法提供上游 URL, 认证请求头或转发身份标识;
/// Content-Length 与 Transfer-Encoding 由客户端库按改写后的请求体重新生成.
///
/// Reports whether a client request header must be dropped.
///
/// The caller selects only a configured route and cannot supply an upstream URL, an
/// authentication header or a forwarding identity; Content-Length and Transfer-Encoding
/// are regenerated by the client library from the rewritten body.
fn is_dropped_request_header(name: &HeaderName, connection_listed: &[HeaderName]) -> bool {
    let name = name.as_str();
    REPLACED.contains(&name)
        || HOP_BY_HOP.contains(&name)
        || FORWARDING.contains(&name)
        || connection_listed
            .iter()
            .any(|listed| listed.as_str() == name)
}

/// 判断配额类拒绝前最多读取的错误响应体字节数.
///
/// 错误状态下的响应体都很小 (观测中的实例是 269 到 320 字节, 见 unavailable.rs 的注释),
/// 上限只是防御上游返回异常大的错误体; 读满上限后剩余部分继续交给客户端, 报文不会被截断.
///
/// The maximum number of error body bytes read before deciding whether a rejection is quota-class.
///
/// An error body is small (the observed instances are 269 to 320 bytes, see the comments in
/// unavailable.rs), so the cap only guards against an unusually large one; once it is reached the
/// remainder is still forwarded to the client, so no message is ever truncated.
pub(crate) const MAX_ERROR_BODY_BYTES: usize = 1 << 20;

/// 记录到日志里的一次上游错误响应体字节上限.
///
/// 错误原文只用于定位故障, 不需要完整正文; 300 字节覆盖常见错误信息, 与搜索轮次失败原因的
/// MAX_UPSTREAM_REASON 取同一数值.
///
/// The byte limit of one upstream error body recorded in a log line.
///
/// The raw error text exists to locate a fault, so the whole body is unnecessary; 300 bytes
/// cover common messages, the same number as MAX_UPSTREAM_REASON for the search rounds'
/// failure reason.
const MAX_LOGGED_ERROR_BYTES: usize = 300;

/// 发送失败阶段的分类, 只用于日志.
///
/// connect 是连接建立阶段 (TCP, DNS, TLS 或代理握手) 的失败, connect_timeout 是其中的超时;
/// first_byte_timeout 是请求已经发出后等待响应头的超时, before_first_byte 是请求已经发出后
/// 在收到任何响应字节之前断开. 流中失败由 upstream_stream 单独记录.
///
/// The stage classification of one send failure, used only in log lines.
///
/// connect is a failure while establishing the connection (TCP, DNS, TLS or a proxy
/// handshake) and connect_timeout a timeout in that stage; first_byte_timeout is a timeout
/// while waiting for response headers after the request went out, and before_first_byte a
/// break in the same wait before any response byte arrived. Mid-stream failures are recorded
/// separately by upstream_stream.
fn send_failure_stage(error: &reqwest::Error) -> &'static str {
    if error.is_connect() {
        if error.is_timeout() {
            "connect_timeout"
        } else {
            "connect"
        }
    } else if error.is_timeout() {
        "first_byte_timeout"
    } else {
        "before_first_byte"
    }
}

/// 报告一次发送失败是否属于可以安全重试的连接建立失败.
///
/// 只有 reqwest 判定为连接阶段的失败且不是超时才可重试, 理由见 send_attempt.
///
/// Reports whether one send failure is a connection-establishment failure that can be safely
/// retried. Only a failure reqwest classifies as a connect failure and that is not a timeout
/// qualifies; the reasoning is in send_attempt.
fn retryable_connect_failure(error: &reqwest::Error) -> bool {
    error.is_connect() && !error.is_timeout()
}

/// 把一次上游失败整理成一行日志用的文本.
///
/// reqwest 错误的外层 Display 会带上游地址, 按 5.9 节不写入日志; 原因链上的各级说明不带
/// 地址, 例如 client error (Connect): tcp connect error: Connection refused (os error 111).
/// 最多取原因链上的三级说明, 结果去掉回车与换行并按 MAX_LOGGED_ERROR_BYTES 截断.
///
/// Builds one line's worth of text for an upstream failure.
///
/// A reqwest error's outer Display carries the upstream address, which section 5.9 keeps out
/// of logs; the messages of the cause chain carry no address, for example "client error
/// (Connect): tcp connect error: Connection refused (os error 111)". At most three levels of
/// the cause chain are taken and the result drops carriage returns and newlines and is
/// truncated to MAX_LOGGED_ERROR_BYTES.
fn describe_failure(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = String::new();
    let mut source = error.source();
    let mut depth = 0;
    while let Some(cause) = source {
        if depth == 3 {
            break;
        }
        if !text.is_empty() {
            text.push_str(": ");
        }
        text.push_str(&cause.to_string());
        source = cause.source();
        depth += 1;
    }
    aria_core::log::single_line(&text, MAX_LOGGED_ERROR_BYTES)
}

/// 读取错误响应体的前缀, 至多 limit 字节.
///
/// 读满上限或读取失败时提前返回已经读到的内容; 未读完的部分留在上游响应流里, 由调用方继续
/// 转发.
///
/// Reads the prefix of an error response body, up to limit bytes.
///
/// Hitting the cap or failing to read returns what has been read so far; the unread remainder stays
/// in the upstream stream for the caller to forward.
pub(crate) async fn read_error_prefix(response: &mut reqwest::Response, limit: usize) -> Bytes {
    let mut collected: Vec<u8> = Vec::new();
    while collected.len() < limit {
        match response.chunk().await {
            Ok(Some(chunk)) => collected.extend_from_slice(&chunk),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    Bytes::from(collected)
}

/// 把上游响应转换为网关响应: 状态码与响应头原样转发, 响应体按块流式转发.
///
/// Converts an upstream response into a gateway response: status and headers are
/// forwarded unchanged and the body is streamed chunk by chunk.
fn stream_response(upstream: reqwest::Response, route: String) -> Response<Body> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let stream = upstream_stream(upstream, route);
    let body: Body = BodyExt::boxed(StreamBody::new(stream));
    copy_upstream_headers(Response::new(body), status, &headers)
}

/// 用已经读到的前缀与上游剩余流拼出响应, 供判断配额类拒绝与记录错误体的路径使用.
///
/// Constructs the response from the prefix already read and the upstream remainder, used by
/// the paths that classify a rejection and record the error body.
fn buffered_prefix_response(
    upstream: reqwest::Response,
    prefix: Bytes,
    route: String,
) -> Response<Body> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let rest = upstream_stream(upstream, route);
    let stream = futures_util::stream::iter([Ok::<_, BoxError>(Frame::data(prefix))]).chain(rest);
    let body: Body = BodyExt::boxed(StreamBody::new(stream));
    copy_upstream_headers(Response::new(body), status, &headers)
}

/// 把上游响应体逐块转成网关响应体的流, 读流失败时记录失败阶段再传给客户端一侧.
///
/// 流中失败是响应头已经发给客户端之后的断开, 与 send 阶段的失败分开记录; 错误文本去掉回车
/// 与换行并按 MAX_LOGGED_ERROR_BYTES 截断. 客户端由此收到被截断的响应体, 完成日志以
/// incomplete 结束.
///
/// Turns the upstream body into the gateway body stream chunk by chunk, recording the failure
/// stage before a read failure is passed on.
///
/// A mid-stream failure is a break after the response headers reached the client, so it is
/// recorded apart from the send-stage failures; the error text drops carriage returns and
/// newlines and is truncated to MAX_LOGGED_ERROR_BYTES. The client therefore receives a
/// truncated body and the completion log ends with incomplete.
fn upstream_stream(
    upstream: reqwest::Response,
    route: String,
) -> impl futures_util::Stream<Item = Result<Frame<Bytes>, BoxError>> {
    upstream
        .bytes_stream()
        .map_err(move |error| -> BoxError {
            aria_core::log!(
                "messages route={} stage=stream error={}",
                route,
                describe_failure(&error)
            );
            Box::new(error)
        })
        .map_ok(Frame::data)
}

/// 按上游响应的状态码与响应头补全网关响应.
///
/// 逐跳头与 Set-Cookie 丢弃, 其余响应头原样转发, 并写入 no-store 缓存指令.
///
/// Completes a gateway response with the status and headers of the upstream response.
///
/// Hop-by-hop headers and Set-Cookie are dropped, every other header is forwarded unchanged, and
/// the no-store cache directive is written.
fn copy_upstream_headers(
    mut response: Response<Body>,
    status: StatusCode,
    headers: &HeaderMap,
) -> Response<Body> {
    *response.status_mut() = status;
    let out = response.headers_mut();
    for (name, value) in headers {
        if name == header::SET_COOKIE || HOP_BY_HOP.contains(&name.as_str()) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::{HeaderValue, Method};
    use http_body_util::BodyExt;
    use serde_json::json;

    use super::*;
    use crate::tests_support::{observed, spawn_stub, test_destination, text_body};

    /// 构造一份客户端请求体, 包含大整数, 不透明签名与未知字段, 用于验证原样转发.
    ///
    /// Builds a client request body with a large integer, an opaque signature and unknown
    /// fields, used to verify unchanged forwarding.
    const CLIENT_BODY: &[u8] = br#"{"model":"vendor-claude/local","future":12345678901234567890,"safeguards":[{"type":"future","signature":"opaque+/=="}],"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#;

    /// 按网关路径改写请求体: 与分派逻辑一样, 只在模型名不同时重新编码.
    ///
    /// Rewrites the request body the way the dispatcher does: it re-encodes only when the
    /// model name differs.
    fn rewritten_body(model: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
        let (mut fields, requested) =
            parse_messages_request(CLIENT_BODY).ok_or("请求体应当可解析")?;
        assert_eq!(requested, "vendor-claude/local");
        if requested == model {
            return Ok(Bytes::from_static(CLIENT_BODY));
        }
        Ok(rewrite_model(&mut fields, model)?)
    }

    /// 构造一份携带全部待丢弃请求头的客户端请求头.
    ///
    /// Builds client headers carrying every header the gateway must drop.
    fn client_headers() -> Result<HeaderMap, Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("authorization", "Bearer client-secret"),
            ("x-api-key", "client-secret"),
            ("anthropic-beta", "future-beta,server-tool-beta"),
            ("anthropic-version", "2023-06-01"),
            ("cookie", "private=value"),
            ("forwarded", "for=forged"),
            ("x-forwarded-for", "forged"),
            ("connection", "X-Hop"),
            ("x-hop", "do-not-forward"),
            ("content-type", "application/json; charset=utf-8"),
        ] {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_str(value)?,
            );
        }
        Ok(headers)
    }

    /// 读取完整响应体, 并把箱装错误转换为测试错误类型.
    ///
    /// Reads a complete response body and converts the boxed error into the test error
    /// type.
    async fn collect_body(response: Response<Body>) -> Result<Bytes, Box<dyn std::error::Error>> {
        Ok(response
            .into_body()
            .collect()
            .await
            .map_err(|error| error.to_string())?
            .to_bytes())
    }

    #[tokio::test]
    async fn native_contract() -> Result<(), Box<dyn std::error::Error>> {
        let response_text = "event: message_start\ndata: {\"type\":\"message_start\",\"unknown\":12345678901234567890}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"interrupted\"}}\n\n";
        let (base, records) = spawn_stub(move |_, _| async move {
            let mut response = Response::new(text_body(response_text));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream"),
            );
            response.headers_mut().insert(
                HeaderName::from_static("request-id"),
                HeaderValue::from_static("upstream-request"),
            );
            response.headers_mut().insert(
                header::SET_COOKIE,
                HeaderValue::from_static("session=upstream"),
            );
            response
        })
        .await?;
        let destination = test_destination(&format!("{base}/prefix"), "upstream-model", false)?;
        let body = rewritten_body(destination.model())?;
        let response = destination
            .forward(&client_headers()?, "/v1/messages?beta=true", body)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("request-id")
                .and_then(|value| value.to_str().ok()),
            Some("upstream-request")
        );
        assert_eq!(
            response
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|value| value.to_str().ok()),
            None,
            "上游 Set-Cookie 不得转发"
        );
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        let body = collect_body(response).await?;
        assert_eq!(body, Bytes::from_static(response_text.as_bytes()));

        let upstream = observed(&records);
        assert_eq!(upstream.len(), 1);
        let upstream = &upstream[0];
        assert_eq!(upstream.method, Method::POST);
        assert_eq!(upstream.path, "/prefix/v1/messages");
        assert_eq!(upstream.query.as_deref(), Some("beta=true"));
        assert_eq!(upstream.header("authorization"), "Bearer upstream-secret");
        assert_eq!(upstream.header("x-api-key"), "upstream-secret");
        assert_eq!(
            upstream.header_values("content-type"),
            ["application/json"],
            "上游只能收到一个 Content-Type, 与 Go 的 Header.Set 相同"
        );
        assert_eq!(
            upstream.header("anthropic-beta"),
            "future-beta,server-tool-beta"
        );
        assert_eq!(upstream.header("anthropic-version"), "2023-06-01");
        for name in [
            "cookie",
            "forwarded",
            "x-forwarded-for",
            "x-hop",
            "accept-encoding",
        ] {
            assert_eq!(upstream.header(name), "", "请求头 {name} 不得转发");
        }
        let fields = upstream.json()?;
        assert_eq!(fields["model"], json!("upstream-model"));
        assert_eq!(fields["future"], json!(12345678901234567890u64));
        assert_eq!(fields["safeguards"][0]["signature"], json!("opaque+/=="));
        assert_eq!(fields["tools"][0]["name"], json!("web_search"));
        Ok(())
    }

    #[test]
    fn rewrites_only_the_model_field() -> Result<(), Box<dyn std::error::Error>> {
        let (mut fields, requested) =
            parse_messages_request(CLIENT_BODY).ok_or("请求体应当可解析")?;
        assert_eq!(requested, "vendor-claude/local");
        let body = rewrite_model(&mut fields, "upstream-model")?;
        let rewritten: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(rewritten["model"], json!("upstream-model"));
        // 其余字段按原始字节保留, 大整数不变成浮点数, 签名不被改写.
        //
        // Every other field keeps its raw bytes: the large integer does not become a
        // float and the signature is not rewritten.
        assert_eq!(rewritten["future"], json!(12345678901234567890u64));
        assert_eq!(rewritten["safeguards"][0]["signature"], json!("opaque+/=="));
        let text = String::from_utf8(body.to_vec())?;
        assert!(text.contains("12345678901234567890"));
        assert!(!text.contains("1.2345678901234567e19"));
        assert!(parse_messages_request(b"null").is_none());
        assert!(parse_messages_request(b"[]").is_none());
        assert!(parse_messages_request(br#"{"model":""}"#).is_none());
        assert!(parse_messages_request(br#"{"model":5}"#).is_none());
        // 合法对象后跟多余字节按无效请求体处理, 与 Go 的 json.Unmarshal 相同.
        //
        // A valid object followed by extra bytes is an invalid body, matching Go's
        // json.Unmarshal.
        assert!(parse_messages_request(br#"{"model":"local"} {}"#).is_none());
        assert!(parse_messages_request(b"{\"model\":\"local\"} trailing").is_none());
        // 尾随空白不属于多余内容.
        //
        // Trailing whitespace is not extra content.
        assert!(parse_messages_request(b"{\"model\":\"local\"}  \n").is_some());
        Ok(())
    }
}
