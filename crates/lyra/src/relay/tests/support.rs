//! relay 测试共用的回环桩服务与夹具.
//!
//! 桩服务只监听 127.0.0.1 的随机端口, 按 1 开始的轮次号把请求交给应答函数, 因此转换转发
//! 的流式行为, 连接中断与容量重试都能在真实 HTTP 路径上验证, 不必访问任何外部地址.
//!
//! 凭据模块自带的桩服务无法流式应答, 也不能为每个请求单独设置响应头, 因此这里另建一套;
//! 它同样只存在于测试编译中.
//!
//! Loopback stubs and fixtures shared by the relay tests.
//!
//! A stub listens on a random 127.0.0.1 port and hands each request to the answer function
//! together with the 1-based round number, so the streaming behaviour of the converting
//! forward, connection interruption and capacity retries are all verified over a real HTTP
//! path without reaching any external address.
//!
//! The stub shipped with the credential module cannot stream and cannot set per-request
//! response headers, so this module provides its own; it also exists only in test builds.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use aria_core::body::{Body, BoxError, full};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body::{Body as HttpBody, Frame};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use aria_core::usage::Observation;

use crate::credential::Entry;
use crate::relay::{AntigravityConfig, ChatGptConfig, Relay, RelayConfig};

/// 桩服务收到的一次请求.
///
/// One request received by the stub service.
#[derive(Clone)]
pub(crate) struct StubRequest {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) query: Option<String>,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

impl StubRequest {
    /// 返回某个请求头的文本取值, 缺失时为空串.
    ///
    /// Returns the text value of one header, or an empty string when absent.
    pub(crate) fn header(&self, name: &str) -> String {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    }

    /// 按 JSON 解析请求体.
    ///
    /// Parses the request body as JSON.
    pub(crate) fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| {
            panic!(
                "请求体不是合法 JSON: {error}: {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

/// 127.0.0.1 上的桩服务.
///
/// The stub service on 127.0.0.1.
pub(crate) struct Stub {
    base: String,
    records: Arc<Mutex<Vec<StubRequest>>>,
}

impl Stub {
    /// 返回 http://127.0.0.1:端口 形式的基址.
    ///
    /// Returns the base URL in the form http://127.0.0.1:port.
    pub(crate) fn base(&self) -> &str {
        &self.base
    }

    pub(crate) fn count(&self) -> usize {
        lock(&self.records).len()
    }

    pub(crate) fn request(&self, index: usize) -> StubRequest {
        let records = lock(&self.records);
        match records.get(index) {
            Some(record) => record.clone(),
            None => panic!("第 {index} 次请求不存在, 共收到 {} 次", records.len()),
        }
    }

    /// 返回每次请求的 Authorization 取值.
    ///
    /// Returns the Authorization value of every request.
    pub(crate) fn authorizations(&self) -> Vec<String> {
        lock(&self.records)
            .iter()
            .map(|record| record.header("authorization"))
            .collect()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 起一个桩服务: 记录每个请求, 并按 1 开始的轮次号调用 answer 生成响应.
///
/// Starts a stub service that records every request and calls answer with the 1-based round
/// number to produce the response.
pub(crate) async fn spawn<A, Fut>(answer: A) -> Stub
where
    A: Fn(usize, StubRequest) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<Body>> + Send + 'static,
{
    let records = Arc::new(Mutex::new(Vec::new()));
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) => panic!("监听回环端口失败: {error}"),
    };
    let address = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => panic!("读取监听地址失败: {error}"),
    };
    let state = (records.clone(), answer);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (records, answer) = state.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let records = records.clone();
                    let answer = answer.clone();
                    async move {
                        let record = read_request(request).await;
                        let round = {
                            let mut list = lock(&records);
                            list.push(record.clone());
                            list.len()
                        };
                        Ok::<_, Infallible>(answer(round, record.clone()).await)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Stub {
        base: format!("http://{address}"),
        records,
    }
}

async fn read_request(request: Request<Incoming>) -> StubRequest {
    let (parts, body) = request.into_parts();
    let body = body
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .unwrap_or_default();
    StubRequest {
        method: parts.method,
        path: parts.uri.path().to_owned(),
        query: parts.uri.query().map(str::to_owned),
        headers: parts.headers,
        body,
    }
}

/// 构造一个 JSON 响应.
///
/// Builds a JSON response.
pub(crate) fn json_response(status: StatusCode, body: &str) -> Response<Body> {
    let mut response = Response::new(full(body.as_bytes().to_vec()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// 构造一个原样写出若干事件的 SSE 响应.
///
/// Builds an SSE response writing the given events as they are.
pub(crate) fn sse_response(events: &[&str]) -> Response<Body> {
    let mut response = Response::new(full(events.concat().into_bytes()));
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
}

/// 构造一个先写出前缀内容, 之后按通道放行的流式响应.
///
/// 发送端关闭通道后响应体结束; 测试据此把上游尾部保持在打开状态, 验证客户端不会等待整段
/// 生成结束.
///
/// Builds a streaming response that writes the prefix and then releases more content over a
/// channel. Closing the sender ends the body; a test uses this to hold the upstream tail open
/// and verify that the client does not wait for the whole generation.
pub(crate) fn channel_response(
    events: &[&str],
    content_type: &'static str,
) -> (Response<Body>, mpsc::Sender<Bytes>) {
    let (sender, receiver) = mpsc::channel(4);
    let mut prefix: VecDeque<Bytes> = VecDeque::new();
    let content = events.concat();
    if !content.is_empty() {
        prefix.push_back(Bytes::from(content));
    }
    let mut response = Response::new(BodyExt::boxed(ChannelBody { prefix, receiver }));
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    (response, sender)
}

/// 先写出固定前缀, 之后按通道放行的响应体.
///
/// A response body writing a fixed prefix and then releasing content over a channel.
struct ChannelBody {
    prefix: VecDeque<Bytes>,
    receiver: mpsc::Receiver<Bytes>,
}

impl HttpBody for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if let Some(chunk) = this.prefix.pop_front() {
            return std::task::Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        match this.receiver.poll_recv(cx) {
            std::task::Poll::Ready(Some(chunk)) => {
                std::task::Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// 不产生任何内容的响应体, 被丢弃时通过通道报告.
///
/// 中继在预算耗尽或客户端断开时丢弃上游响应, 连接随之关闭; 桩服务的连接任务结束并丢弃
/// 响应体, 测试据此确认上游请求确实被取消.
///
/// A response body producing nothing that reports through a channel when dropped. The relay
/// drops the upstream response when the budget runs out or the client disconnects, closing the
/// connection; the stub's connection task then ends and drops the body, which is how a test
/// confirms that the upstream request was really cancelled.
pub(crate) struct PendingBody {
    prefix: Option<Bytes>,
    signal: Option<oneshot::Sender<()>>,
}

impl PendingBody {
    /// 返回先写出 prefix 然后不再产生内容的响应体, 以及它被丢弃时收到通知的接收端.
    ///
    /// Returns a body that writes prefix and then produces nothing, together with the receiver
    /// notified when the body is dropped.
    pub(crate) fn new(prefix: &str) -> (PendingBody, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        let prefix = if prefix.is_empty() {
            None
        } else {
            Some(Bytes::from(prefix.to_owned()))
        };
        (
            PendingBody {
                prefix,
                signal: Some(sender),
            },
            receiver,
        )
    }

    /// 用给定的通知发送端替换默认的一个. 调用方通常在拿到接收端之后再把它接到响应体上.
    ///
    /// Replaces the default notification sender. A caller usually takes the receiver first and
    /// then attaches one to the body.
    pub(crate) fn with_signal(mut self, sender: oneshot::Sender<()>) -> Self {
        self.signal = Some(sender);
        self
    }
}

impl HttpBody for PendingBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        match this.prefix.take() {
            Some(chunk) => std::task::Poll::Ready(Some(Ok(Frame::data(chunk)))),
            None => std::task::Poll::Pending,
        }
    }
}

impl Drop for PendingBody {
    fn drop(&mut self) {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(());
        }
    }
}

/// 先写出固定内容, 之后以错误结束的响应体.
///
/// 用于模拟上游在响应中途断开: 服务端据此中止连接, 客户端读到确定的响应体错误.
///
/// A response body writing a fixed prefix and then failing. It stands for an upstream that
/// breaks off mid-response: the server aborts the connection and the client reads a definite
/// body error.
pub(crate) struct FailingBody {
    prefix: Option<Bytes>,
    /// 中断前的等待. 响应头与首块内容需要先真正写出, 让客户端确实拿到 200 之后才读到错误;
    /// 立即出错会让服务端在响应头发出之前就中止连接, 客户端看到的是连接失败.
    ///
    /// The wait before breaking off. The headers and the first chunk must really reach the client
    /// so the error arrives after a 200; failing at once makes the server abort before the
    /// headers and the client sees a connection failure instead.
    delay: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl FailingBody {
    pub(crate) fn new(prefix: &str) -> Self {
        let prefix = if prefix.is_empty() {
            None
        } else {
            Some(Bytes::from(prefix.to_owned()))
        };
        Self {
            prefix,
            delay: None,
        }
    }
}

impl HttpBody for FailingBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if let Some(chunk) = this.prefix.take() {
            return std::task::Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        let delay = this
            .delay
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_millis(50))));
        match delay.as_mut().poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(()) => {
                std::task::Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "upstream connection broke off",
                )))))
            }
        }
    }
}

/// 构造一个先写出内容再中断的响应.
///
/// Builds a response that writes content and then breaks off.
pub(crate) fn failing_response(prefix: &str, content_type: &'static str) -> Response<Body> {
    let mut response = Response::new(BodyExt::boxed(FailingBody::new(prefix)));
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    response
}

/// 构造一条 Anthropic 请求体, stream 决定是否流式.
///
/// Builds one Anthropic request body; stream decides the streaming flag.
pub(crate) fn anthropic_body(stream: bool) -> String {
    format!(
        r#"{{"model":"gemini-3.8-flash-high","max_tokens":64,"stream":{stream},
        "messages":[{{"role":"user","content":"你好"}}]}}"#
    )
}

/// Go 版测试使用的最小 Gemini 事件.
///
/// The minimal Gemini events the Go tests use.
pub(crate) const FIRST_GEMINI_EVENT: &str =
    "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"first\"}]}}]}}\n\n";
pub(crate) const LAST_GEMINI_EVENT: &str = "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"last\"}]},\"finishReason\":\"STOP\"}]}}\n\n";

/// 构造一条 Google 凭据条目.
///
/// Builds one Google credential entry.
pub(crate) fn google_entry(name: &str, token: &str) -> Entry {
    match json!({
        "name": name,
        "email": format!("{name}@example.com"),
        "project_id": "aicode-consumers",
        "access_token": token,
        "refresh_token": format!("refresh-{name}"),
        "expires_at": chrono::Utc::now().timestamp() + 3600,
        "enabled": true,
        "priority": 0,
    }) {
        Value::Object(entry) => entry,
        _ => Entry::new(),
    }
}

/// 构造一条 ChatGPT 凭据条目. 到期时间取十天之后: 凭据池的刷新余量是 24 小时, 只有明显
/// 未过期的令牌才不会触发刷新请求, 测试因此不会访问任何外部地址.
///
/// Builds one ChatGPT credential entry expiring in ten days: the pool refreshes within a
/// 24-hour margin, so only a clearly unexpired token avoids a refresh request and no test ever
/// reaches an external address.
pub(crate) fn chatgpt_entry(name: &str) -> Entry {
    match json!({
        "name": name,
        "account_id": format!("acct-{name}"),
        "plan": "pro",
        "access_token": format!("token-{name}"),
        "refresh_token": format!("refresh-{name}"),
        "expires_at": chrono::Utc::now().timestamp() + 10 * 24 * 3600,
        "enabled": true,
    }) {
        Value::Object(entry) => entry,
        _ => Entry::new(),
    }
}

/// 把凭据条目写进临时目录并返回路径.
///
/// Writes credential entries into a temporary directory and returns the path.
pub(crate) fn write_entry_file(
    dir: &tempfile::TempDir,
    name: &str,
    entries: &[Entry],
) -> std::path::PathBuf {
    let path = dir.path().join(name);
    let payload = json!({"credentials": entries}).to_string();
    if let Err(error) = std::fs::write(&path, payload) {
        panic!("写入凭据夹具失败: {error}");
    }
    path
}

/// 测试用的上游客户端: 不读取系统代理, 避免环境变量影响回环请求.
///
/// The upstream client used by tests: it ignores system proxy settings so environment
/// variables cannot affect loopback requests.
pub(crate) fn test_client() -> reqwest::Client {
    match reqwest::Client::builder().no_proxy().build() {
        Ok(client) => client,
        Err(error) => panic!("构造测试客户端失败: {error}"),
    }
}

/// 空测试客户端, 供不需要真实请求的一侧使用.
///
/// An unused client for the side that issues no real request.
pub(crate) fn relay(
    credentials: std::path::PathBuf,
    antigravity_base: Option<String>,
    chatgpt_base: Option<String>,
    budget: Duration,
) -> Relay {
    relay_with_timeouts(
        credentials,
        antigravity_base,
        chatgpt_base,
        budget,
        Duration::from_secs(120),
    )
}

pub(crate) fn relay_with_timeouts(
    credentials: std::path::PathBuf,
    antigravity_base: Option<String>,
    chatgpt_base: Option<String>,
    budget: Duration,
    first_byte_timeout: Duration,
) -> Relay {
    relay_with_full_options(
        credentials,
        antigravity_base,
        chatgpt_base,
        budget,
        first_byte_timeout,
        2,
        Duration::from_secs(300),
    )
}

pub(crate) fn relay_with_full_options(
    credentials: std::path::PathBuf,
    antigravity_base: Option<String>,
    chatgpt_base: Option<String>,
    budget: Duration,
    first_byte_timeout: Duration,
    timeout_cooldown_threshold: u32,
    timeout_cooldown_duration: Duration,
) -> Relay {
    let antigravity = antigravity_base.map(|base_url| AntigravityConfig {
        credentials: credentials.clone(),
        base_url,
        client_id: "client-id".to_owned(),
        client_secret: "client-secret".to_owned(),
        user_agent: antigravity_identity(),
        timeout_cooldown_threshold,
        timeout_cooldown_duration,
    });
    let chatgpt = chatgpt_base.map(|base_url| ChatGptConfig {
        source: crate::relay::ChatGptSourceConfig::Credentials(credentials.clone()),
        base_url,
        models: vec!["gpt-6-astra".to_owned()],
        user_agent: chatgpt_identity(),
    });
    // 令牌端点指向本机一个没有监听的端口: 用例若意外触发刷新, 会在回环地址上立刻失败,
    // 而不是访问生产端点. 需要真实刷新路径的用例改用 with_token_endpoints 指向桩服务.
    //
    // Token endpoints point at an unlistened loopback port: a case that accidentally triggers a
    // refresh fails immediately on loopback instead of reaching a production endpoint. Cases
    // that need the real refresh path use with_token_endpoints against a stub.
    match Relay::with_token_endpoints(
        RelayConfig {
            antigravity,
            chatgpt,
            xai: None,
            commandcode: None,
            capacity_budget: budget,
            first_byte_timeout,
        },
        test_client(),
        test_client(),
        "http://127.0.0.1:1/token",
        "http://127.0.0.1:1/token",
    ) {
        Ok(relay) => relay,
        Err(error) => panic!("构造中继失败: {error}"),
    }
}

/// 用指定 Antigravity 标识构造一条 Antigravity 线路的中继, 供验证标识更新后请求跟随的用例使用.
///
/// Builds a relay on the Antigravity route with a given identifier, for cases that verify a
/// request follows an updated identifier.
pub(crate) fn relay_with_identity(
    credentials: std::path::PathBuf,
    antigravity_base: String,
    user_agent: std::sync::Arc<crate::client_id::ClientIdentity>,
) -> Relay {
    match Relay::with_token_endpoints(
        RelayConfig {
            antigravity: Some(AntigravityConfig {
                credentials,
                base_url: antigravity_base,
                client_id: "client-id".to_owned(),
                client_secret: "client-secret".to_owned(),
                user_agent,
                timeout_cooldown_threshold: 2,
                timeout_cooldown_duration: Duration::from_secs(300),
            }),
            chatgpt: None,
            xai: None,
            commandcode: None,
            capacity_budget: Duration::from_secs(25),
            first_byte_timeout: Duration::from_secs(120),
        },
        test_client(),
        test_client(),
        "http://127.0.0.1:1/token",
        "http://127.0.0.1:1/token",
    ) {
        Ok(relay) => relay,
        Err(error) => panic!("构造中继失败: {error}"),
    }
}

/// 测试用的 Antigravity 标识: 指向没有监听的回环端口, 永不触发获取, 用后备版本渲染.
///
/// The test Antigravity identifier: it points at an unlistened loopback port so no fetch ever
/// happens, and renders from the fallback version.
pub(crate) fn antigravity_identity() -> std::sync::Arc<crate::client_id::ClientIdentity> {
    crate::client_id::ClientIdentity::new(crate::client_id::ClientConfig {
        user_agent_template: "antigravity/cli/{version} (aidev_client; os_type=darwin; arch=arm64; cl=989022288; auth_method=consumer)".to_owned(),
        version_url: "http://127.0.0.1:1/releases/latest".to_owned(),
        fallback_version: "1.2.17".to_owned(),
    })
}

/// 版本清单桩: 起一个回环服务, 按顺序返回预设的清单内容.
///
/// A version-manifest stub: a loopback server returning preset manifest contents in order.
pub(crate) struct ManifestStub {
    pub(crate) base: String,
}

impl ManifestStub {
    pub(crate) async fn start(bodies: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|error| panic!("监听回环端口失败: {error}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("读取地址失败: {error}"));
        let queue: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(bodies.into()));
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let queue = queue.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |_request: hyper::Request<hyper::body::Incoming>| {
                                let queue = queue.clone();
                                async move {
                                    let body = {
                                        let mut guard =
                                            queue.lock().unwrap_or_else(PoisonError::into_inner);
                                        guard
                                            .pop_front()
                                            .unwrap_or_else(|| "version: 0.0.0\n".to_owned())
                                    };
                                    let response = hyper::Response::builder()
                                        .status(200)
                                        .header("content-type", "text/yaml")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap_or_else(|_| {
                                            hyper::Response::new(Full::new(Bytes::new()))
                                        });
                                    Ok::<_, Infallible>(response)
                                }
                            }),
                        )
                        .await;
                });
            }
        });
        Self {
            base: format!("http://{address}/manifest.yml"),
        }
    }
}

/// 测试用的 Codex 标识: 指向没有监听的回环端口, 永不触发获取, 用后备版本渲染.
///
/// The test Codex identifier: it points at an unlistened loopback port so no fetch ever happens,
/// and renders from the fallback version.
pub(crate) fn chatgpt_identity() -> std::sync::Arc<crate::client_id::ClientIdentity> {
    crate::client_id::ClientIdentity::new(crate::client_id::ClientConfig {
        user_agent_template: "codex_cli_rs/{version}".to_owned(),
        version_url: "http://127.0.0.1:1/latest".to_owned(),
        fallback_version: "0.160.1".to_owned(),
    })
}

/// 读取响应体的全部内容.
///
/// Reads the whole response body.
pub(crate) async fn body_text(response: Response<Body>) -> String {
    match response.into_body().collect().await {
        Ok(collected) => String::from_utf8_lossy(&collected.to_bytes()).into_owned(),
        Err(error) => panic!("读取响应体失败: {error}"),
    }
}

/// 构造一次 Messages 请求的请求头.
///
/// Builds the headers of one Messages request.
pub(crate) fn message_headers(session: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Some(session) = session {
        headers.insert(
            "X-Claude-Code-Session-Id",
            HeaderValue::from_str(session).unwrap_or_else(|error| panic!("{error}")),
        );
    }
    headers
}

/// 把中继挂成真实 HTTP 服务并返回基址, 供需要真实连接行为的用例使用: 首块到达时间, 客户端
/// 断开与上游取消都只有经过真实连接才能观察.
///
/// Serves the relay over a real HTTP listener and returns its base URL, used by cases that need
/// real connection behaviour: first-chunk timing, client disconnects and upstream cancellation
/// are observable only through a real connection.
pub(crate) async fn serve_messages(
    relay: Arc<Relay>,
    route: &str,
    observation: Option<Observation>,
) -> String {
    serve(relay, route, observation, Route::Messages).await
}

/// 把中继的 Responses 透传挂成真实 HTTP 服务并返回基址.
///
/// Serves the relay's Responses pass-through over a real HTTP listener and returns its base URL.
pub(crate) async fn serve_responses(
    relay: Arc<Relay>,
    path: &str,
    observation: Option<Observation>,
) -> String {
    serve(relay, path, observation, Route::Responses).await
}

/// 服务按线路调用的入口.
///
/// The entry point the served route calls.
enum Route {
    Messages,
    Responses,
}

async fn serve(
    relay: Arc<Relay>,
    name: &str,
    observation: Option<Observation>,
    route: Route,
) -> String {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) => panic!("监听回环端口失败: {error}"),
    };
    let address = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => panic!("读取监听地址失败: {error}"),
    };
    let name = name.to_owned();
    let route = Arc::new(route);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let relay = relay.clone();
            let name = name.clone();
            let route = route.clone();
            let observation = observation.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let relay = relay.clone();
                    let name = name.clone();
                    let route = route.clone();
                    let observation = observation.clone();
                    async move {
                        let (parts, body) = request.into_parts();
                        let body = body
                            .collect()
                            .await
                            .map(|collected| collected.to_bytes())
                            .unwrap_or_default();
                        let response = match route.as_ref() {
                            Route::Messages => {
                                relay
                                    .messages(&parts.headers, body, &name, observation)
                                    .await
                            }
                            Route::Responses => {
                                relay
                                    .responses(&parts.headers, &name, body, observation)
                                    .await
                            }
                        };
                        Ok::<_, Infallible>(response)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    format!("http://{address}")
}
