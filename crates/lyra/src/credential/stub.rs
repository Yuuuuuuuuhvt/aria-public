//! 凭据测试共用的本机桩服务与夹具助手. 只编译进测试.
//!
//! 桩服务只监听 127.0.0.1, 逐个记录收到的请求并按顺序返回预设响应, 因此刷新与项目
//! 标识推导可以走真实的 HTTP 路径, 不必用替身跳过请求构造与响应解析. 测试不会访问
//! 真实上游, 也不读取本机任何真实凭据.
//!
//! The local stub and fixture helpers shared by the credential tests. Compiled into tests
//! only.
//!
//! The stub listens on 127.0.0.1 only, records every request it receives and returns the
//! preset responses in order, so refresh and project id discovery exercise the real HTTP
//! path instead of skipping request construction and response parsing behind a stand-in.
//! Tests never reach a real upstream and never read any real credential on this machine.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::{Entry, file::decode_entries};

/// 桩服务收到的一次请求. 头与正文都记录, 使测试可以断言真实的请求构造.
///
/// One request received by the stub. Headers and body are recorded so tests can assert the
/// real request construction.
#[derive(Debug, Clone)]
pub struct StubRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl StubRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// 一条预设响应. 状态码与正文由测试指定.
///
/// One preset response; the test decides its status and body.
#[derive(Debug, Clone)]
pub struct StubResponse {
    pub status: u16,
    pub body: String,
}

impl StubResponse {
    pub fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
        }
    }
}

struct StubState {
    requests: Vec<StubRequest>,
    responses: VecDeque<StubResponse>,
    fallback: StubResponse,
}

/// 127.0.0.1 上的桩服务. 响应按顺序取用, 队列取空后重复最后一条.
///
/// The stub on 127.0.0.1. Responses are taken in order and the last one repeats once the
/// queue is empty.
pub struct StubServer {
    addr: SocketAddr,
    state: Arc<Mutex<StubState>>,
}

impl StubServer {
    /// 启动桩服务. responses 为空时所有请求都返回 200 与空对象.
    ///
    /// Starts the stub. With an empty response list every request returns 200 and an
    /// empty object.
    pub async fn start(responses: Vec<StubResponse>) -> Self {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) => panic!("启动桩服务失败: {error}"),
        };
        let addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => panic!("读取桩服务地址失败: {error}"),
        };
        let state = Arc::new(Mutex::new(StubState {
            requests: Vec::new(),
            responses: responses.into(),
            fallback: StubResponse::ok("{}"),
        }));
        let served = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = served.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |request| handle(request, state.clone())),
                        )
                        .await;
                });
            }
        });
        Self { addr, state }
    }

    /// 返回 http://127.0.0.1:端口 形式的基址.
    ///
    /// Returns the base URL in the form http://127.0.0.1:port.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StubState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn requests(&self) -> Vec<StubRequest> {
        self.lock().requests.clone()
    }

    pub fn request_count(&self) -> usize {
        self.lock().requests.len()
    }
}

async fn handle(
    request: Request<Incoming>,
    state: Arc<Mutex<StubState>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let body = match request.into_body().collect().await {
        Ok(collected) => String::from_utf8_lossy(&collected.to_bytes()).into_owned(),
        Err(_) => String::new(),
    };
    let mut guard = state.lock().unwrap_or_else(PoisonError::into_inner);
    guard.requests.push(StubRequest {
        method,
        path,
        headers,
        body,
    });
    let response = guard
        .responses
        .pop_front()
        .unwrap_or_else(|| guard.fallback.clone());
    drop(guard);
    Ok(Response::builder()
        .status(response.status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(response.body)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))))
}

/// 读取凭据文件里的条目, 用于断言写回结果.
///
/// Reads the entries in a credential file, used to assert a write-back.
pub fn read_pool(path: &Path) -> Vec<Entry> {
    match std::fs::read(path) {
        Ok(data) => match decode_entries(&data) {
            Ok(entries) => entries,
            Err(error) => panic!("解析凭据文件失败: {error}"),
        },
        Err(error) => panic!("读取凭据文件失败: {error}"),
    }
}

/// 不读取系统代理设置的测试客户端, 避免环境中的代理变量影响回环地址请求.
///
/// A test client that ignores system proxy settings, keeping proxy variables in the
/// environment from affecting loopback requests.
pub fn test_client() -> reqwest::Client {
    match reqwest::Client::builder().no_proxy().build() {
        Ok(client) => client,
        Err(error) => panic!("构造测试客户端失败: {error}"),
    }
}

pub fn temp_dir() -> tempfile::TempDir {
    match tempfile::TempDir::new() {
        Ok(dir) => dir,
        Err(error) => panic!("创建临时目录失败: {error}"),
    }
}
