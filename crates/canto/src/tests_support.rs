//! 测试用的回环桩服务与共享夹具. 只在测试编译时存在.
//!
//! 桩服务监听 127.0.0.1 的随机端口, 记录收到的每一个请求, 并按 1 开始的轮次号把请求
//! 交给应答函数, 因此测试覆盖真实的 HTTP 路径而不访问任何外部地址.
//!
//! Loopback stub services and shared fixtures for tests. Present only in test builds.
//!
//! A stub listens on a random 127.0.0.1 port, records every request it receives, and
//! hands the request to an answer function together with the 1-based round number, so
//! tests exercise real HTTP paths without reaching any external address.

use std::convert::Infallible;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aria_core::body::{Body, full};
use aria_core::upstream::{ClientOptions, build_client};
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::TcpListener;

use crate::forward::{MessagesDestination, MessagesRoute};

/// 桩服务收到的一个请求.
///
/// One request received by a stub service.
#[derive(Clone)]
pub(crate) struct Record {
    /// 请求方法.
    ///
    /// The request method.
    pub(crate) method: Method,
    /// 不含查询串的路径.
    ///
    /// The path without the query string.
    pub(crate) path: String,
    /// 查询串.
    ///
    /// The query string.
    pub(crate) query: Option<String>,
    /// 全部请求头.
    ///
    /// Every request header.
    pub(crate) headers: HeaderMap,
    /// 完整请求体.
    ///
    /// The complete request body.
    pub(crate) body: Bytes,
}

impl Record {
    /// 按 JSON 解析请求体.
    ///
    /// Parses the request body as JSON.
    pub(crate) fn json(&self) -> Result<Value, Box<dyn std::error::Error>> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    /// 返回某个请求头的文本取值.
    ///
    /// Returns the text value of one request header.
    pub(crate) fn header(&self, name: &str) -> String {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    }

    /// 返回某个请求头的全部取值, 便于断言只有一个取值.
    ///
    /// Returns every value of one request header so a test can assert there is exactly
    /// one.
    pub(crate) fn header_values(&self, name: &str) -> Vec<String> {
        self.headers
            .get_all(name)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .map(str::to_owned)
            .collect()
    }
}

/// 桩服务按调用顺序记录的请求表.
///
/// The requests a stub service recorded, in call order.
pub(crate) type Records = Arc<Mutex<Vec<Record>>>;

/// 创建一个空的请求记录表.
///
/// Creates an empty request record list.
pub(crate) fn records() -> Records {
    Arc::new(Mutex::new(Vec::new()))
}

/// 按调用顺序复制已经记录的请求.
///
/// Copies the recorded requests in call order.
pub(crate) fn observed(records: &Records) -> Vec<Record> {
    records.lock().map(|list| list.clone()).unwrap_or_default()
}

/// 起一个桩服务: 记录每个请求, 并按 1 开始的轮次号调用 answer 生成响应.
///
/// Starts a stub service that records every request and calls answer with the 1-based
/// round number to produce the response.
pub(crate) async fn spawn_stub<A, Fut, B>(
    answer: A,
) -> Result<(String, Records), Box<dyn std::error::Error>>
where
    A: Fn(usize, &Record) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<B>> + Send + 'static,
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let records = records();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let state = (records.clone(), answer);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (records, answer) = state.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let records = records.clone();
                    let answer = answer.clone();
                    async move {
                        let record = read_request(request).await.unwrap_or_else(|_| Record {
                            method: Method::GET,
                            path: String::new(),
                            query: None,
                            headers: HeaderMap::new(),
                            body: Bytes::new(),
                        });
                        let round = records
                            .lock()
                            .map(|mut list| {
                                list.push(record.clone());
                                list.len()
                            })
                            .unwrap_or_default();
                        let response = answer(round, &record).await;
                        Ok::<_, Infallible>(response)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok((format!("http://{address}"), records))
}

/// 读取一个请求的方法, 路径, 请求头与请求体.
///
/// Reads the method, path, headers and body of one request.
async fn read_request(request: Request<Incoming>) -> Result<Record, hyper::Error> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok(Record {
        method: parts.method,
        path: parts.uri.path().to_owned(),
        query: parts.uri.query().map(str::to_owned),
        headers: parts.headers,
        body,
    })
}

/// 构造一个文本响应体.
///
/// Builds a text response body.
pub(crate) fn text_body(text: &str) -> Body {
    full(text.as_bytes().to_vec())
}

/// 构造一个携带上游模型凭据的测试客户端.
///
/// Builds the test client used for upstream model credentials.
pub(crate) fn test_client() -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    Ok(build_client(&ClientOptions {
        http2: true,
        connect_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(30),
    })?)
}

/// 构造一条指向给定基址的测试路由, 与 Go 版 testConfig 的路由相同.
///
/// Builds a test route pointing at the given base URL, matching the route of the Go
/// testConfig.
pub(crate) fn test_route(base_url: &str) -> MessagesRoute {
    MessagesRoute {
        name: "local".to_owned(),
        display_name: "Test".to_owned(),
        aliases: vec![
            "vendor-claude/local[1m]".to_owned(),
            "vendor-claude/local".to_owned(),
        ],
        model: "upstream-model".to_owned(),
        base_url: base_url.to_owned(),
        base_url_env: String::new(),
        key_env: "UPSTREAM_KEY".to_owned(),
        upstreams: Vec::new(),
        moonshot_search: false,
    }
}

/// 解析以 _KEY 结尾的环境变量为固定上游密钥, 与 Go 版 testHandler 的取值函数相同.
///
/// Resolves any environment variable ending in _KEY to a fixed upstream secret, matching
/// the getter of the Go testHandler.
pub(crate) fn test_getenv(name: &str) -> Option<String> {
    match name {
        "SEARCH_KEY" => Some("search-secret".to_owned()),
        "UPSTREAM_KEY" => Some("upstream-secret".to_owned()),
        _ => None,
    }
}

/// 按测试路由构造一个 messages 目标.
///
/// Builds a messages destination from the test route.
pub(crate) fn test_destination(
    base_url: &str,
    model: &str,
    moonshot_search: bool,
) -> Result<MessagesDestination, Box<dyn std::error::Error>> {
    let mut route = test_route(base_url);
    route.model = model.to_owned();
    route.moonshot_search = moonshot_search;
    Ok(MessagesDestination::new(
        &route,
        test_client()?,
        &test_getenv,
    )?)
}
