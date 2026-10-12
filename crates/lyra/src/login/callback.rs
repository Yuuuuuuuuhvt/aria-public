//! 浏览器授权的回调接收端.
//!
//! 只绑定 127.0.0.1, 除回调路径以外的请求一律 404; 回调本身校验 state 并按结果回答授权码或
//! 失败原因. 第一个结果决定等待结束, 之后的回调只得到 409: 授权码只能用一次, 重复处理会让
//! 后到的请求影响已经确定的流程.
//!
//! The callback receiver of a browser authorization.
//!
//! It binds 127.0.0.1 only and answers 404 to every request outside the callback path; the callback
//! itself checks state and answers with the authorization code or the failure reason. The first
//! result ends the wait and later callbacks only receive 409: an authorization code can be used
//! once, and handling it twice would let a late request disturb a decision already made.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aria_core::body::{Body, full};
use http::{HeaderValue, Request, Response, StatusCode};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// 授权被拒绝或回调不合要求时的说明, 与 Go 版回调服务的文本一致.
///
/// Descriptions used when authorization is refused or the callback does not match, matching the
/// text of Go's callback service.
const DENIED: &str = "授权被拒绝";
const STATE_MISMATCH: &str = "回调 state 与请求不一致";
const MISSING_CODE: &str = "回调中没有授权码";
const SUCCESS: &str = "登录成功, 凭据已接收, 可以关闭此页面。";

/// 一次授权的回调服务.
///
/// Callback service of one authorization.
pub(crate) struct CallbackServer {
    receiver: oneshot::Receiver<Result<String, String>>,
    acceptor: tokio::task::JoinHandle<()>,
}

impl CallbackServer {
    /// 绑定回环端口并开始接收回调.
    ///
    /// 只监听 127.0.0.1: 回调里带着授权码, 不能让它出现在本机之外的网络上.
    ///
    /// Binds the loopback port and starts receiving callbacks. Only 127.0.0.1 is listened on: the
    /// callback carries the authorization code and must not appear on a network outside this
    /// machine.
    pub(crate) async fn start(
        port: u16,
        path: &'static str,
        state: String,
    ) -> Result<CallbackServer, String> {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|error| format!("监听 {port} 端口失败: {error}"))?;
        let (sender, receiver) = oneshot::channel();
        let sender = Arc::new(Mutex::new(Some(sender)));
        let acceptor = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = state.clone();
                let sender = sender.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let state = state.clone();
                        let sender = sender.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(handle(
                                request, path, &state, &sender,
                            ))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Ok(CallbackServer { receiver, acceptor })
    }

    /// 等待回调结果, 超过给定时限时报错.
    ///
    /// Waits for the callback result and reports an error once the given limit has passed.
    pub(crate) async fn wait(mut self, timeout: Duration) -> Result<String, String> {
        // 接收端按引用等待, 使 CallbackServer 的析构仍然负责结束监听任务; 等待超时后调用方
        // 丢弃整个服务, 端口随之释放.
        //
        // The receiver is awaited by reference so CallbackServer's drop still ends the accept task;
        // when the wait times out the caller drops the whole service and the port is released.
        match tokio::time::timeout(timeout, &mut self.receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("回调通道意外关闭".to_owned()),
            Err(_) => Err(format!("{timeout:?} 内没有收到回调")),
        }
    }
}

impl Drop for CallbackServer {
    fn drop(&mut self) {
        // 等待结束后不再接受新的回调: 授权码只能用一次, 端口也不应继续被占用.
        //
        // No callback is accepted after the wait ends: the authorization code can be used once and
        // the port must not stay occupied.
        self.acceptor.abort();
    }
}

/// 处理一次回调请求.
///
/// Handles one callback request.
fn handle<B>(
    request: Request<B>,
    path: &str,
    state: &str,
    sender: &Mutex<Option<oneshot::Sender<Result<String, String>>>>,
) -> Response<Body> {
    if request.uri().path() != path {
        return text_response(StatusCode::NOT_FOUND, "未知路径");
    }
    let query = request.uri().query().unwrap_or_default();
    let mut error = None;
    let mut received_state = None;
    let mut code = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "error" => error = Some(value.into_owned()),
            "state" => received_state = Some(value.into_owned()),
            "code" => code = Some(value.into_owned()),
            _ => {}
        }
    }
    let result = if let Some(message) = error.filter(|message| !message.is_empty()) {
        Err(format!("{DENIED}: {message}"))
    } else if received_state.as_deref() != Some(state) {
        Err(STATE_MISMATCH.to_owned())
    } else if code.as_deref().unwrap_or_default().is_empty() {
        Err(MISSING_CODE.to_owned())
    } else {
        Ok(code.unwrap_or_default())
    };

    let (status, text) = match &result {
        Ok(_) => (StatusCode::OK, SUCCESS.to_owned()),
        Err(message) => (StatusCode::BAD_REQUEST, message.clone()),
    };
    let delivered = {
        let mut slot = sender.lock().unwrap_or_else(PoisonError::into_inner);
        match slot.take() {
            Some(sender) => {
                let _ = sender.send(result.clone());
                true
            }
            None => false,
        }
    };
    if !delivered {
        return text_response(StatusCode::CONFLICT, "回调已经处理");
    }
    text_response(status, &text)
}

fn text_response(status: StatusCode, text: &str) -> Response<Body> {
    let mut response = Response::new(full(format!("{text}\n").into_bytes()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}
