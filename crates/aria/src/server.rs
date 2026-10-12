//! 监听, 连接处理, 超时与优雅停机.
//!
//! 每个连接都包一层写超时, 因此停滞的客户端无法永久占用一个并发槽位; 请求头的读取时间单独
//! 限制, 请求体则留给 dispatch 的读取期限, 这样长时间上传与长时间流式响应都不会被整请求
//! 超时切断.
//!
//! Listener, connection handling, timeouts and graceful shutdown.
//!
//! Every connection carries a write timeout so a stalled client cannot hold a concurrency
//! slot forever; the request header read time is bounded separately and the body is left to
//! the dispatch read deadline, so neither a long upload nor a long streaming response is
//! cut off by a whole-request timeout.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::{GracefulConnection, GracefulShutdown};
use tokio::net::{TcpListener, TcpStream};
use tokio_io_timeout::TimeoutStream;

use crate::dispatch::{AppState, handle};

/// 单个连接的写超时. 停滞的客户端超过该时间没有读走数据时连接被中止, 与 Canto 的写截止
/// 时间相同.
///
/// The write timeout of one connection. A stalled client that reads nothing for longer is
/// cut off, matching Canto's write deadline.
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);

/// 请求头的读取超时, 与 Canto 的 ReadHeaderTimeout 相同.
///
/// The request header read timeout, matching Canto's ReadHeaderTimeout.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// 停机时已有请求的最长等待时间. 超过之后取消剩余请求并退出.
///
/// The longest wait for active requests during shutdown; afterwards the remaining requests
/// are cancelled and the process exits.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(60);

/// 开始监听并服务请求, 直到收到 SIGTERM 或 SIGINT.
///
/// 监听失败时返回错误, 由调用方以非零状态退出.
///
/// Starts listening and serving until SIGTERM or SIGINT arrives.
///
/// A failed bind returns an error, and the caller exits with a non-zero status.
pub async fn run(state: Arc<AppState>, listen: &str) -> Result<(), String> {
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|error| format!("监听 {listen} 失败: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("读取监听地址失败: {error}"))?;
    // 启动行写在绑定成功之后, 因此它打印的是实际地址; 监听 0 端口时测试据此取得真实端口.
    //
    // The startup line is written after a successful bind, so it prints the actual address;
    // a test that listens on port 0 reads the real port from it.
    aria_core::log!(
        "aria listening on {} with {} routes",
        address,
        state.route_count()
    );

    let graceful = GracefulShutdown::new();
    let mut stopping = Box::pin(stopping_signal());
    loop {
        tokio::select! {
            _ = &mut stopping => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        // 单个连接接受失败不终止服务, 与 Go 的 Accept 循环相同.
                        //
                        // One failed accept does not stop the service, as in Go's Accept loop.
                        aria_core::log!("accept failed: {error}");
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let connection = connection(state.clone(), stream, peer);
                let watched = graceful.watch(connection);
                tokio::spawn(async move {
                    // 连接错误只影响该连接, 客户端断开是常见结果, 因此不写日志.
                    //
                    // A connection error only affects that connection, and a client
                    // disconnect is an ordinary outcome, so nothing is logged here.
                    let _ = watched.await;
                });
            }
        }
    }

    // 先停止接受新连接, 再给已有请求最多 SHUTDOWN_GRACE 秒; 超过期限后剩余连接随进程退出被
    // 取消, 与 architecture 5.10 的停机规则相同.
    //
    // New connections stop first, then active requests get up to SHUTDOWN_GRACE seconds;
    // after that the remaining connections are cancelled when the process exits, matching
    // the shutdown rule in architecture 5.10.
    drop(listener);
    aria_core::log!(
        "stopping, waiting up to {}s for active requests",
        SHUTDOWN_GRACE.as_secs()
    );
    if tokio::time::timeout(SHUTDOWN_GRACE, graceful.shutdown())
        .await
        .is_err()
    {
        aria_core::log!("graceful shutdown incomplete, cancelling remaining requests");
    }
    // 用量写入在响应体结束或客户端断开时完成, 因此关闭存储在等待之后.
    //
    // Usage writes finish when a body ends or a client disconnects, so the store is closed
    // after the wait.
    state.close_usage();
    Ok(())
}

/// 构造一个连接的完整服务管线: 写超时, 请求头读取超时与请求处理.
///
/// Builds the full pipeline of one connection: write timeout, header read timeout and
/// request handling.
fn connection(
    state: Arc<AppState>,
    stream: TcpStream,
    peer: std::net::SocketAddr,
) -> impl GracefulConnection<Error = hyper::Error> {
    // 只设置写超时. 读超时会同时限制请求体上传, 与 5.3 的 60 秒读取期限冲突, 因此请求头的
    // 读取时间改用 hyper 自带的期限, 它只覆盖请求头.
    //
    // Only the write timeout is set here. A read timeout would also bound body uploads and
    // conflict with the 60 second read deadline of 5.3, so the header read time uses
    // hyper's own bound, which covers request headers only.
    let mut timeout = TimeoutStream::new(stream);
    timeout.set_write_timeout(Some(WRITE_TIMEOUT));
    let service = service_fn(move |request| {
        let state = state.clone();
        async move { Ok::<_, Infallible>(handle(state, Some(peer), request).await) }
    });
    // TimeoutStream 内部持有定时器, 因此不是 Unpin; 装箱并钉住之后 Pin<Box<..>> 本身是 Unpin,
    // 并且 tokio 为 Pin<P> 实现了 AsyncRead 与 AsyncWrite, 于是 hyper 的 Unpin 约束得到满足.
    //
    // TimeoutStream holds a timer inside and is therefore not Unpin; boxing and pinning it
    // makes Pin<Box<..>> itself Unpin, and tokio implements AsyncRead and AsyncWrite for
    // Pin<P>, which satisfies hyper's Unpin bound.
    // header_read_timeout 依赖 hyper 的定时器接口; 不设置定时器时 hyper 会在第一条连接上
    // panic, 因此这里显式给出基于 tokio 的实现.
    //
    // header_read_timeout relies on hyper's timer interface; without a timer hyper panics
    // on the first connection, so the tokio-based implementation is set explicitly.
    http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .serve_connection(TokioIo::new(Box::pin(timeout)), service)
}

/// 等待停止信号.
///
/// SIGTERM 在测试与非交互式停机中使用, SIGINT 供交互式运行时使用, 两者都触发同一套停机
/// 流程.
///
/// Waits for a stop signal.
///
/// SIGTERM serves tests and non-interactive shutdown and SIGINT the interactive case; both
/// trigger the same shutdown path.
async fn stopping_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                // 注册失败时该分支永不完成, 进程仍然响应 SIGINT.
                //
                // When registration fails this branch never completes and the process still
                // responds to SIGINT.
                aria_core::log!("cannot listen for SIGTERM: {error}");
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }
}
