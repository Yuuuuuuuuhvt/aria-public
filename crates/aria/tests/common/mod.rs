//! 端到端测试的公共夹具: 回环桩服务, aria 子进程与测试客户端.
//!
//! 测试通过环境变量 CARGO_BIN_EXE_aria 启动真实的可执行文件, 因此覆盖命令行解析, 配置加载,
//! 监听与分派的完整路径. 所有网络流量都在 127.0.0.1 上, 不访问任何真实上游, 也不读取本机
//! 凭据.
//!
//! Shared fixtures for the end-to-end tests: loopback stub services, the aria child process
//! and the test client.
//!
//! The tests start the real executable through the CARGO_BIN_EXE_aria environment variable,
//! so they cover the whole path from command line parsing and configuration loading to
//! listening and dispatch. All traffic stays on 127.0.0.1; no real upstream is reached and
//! no local credential is read.

#![allow(dead_code)]

use std::convert::Infallible;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aria_core::body::Body;
use aria_core::upstream::{ClientOptions, build_client};
use bytes::Bytes;
use http::{HeaderMap, Request, Response};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpListener;

/// 启动子进程与等待响应的超时.
///
/// The timeout for starting a child process and for waiting for responses.
pub const TIMEOUT: Duration = Duration::from_secs(10);

/// 桩服务收到的一个请求.
///
/// One request received by a stub service.
#[derive(Clone)]
pub struct Record {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Record {
    /// 按 JSON 解析请求体.
    ///
    /// Parses the request body as JSON.
    pub fn json(&self) -> Result<Value, Box<dyn std::error::Error>> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    /// 返回某个请求头的文本取值.
    ///
    /// Returns the text value of one request header.
    pub fn header(&self, name: &str) -> String {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    }
}

/// 桩服务按调用顺序记录的请求表.
///
/// The requests a stub service recorded, in call order.
pub type Records = Arc<Mutex<Vec<Record>>>;

/// 创建一个空的请求记录表.
///
/// Creates an empty request record list.
pub fn records() -> Records {
    Arc::new(Mutex::new(Vec::new()))
}

/// 按调用顺序复制已经记录的请求.
///
/// Copies the recorded requests in call order.
pub fn observed(records: &Records) -> Vec<Record> {
    records.lock().map(|list| list.clone()).unwrap_or_default()
}

/// 起一个桩服务: 记录每个请求, 并按 1 开始的轮次号调用 answer 生成响应.
///
/// Starts a stub service that records every request and calls answer with the 1-based round
/// number to produce the response.
pub async fn spawn_stub<A, Fut, B>(
    answer: A,
) -> Result<(String, Records), Box<dyn std::error::Error>>
where
    A: Fn(usize, Record) -> Fut + Clone + Send + Sync + 'static,
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
                            method: String::new(),
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
                        Ok::<_, Infallible>(answer(round, record).await)
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

/// 起一个对每个请求都返回同一响应的桩服务.
///
/// Starts a stub service that answers every request with one fixed response.
pub async fn spawn_fixed_stub(
    status: http::StatusCode,
    body: &str,
) -> Result<(String, Records), Box<dyn std::error::Error>> {
    let body = body.to_owned();
    spawn_stub(move |_, _| {
        let body = body.clone();
        async move {
            let mut response = Response::new(aria_core::body::full(body.into_bytes()));
            *response.status_mut() = status;
            response
        }
    })
    .await
}

/// 读取一个请求的方法, 路径, 请求头与请求体.
///
/// Reads the method, path, headers and body of one request.
async fn read_request(request: Request<Incoming>) -> Result<Record, hyper::Error> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok(Record {
        method: parts.method.to_string(),
        path: parts.uri.path().to_owned(),
        query: parts.uri.query().map(str::to_owned),
        headers: parts.headers,
        body,
    })
}

/// 构造一个 JSON 响应体.
///
/// Builds a JSON response body.
pub fn json_body(value: &Value) -> Body {
    aria_core::body::full(value.to_string().into_bytes())
}

/// 构造一个文本响应体.
///
/// Builds a text response body.
pub fn text_body(text: &str) -> Body {
    aria_core::body::full(text.as_bytes().to_vec())
}

/// 读取响应体并按 JSON 解析.
///
/// 工作区没有启用 reqwest 的 json 功能, 因此测试统一通过这个辅助函数解析响应体.
///
/// Reads a response body and parses it as JSON.
///
/// The workspace does not enable reqwest's json feature, so the tests parse response bodies
/// through this helper.
pub async fn json(response: reqwest::Response) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

/// 构造测试客户端.
///
/// 复用 aria_core 的上游客户端构造, 因此测试同样关闭系统代理读取并排除回环地址, 不会因为本机
/// 代理配置而把回环请求送出去.
///
/// Builds the test client.
///
/// It reuses aria_core's upstream client construction, so the tests also disable system
/// proxy discovery and exclude loopback addresses, and a local proxy configuration cannot
/// send loopback requests elsewhere.
pub fn test_client() -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    Ok(build_client(&ClientOptions {
        http2: true,
        connect_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(30),
    })?)
}

/// 写入一个测试用的客户端密钥文件.
///
/// Writes one client key file for tests.
pub fn write_clients(directory: &Path, key: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = directory.join("clients.json");
    std::fs::write(
        &path,
        format!(r#"{{"keys":[{{"key":"{key}","enabled":true}}]}}"#),
    )?;
    Ok(path)
}

/// 写入一个配置文件.
///
/// Writes one configuration file.
pub fn write_config(
    directory: &Path,
    config: &Value,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = directory.join("config.json");
    std::fs::write(&path, serde_json::to_vec_pretty(config)?)?;
    Ok(path)
}

/// 一个已经启动的 aria 服务进程.
///
/// A started aria server process.
pub struct Aria {
    child: Child,
    /// 服务基址, 例如 http://127.0.0.1:52341.
    ///
    /// The service base URL, for example http://127.0.0.1:52341.
    pub base: String,
    log: Arc<Mutex<Vec<String>>>,
}

impl Aria {
    /// 启动 aria serve, 监听随机端口, 直到启动行出现.
    ///
    /// 端口写 0 让内核分配, 因此并行运行的测试不会互相占用端口; 真实端口从启动行读取, 因此
    /// 不需要猜测或重试.
    ///
    /// Starts aria serve on a random port and waits for the startup line.
    ///
    /// Port 0 lets the kernel assign one, so tests running in parallel never collide; the
    /// real port is read from the startup line instead of being guessed or retried.
    pub fn start(config: &Path, env: &[(&str, &str)]) -> Result<Aria, Box<dyn std::error::Error>> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_aria"));
        command
            .arg("serve")
            .arg("--config")
            .arg(config)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command.spawn()?;
        let stderr = child.stderr.take().ok_or("子进程没有标准错误")?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let collected = log.clone();
        let (sender, receiver) = std::sync::mpsc::channel::<String>();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if let Some(rest) = line.split("aria listening on ").nth(1)
                    && let Some(address) = rest.split_whitespace().next()
                {
                    let _ = sender.send(address.to_owned());
                }
                if let Ok(mut lines) = collected.lock() {
                    lines.push(line);
                }
            }
        });
        let address = receiver
            .recv_timeout(TIMEOUT)
            .map_err(|_| "aria 没有在期限内开始监听")?;
        Ok(Aria {
            child,
            base: format!("http://{address}"),
            log,
        })
    }

    /// 返回已经收集到的日志行.
    ///
    /// Returns the collected log lines.
    pub fn log(&self) -> Vec<String> {
        self.log
            .lock()
            .map(|lines| lines.clone())
            .unwrap_or_default()
    }

    /// 发送 SIGTERM 并等待进程退出, 返回退出码.
    ///
    /// Sends SIGTERM, waits for the process to exit and returns its exit code.
    pub fn terminate(&mut self) -> Result<i32, Box<dyn std::error::Error>> {
        self.signal_terminate()?;
        self.wait()
    }

    /// 只发送 SIGTERM, 不等待退出.
    ///
    /// 停机测试需要在信号之后继续与桩服务交互, 因此把发送信号与等待退出分开.
    ///
    /// Only sends SIGTERM without waiting for the exit.
    ///
    /// The shutdown test keeps interacting with a stub after the signal, so sending the
    /// signal and waiting for the exit are separate.
    pub fn signal_terminate(&self) -> Result<(), Box<dyn std::error::Error>> {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()?;
        if !status.success() {
            return Err("发送 SIGTERM 失败".into());
        }
        Ok(())
    }

    /// 等待进程退出并返回退出码.
    ///
    /// Waits for the process to exit and returns its exit code.
    pub fn wait(&mut self) -> Result<i32, Box<dyn std::error::Error>> {
        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status.code().unwrap_or(-1));
            }
            if std::time::Instant::now() > deadline {
                return Err("aria 没有在期限内退出".into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 等待直到服务不再接受新连接.
    ///
    /// Waits until the service no longer accepts new connections.
    pub async fn wait_until_closed(&self) -> bool {
        let deadline = std::time::Instant::now() + TIMEOUT;
        while std::time::Instant::now() < deadline {
            if tokio::net::TcpStream::connect(self.address())
                .await
                .is_err()
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// 返回监听地址.
    ///
    /// Returns the listen address.
    pub fn address(&self) -> String {
        self.base.trim_start_matches("http://").to_owned()
    }
}

impl Drop for Aria {
    fn drop(&mut self) {
        // 测试失败时也要收回子进程, 避免遗留监听端口的进程.
        //
        // A failing test must still reap the child, so no process keeps a listening port.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 直接运行一次 aria 命令并收集结果, 用于 check, version 与参数错误.
///
/// Runs one aria command and collects its result, used by check, version and argument
/// errors.
pub fn run_cli(
    args: &[&str],
    env: &[(&str, &str)],
) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aria"));
    command.args(args);
    for (name, value) in env {
        command.env(name, value);
    }
    Ok(command.output()?)
}

/// 写入一个 JSON 文件并返回路径.
///
/// Writes a JSON file and returns its path.
pub fn write_file(
    directory: &Path,
    name: &str,
    value: &Value,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = directory.join(name);
    std::fs::write(&path, serde_json::to_vec(value)?)?;
    Ok(path)
}

/// 构造一条 Google 凭据条目.
///
/// 到期时间取一小时之后, 明显超出 5 分钟的提前刷新余量, 因此测试不会触发令牌刷新, 也就不会
/// 访问任何外部地址. project_id 必须给出, 否则凭据池会通过 loadCodeAssist 推导它.
///
/// Builds one Google credential entry.
///
/// The expiry is one hour ahead, clearly beyond the 5 minute early-refresh margin, so the tests
/// never trigger a token refresh and never reach an external address. project_id must be present,
/// otherwise the pool derives it through loadCodeAssist.
pub fn google_entry(name: &str) -> Value {
    json!({
        "name": name,
        "email": format!("{name}@example.com"),
        "project_id": "aicode-consumers",
        "access_token": format!("token-{name}"),
        "refresh_token": format!("refresh-{name}"),
        "expires_at": chrono::Utc::now().timestamp() + 3600,
        "enabled": true,
        "priority": 0,
    })
}

/// 构造一条 ChatGPT 凭据条目, 到期时间取十天之后, 超出 24 小时的提前刷新余量.
///
/// Builds one ChatGPT credential entry expiring in ten days, beyond the 24 hour early-refresh
/// margin.
pub fn chatgpt_entry(name: &str) -> Value {
    json!({
        "name": name,
        "account_id": format!("acct-{name}"),
        "plan": "pro",
        "access_token": format!("token-{name}"),
        "refresh_token": format!("refresh-{name}"),
        "expires_at": chrono::Utc::now().timestamp() + 10 * 24 * 3600,
        "enabled": true,
    })
}

/// 写入一个提供方凭据文件并返回路径.
///
/// Writes one provider credential file and returns its path.
pub fn write_credentials(
    directory: &Path,
    name: &str,
    entries: &[Value],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    write_file(directory, name, &json!({"credentials": entries}))
}

/// 读取用量目录下已经写出的全部记录.
///
/// 查询接口只返回汇总, 因此记录级别的字段, 例如请求编号与 outcome, 直接按日文件核对.
///
/// Reads every record written into the usage directory.
///
/// The query endpoint returns summaries only, so record-level fields such as the request
/// identifier and the outcome are checked against the daily files directly.
pub fn read_usage_records(directory: &Path) -> Vec<Value> {
    let mut records = Vec::new();
    let Ok(entries) = std::fs::read_dir(directory) else {
        return records;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            if let Ok(value) = serde_json::from_str::<Value>(line) {
                records.push(value);
            }
        }
    }
    records
}

/// 等待用量目录中出现指定数量的记录, 超时后返回已经读到的部分.
///
/// 记录在响应体结束时由后台任务写入, 因此这里轮询而不是读一次就断言.
///
/// Waits for the given number of records to appear in the usage directory and returns what was
/// read when the deadline passes.
///
/// A record is written by a background task when the response body ends, so this polls instead of
/// asserting after a single read.
pub async fn wait_for_records(directory: &Path, count: usize) -> Vec<Value> {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let records = read_usage_records(directory);
        if records.len() >= count || std::time::Instant::now() > deadline {
            return records;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
