//! Antigravity 线路的精简测试: 额度耗尽跳过与首字节超时合并验证.
//!
//! Slimmed tests for the Antigravity route: quota exhaustion skipping and combined first-byte timeout verification.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aria_core::body::Body;
use bytes::Bytes;
use http::{Response, StatusCode};

use super::support::*;
use crate::credential::Entry;
use crate::relay::Relay;

/// 构造一条指定容量预算与首字节超时的 Antigravity 线路中继与桩上游.
///
/// Builds a relay on the Antigravity route with explicit budget and first-byte timeout.
async fn antigravity_relay_with_timeouts<A, Fut>(
    accounts: &[(&str, &str)],
    budget: Duration,
    first_byte_timeout: Duration,
    answer: A,
) -> (tempfile::TempDir, Arc<Relay>, Stub)
where
    A: Fn(usize, StubRequest) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<Body>> + Send + 'static,
{
    let stub = spawn(answer).await;
    let dir = match tempfile::TempDir::new() {
        Ok(dir) => dir,
        Err(error) => panic!("创建临时目录失败: {error}"),
    };
    let entries: Vec<Entry> = accounts
        .iter()
        .map(|(name, token)| google_entry(name, token))
        .collect();
    let path = write_entry_file(&dir, "google.json", &entries);
    let relay = relay_with_timeouts(
        path,
        Some(stub.base().to_owned()),
        None,
        budget,
        first_byte_timeout,
    );
    (dir, Arc::new(relay), stub)
}

/// 构造一条指定容量预算, 首字节超时与超时冷却配置的 Antigravity 线路中继与桩上游.
///
/// Builds a relay on the Antigravity route with full timeout and cooldown configurations.
async fn antigravity_relay_with_full_options<A, Fut>(
    accounts: &[(&str, &str)],
    budget: Duration,
    first_byte_timeout: Duration,
    timeout_cooldown_threshold: u32,
    timeout_cooldown_duration: Duration,
    answer: A,
) -> (tempfile::TempDir, Arc<Relay>, Stub)
where
    A: Fn(usize, StubRequest) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<Body>> + Send + 'static,
{
    let stub = spawn(answer).await;
    let dir = match tempfile::TempDir::new() {
        Ok(dir) => dir,
        Err(error) => panic!("创建临时目录失败: {error}"),
    };
    let entries: Vec<Entry> = accounts
        .iter()
        .map(|(name, token)| google_entry(name, token))
        .collect();
    let path = write_entry_file(&dir, "google.json", &entries);
    let relay = relay_with_full_options(
        path,
        Some(stub.base().to_owned()),
        None,
        budget,
        first_byte_timeout,
        timeout_cooldown_threshold,
        timeout_cooldown_duration,
    );
    (dir, Arc::new(relay), stub)
}

/// 发一次 Messages 请求并读完响应.
///
/// Sends one Messages request and reads the whole response.
async fn post(relay: &Relay, body: &str) -> (StatusCode, String) {
    let response = relay
        .messages(
            &message_headers(None),
            Bytes::from(body.to_owned()),
            "test-route",
            None,
        )
        .await;
    let status = response.status();
    (status, body_text(response).await)
}

/// 发送一次带会话标识的 Messages 请求并返回完整响应.
///
/// Sends one Messages request carrying a session identifier and returns the whole response.
async fn post_session(relay: &Relay, body: &str, session: &str) -> Response<Body> {
    relay
        .messages(
            &message_headers(Some(session)),
            Bytes::from(body.to_owned()),
            "test-route",
            None,
        )
        .await
}

fn quota_body(reset_delay: &str) -> String {
    format!(
        r#"{{"error":{{"code":429,"message":"Individual quota reached.",
        "details":[{{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED",
        "metadata":{{"quotaResetDelay":"{reset_delay}"}}}}]}}}}"#
    )
}

fn prioritized_relay(dir: &tempfile::TempDir, stub_base: &str) -> Arc<Relay> {
    let mut entry_a = google_entry("a", "token-a");
    let mut entry_b = google_entry("b", "token-b");
    entry_a.insert("priority".to_owned(), serde_json::json!(0));
    entry_b.insert("priority".to_owned(), serde_json::json!(1));
    let path = write_entry_file(dir, "google.json", &[entry_a, entry_b]);
    Arc::new(relay(
        path,
        Some(stub_base.to_owned()),
        None,
        Duration::from_secs(25),
    ))
}

fn session_body(first_message: &str) -> String {
    format!(
        r#"{{"model":"gemini-3.8-flash-high","max_tokens":16,"system":"你是助手","messages":[{{"role":"user","content":"{first_message}"}}]}}"#
    )
}

#[tokio::test]
async fn quota_exhausted_account_is_skipped_until_its_reset() {
    // 账号 a 在第一轮返回带 1 秒重置时间的 429: 记录期间同一模型跳过 a, 记录到期后 a 自动回到
    // 选号范围, 成功一次即清除记录.
    //
    // Account a answers 429 with a one second reset in the first round: while the record holds the
    // same model skips a, and once it expires a returns to selection where one success clears it.
    let exhausted = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let exhausted_clone = exhausted.clone();
    let stub = spawn(move |_, request| {
        let exhausted = exhausted_clone.clone();
        async move {
            if request.header("authorization") == "Bearer token-a"
                && exhausted.load(std::sync::atomic::Ordering::SeqCst)
            {
                return json_response(StatusCode::TOO_MANY_REQUESTS, &quota_body("1s"));
            }
            sse_response(&[FIRST_GEMINI_EVENT, LAST_GEMINI_EVENT])
        }
    })
    .await;
    let dir = match tempfile::TempDir::new() {
        Ok(dir) => dir,
        Err(error) => panic!("创建临时目录失败: {error}"),
    };
    let relay_handle = prioritized_relay(&dir, stub.base());
    let body = anthropic_body(true);

    // 1. 第一次请求: a 返回 429 被记录, 换 b 成功.
    let (status, text) = post(&relay_handle, &body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(stub.authorizations(), ["Bearer token-a", "Bearer token-b"]);
    let recorded = relay_handle.unavailable_models();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].name, "a");
    assert_eq!(recorded[0].model, "gemini-3.8-flash-high");
    assert_eq!(recorded[0].reason, "quota_exhausted");
    assert!(recorded[0].remaining_seconds <= 1, "{recorded:?}");

    // 2. 记录未到期: 同一模型只请求 b.
    let count_before = stub.count();
    let response = post_session(&relay_handle, &body, "session-2").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        &stub.authorizations()[count_before..],
        ["Bearer token-b"],
        "记录期间跳过 a"
    );

    // 3. 到期自动恢复: a 重新参与选号, 成功一次即清除记录.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    exhausted.store(false, std::sync::atomic::Ordering::SeqCst);
    let count_before = stub.count();
    let response = post_session(&relay_handle, &body, "session-3").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        &stub.authorizations()[count_before..],
        ["Bearer token-a"],
        "记录到期后 a 回到选号范围"
    );
    assert!(
        relay_handle.unavailable_models().is_empty(),
        "成功一次即清除记录: {:?}",
        relay_handle.unavailable_models()
    );
}

#[tokio::test]
async fn first_byte_timeout_rotates_cools_and_spares_healthy_streams() {
    // 阶段一: 正常请求与首字节之后的慢速输出不受首字节超时影响, 不被误伤.
    // 1. 正常请求不受首字节超时影响, 流式与非流式都能成功响应.
    let (_dir_normal, relay_normal, stub_normal) = antigravity_relay_with_timeouts(
        &[("a", "token-a")],
        Duration::from_secs(25),
        Duration::from_millis(50),
        |_, _| async move { sse_response(&[FIRST_GEMINI_EVENT, LAST_GEMINI_EVENT]) },
    )
    .await;

    let (status, body) = post(&relay_normal, &anthropic_body(true)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("first"), "{body}");

    let (status, body) = post(&relay_normal, &anthropic_body(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(stub_normal.count(), 2);

    // 2. 流式首字节之后的慢速输出不被首字节期限打断.
    let (_dir_slow, relay_slow, stub_slow) = antigravity_relay_with_timeouts(
        &[("a", "token-a")],
        Duration::from_secs(25),
        Duration::from_millis(50),
        |_, _| async move {
            let (response, sender) = channel_response(&[FIRST_GEMINI_EVENT], "text/event-stream");
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = sender.send(Bytes::from(LAST_GEMINI_EVENT)).await;
            });
            response
        },
    )
    .await;

    let (status, body) = post(&relay_slow, &anthropic_body(true)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("first"), "{body}");
    assert!(
        body.contains("last"),
        "慢速输出不应被首字节期限误伤: {body}"
    );
    assert_eq!(stub_slow.count(), 1);

    // 阶段二: 上游不返回响应头时, 在首字节期限后换号并前移会话固定位置.
    let (_dir_rotate, relay_rotate, stub_rotate) = antigravity_relay_with_timeouts(
        &[("a", "token-a"), ("b", "token-b")],
        Duration::from_secs(25),
        Duration::from_millis(50),
        |_, request| async move {
            if request.header("authorization") == "Bearer token-a" {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            sse_response(&[FIRST_GEMINI_EVENT, LAST_GEMINI_EVENT])
        },
    )
    .await;

    let body = session_body("首字节挂起换号用例");
    let started = std::time::Instant::now();
    let (status, response) = post(&relay_rotate, &body).await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK, "{response}");
    assert!(response.contains("first"), "{response}");
    assert!(
        elapsed < Duration::from_millis(250),
        "应在 50ms 首字节超时后换号完成, 而不是等待 300ms: {elapsed:?}"
    );

    assert_eq!(
        stub_rotate.authorizations(),
        ["Bearer token-a", "Bearer token-b"],
        "应先尝试挂起的 token-a, 超时后换到 token-b"
    );

    let (status2, response2) = post(&relay_rotate, &body).await;
    assert_eq!(status2, StatusCode::OK, "{response2}");
    assert_eq!(
        &stub_rotate.authorizations()[2..],
        ["Bearer token-b"],
        "同一会话固定应已前移到 token-b"
    );

    // 阶段三: 连续超时达到阈值触发冷却并在冷却期内跳过该账号, 冷却期满恢复参与选号.
    let (_dir_cooldown, relay_cooldown, stub_cooldown) = antigravity_relay_with_full_options(
        &[("a", "token-a"), ("b", "token-b")],
        Duration::from_secs(25),
        Duration::from_millis(30),
        2,
        Duration::from_millis(100),
        |_, request| async move {
            if request.header("authorization") == "Bearer token-a" {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            sse_response(&[FIRST_GEMINI_EVENT, LAST_GEMINI_EVENT])
        },
    )
    .await;

    // 第 1 次请求 (会话一, 轮到 a): a 发生第 1 次超时, 换到 b 成功
    let (status1, _) = post(&relay_cooldown, &session_body("会话一")).await;
    assert_eq!(status1, StatusCode::OK);
    assert!(
        relay_cooldown.cooldowns().is_empty(),
        "第 1 次超时不应触发冷却"
    );

    // 第 2 次请求 (会话二, 轮到 b): b 成功响应
    let (status2, _) = post(&relay_cooldown, &session_body("会话二")).await;
    assert_eq!(status2, StatusCode::OK);

    // 第 3 次请求 (会话三, 轮到 a): a 发生第 2 次超时, 达到阈值 2, 触发冷却
    let (status3, _) = post(&relay_cooldown, &session_body("会话三")).await;
    assert_eq!(status3, StatusCode::OK);
    let cooldowns = relay_cooldown.cooldowns();
    assert_eq!(cooldowns.len(), 1, "连续两次超时后应进入冷却");
    assert_eq!(cooldowns[0].provider, "antigravity");
    assert_eq!(cooldowns[0].name, "a");

    // 第 4 次请求 (会话四, 轮到 b): b 正常响应
    let _ = post(&relay_cooldown, &session_body("会话四")).await;

    // 第 5 次请求 (会话五, 本应轮到 a, 但 a 在冷却中): 选号跳过 a, 直接由 b 响应
    let completed = stub_cooldown.count();
    let (status5, _) = post(&relay_cooldown, &session_body("会话五")).await;
    assert_eq!(status5, StatusCode::OK);
    assert_eq!(
        &stub_cooldown.authorizations()[completed..],
        ["Bearer token-b"],
        "冷却期间应直接跳过 token-a"
    );

    // 等待 120ms (超过 100ms 冷却时长)
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        relay_cooldown.cooldowns().is_empty(),
        "冷却期满后健康检查列表应清空"
    );

    // 第 6 次请求 (会话六, 消耗游标 b)
    let _ = post(&relay_cooldown, &session_body("会话六")).await;

    // 第 7 次请求 (会话七, 轮到 a): 冷却期满后恢复参与选号
    let completed_after = stub_cooldown.count();
    let (status7, _) = post(&relay_cooldown, &session_body("会话七")).await;
    assert_eq!(status7, StatusCode::OK);
    assert!(
        stub_cooldown.authorizations()[completed_after..].contains(&"Bearer token-a".to_owned()),
        "冷却期满后 token-a 应恢复参与选号: {:?}",
        &stub_cooldown.authorizations()[completed_after..]
    );
}
