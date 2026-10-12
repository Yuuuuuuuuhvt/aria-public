//! Pool 的精简契约测试.

use std::path::Path;

use serde_json::json;

use super::*;
use crate::credential::stub::{StubResponse, StubServer, read_pool, temp_dir, test_client};

async fn make_pool(path: &Path, responses: Vec<StubResponse>) -> (Pool, StubServer) {
    let stub = StubServer::start(responses).await;
    let pool = Pool::new(
        path,
        format!("{}/oauth/token", stub.base_url()),
        test_client(),
    );
    (pool, stub)
}

fn integer_field(text: &str, field: &str) -> i64 {
    let marker = format!("\"{field}\": ");
    let start = match text.find(&marker) {
        Some(index) => index + marker.len(),
        None => panic!("字段 {field} 不存在: {text}"),
    };
    let rest = &text[start..];
    let end = rest
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(rest.len());
    match rest.as_bytes().get(end) {
        Some(b',') | Some(b'\n') | Some(b'}') | None => {}
        other => panic!("字段 {field} 不是整数字面量: {:?}", (other, rest)),
    }
    match rest[..end].parse::<i64>() {
        Ok(value) => value,
        Err(error) => panic!("字段 {field} 不是整数: {error}"),
    }
}

/// 写入一条只带姓名, 账号, 令牌与到期时间的凭据条目, 供刷新余量场景控制到期时刻.
///
/// Writes one credential entry carrying just the name, account, tokens and expiry, so the
/// refresh-margin cases can control the expiry.
fn write_entry(path: &Path, expires_at: i64) {
    let raw = json!({"credentials": [{
        "name": "a",
        "account_id": "acc-a",
        "access_token": "token-a",
        "refresh_token": "refresh-a",
        "expires_at": expires_at,
    }]});
    if let Err(error) = std::fs::write(path, raw.to_string()) {
        panic!("写入夹具失败: {error}");
    }
}

#[tokio::test]
async fn refresh_preserves_unknown_fields() {
    let dir = temp_dir();
    let path = dir.path().join("credentials.json");
    let raw = r#"{"credentials":[{"name":"a","account_id":"acc-a","access_token":"token-a","refresh_token":"refresh-a","expires_at":1,"priority":0,"note":"会话已结束","source":"manual","updated_at":"2026-09-28T01:00:00+0800","weight":7}]}"#;
    if let Err(error) = std::fs::write(&path, raw) {
        panic!("写入夹具失败: {error}");
    }
    let (pool, _stub) = make_pool(
        &path,
        vec![StubResponse::ok(
            r#"{"access_token":"refreshed","expires_in":3600}"#,
        )],
    )
    .await;
    let stored = read_pool(&path);
    if let Err(error) = pool.authorize(stored[0].clone(), false).await {
        panic!("取鉴权头失败: {error}");
    }
    let written = read_pool(&path);
    for field in ["note", "source", "updated_at", "weight", "priority"] {
        assert!(written[0].contains_key(field), "刷新后字段 {field} 丢失");
    }
    assert_eq!(
        written[0].get("note").and_then(Value::as_str),
        Some("会话已结束")
    );
    assert_eq!(written[0].get("weight"), Some(&json!(7)));
    assert!(matches!(priority_of(&written[0]), Ok(0)));

    let raw_written = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) => panic!("读取写回文件失败: {error}"),
    };
    assert_eq!(integer_field(&raw_written, "weight"), 7);

    // 刷新表单与提前刷新余量跟随线路: ChatGPT 的表单按字节不变; xai 用配置的 client_id 与
    // scope; 二小时有效的令牌在一小时余量之外不刷新, 三十分钟有效的令牌刷新.
    //
    // The refresh form and early-refresh margin follow the line: the ChatGPT form stays byte for
    // byte unchanged; xai uses the configured client_id and scope; a two-hour token is beyond the
    // one-hour margin and is not refreshed, while a thirty-minute token is refreshed.
    use crate::xai::{XAI_OAUTH_CLIENT_ID, XAI_OAUTH_SCOPE, XAI_REFRESH_MARGIN_SECONDS};

    let xai_path = dir.path().join("xai.json");
    let stub = StubServer::start(vec![StubResponse::ok(
        r#"{"access_token":"token-b","refresh_token":"refresh-b","expires_in":21600}"#,
    )])
    .await;
    let xai_pool = Pool::with_oauth(
        &xai_path,
        format!("{}/oauth2/token", stub.base_url()),
        test_client(),
        XAI_OAUTH_CLIENT_ID,
        XAI_OAUTH_SCOPE,
        XAI_REFRESH_MARGIN_SECONDS,
    );

    // 剩余两小时的令牌在 xai 的一小时余量之外, 不刷新.
    //
    // A token with two hours left is beyond xai's one-hour margin and is not refreshed.
    write_entry(&xai_path, unix_now() + 7200);
    if let Err(error) = xai_pool
        .authorize(read_pool(&xai_path)[0].clone(), false)
        .await
    {
        panic!("取鉴权头失败: {error}");
    }
    assert_eq!(stub.request_count(), 0);

    // 剩余半小时时刷新, 表单使用 xai 的 client_id 与 scope.
    //
    // With half an hour left the token is refreshed with xai's client_id and scope.
    write_entry(&xai_path, unix_now() + 1800);
    if let Err(error) = xai_pool
        .authorize(read_pool(&xai_path)[0].clone(), false)
        .await
    {
        panic!("取鉴权头失败: {error}");
    }
    let requests = stub.requests();
    assert_eq!(requests.len(), 1);
    let form: Vec<(String, String)> = url::form_urlencoded::parse(requests[0].body.as_bytes())
        .into_owned()
        .collect();
    let expected: Vec<(String, String)> = [
        ("client_id", XAI_OAUTH_CLIENT_ID),
        ("grant_type", "refresh_token"),
        ("refresh_token", "refresh-a"),
        ("scope", XAI_OAUTH_SCOPE),
    ]
    .iter()
    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
    .collect();
    assert_eq!(form, expected);

    // ChatGPT 线路的刷新表单按字节保持不变.
    //
    // The refresh form of the ChatGPT line stays byte for byte unchanged.
    let chatgpt_path = dir.path().join("chatgpt.json");
    write_entry(&chatgpt_path, 1);
    let (chatgpt, chatgpt_stub) = make_pool(
        &chatgpt_path,
        vec![StubResponse::ok(
            r#"{"access_token":"token-c","expires_in":3600}"#,
        )],
    )
    .await;
    if let Err(error) = chatgpt
        .authorize(read_pool(&chatgpt_path)[0].clone(), false)
        .await
    {
        panic!("取鉴权头失败: {error}");
    }
    assert_eq!(
        chatgpt_stub.requests()[0].body,
        "client_id=app_EMoamEEZ73f0CkXaXp7hrann&grant_type=refresh_token&refresh_token=refresh-a&scope=openid+profile+email+offline_access"
    );
}
