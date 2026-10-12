//! GoogleSource 的精简契约测试.

use std::path::{Path, PathBuf};

use super::*;
use crate::credential::stub::{StubResponse, StubServer, read_pool, temp_dir, test_client};

fn identity(email: &str) -> Entry {
    let mut entry = Entry::new();
    entry.insert("email".to_owned(), Value::String(email.to_owned()));
    entry
}

fn google_file(dir: &tempfile::TempDir, body: &str) -> PathBuf {
    let path = dir.path().join("google.json");
    if let Err(error) = std::fs::write(&path, body) {
        panic!("写入夹具失败: {error}");
    }
    path
}

fn test_identity() -> std::sync::Arc<crate::client_id::ClientIdentity> {
    let config = crate::client_id::ClientConfig {
        user_agent_template: "antigravity/cli/{version} (aidev_client; os_type=darwin; arch=arm64; cl=989022288; auth_method=consumer)".to_owned(),
        version_url: "http://127.0.0.1:1/releases/latest".to_owned(),
        fallback_version: "1.2.17".to_owned(),
    };
    crate::client_id::ClientIdentity::new(config)
}

async fn make_source(path: &Path, responses: Vec<StubResponse>) -> (GoogleSource, StubServer) {
    let stub = StubServer::start(responses).await;
    let source = GoogleSource::new(
        path,
        "id",
        "secret",
        stub.base_url(),
        format!("{}/token", stub.base_url()),
        test_client(),
        test_identity(),
    );
    (source, stub)
}

const REFRESHED: &str = r#"{"access_token":"refreshed-token","expires_in":3600}"#;

#[tokio::test]
async fn authorize_refreshes_expired_token_and_writes_back() {
    let dir = temp_dir();
    let path = google_file(
        &dir,
        r#"{"credentials":[
            {"name":"a","email":"a@example.com","project_id":"proj","refresh_token":"r",
             "access_token":"stale","expires_at":100,"enabled":true,"priority":0,
             "note":"不要丢","custom_field":"保留我"}]}"#,
    );
    let (source, stub) = make_source(&path, vec![StubResponse::ok(REFRESHED)]).await;
    let headers = match source.authorize(identity("a@example.com"), false).await {
        Ok(headers) => headers,
        Err(error) => panic!("鉴权失败: {error}"),
    };
    assert_eq!(headers[0].1, "Bearer refreshed-token");
    let requests = stub.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/token");
    assert!(requests[0].body.contains("grant_type=refresh_token"));
    assert!(requests[0].body.contains("refresh_token=r"));
    assert!(requests[0].body.contains("client_id=id"));
    assert!(requests[0].body.contains("client_secret=secret"));
    assert!(
        requests[0].header("authorization").is_none(),
        "令牌请求不应带 Authorization"
    );
    assert_eq!(
        requests[0].header("user-agent"),
        Some(GO_HTTP_CLIENT_USER_AGENT),
        "刷新请求缺少 Go 客户端的默认标识"
    );
    let written = read_pool(&path);
    assert_eq!(
        written[0].get("access_token").and_then(Value::as_str),
        Some("refreshed-token")
    );
    assert_eq!(
        written[0].get("note").and_then(Value::as_str),
        Some("不要丢")
    );
    assert_eq!(
        written[0].get("custom_field").and_then(Value::as_str),
        Some("保留我")
    );
    assert_eq!(
        written[0].get("project_id").and_then(Value::as_str),
        Some("proj")
    );
    assert!(written[0].contains_key("updated_at"));
}

#[tokio::test]
async fn authorize_rejects_removed_entry() {
    let dir = temp_dir();
    let path = google_file(
        &dir,
        r#"{"credentials":[
            {"name":"a","email":"a@example.com","refresh_token":"r","access_token":"t","expires_at":1}]}"#,
    );
    let (source, stub) = make_source(&path, Vec::new()).await;
    assert!(
        source
            .authorize(identity("gone@example.com"), true)
            .await
            .is_err()
    );
    assert_eq!(stub.request_count(), 0);
}
