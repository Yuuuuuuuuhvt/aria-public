//! messages 路由的端到端测试: 流式转发, 模型改写契约与搜索编排.
//!
//! 本文件覆盖三条路径: 普通 messages 路由的流式转发与模型改写; 启用 moonshot_search 的
//! messages 路由在客户端强制工具选择或调整思考档位时的内部请求改写, 以及上游错误原因的透出;
//! qoder 路由带 web_search 工具时完成的完整搜索编排与用量记录.
//!
//! End-to-end tests of the messages route: streaming forwarding, the model rewriting contract and
//! the search orchestration.
//!
//! This file covers three paths: streaming forwarding and model rewriting on an ordinary messages
//! route; the internal request rewriting when a moonshot_search messages route sees a forced tool
//! choice or an adjusted thinking level, together with the surfacing of the upstream error reason;
//! and the full search orchestration with its usage record when a qoder route carries the
//! web_search tool.

mod common;

use std::path::Path;

use common::{
    Aria, Records, json, observed, spawn_stub, test_client, text_body, wait_for_records,
    write_clients, write_config,
};
use serde_json::{Value, json};

/// 构造一份配置, routes 由调用方给出.
///
/// Builds a configuration whose routes come from the caller.
fn config_with(clients: &Path, routes: Value, extra: Value) -> Value {
    let mut config = json!({"clients": clients.to_str(), "routes": routes});
    if let (Some(base), Some(extra)) = (config.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            base.insert(key.clone(), value.clone());
        }
    }
    config
}

/// 构造一条单上游的 messages 路由.
///
/// Builds one single-upstream messages route.
fn messages_route(name: &str, base_url: &str) -> Value {
    json!({
        "name": name,
        "model": "upstream-model",
        "upstream": "messages",
        "base_url": base_url,
        "key_env": "UPSTREAM_KEY",
    })
}

/// 客户端请求体: 含大整数, 不透明签名与未知字段, 用于验证原样转发.
///
/// The client body: it carries a large integer, an opaque signature and unknown fields, used
/// to verify unchanged forwarding.
const CLIENT_BODY: &str = r#"{"model":"local","future":12345678901234567890,"safeguards":[{"type":"future","signature":"opaque+/=="}],"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#;

/// 构造一条启用搜索编排的单上游 messages 路由.
///
/// Builds one single-upstream messages route with the search orchestration enabled.
fn search_route(name: &str, base_url: &str) -> Value {
    json!({
        "name": name,
        "model": "upstream-model",
        "upstream": "messages",
        "base_url": base_url,
        "key_env": "UPSTREAM_KEY",
        "moonshot_search": true,
    })
}

/// 构造一份声明搜索服务的配置, 服务地址指向给定的桩服务.
///
/// Builds a configuration declaring the search service and pointing it at the given stub.
fn search_extra(url: &str) -> Value {
    json!({"moonshot_search": {"url": url, "key_env": "SEARCH_KEY"}})
}

/// 构造一次独立网页搜索请求体, tool_choice 由调用方给出.
///
/// Builds one standalone web search request body whose tool_choice comes from the caller.
fn search_body(tool_choice: &str) -> String {
    format!(
        r#"{{"model":"local","max_tokens":128,"messages":[{{"role":"user","content":"find something"}}],"tools":[{{"type":"web_search_20250305","name":"web_search"}}],"tool_choice":{tool_choice}}}"#
    )
}

/// 构造一次带 thinking 字段的独立网页搜索请求体.
///
/// Builds one standalone web search request body carrying a thinking field.
fn search_body_with_thinking(tool_choice: &str, thinking: &str) -> String {
    format!(
        r#"{{"model":"local","max_tokens":128,"messages":[{{"role":"user","content":"find something"}}],"tools":[{{"type":"web_search_20250305","name":"web_search"}}],"tool_choice":{tool_choice},"thinking":{thinking}}}"#
    )
}

/// 桩上游返回的一条终止模型消息, stop_reason 为 end_turn, 即模型没有请求工具调用.
///
/// The terminal model message returned by a stub upstream, with stop_reason end_turn, so the
/// model requested no tool call.
const SEARCH_REPLY: &str = r#"{"type":"message","role":"assistant","content":[{"type":"text","text":"answer"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}"#;

/// 启动一个对每个模型轮次都返回同一终止消息的桩上游.
///
/// Starts a stub upstream that answers every model round with the same terminal message.
async fn search_reply_stub() -> Result<(String, Records), Box<dyn std::error::Error>> {
    spawn_stub(|_, _| async move {
        let mut response = http::Response::new(text_body(SEARCH_REPLY));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        response
    })
    .await
}

/// 构造一条 qoder 网关的 SSE 事件行.
///
/// 事件外层是网关的状态包裹, 内层 body 的 choices delta 携带增量; 凭据是会话内嵌的,
/// 因此桩网关不校验签名与加解密.
///
/// Builds one SSE event line of the Qoder gateway.
///
/// The event is the gateway's status wrapper and the inner body's choices delta carries the
/// increment; the credential is in-band, so the stub gateway neither verifies signatures nor
/// performs encryption.
fn qoder_event(body: &str) -> String {
    format!("data: {{\"body\":{body},\"statusCodeValue\":200}}\n")
}

/// 第一轮业务事件: 模型以内部搜索函数发起工具调用.
///
/// The first business round's events: the model issues a tool call through the internal
/// search function.
fn qoder_tool_round() -> String {
    qoder_event(
        r#"{"choices":[{"delta":{"tool_calls":[{"id":"call_1","function":{"name":"self_infra_search_web","arguments":"{\"query\":\"q\"}"}}]}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#,
    )
}

/// 第二轮业务事件: 模型依据检索结果给出正文.
///
/// The second business round's events: the model answers from the search results.
fn qoder_answer_round() -> String {
    qoder_event(
        r#"{"choices":[{"delta":{"content":"done"}}],"usage":{"prompt_tokens":5,"completion_tokens":2}}"#,
    )
}

/// 起一个桩 Qoder 网关: jobToken 交换返回最小令牌, 模型目录接口返回路由模型的显示名映射,
/// 其余业务请求按业务轮次返回 SSE 事件.
///
/// Starts a stub Qoder gateway: the jobToken exchange returns a minimal token, the model list
/// endpoint answers the display name mapping of the route model, and every other request is a
/// business round answering with SSE events.
async fn spawn_gateway() -> Result<(String, Records), Box<dyn std::error::Error>> {
    // 目录请求与认证交换不计入业务轮次, 业务轮次单独计数: 第一轮是工具轮, 之后是作答轮.
    //
    // The catalog fetch and the auth exchange do not count as business rounds; business rounds
    // are counted separately: the first one is the tool round and every later one is an answer.
    let business_round = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    spawn_stub(move |_round, record| {
        let business_round = business_round.clone();
        async move {
            if record.path.contains("/user/jobToken") {
                return http::Response::new(text_body(
                    r#"{"name":"tester","id":"u-1","userType":"personal_standard","securityOauthToken":"sec","refreshToken":"ref","expireTime":4102444800000}"#,
                ));
            }
            if record.path.contains("/model/list") {
                return http::Response::new(text_body(
                    r#"{"chat":[{"key":"qmodel_38max","display_name":"qwen3-max","enable":true}]}"#,
                ));
            }
            let index = business_round.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body = if index == 0 {
                qoder_tool_round()
            } else {
                qoder_answer_round()
            };
            let mut response = http::Response::new(text_body(&body));
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("text/event-stream"),
            );
            response
        }
    })
    .await
}

/// 起一个桩搜索服务, 对每次检索返回同一条结果.
///
/// Starts a stub search service that answers every lookup with one fixed result.
async fn spawn_search_stub() -> Result<(String, Records), Box<dyn std::error::Error>> {
    spawn_stub(|_, _| async move {
        http::Response::new(text_body(
            r#"{"search_results":[{"title":"T","url":"https://example.com/a","snippet":"S"}]}"#,
        ))
    })
    .await
}

/// 配置一条 qoder 路由与桩服务, 返回已经写好的配置文件路径.
///
/// Configures one qoder route with the stub services and returns the written configuration
/// path.
fn qoder_config(
    directory: &Path,
    upstream: &str,
    search: &str,
    usage: &Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let clients = write_clients(directory, "client-secret")?;
    write_config(
        directory,
        &json!({
            "clients": clients.to_str(),
            "usage_dir": usage.to_str(),
            "moonshot_search": {"url": search, "key_env": "SEARCH_KEY"},
            "providers": {"qoder": {
                "base_url": upstream,
                "token_env": "QODER_CN_PAT",
                "machine_file": directory.join("qoder_machine.json").to_str(),
                "max_concurrency": 2,
            }},
            "routes": [{
                "name": "qoder-route",
                "model": "qwen3-max",
                "upstream": "qoder",
                "moonshot_search": true,
            }],
        }),
    )
}

#[tokio::test]
async fn forwards_with_the_canto_contract() -> Result<(), Box<dyn std::error::Error>> {
    // 场景一: 普通 messages 路由的流式转发与模型改写契约.
    //
    // Case one: streaming forwarding and the model rewriting contract on an ordinary messages
    // route.
    let directory = tempfile::tempdir()?;
    let response_text = "event: message_start\ndata: {\"type\":\"message_start\",\"unknown\":12345678901234567890}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"interrupted\"}}\n\n";
    let (upstream, records) = spawn_stub(move |_, _| async move {
        let mut response = http::Response::new(text_body(response_text));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("text/event-stream"),
        );
        response.headers_mut().insert(
            http::HeaderName::from_static("request-id"),
            http::HeaderValue::from_static("upstream-request"),
        );
        response.headers_mut().insert(
            http::header::SET_COOKIE,
            http::HeaderValue::from_static("session=upstream"),
        );
        response
    })
    .await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    let config = write_config(
        directory.path(),
        &config_with(
            &clients,
            json!([messages_route("local", &format!("{upstream}/prefix"))]),
            json!({}),
        ),
    )?;
    let aria = Aria::start(
        &config,
        &[
            ("UPSTREAM_KEY", "upstream-secret"),
            ("SEARCH_KEY", "search-secret"),
        ],
    )?;
    let client = test_client()?;

    let response = client
        .post(format!("{}/v1/messages?beta=true", aria.base))
        .header("authorization", "Bearer client-secret")
        .header("x-api-key", "client-secret")
        .header("anthropic-beta", "future-beta,server-tool-beta")
        .header("anthropic-version", "2023-06-01")
        .header("cookie", "private=value")
        .header("forwarded", "for=forged")
        .header("x-forwarded-for", "forged")
        .header("connection", "x-hop")
        .header("x-hop", "do-not-forward")
        .header("content-type", "application/json")
        .body(CLIENT_BODY)
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("request-id")
            .and_then(|value| value.to_str().ok()),
        Some("upstream-request"),
        "上游响应头应当原样转发"
    );
    assert!(
        response.headers().get("set-cookie").is_none(),
        "上游 Set-Cookie 不得转发"
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    assert_eq!(response.text().await?, response_text);

    let calls = observed(&records);
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.method, "POST");
    assert_eq!(call.path, "/prefix/v1/messages");
    assert_eq!(call.query.as_deref(), Some("beta=true"));
    assert_eq!(call.header("authorization"), "Bearer upstream-secret");
    assert_eq!(call.header("x-api-key"), "upstream-secret");
    assert_eq!(call.header("content-type"), "application/json");
    assert_eq!(
        call.header("anthropic-beta"),
        "future-beta,server-tool-beta"
    );
    assert_eq!(call.header("anthropic-version"), "2023-06-01");
    for name in [
        "cookie",
        "forwarded",
        "x-forwarded-for",
        "x-hop",
        "accept-encoding",
    ] {
        assert_eq!(call.header(name), "", "请求头 {name} 不得转发");
    }
    let fields = call.json()?;
    assert_eq!(fields["model"], json!("upstream-model"));
    // 除模型名之外的取值按原始 JSON 保留: 大整数不变成浮点数, 签名不被改写.
    //
    // Every value except the model name keeps its raw JSON: the large integer does not
    // become a float and the signature is not rewritten.
    assert_eq!(fields["future"], json!(12345678901234567890u64));
    assert_eq!(fields["safeguards"][0]["signature"], json!("opaque+/=="));
    assert!(
        call.body
            .windows(20)
            .any(|window| window == b"12345678901234567890")
    );

    // 场景二: 启用搜索编排的路由改写第一轮内部请求的工具选择与思考档位.
    //
    // 客户端强制选择 web_search 并带 disable_parallel_tool_use 时, 部分模型拒绝 tool 与 any,
    // 强制选择会让搜索请求必然失败, 因此内部请求只保留 auto 与其余字段; 名字被去掉, 因为内部
    // 工具名与公开工具名不同. 客户端关闭思考时内部请求省略 thinking, 因为部分模型不允许关闭推理;
    // 启用思考时 thinking 必须原样保留, 否则误删会改变模型的思考档位.
    //
    // Case two: a route with the search orchestration enabled rewrites the first internal request's
    // tool choice and thinking level.
    //
    // When the client forces web_search with disable_parallel_tool_use, some models reject tool and
    // any, so a forced choice would make every search request fail; the internal request keeps auto
    // and the remaining fields, and the name is dropped because the internal tool name differs from
    // the public one. When the client disables thinking the internal request omits it, because some
    // models forbid disabling reasoning; when thinking is enabled it must stay unchanged, or an
    // accidental drop would change the model's thinking level.
    let directory = tempfile::tempdir()?;
    let (upstream, records) = search_reply_stub().await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    let config = write_config(
        directory.path(),
        &config_with(
            &clients,
            json!([search_route("local", &upstream)]),
            search_extra(&upstream),
        ),
    )?;
    let aria = Aria::start(
        &config,
        &[
            ("UPSTREAM_KEY", "upstream-secret"),
            ("SEARCH_KEY", "search-secret"),
        ],
    )?;
    let client = test_client()?;

    // 关闭思考且强制选择 web_search: 第一轮内部请求 tool_choice 为 auto, 没有 name, 保留
    // disable_parallel_tool_use, 且不含 thinking.
    //
    // Thinking disabled with a forced web_search choice: the first internal request carries auto
    // tool_choice without a name, keeps disable_parallel_tool_use and has no thinking field.
    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("authorization", "Bearer client-secret")
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(search_body_with_thinking(
            r#"{"type":"tool","name":"web_search","disable_parallel_tool_use":true}"#,
            r#"{"type":"disabled"}"#,
        ))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    // 模型第一轮直接作答时, 编排按 finish 的规则原样返回该回答, 不再发起检索.
    //
    // When the model answers on the first round, the orchestration returns that answer by the
    // finish rule and performs no lookup.
    let body = json(response).await?;
    assert_eq!(body["content"][0]["text"], json!("answer"));
    assert_eq!(body["stop_reason"], json!("end_turn"));

    let calls = observed(&records);
    assert_eq!(calls.len(), 1);
    let fields = calls[0].json()?;
    assert_eq!(fields["tool_choice"]["type"], json!("auto"));
    assert_eq!(
        fields["tool_choice"]["disable_parallel_tool_use"],
        json!(true)
    );
    assert!(fields["tool_choice"].get("name").is_none());
    assert!(
        fields.get("thinking").is_none(),
        "关闭思考的内部请求不得携带 thinking"
    );

    // 启用思考: thinking 原样保留, 防止误删.
    //
    // Thinking enabled: thinking is kept unchanged, guarding against an accidental drop.
    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("authorization", "Bearer client-secret")
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(search_body_with_thinking(
            r#"{"type":"auto"}"#,
            r#"{"type":"enabled","budget_tokens":64}"#,
        ))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let calls = observed(&records);
    assert_eq!(calls.len(), 2);
    let fields = calls[1].json()?;
    assert_eq!(fields["thinking"]["type"], json!("enabled"));
    // budget_tokens 为 64, 小于本轮 max_tokens 128, 因此 cap_thinking_budget 不改写, 原样保留.
    //
    // budget_tokens is 64, below this round's max_tokens 128, so cap_thinking_budget does not
    // rewrite it and the value stays as requested.
    assert_eq!(fields["thinking"]["budget_tokens"], json!(64));

    // 场景三: 上游错误正文超过 1 KB 且 error.message 之后带长 metadata 时, 客户端仍收到
    // error.message.
    //
    // 真实上游的错误正文可能超过 1 KB, error.message 之后还带 metadata; 若轮次按原因长度 300
    // 字节截断正文, JSON 会解析失败, 客户端只能看到正文原文. 本场景断言原因取自 error.message,
    // 且被截断掉的长 metadata 不出现在错误消息里.
    //
    // Case three: when the upstream error body exceeds 1 KB and carries long metadata after
    // error.message, the client still receives error.message.
    //
    // A real upstream error body can exceed 1 KB with metadata after error.message; truncating the
    // body to the 300-byte reason length would break JSON parsing, leaving the client with raw body
    // text. This case asserts the reason comes from error.message and the dropped metadata does not
    // appear in the error message.
    let directory = tempfile::tempdir()?;
    let metadata = "M".repeat(2_000);
    let upstream_body = format!(
        r#"{{"type":"error","error":{{"type":"invalid_request_error","message":"model rejects forced tool choice"}},"metadata":{{"padding":"{metadata}"}}}}"#
    );
    assert!(upstream_body.len() > 1024);
    let (upstream, _) = spawn_stub(move |_, _| {
        let upstream_body = upstream_body.clone();
        async move {
            let mut response = http::Response::new(text_body(&upstream_body));
            *response.status_mut() = http::StatusCode::BAD_REQUEST;
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
            response
        }
    })
    .await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    let config = write_config(
        directory.path(),
        &config_with(
            &clients,
            json!([search_route("local", &upstream)]),
            search_extra(&upstream),
        ),
    )?;
    let aria = Aria::start(
        &config,
        &[
            ("UPSTREAM_KEY", "upstream-secret"),
            ("SEARCH_KEY", "search-secret"),
        ],
    )?;
    let client = test_client()?;

    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("authorization", "Bearer client-secret")
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(search_body(r#"{"type":"auto"}"#))
        .send()
        .await?;
    assert_eq!(response.status(), 400);
    let body = json(response).await?;
    let message = body["error"]["message"].as_str().ok_or("错误消息缺失")?;
    assert!(
        message.contains("model rejects forced tool choice"),
        "错误消息应包含 error.message 的文本, 实际为 {message}"
    );
    assert!(
        !message.contains("metadata"),
        "错误消息不应包含长 metadata 字样, 实际为 {message}"
    );

    // 场景四: 带 web_search 工具的 qoder 请求完成搜索编排.
    //
    // Case four: a qoder request carrying the web_search tool completes the search orchestration.
    let directory = tempfile::tempdir()?;
    let (upstream, gateway_records) = spawn_gateway().await?;
    let (search, search_records) = spawn_search_stub().await?;
    let usage = directory.path().join("usage");
    let config = qoder_config(directory.path(), &upstream, &search, &usage)?;
    let aria = Aria::start(
        &config,
        &[("QODER_CN_PAT", "pat"), ("SEARCH_KEY", "search-key")],
    )?;
    let client = test_client()?;
    let body = json!({
        "model": "qoder-route",
        "max_tokens": 1024,
        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        "messages": [{"role": "user", "content": "hi"}],
    });

    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    let message = json(response).await?;
    assert_eq!(message["type"], json!("message"));
    assert_eq!(message["role"], json!("assistant"));
    assert_eq!(message["stop_reason"], json!("end_turn"));
    let blocks = message["content"].as_array().cloned().unwrap_or_default();
    let kinds: Vec<&str> = blocks
        .iter()
        .filter_map(|block| block["type"].as_str())
        .collect();
    assert!(kinds.contains(&"server_tool_use"), "{message}");
    assert!(kinds.contains(&"web_search_tool_result"), "{message}");
    assert_eq!(blocks[blocks.len() - 1]["text"], json!("done"));

    // 除 jobToken 交换与模型目录拉取外, 两轮业务模型调用到达网关, 一次检索到达搜索服务;
    // 每一轮业务请求的 Accept 都是 text/event-stream, 因为真实网关对 Accept:
    // application/json 返回 500.
    //
    // Apart from the jobToken exchange and the model catalog fetch, two business model rounds
    // reach the gateway and one lookup reaches the search service; every business request
    // carries Accept: text/event-stream, because the real gateway answers Accept:
    // application/json with 500.
    let gateway_log = observed(&gateway_records);
    let catalog_fetches = gateway_log
        .iter()
        .filter(|record| record.path.contains("/model/list"))
        .count();
    assert_eq!(catalog_fetches, 1, "模型目录应只在首轮拉取一次");
    let business: Vec<_> = gateway_log
        .into_iter()
        .filter(|record| !record.path.contains("/user/jobToken"))
        .filter(|record| !record.path.contains("/model/list"))
        .collect();
    assert_eq!(business.len(), 2);
    // 路由配置写的是显示名 qwen3-max, 到达网关的请求体必须携带翻译后的内部键.
    let first_body = lyra::qoder::codec::decode(
        std::str::from_utf8(&business[0].body).map_err(|err| err.to_string())?,
    )
    .map_err(|err| format!("解码业务请求体失败: {err}"))?;
    let first_json: serde_json::Value = serde_json::from_slice(&first_body)?;
    assert_eq!(first_json["model_config"]["key"], json!("qmodel_38max"));
    assert_eq!(observed(&search_records).len(), 1);
    for record in &business {
        assert_eq!(
            record
                .headers
                .get("accept")
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream")
        );
    }

    // 用量记录 provider 为 messages, account 为路由名.
    //
    // The usage record carries provider messages and the route name as account.
    let usage_records = wait_for_records(&usage, 1).await;
    assert_eq!(usage_records.len(), 1, "{usage_records:?}");
    assert_eq!(usage_records[0]["provider"], json!("messages"));
    assert_eq!(usage_records[0]["account"], json!("qoder-route"));
    assert_eq!(usage_records[0]["outcome"], json!("completed"));
    Ok(())
}
