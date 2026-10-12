//! 端点, 鉴权与健康检查状态的端到端测试.
//!
//! healthz_reports_credentials 除了 messages 路由的健康检查, 还覆盖 xai 路由经回环桩上游完成
//! 流式与非流式请求, 访问令牌按 providers.xai 的配置提前刷新, 用量记为 provider xai.
//!
//! End-to-end tests of endpoints, authentication and health status.
//!
//! Besides the health check of a messages route, healthz_reports_credentials also covers the xai
//! route serving streaming and non-streaming requests through a loopback stub upstream, the access
//! token refreshed early with the providers.xai configuration, and usage recorded under provider
//! xai.

mod common;

use std::path::Path;

use common::{
    Aria, json, observed, spawn_fixed_stub, spawn_stub, test_client, text_body, wait_for_records,
    write_clients, write_config, write_credentials,
};
use serde_json::{Value, json};

/// 构造一份包含一条 messages 路由的配置.
///
/// Builds a configuration with one messages route.
fn standard_config(clients: &Path, base_url: &str) -> Value {
    json!({
        "clients": clients.to_str(),
        "routes": [{
            "name": "local",
            "display_name": "Test",
            "aliases": ["vendor-claude/local[1m]", "vendor-claude/local"],
            "model": "upstream-model",
            "upstream": "messages",
            "base_url": base_url,
            "key_env": "UPSTREAM_KEY",
        }],
    })
}

/// 启动一个只返回固定 JSON 的上游.
///
/// Starts an upstream that answers with fixed JSON.
async fn ok_upstream() -> Result<String, Box<dyn std::error::Error>> {
    Ok(
        spawn_fixed_stub(http::StatusCode::OK, r#"{"type":"message"}"#)
            .await?
            .0,
    )
}

/// 探针记录的一条 xai 流式事件序列: 推理条目带 encrypted_content, 文本经 content_part 与
/// output_text 事件给出.
///
/// The xai streaming event sequence recorded by the probe: the reasoning item carries
/// encrypted_content and the text arrives through content_part and output_text events.
const GROK_EVENTS: &str = concat!(
    "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"grok-4.7\",\"status\":\"in_progress\"}}\n\n",
    "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n",
    "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[],\"encrypted_content\":\"enc-1\"}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
    "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n",
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
    "event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"text\":\"Hello\"}\n\n",
    "event: response.content_part.done\ndata: {\"type\":\"response.content_part.done\",\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"Hello\"}}\n\n",
    "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello\"}]}}\n\n",
    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"grok-4.7\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1237,\"input_tokens_details\":{\"cached_tokens\":1152},\"output_tokens\":322,\"output_tokens_details\":{\"reasoning_tokens\":301},\"total_tokens\":1559,\"cost_in_usd_ticks\":26780000}}}\n\n",
);

/// Command Code 线路的流式事件序列: 思考事件为 response.reasoning.delta 与 response.reasoning.done,
/// 推理条目无 encrypted_content.
///
/// The Command Code streaming event sequence: reasoning arrives through response.reasoning.delta
/// and response.reasoning.done, without encrypted_content on the reasoning item.
const COMMANDCODE_EVENTS: &str = concat!(
    "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cc_1\",\"model\":\"zai-org/GLM-5.3\",\"status\":\"in_progress\"}}\n\n",
    "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_cc_1\",\"status\":\"in_progress\"}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_cc_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n",
    "event: response.reasoning.delta\ndata: {\"type\":\"response.reasoning.delta\",\"item_id\":\"rs_cc_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Thinking of GLM\"}\n\n",
    "event: response.reasoning.done\ndata: {\"type\":\"response.reasoning.done\",\"item_id\":\"rs_cc_1\",\"output_index\":0,\"content_index\":0,\"text\":\"Thinking of GLM\"}\n\n",
    "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"rs_cc_1\",\"type\":\"reasoning\",\"summary\":[],\"content\":[{\"type\":\"reasoning_text\",\"text\":\"Thinking of GLM\"}]}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"msg_cc_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
    "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_cc_1\",\"output_index\":1,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n",
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_cc_1\",\"output_index\":1,\"content_index\":0,\"delta\":\"Hello from GLM\"}\n\n",
    "event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_cc_1\",\"output_index\":1,\"content_index\":0,\"text\":\"Hello from GLM\"}\n\n",
    "event: response.content_part.done\ndata: {\"type\":\"response.content_part.done\",\"item_id\":\"msg_cc_1\",\"output_index\":1,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"Hello from GLM\"}}\n\n",
    "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"id\":\"msg_cc_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello from GLM\"}]}}\n\n",
    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_cc_1\",\"model\":\"zai-org/GLM-5.3\",\"status\":\"completed\",\"usage\":{\"input_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":30},\"output_tokens\":60,\"output_tokens_details\":{\"reasoning_tokens\":20},\"total_tokens\":180}}}\n\n",
);

/// 达到输出上限的事件序列: 与 COMMANDCODE_EVENTS 相同, 但以 response.incomplete 结束,
/// incomplete_details.reason 为 length. Command Code Provider API 用 length 表达达到 max_output_tokens,
/// 网关必须按 stop_reason max_tokens 正常结束, 不能当作上游响应不完整.
///
/// The output-limit event sequence: identical to COMMANDCODE_EVENTS but ending with
/// response.incomplete whose incomplete_details.reason is length. The Command Code Provider API
/// uses length for reaching max_output_tokens, and the gateway must end normally with stop_reason
/// max_tokens rather than treat it as a truncated upstream response.
const COMMANDCODE_LENGTH_EVENTS: &str = concat!(
    "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cc_2\",\"model\":\"zai-org/GLM-5.3\",\"status\":\"in_progress\"}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_cc_2\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_cc_2\",\"output_index\":0,\"content_index\":0,\"delta\":\"Truncated\"}\n\n",
    "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_cc_2\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Truncated\"}]}}\n\n",
    "event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_cc_2\",\"model\":\"zai-org/GLM-5.3\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"length\"},\"usage\":{\"input_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":30},\"output_tokens\":60,\"output_tokens_details\":{\"reasoning_tokens\":20},\"total_tokens\":180}}}\n\n",
);

/// 解码 application/x-www-form-urlencoded 请求体. aria 的依赖里没有 url, 而查询串与表单体是同
/// 一种编码, 因此借 reqwest 重新导出的 Url 解析.
///
/// Decodes an application/x-www-form-urlencoded body. aria does not depend on url, and a query
/// string uses the same encoding as a form body, so the Url type re-exported by reqwest parses it.
fn url_form(body: &[u8]) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let text = std::str::from_utf8(body)?;
    let parsed = reqwest::Url::parse(&format!("http://localhost/?{text}"))?;
    Ok(parsed.query_pairs().into_owned().collect())
}

#[tokio::test]
async fn rejects_before_upstream() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (upstream, records) = spawn_fixed_stub(http::StatusCode::OK, "{}").await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    let mut config = standard_config(&clients, &upstream);
    config["max_body_bytes"] = json!(64);
    let config = write_config(directory.path(), &config)?;
    let aria = Aria::start(&config, &[("UPSTREAM_KEY", "upstream-secret")])?;
    let client = test_client()?;

    let oversized = format!(r#"{{"model":"local","pad":"{}"}}"#, "x".repeat(65));
    // 每一行是: 方法, 路径, 密钥, 请求体, 期望状态码.
    //
    // Each row is: method, path, key, body and the expected status code.
    let cases: [(&str, &str, &str, &str, u16); 7] = [
        ("POST", "/v1/messages", "wrong", r#"{"model":"local"}"#, 401),
        (
            "POST",
            "/v1/chat/completions",
            "client-secret",
            r#"{"model":"local"}"#,
            404,
        ),
        (
            "POST",
            "/v1/messages",
            "client-secret",
            r#"{"model":"unknown"}"#,
            400,
        ),
        ("POST", "/v1/messages", "client-secret", "null", 400),
        (
            "POST",
            "/v1/messages",
            "client-secret",
            r#"{"model":"local"} {}"#,
            400,
        ),
        ("POST", "/v1/messages", "client-secret", &oversized, 413),
        ("POST", "/v1/responses", "client-secret", "{}", 404),
    ];
    for (method, path, key, body, status) in cases {
        let response = client
            .request(method.parse()?, format!("{}{path}", aria.base))
            .header("authorization", format!("Bearer {key}"))
            .body(body.to_owned())
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), status, "路径 {path}");
    }
    assert!(observed(&records).is_empty(), "被拒绝的请求到达了上游");
    Ok(())
}

// 用同一事件序列验证三条共用 Responses 转换器的线路, 并按客户端追加规则重建工具参数.
// Exercise all three Responses dialects and rebuild arguments as an append-only SSE client.
fn check_parallel_function_arguments() -> Result<(), Box<dyn std::error::Error>> {
    use lyra::chatgpt::response::ChatGptTranslator;

    for bridge in [
        "aria.chatgpt.reasoning",
        "aria.xai.reasoning",
        "aria.commandcode.reasoning",
    ] {
        for stream in [false, true] {
            for identity in ["both", "id", "index"] {
                for overlap_text in [false, true] {
                    let mut translator = ChatGptTranslator::for_line("test", stream, bridge);
                    let mut events = vec![
                        json!({"type":"response.output_item.added","item":{"type":"message","id":"text"}}),
                        json!({"type":"response.output_text.delta","delta":"prefix"}),
                        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"a","call_id":"call_a","name":"lookup"}}),
                        json!({"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","id":"b","call_id":"call_b","name":"lookup"}}),
                        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"a","delta":"{\"city\":"}),
                        json!({"type":"response.function_call_arguments.delta","output_index":2,"item_id":"b","delta":"{\"city\":\"wrong\"}"}),
                        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"a","delta":"\"北京\"}"}),
                        json!({"type":"response.output_item.done","output_index":2,"item":{"type":"function_call","id":"b","call_id":"call_b","name":"lookup","arguments":"{\"city\":\"上海\"}"}}),
                        json!({"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"a","call_id":"call_a","name":"lookup","arguments":"{\"city\":\"北京\"}"}}),
                        json!({"type":"response.output_text.delta","delta":" suffix"}),
                        json!({"type":"response.output_item.done","item":{"type":"message","id":"text"}}),
                        json!({"type":"response.completed","response":{}}),
                    ];
                    for event in &mut events {
                        if identity == "id" {
                            event
                                .as_object_mut()
                                .ok_or("event object")?
                                .remove("output_index");
                        } else if identity == "index" {
                            event
                                .as_object_mut()
                                .ok_or("event object")?
                                .remove("item_id");
                            if let Some(item) = event.get_mut("item").and_then(Value::as_object_mut)
                            {
                                item.remove("id");
                            }
                        }
                    }
                    let mut wire = Vec::new();
                    for (index, event) in events.iter().enumerate() {
                        if !overlap_text && [0, 1, 9, 10].contains(&index) {
                            continue;
                        }
                        let chunk = translator.event(&serde_json::to_vec(event)?)?;
                        if (2..=6).contains(&index) || (overlap_text && (7..=8).contains(&index)) {
                            assert!(
                                chunk.is_empty(),
                                "unfinished text must keep its block index"
                            );
                        }
                        wire.extend(chunk);
                    }
                    let content = if stream {
                        assert!(translator.end()?.is_empty());
                        let text = String::from_utf8(wire)?;
                        let mut blocks = Vec::new();
                        let mut arguments = String::new();
                        let mut open = false;
                        for data in text.lines().filter_map(|line| line.strip_prefix("data: ")) {
                            let event: Value = serde_json::from_str(data)?;
                            match event["type"].as_str().unwrap_or_default() {
                                "content_block_start" => {
                                    assert!(!open);
                                    assert_eq!(event["index"], blocks.len());
                                    blocks.push(event["content_block"].clone());
                                    arguments.clear();
                                    open = true;
                                }
                                "content_block_delta" => {
                                    assert!(open);
                                    assert_eq!(event["index"], blocks.len() - 1);
                                    if let Some(part) = event["delta"]["partial_json"].as_str() {
                                        arguments.push_str(part);
                                    } else if let Some(part) = event["delta"]["text"].as_str() {
                                        let block = blocks.last_mut().ok_or("text block")?;
                                        block["text"] = json!(format!(
                                            "{}{part}",
                                            block["text"].as_str().unwrap_or_default()
                                        ));
                                    }
                                }
                                "content_block_stop" => {
                                    assert!(open);
                                    assert_eq!(event["index"], blocks.len() - 1);
                                    if !arguments.is_empty() {
                                        blocks.last_mut().ok_or("tool block")?["input"] =
                                            serde_json::from_str(&arguments)?;
                                    }
                                    open = false;
                                }
                                _ => {}
                            }
                        }
                        assert!(!open);
                        Value::Array(blocks)
                    } else {
                        let message: Value = serde_json::from_slice(&translator.end()?)?;
                        assert_eq!(message["stop_reason"], "tool_use");
                        message["content"].clone()
                    };
                    let a = json!({"type":"tool_use","id":"call_a","name":"lookup","input":{"city":"北京"}});
                    let b = json!({"type":"tool_use","id":"call_b","name":"lookup","input":{"city":"上海"}});
                    let expected = if overlap_text {
                        json!([{"type":"text","text":"prefix suffix"}, a, b])
                    } else {
                        json!([b, a])
                    };
                    assert_eq!(content, expected);
                }
            }

            // 单调用旧事件可以不带条目身份, 但增量绝不能代替缺失的完成参数.
            // Legacy single calls may omit item identity, but deltas cannot replace final arguments.
            for final_args in [Value::Null, json!(""), json!("{"), json!("[]")] {
                let mut translator = ChatGptTranslator::for_line("test", stream, bridge);
                translator.event(&serde_json::to_vec(&json!({"type":"response.output_item.added","item":{"type":"function_call","call_id":"c","name":"lookup"}}))?)?;
                assert!(
                    translator
                        .event(&serde_json::to_vec(
                            &json!({"type":"response.function_call_arguments.delta","delta":"{}"})
                        )?)?
                        .is_empty()
                );
                assert!(translator.event(&serde_json::to_vec(&json!({"type":"response.output_item.done","item":{"type":"function_call","arguments":final_args}}))?).is_err());
                assert!(translator.end().is_err());
            }
            for terminal in ["response.completed", "response.incomplete"] {
                let mut translator = ChatGptTranslator::for_line("test", stream, bridge);
                translator.event(&serde_json::to_vec(&json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"pending","call_id":"c","name":"lookup"}}))?)?;
                assert!(translator.event(&serde_json::to_vec(&json!({"type":"response.function_call_arguments.delta","item_id":"wrong","output_index":0,"delta":"{}"}))?).is_err());
                assert!(translator.event(&serde_json::to_vec(&json!({"type":terminal,"response":{"incomplete_details":{"reason":"length"}}}))?).is_err());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn healthz_reports_credentials() -> Result<(), Box<dyn std::error::Error>> {
    check_parallel_function_arguments()?;
    // messages 路由的健康检查: 没有凭据, 冷却与不可用模型/路由时, 各列表为空.
    //
    // The health check of a messages route: with no credential, cooldown or unavailable
    // model/route, every list is empty.
    let directory = tempfile::tempdir()?;
    let upstream = ok_upstream().await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    let config = write_config(directory.path(), &standard_config(&clients, &upstream))?;
    let aria = Aria::start(&config, &[("UPSTREAM_KEY", "upstream-secret")])?;
    let client = test_client()?;

    for path in ["/healthz", "/v1/healthz"] {
        let body: Value = json(
            client
                .get(format!("{}{path}", aria.base))
                .header("x-api-key", "client-secret")
                .send()
                .await?,
        )
        .await?;
        assert_eq!(body["status"], json!("ok"), "路径 {path}");
        assert_eq!(body["credentials"], json!([]), "路径 {path}");
        assert_eq!(body["cooldowns"], json!([]), "路径 {path}");
        assert_eq!(body["unavailable_models"], json!([]), "路径 {path}");
        assert_eq!(body["unavailable_routes"], json!([]), "路径 {path}");
    }

    // xai 路由: 流式与非流式请求经回环桩上游完成, 健康检查报告 client.xai 的版本与来源,
    // 用量记为 provider xai.
    //
    // The xai route: streaming and non-streaming requests complete through the loopback stub
    // upstream, the health check reports the version and source of client.xai, and usage is
    // recorded under provider xai.
    let directory = tempfile::tempdir()?;
    let (token_base, token_records) = spawn_stub(|_, _| async move {
        http::Response::new(text_body(
            r#"{"access_token":"token-new","refresh_token":"refresh-new","token_type":"Bearer","expires_in":21600}"#,
        ))
    })
    .await?;
    let (upstream, records) = spawn_stub(|_, _| async move {
        let mut response = http::Response::new(text_body(GROK_EVENTS));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("text/event-stream"),
        );
        response
    })
    .await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    // 访问令牌只剩十分钟, 在一小时的提前刷新余量之内, 第一次请求先按 providers.xai 的令牌端点
    // 刷新; 刷新后的令牌有六小时, 第二次请求不再刷新.
    //
    // The access token has ten minutes left, inside the one-hour early-refresh margin, so the first
    // request refreshes it at the providers.xai token endpoint; the refreshed token lives six hours
    // and the second request does not refresh again.
    let credentials = write_credentials(
        directory.path(),
        "xai.json",
        &[json!({
            "name": "tester@example.com",
            "account_id": "user-123",
            "plan": "superGrok",
            "access_token": "token-old",
            "refresh_token": "refresh-old",
            "expires_at": chrono::Utc::now().timestamp() + 600,
            "enabled": true,
        })],
    )?;
    let usage = directory.path().join("usage");
    let config = write_config(
        directory.path(),
        &json!({
            "clients": clients.to_str(),
            "usage_dir": usage.to_str(),
            "providers": {"xai": {
                "credentials": credentials.to_str(),
                "base_url": format!("{upstream}/v1"),
                "token_url": format!("{token_base}/oauth2/token"),
                "client_id": "test-client",
                "scope": "openid grok-cli:access",
                // 版本清单指向没有监听的回环端口: 启动时的获取立即失败并沿用后备版本, 测试不访问
                // 任何外部地址.
                //
                // The version manifest points at an unlistened loopback port: the startup fetch
                // fails at once and the fallback version stays, so the test reaches no external
                // address.
                "client": {"version_url": "http://127.0.0.1:1/latest"},
            }},
            "routes": [{
                "name": "grok",
                "model": "grok-4.7",
                "upstream": "xai",
                "reasoning_effort": "medium",
            }],
        }),
    )?;
    let aria = Aria::start(&config, &[])?;
    let client = test_client()?;
    let messages = json!([{"role": "user", "content": "你好"}]);

    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(
            json!({"model": "grok", "stream": true, "max_tokens": 1024, "messages": messages})
                .to_string(),
        )
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    assert!(response.headers().get("x-relay-request-id").is_some());
    let text = response.text().await?;
    let names: Vec<&str> = text
        .lines()
        .filter_map(|line| line.strip_prefix("event: "))
        .collect();
    assert_eq!(
        names,
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ],
        "{text}"
    );

    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(json!({"model": "grok", "max_tokens": 1024, "messages": messages}).to_string())
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let message = json(response).await?;
    assert_eq!(message["type"], json!("message"));
    assert_eq!(message["role"], json!("assistant"));
    assert_eq!(message["model"], json!("grok-4.7"));
    assert_eq!(message["stop_reason"], json!("end_turn"));
    assert_eq!(message["content"][0]["type"], json!("thinking"));
    assert!(message["content"][0]["signature"].is_string(), "{message}");
    assert_eq!(
        message["content"][1],
        json!({"type": "text", "text": "Hello"})
    );
    assert_eq!(
        message["usage"],
        json!({"input_tokens": 85, "output_tokens": 322, "cache_read_input_tokens": 1152})
    );

    // 刷新使用 providers.xai 中配置的 client_id 与 scope, 证明配置一路传到了账号池.
    //
    // The refresh uses the client_id and scope configured in providers.xai, proving the
    // configuration reaches the account pool.
    let refresh = observed(&token_records);
    assert_eq!(refresh.len(), 1);
    let form = url_form(&refresh[0].body)?;
    for pair in [
        ("client_id", "test-client"),
        ("grant_type", "refresh_token"),
        ("refresh_token", "refresh-old"),
        ("scope", "openid grok-cli:access"),
    ] {
        assert!(
            form.contains(&(pair.0.to_owned(), pair.1.to_owned())),
            "{form:?}"
        );
    }

    let calls = observed(&records);
    assert_eq!(calls.len(), 2);
    for call in &calls {
        assert_eq!(call.path, "/v1/responses");
        assert_eq!(call.header("authorization"), "Bearer token-new");
        assert!(call.header("user-agent").starts_with("grok-cli/"));
        assert_eq!(call.header("x-xai-token-auth"), "xai-grok-cli");
        let body = call.json()?;
        assert_eq!(body["model"], json!("grok-4.7"));
        assert_eq!(body["stream"], json!(true));
        assert_eq!(body["reasoning"]["effort"], json!("medium"));
    }

    let health = json(
        client
            .get(format!("{}/healthz", aria.base))
            .header("x-api-key", "client-secret")
            .send()
            .await?,
    )
    .await?;
    assert_eq!(
        health["credentials"][0]["name"],
        json!("tester@example.com")
    );
    assert_eq!(health["credentials"][0]["plan"], json!("superGrok"));
    assert_eq!(
        health["client"]["xai"],
        json!({"version": "1.0.50", "source": "fallback"})
    );

    let usage_records = wait_for_records(&usage, 2).await;
    assert_eq!(usage_records.len(), 2, "{usage_records:?}");
    for record in &usage_records {
        assert_eq!(record["provider"], json!("xai"));
        assert_eq!(record["account"], json!("tester@example.com"));
        assert_eq!(record["model"], json!("grok-4.7"));
        assert_eq!(record["outcome"], json!("completed"));
        assert_eq!(record["http_status"], json!(200));
        assert_eq!(
            record["usage"],
            json!({
                "input_tokens": 1237,
                "output_tokens": 322,
                "cache_read_input_tokens": 1152,
                "cache_write_input_tokens": 0,
                "reasoning_output_tokens": 301,
                "total_tokens": 1559,
            })
        );
    }

    // commandcode 线路: 单一固定密钥, 流式与非流式经回环桩上游完成, 思考来自 response.reasoning.delta,
    // 403 错误直接返回客户端且不进入冷却.
    //
    // The commandcode route: a single static key, streaming and non-streaming through loopback stub,
    // reasoning from response.reasoning.delta, and 403 returned without entering cooldown.
    let directory = tempfile::tempdir()?;
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_clone = count.clone();
    let (upstream, records) = spawn_stub(move |_, _| {
        let current = count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async move {
            if current == 3 {
                let mut response = http::Response::new(text_body(
                    r#"{"error":{"message":"MODEL_NOT_IN_PLAN: Model not included in plan","type":"forbidden","code":"MODEL_NOT_IN_PLAN"}}"#,
                ));
                *response.status_mut() = http::StatusCode::FORBIDDEN;
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/json"),
                );
                response
            } else {
                let events = if current == 2 {
                    COMMANDCODE_LENGTH_EVENTS
                } else {
                    COMMANDCODE_EVENTS
                };
                let mut response = http::Response::new(text_body(events));
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("text/event-stream"),
                );
                response
            }
        }
    })
    .await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    let usage = directory.path().join("usage");
    let config = write_config(
        directory.path(),
        &json!({
            "clients": clients.to_str(),
            "usage_dir": usage.to_str(),
            "providers": {"commandcode": {
                "base_url": upstream,
                "key_env": "COMMAND_CODE_API_KEY",
            }},
            "routes": [{
                "name": "glm",
                "model": "zai-org/GLM-5.3",
                "upstream": "commandcode",
                "reasoning_effort": "high",
            }],
        }),
    )?;
    let aria = Aria::start(&config, &[("COMMAND_CODE_API_KEY", "cc-secret-key")])?;
    let client = test_client()?;
    let messages = json!([{"role": "user", "content": "你好"}]);

    // 1. 流式请求
    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(
            json!({"model": "glm", "stream": true, "max_tokens": 1024, "messages": messages})
                .to_string(),
        )
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let text = response.text().await?;
    let names: Vec<&str> = text
        .lines()
        .filter_map(|line| line.strip_prefix("event: "))
        .collect();
    assert_eq!(
        names,
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ],
        "{text}"
    );
    assert!(text.contains("Thinking of GLM"), "{text}");
    assert!(text.contains("Hello from GLM"), "{text}");

    let calls = observed(&records);
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].path, "/responses");
    assert_eq!(calls[0].header("authorization"), "Bearer cc-secret-key");
    assert_eq!(calls[0].header("user-agent"), "aria");
    assert_eq!(calls[0].header("content-type"), "application/json");
    assert_eq!(calls[0].header("accept"), "text/event-stream");
    let body = calls[0].json()?;
    assert_eq!(body["model"], json!("zai-org/GLM-5.3"));
    assert_eq!(body["stream"], json!(true));
    assert_eq!(body["max_output_tokens"], json!(1024));

    // 2. 非流式请求
    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(json!({"model": "glm", "max_tokens": 1024, "messages": messages}).to_string())
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let message = json(response).await?;
    assert_eq!(message["type"], json!("message"));
    assert_eq!(message["role"], json!("assistant"));
    assert_eq!(message["model"], json!("zai-org/GLM-5.3"));
    assert_eq!(message["stop_reason"], json!("end_turn"));
    assert_eq!(message["content"][0]["type"], json!("thinking"));
    assert_eq!(message["content"][0]["thinking"], json!("Thinking of GLM"));
    assert!(message["content"][0].get("signature").is_none());
    assert_eq!(
        message["content"][1],
        json!({"type": "text", "text": "Hello from GLM"})
    );
    assert_eq!(
        message["usage"],
        json!({"input_tokens": 90, "output_tokens": 60, "cache_read_input_tokens": 30})
    );

    // 3. 非流式请求达到输出上限: incomplete_details.reason 为 length 时转换为 stop_reason max_tokens
    //    并返回 200, 不能当作上游响应不完整返回 502.
    //
    // Non-streaming request hitting the output limit: incomplete_details.reason length converts to
    // stop_reason max_tokens with a 200 rather than a 502 for a truncated upstream response.
    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(json!({"model": "glm", "max_tokens": 20, "messages": messages}).to_string())
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let message = json(response).await?;
    assert_eq!(message["stop_reason"], json!("max_tokens"));
    assert_eq!(
        message["content"][0],
        json!({"type": "text", "text": "Truncated"})
    );
    assert_eq!(
        message["usage"],
        json!({"input_tokens": 90, "output_tokens": 60, "cache_read_input_tokens": 30})
    );

    // 4. 健康检查报告单个凭据条目且不暴露密钥
    let health = json(
        client
            .get(format!("{}/healthz", aria.base))
            .header("x-api-key", "client-secret")
            .send()
            .await?,
    )
    .await?;
    assert_eq!(health["credentials"][0]["name"], json!("commandcode"));
    let health_text = serde_json::to_string(&health)?;
    assert!(!health_text.contains("cc-secret-key"));

    // 5. 用量记录检查. 前两条为正常完成, 第三条以 response.incomplete 结束, 用量采集按事件类型
    //    记为 incomplete, 与 Codex 达到上限时一致; 三条的 HTTP 状态都是 200, 说明 length 没有
    //    被当作上游错误.
    //
    // Usage records. The first two completed normally; the third ended with response.incomplete,
    // which the usage capture marks incomplete by event type, matching Codex output-limit
    // requests. All three carry HTTP 200, showing length is not treated as an upstream error.
    let usage_records = wait_for_records(&usage, 3).await;
    assert_eq!(usage_records.len(), 3, "{usage_records:?}");
    for record in &usage_records {
        assert_eq!(record["provider"], json!("commandcode"));
        assert_eq!(record["account"], json!("commandcode"));
        assert_eq!(record["model"], json!("zai-org/GLM-5.3"));
        assert_eq!(record["http_status"], json!(200));
    }
    let outcomes: Vec<&str> = usage_records
        .iter()
        .map(|record| record["outcome"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(outcomes, ["completed", "completed", "incomplete"]);

    // 6. 桩上游返回 403 MODEL_NOT_IN_PLAN
    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(json!({"model": "glm", "max_tokens": 1024, "messages": messages}).to_string())
        .send()
        .await?;
    assert_eq!(response.status(), 403);
    let error_text = response.text().await?;
    assert!(error_text.contains("MODEL_NOT_IN_PLAN"), "{error_text}");

    // 7. 紧接着的下一个请求仍然到达桩上游证明未进入冷却
    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(json!({"model": "glm", "max_tokens": 1024, "messages": messages}).to_string())
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        observed(&records).len(),
        5,
        "第五个请求必须到达桩上游证明未进入冷却"
    );

    Ok(())
}

#[tokio::test]
async fn quota_403_marks_the_route_unavailable_and_healthz_reports_it()
-> Result<(), Box<dyn std::error::Error>> {
    // 上游对配额耗尽的 403: 第一次请求原样透传, /healthz 列出该路由的原因与剩余时间,
    // 窗口内的下一次请求直接返回 503, 不再转发上游, 见 architecture.md 5.13.
    //
    // The upstream answers 403 for an exhausted quota: the first request passes through, /healthz
    // lists the route with its reason and remaining time, and the next request inside the window
    // returns 503 directly without reaching upstream, see section 5.13 of architecture.md.
    let usage_limit = r#"{"error":{"message":"You've reached your 5-hour usage limit."}}"#;
    let directory = tempfile::tempdir()?;
    let (upstream, records) = spawn_fixed_stub(http::StatusCode::FORBIDDEN, usage_limit).await?;
    let clients = write_clients(directory.path(), "client-secret")?;
    let config = write_config(directory.path(), &standard_config(&clients, &upstream))?;
    let aria = Aria::start(&config, &[("UPSTREAM_KEY", "upstream-secret")])?;
    let client = test_client()?;
    let body = r#"{"model":"local","max_tokens":16,"messages":[]}"#;

    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?;
    assert_eq!(response.status(), 403);
    assert_eq!(response.text().await?, usage_limit);
    assert_eq!(observed(&records).len(), 1);

    let status: Value = json(
        client
            .get(format!("{}/healthz", aria.base))
            .header("x-api-key", "client-secret")
            .send()
            .await?,
    )
    .await?;
    let routes = status["unavailable_routes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(routes.len(), 1, "{status}");
    assert_eq!(routes[0]["route"], json!("local"));
    assert_eq!(routes[0]["reason"], json!("quota_exhausted"));
    assert!(
        routes[0]["remaining_seconds"].as_u64().unwrap_or_default() <= 60,
        "{status}"
    );

    let response = client
        .post(format!("{}/v1/messages", aria.base))
        .header("x-api-key", "client-secret")
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?;
    assert_eq!(response.status(), 503);
    let retry_after: u64 = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_default();
    assert!(
        (59..=60).contains(&retry_after),
        "Retry-After={retry_after}"
    );
    let text = response.text().await?;
    assert!(text.contains("配额已耗尽"), "{text}");
    assert_eq!(observed(&records).len(), 1, "窗口内不应转发上游");
    Ok(())
}
