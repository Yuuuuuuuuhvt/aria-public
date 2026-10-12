//! 为订阅账号线路的路由实现 canto::search::SearchTurn.
//!
//! Moonshot 搜索编排需要多次以非流式方式调用原模型, 而 antigravity, chatgpt 与 xai 路由的每
//! 一次调用都必须经过 lyra 的账号选择与协议转换. 因此这里把 lyra 的缓冲入口包装成一轮搜索,
//! 编排本身仍然由 canto::search 负责, 各类路由共用同一套轮数与校验规则.
//!
//! Implements canto::search::SearchTurn for routes on subscription-backed lines.
//!
//! The Moonshot orchestration calls the original model several times in non-streaming form,
//! and every call on an antigravity, chatgpt or xai route must go through lyra's account
//! selection and protocol translation. This module therefore wraps lyra's buffered entry points
//! as one round, while canto::search keeps owning the orchestration, so every route type shares
//! one set of round counts and validation rules.

use bytes::Bytes;
use canto::search::{SearchTurn, TurnFuture, TurnResponse};
use http::HeaderMap;
use http_body_util::BodyExt;
use lyra::qoder::auth::GatewayAuth;
use lyra::relay::Relay;

/// 一轮经过 lyra 的 antigravity 调用.
///
/// 状态码不是 200 时编排依据 TurnResponse 生成客户端可见的失败, 因此这里不再读取响应体,
/// 与 canto::search 中 messages 路由的轮次保持一致.
///
/// One antigravity round going through lyra.
///
/// A non-200 status becomes a client-visible failure decided by the orchestration from the
/// TurnResponse, so the body is not read here, matching the messages-route round in
/// canto::search.
pub struct AntigravityTurn<'a> {
    relay: &'a Relay,
}

impl<'a> AntigravityTurn<'a> {
    /// 构造一轮调用, 借用共享的 relay.
    ///
    /// Builds one round borrowing the shared relay.
    pub fn new(relay: &'a Relay) -> AntigravityTurn<'a> {
        AntigravityTurn { relay }
    }
}

impl SearchTurn for AntigravityTurn<'_> {
    fn turn<'a>(&'a self, headers: &'a HeaderMap, body: Bytes) -> TurnFuture<'a> {
        Box::pin(async move {
            let buffered = self.relay.messages_buffered(headers, body).await;
            Ok(TurnResponse {
                status: buffered.status,
                retry_after: buffered.retry_after,
                body: buffered.body,
            })
        })
    }
}

/// 把 Anthropic Messages 转换为 Responses 协议的两条订阅线路.
///
/// chatgpt 与 xai 两条线路在 aria 一侧的处理完全相同: 声明搜索的路由走 Kimi 编排, 未声明搜索的
/// 路由拒绝独立网页搜索, 其余请求交给 lyra 的转换入口. 区别只在调用 lyra 的哪一组入口与用量记录
/// 的 provider, 因此分派与搜索轮次各自只有一份实现, 由这个值选择线路.
///
/// The two subscription lines that convert Anthropic Messages into the Responses protocol.
///
/// Aria handles the chatgpt and xai lines identically: a route declaring search goes to the Kimi
/// orchestration, a route without it rejects the standalone web search, and every other request
/// goes to lyra's conversion entry points. The lines differ only in which pair of lyra entry
/// points is called and in the provider of the usage record, so dispatch and the search rounds
/// each have one implementation and this value selects the line.
#[derive(Clone, Copy)]
pub enum ResponsesLine {
    ChatGpt,
    Xai,
    CommandCode,
}

impl ResponsesLine {
    /// 用量记录的 provider 字段, 见 architecture.md 5.8 第 3 条.
    ///
    /// The provider field of the usage record, see section 5.8 item 3 of architecture.md.
    pub fn provider(self) -> &'static str {
        match self {
            ResponsesLine::ChatGpt => "chatgpt",
            ResponsesLine::Xai => "xai",
            ResponsesLine::CommandCode => "commandcode",
        }
    }
}

/// 一轮经过 lyra 的 chatgpt 或 xai 调用.
///
/// One chatgpt or xai round going through lyra.
pub struct ResponsesTurn<'a> {
    relay: &'a Relay,
    line: ResponsesLine,
    reasoning_effort: &'a str,
    upstream_model: &'a str,
}

impl<'a> ResponsesTurn<'a> {
    /// 构造一轮调用, 借用共享的 relay 与思考强度档位.
    ///
    /// Builds one round borrowing the shared relay and reasoning effort.
    pub fn new(
        relay: &'a Relay,
        line: ResponsesLine,
        reasoning_effort: &'a str,
        upstream_model: &'a str,
    ) -> ResponsesTurn<'a> {
        ResponsesTurn {
            relay,
            line,
            reasoning_effort,
            upstream_model,
        }
    }
}

impl SearchTurn for ResponsesTurn<'_> {
    fn turn<'a>(&'a self, headers: &'a HeaderMap, body: Bytes) -> TurnFuture<'a> {
        Box::pin(async move {
            let buffered = match self.line {
                ResponsesLine::ChatGpt => {
                    self.relay
                        .chatgpt_messages_buffered(
                            headers,
                            body,
                            self.reasoning_effort,
                            self.upstream_model,
                        )
                        .await
                }
                ResponsesLine::Xai => {
                    self.relay
                        .xai_messages_buffered(
                            headers,
                            body,
                            self.reasoning_effort,
                            self.upstream_model,
                        )
                        .await
                }
                ResponsesLine::CommandCode => {
                    self.relay
                        .commandcode_messages_buffered(
                            headers,
                            body,
                            self.reasoning_effort,
                            self.upstream_model,
                        )
                        .await
                }
            };
            Ok(TurnResponse {
                status: buffered.status,
                retry_after: buffered.retry_after,
                body: buffered.body,
            })
        })
    }
}

/// 一轮经过 lyra 的 qoder 调用.
///
/// qoder 线路与订阅账号线路不同, 它的凭据是单用户访问令牌而不是账号池, 因此这里直接以
/// Qoder 网关的缓冲入口执行一轮. 网关返回 Anthropic 事件流, 缓冲入口把它汇总成完整消息,
/// 与 antigravity 与 Responses 线路给编排的报文同形.
///
/// One qoder round going through lyra.
///
/// The qoder line differs from the subscription-backed lines: its credential is a single-user
/// access token rather than an account pool, so a round runs directly on the Qoder gateway's
/// buffered entry point. The gateway returns an Anthropic event stream and the buffered entry
/// point folds it into a complete message, the same shape the antigravity and Responses lines
/// give the orchestration.
pub struct QoderTurn<'a> {
    client: &'a reqwest::Client,
    auth: &'a GatewayAuth,
    upstream_model: &'a str,
}

impl<'a> QoderTurn<'a> {
    /// 构造一轮调用, 借用共享的客户端, 凭据与上游模型名.
    ///
    /// Builds one round borrowing the shared client, credential and upstream model name.
    pub fn new(
        client: &'a reqwest::Client,
        auth: &'a GatewayAuth,
        upstream_model: &'a str,
    ) -> QoderTurn<'a> {
        QoderTurn {
            client,
            auth,
            upstream_model,
        }
    }
}

impl SearchTurn for QoderTurn<'_> {
    fn turn<'a>(&'a self, _headers: &'a HeaderMap, body: Bytes) -> TurnFuture<'a> {
        Box::pin(async move {
            // qoder 线路的请求由 lyra 依据路由模型重新构造, 客户端头不参与, 因此这里忽略
            // headers, 与 serve_qoder 的常规转发一致. 非 200 时编排依据状态码生成失败, 因此
            // 这里只读取响应体用于成功分支.
            //
            // The qoder line's request is rebuilt by lyra from the route model and the client
            // headers do not take part, so headers is ignored here, matching serve_qoder's
            // ordinary forward. On a non-200 the orchestration decides the failure from the
            // status, so the body is only read for the success branch.
            let response = lyra::qoder::qoder_buffered_request(
                self.client.clone(),
                self.auth,
                self.upstream_model,
                &body,
                None,
            )
            .await;
            let status = response.status();
            if status != http::StatusCode::OK {
                return Ok(TurnResponse {
                    status,
                    retry_after: None,
                    body: Bytes::new(),
                });
            }
            let collected = match response.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => {
                    return Err(canto::search::SearchFailure {
                        status: http::StatusCode::BAD_GATEWAY,
                        retry_after: None,
                        message: "search workflow failed".to_owned(),
                    });
                }
            };
            Ok(TurnResponse {
                status,
                retry_after: None,
                body: collected,
            })
        })
    }
}
