//! Qoder 直接 HTTP 网关上游集成模块.
//!
//! 提供基于官方网关协议的无状态请求转码, 响应转码, 凭据交换与签名生成.
//!
//! Copyright (c) 2026 ggbdpq (MIT License)
//! Copyright (c) 2026 wangjunyao (MIT License)

pub mod auth;
pub mod catalog;
pub mod codec;
pub mod request;
pub mod response;
pub mod signatures;

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use aria_core::body::{Body, BoxError};
use aria_core::errors::anthropic_error;
use aria_core::usage::Observation;
use bytes::Bytes;
use http::{HeaderValue, Response, StatusCode};
use http_body::{Body as HttpBody, Frame};
use http_body_util::BodyExt;
use tokio::sync::mpsc;

use self::auth::{GatewayAuth, to_reqwest_headers};
use self::response::QoderStreamState;

/// Qoder 官方网关生成端点相对路径.
pub const CHAT_SSE_ENDPOINT: &str =
    "/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=1&Encode=1";

/// 向 Qoder 官方网关发送请求并返回通过状态码校验的响应.
///
/// 流式入口与缓冲入口共用同一份请求构造, 会话维持, 签名与状态码校验, 两者的区别只有 Accept
/// 头以及拿到响应后的读取方式. 返回的 Err 携带已经填好的 Anthropic 错误响应, 两个入口都能
/// 直接把它交给客户端.
///
/// Sends a request to the Qoder gateway and returns the response once its status is accepted.
///
/// The streaming and buffered entry points share one request construction, session upkeep,
/// signing and status check; they differ only in the Accept header and in how the response is
/// read afterwards. The returned Err carries an already built Anthropic error response, so
/// either entry point can hand it straight to the client.
async fn send_gateway_request(
    client: &reqwest::Client,
    auth: &GatewayAuth,
    upstream_model: &str,
    body: &[u8],
    accept: &str,
) -> Result<reqwest::Response, Box<Response<Body>>> {
    // 网关只认内部目录键, 路由配置中的显示名先经模型目录翻译; 客户端可见的模型名不受
    // 影响, QoderStreamState 仍持有路由的原始模型名.
    let model_key = match auth.resolve_model_key(upstream_model).await {
        Ok(key) => key,
        Err(err) => {
            return Err(Box::new(anthropic_error(
                StatusCode::BAD_GATEWAY,
                None,
                &format!("解析 Qoder 模型名失败: {err}"),
            )));
        }
    };

    let gateway_body = match request::build_gateway_request(body, &model_key, "") {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err(Box::new(anthropic_error(
                StatusCode::BAD_GATEWAY,
                None,
                &format!("构建网关请求失败: {err}"),
            )));
        }
    };

    let session = match auth.ensure_session().await {
        Ok(sess) => sess,
        Err(err) => {
            let status = if err.contains("401") || err.contains("403") || err.contains("105") {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::BAD_GATEWAY
            };
            return Err(Box::new(anthropic_error(
                status,
                None,
                &format!("Qoder 认证会话维持失败: {err}"),
            )));
        }
    };

    let body_str = match std::str::from_utf8(&gateway_body) {
        Ok(s) => s,
        Err(err) => {
            return Err(Box::new(anthropic_error(
                StatusCode::BAD_GATEWAY,
                None,
                &format!("请求体编码非有效字符序列: {err}"),
            )));
        }
    };

    let endpoint = format!(
        "{}{CHAT_SSE_ENDPOINT}",
        auth.gateway_url().trim_end_matches('/')
    );

    let header_map =
        match GatewayAuth::build_business_headers(&session, body_str, &endpoint, Some(accept)) {
            Ok(headers) => headers,
            Err(err) => {
                return Err(Box::new(anthropic_error(
                    StatusCode::BAD_GATEWAY,
                    None,
                    &format!("构建业务请求签名失败: {err}"),
                )));
            }
        };

    let reqwest_headers = match to_reqwest_headers(&header_map) {
        Ok(headers) => headers,
        Err(err) => {
            return Err(Box::new(anthropic_error(
                StatusCode::BAD_GATEWAY,
                None,
                &format!("转换请求头失败: {err}"),
            )));
        }
    };

    let response = match client
        .post(&endpoint)
        .headers(reqwest_headers)
        .body(gateway_body)
        .send()
        .await
    {
        Ok(res) => res,
        Err(err) => {
            return Err(Box::new(anthropic_error(
                StatusCode::BAD_GATEWAY,
                None,
                &format!("发送网关请求失败: {err}"),
            )));
        }
    };

    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return Err(Box::new(anthropic_error(
            StatusCode::UNAUTHORIZED,
            None,
            "Qoder 网关认证拒绝",
        )));
    }
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(Box::new(anthropic_error(
            StatusCode::TOO_MANY_REQUESTS,
            None,
            "Qoder 网关速率受限",
        )));
    }
    if !status.is_success() {
        let msg = response
            .text()
            .await
            .unwrap_or_else(|_| "未知网关错误".to_owned());
        return Err(Box::new(anthropic_error(
            StatusCode::BAD_GATEWAY,
            None,
            &format!("Qoder 网关返回错误状态: {msg}"),
        )));
    }

    Ok(response)
}

/// 向 Qoder 官方网关发送请求并转码返回 Anthropic Messages 流式响应.
pub async fn forward_qoder_request(
    client: reqwest::Client,
    auth: &GatewayAuth,
    upstream_model: &str,
    body: &[u8],
    observation: Option<Observation>,
) -> Response<Body> {
    let mut response = match send_gateway_request(
        &client,
        auth,
        upstream_model,
        body,
        "text/event-stream",
    )
    .await
    {
        Ok(response) => response,
        Err(error) => return *error,
    };

    let mut state = QoderStreamState::new(upstream_model);
    let mut line_buf = String::new();
    let mut prefix_events = VecDeque::new();
    // 上游原文摘要只在空响应日志里使用, 因此最多保留探测上限的字节.
    //
    // The upstream text summary only serves the empty-response log line, so at most the
    // probe limit is kept.
    let mut upstream_probe = String::new();

    // Feed events to observation if provided
    let observation = observation;

    // 等到上游给出内容或停止原因再返回响应. 上游只发 message_start 或什么都不发时继续读到
    // 流结束, 使空响应能在写出 200 之前被识别; 若在首个有效数据就返回, 转码器合成的
    // end_turn 与 message_stop 会把空响应伪装成正常完成, 客户端看到一个立即结束的空回合.
    //
    // Wait for upstream content or a stop reason before returning the response. When the
    // upstream sends only a message_start or nothing, keep reading to the end of the stream
    // so an empty response is recognised before a 200 is written; returning on the first
    // valid data line would let the synthesized end_turn and message_stop disguise the empty
    // response as a normal completion, which the client sees as a turn that ends at once
    // with no output.
    while !(state.started && state.has_output()) {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                probe_push(&mut upstream_probe, &chunk);
                let text = match std::str::from_utf8(&chunk) {
                    Ok(s) => s,
                    Err(err) => {
                        return anthropic_error(
                            StatusCode::BAD_GATEWAY,
                            None,
                            &format!("解析网关事件编码失败: {err}"),
                        );
                    }
                };
                line_buf.push_str(text);
                while let Some(idx) = line_buf.find('\n') {
                    let line = line_buf[..idx].trim_end_matches('\r').to_owned();
                    line_buf.drain(..=idx);
                    match state.process_line(&line) {
                        Ok(events) => {
                            for evt in events {
                                if let Some(ref obs) = observation {
                                    capture_frame(obs, &evt);
                                }
                                prefix_events.push_back(evt);
                            }
                        }
                        Err(err) => {
                            return anthropic_error(err.status, None, &err.message);
                        }
                    }
                    if state.started && state.has_output() {
                        break;
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                return anthropic_error(
                    StatusCode::BAD_GATEWAY,
                    None,
                    &format!("读取网关响应首包失败: {err}"),
                );
            }
        }
    }

    if !state.started {
        return anthropic_error(StatusCode::BAD_GATEWAY, None, "Qoder 网关未返回有效事件");
    }
    if !state.has_output() {
        log_empty_upstream(upstream_model, &upstream_probe);
        return anthropic_error(
            StatusCode::BAD_GATEWAY,
            None,
            "Qoder 上游返回空响应, 没有内容块与停止原因",
        );
    }

    let (tx, rx) = mpsc::channel(16);

    // 响应体持有观测的另一份副本, 在流完整写出后收尾记录.
    let body_observation = observation.clone();

    tokio::spawn(async move {
        let obs = &observation;

        while let Some(idx) = line_buf.find('\n') {
            let line = line_buf[..idx].trim_end_matches('\r').to_owned();
            line_buf.drain(..=idx);
            match state.process_line(&line) {
                Ok(events) => {
                    for evt in events {
                        if let Some(o) = obs {
                            capture_frame(o, &evt);
                        }
                        if tx.send(evt).await.is_err() {
                            return;
                        }
                    }
                }
                Err(_) => return,
            }
        }

        while let Ok(Some(chunk)) = response.chunk().await {
            if let Ok(text) = std::str::from_utf8(&chunk) {
                line_buf.push_str(text);
                while let Some(idx) = line_buf.find('\n') {
                    let line = line_buf[..idx].trim_end_matches('\r').to_owned();
                    line_buf.drain(..=idx);
                    match state.process_line(&line) {
                        Ok(events) => {
                            for evt in events {
                                if let Some(o) = obs {
                                    capture_frame(o, &evt);
                                }
                                if tx.send(evt).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(_) => return,
                    }
                }
            }
        }

        let remaining = line_buf.trim();
        if !remaining.is_empty()
            && remaining != "[DONE]"
            && let Ok(events) = state.process_line(remaining)
        {
            for evt in events {
                if let Some(o) = obs {
                    capture_frame(o, &evt);
                }
                if tx.send(evt).await.is_err() {
                    return;
                }
            }
        }

        for evt in state.finish() {
            if let Some(o) = obs {
                capture_frame(o, &evt);
            }
            if tx.send(evt).await.is_err() {
                return;
            }
        }
    });

    let body = BodyExt::boxed(QoderStreamBody {
        prefix: prefix_events,
        receiver: rx,
        observation: body_observation,
        observation_finished: false,
    });

    let mut res = Response::new(body);
    *res.status_mut() = StatusCode::OK;
    res.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    res.headers_mut().insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache"),
    );
    res
}

/// 空响应日志里保留的上游原文字节上限.
///
/// 摘要只用于定位故障, 与 messages 路由失败记录的 300 字节取同一数值.
///
/// The byte limit of the upstream text kept in the empty-response log line.
///
/// The summary exists to locate a fault; 300 bytes matches the failure records of the
/// messages routes.
const MAX_UPSTREAM_SUMMARY: usize = 300;

/// 探测缓冲保留的上游响应字节上限.
///
/// The byte limit of the upstream response kept in the probe buffer.
const UPSTREAM_PROBE_BYTES: usize = 4096;

/// 把上游响应片段追加到探测缓冲, 达到上限后不再追加.
///
/// 上限之外的内容不再进入缓冲, 因此内存占用与上游响应大小无关; 摘要按字节上限截断由
/// single_line 完成.
///
/// Appends one upstream response piece to the probe buffer and stops once the limit is
/// reached. Content beyond the limit never enters the buffer, so memory use does not depend
/// on the response size; single_line truncates the summary to its byte limit.
fn probe_push(probe: &mut String, chunk: &[u8]) {
    if probe.len() >= UPSTREAM_PROBE_BYTES {
        return;
    }
    let room = UPSTREAM_PROBE_BYTES - probe.len();
    let taken = chunk.len().min(room);
    probe.push_str(&String::from_utf8_lossy(&chunk[..taken]));
}

/// 记录一次上游空响应: 阶段与截断后的上游原文摘要.
///
/// 上游没有给出内容块与停止原因时转码器只能合成空回复, 客户端会看到立即结束的空回合;
/// 这条日志把当次上游实际返回的内容留下来, 供定位是错误包裹, 空负载还是其他形态.
///
/// Records one empty upstream response: the stage and the truncated upstream text summary.
///
/// Without content blocks and a stop reason the translator could only synthesize an empty
/// reply, which the client sees as a turn that ends at once; this log keeps what the
/// upstream actually returned so an error envelope, an empty payload and other shapes can be
/// told apart.
fn log_empty_upstream(model: &str, probe: &str) {
    aria_core::log!(
        "Qoder 上游返回空响应 model={} stage=empty 上游原文摘要={}",
        model,
        aria_core::log::single_line(probe, MAX_UPSTREAM_SUMMARY)
    );
}

/// Qoder 缓冲响应的字节上限, 与搜索轮次读取响应体的上限一致.
///
/// The byte limit of a buffered Qoder response, matching the response body limit the search
/// rounds read.
const MAX_BUFFERED_BYTES: usize = 4 << 20;

/// 以非流式方式完成一次 Qoder 请求并返回完整 Anthropic 消息 JSON, 供搜索轮次使用.
///
/// 网关只提供事件流, 而搜索编排的每一轮都需要非流式的完整报文, 因此这里复用
/// QoderStreamState 的转换结果把事件流汇总成一条消息. 发往网关的请求与流式入口完全相同,
/// Accept 同样取 text/event-stream, 区别只在于读完整个响应体后再汇总: 2026-10-09 对真实
/// 网关实测, 同一请求以 Accept: application/json 发送时网关返回 500 Internal Server Error.
/// 任何非 200 状态已经由 send_gateway_request 转成 Anthropic 错误响应, 调用方把非 200
/// 映射为搜索失败.
///
/// Completes one Qoder request without streaming and returns the complete Anthropic message
/// JSON for the search rounds.
///
/// The gateway only offers an event stream while every search round needs a non-streaming
/// complete payload, so the aggregation reuses QoderStreamState's conversion output to fold
/// the stream into one message. The request sent to the gateway is identical to the streaming
/// one, Accept included as text/event-stream; the only difference is reading the whole body
/// before folding it. Against the real gateway on 2026-10-09 the same request sent with
/// Accept: application/json returned 500 Internal Server Error. Any non-200 status has already
/// become an Anthropic error response inside send_gateway_request, and the caller maps a
/// non-200 to a search failure.
pub async fn qoder_buffered_request(
    client: reqwest::Client,
    auth: &GatewayAuth,
    upstream_model: &str,
    body: &[u8],
    observation: Option<Observation>,
) -> Response<Body> {
    let mut response = match send_gateway_request(
        &client,
        auth,
        upstream_model,
        body,
        "text/event-stream",
    )
    .await
    {
        Ok(response) => response,
        Err(error) => return *error,
    };

    let mut state = QoderStreamState::new(upstream_model);
    let mut line_buf = String::new();
    let mut events: Vec<Bytes> = Vec::new();
    let mut buffered_bytes: usize = 0;
    // 上游原文摘要只在空响应日志里使用, 因此最多保留探测上限的字节.
    //
    // The upstream text summary only serves the empty-response log line, so at most the
    // probe limit is kept.
    let mut upstream_probe = String::new();

    // Feed events to observation if provided
    let observation = &observation;

    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                probe_push(&mut upstream_probe, &chunk);
                let text = match std::str::from_utf8(&chunk) {
                    Ok(s) => s,
                    Err(err) => {
                        return anthropic_error(
                            StatusCode::BAD_GATEWAY,
                            None,
                            &format!("解析网关事件编码失败: {err}"),
                        );
                    }
                };
                line_buf.push_str(text);
                while let Some(idx) = line_buf.find('\n') {
                    let line = line_buf[..idx].trim_end_matches('\r').to_owned();
                    line_buf.drain(..=idx);
                    match state.process_line(&line) {
                        Ok(parsed) => {
                            for evt in &parsed {
                                buffered_bytes = buffered_bytes.saturating_add(evt.len());
                                if let Some(obs) = observation {
                                    capture_frame(obs, evt);
                                }
                            }
                            events.extend(parsed);
                        }
                        Err(err) => return anthropic_error(err.status, None, &err.message),
                    }
                    if buffered_bytes > MAX_BUFFERED_BYTES {
                        return anthropic_error(
                            StatusCode::BAD_GATEWAY,
                            None,
                            "Qoder 网关响应超过缓冲上限",
                        );
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                return anthropic_error(
                    StatusCode::BAD_GATEWAY,
                    None,
                    &format!("读取网关响应失败: {err}"),
                );
            }
        }
    }

    let remaining = line_buf.trim();
    if !remaining.is_empty()
        && remaining != "[DONE]"
        && let Ok(parsed) = state.process_line(remaining)
    {
        for evt in &parsed {
            if let Some(obs) = observation {
                capture_frame(obs, evt);
            }
        }
        events.extend(parsed);
    }

    // 首包到达之前没有任何事件时无法汇总出有效消息, 与流式入口的判定保持一致; 只有
    // message_start 而没有内容也没有上游停止原因时同样无法汇总出有效消息, 合成的 end_turn
    // 与 message_stop 只会把它伪装成一条空的正常消息.
    //
    // Without any event before the first packet there is nothing to aggregate into a valid
    // message, matching the streaming entry point's check; a stream with only a
    // message_start and neither content nor an upstream stop reason is the same case, and the
    // synthesized end_turn and message_stop would only disguise it as an empty normal
    // message.
    if !state.started {
        return anthropic_error(StatusCode::BAD_GATEWAY, None, "Qoder 网关未返回有效事件");
    }
    if !state.has_output() {
        log_empty_upstream(upstream_model, &upstream_probe);
        return anthropic_error(
            StatusCode::BAD_GATEWAY,
            None,
            "Qoder 上游返回空响应, 没有内容块与停止原因",
        );
    }

    let finish_events = state.finish();
    for evt in &finish_events {
        if let Some(obs) = observation {
            capture_frame(obs, evt);
        }
    }
    events.extend(finish_events);

    let mut collected = Vec::with_capacity(buffered_bytes);
    for evt in &events {
        collected.extend_from_slice(evt);
    }
    match response::aggregate_message(&Bytes::from(collected)) {
        Ok(encoded) => {
            // 聚合报文只有一帧; 复用 QoderStreamBody 是为了让用量观测与流式入口走同一条
            // 收尾路径: 客户端完整收到响应后记录完成, 提前断开则由析构按客户端离开记录.
            //
            // The aggregated payload is a single frame; reusing QoderStreamBody keeps the
            // observation teardown identical to the streaming entry point: completion is
            // recorded once the client has received the whole response, while an early
            // disconnect is recorded as the client having left by the destructor.
            let (sender, receiver) = mpsc::channel(1);
            drop(sender);
            let body = BodyExt::boxed(QoderStreamBody {
                prefix: VecDeque::from([Bytes::from(encoded)]),
                receiver,
                observation: observation.clone(),
                observation_finished: false,
            });
            let mut res = Response::new(body);
            *res.status_mut() = StatusCode::OK;
            res.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            res.headers_mut().insert(
                http::header::CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
            );
            res
        }
        Err(err) => anthropic_error(err.status, None, &err.message),
    }
}

/// 把一条转码产出的事件帧喂给用量观测.
///
/// 帧由 response.rs 的 format_sse_event 生成, 已经是完整的 SSE 帧; Observation::capture_event
/// 与其他转换线路的用法一致, 只接受事件的 data 载荷并在内部重新包装成帧. 因此这里必须先取出
/// 载荷再传入: 把整帧直接传入会被多重包装, 采集器的 SSE 解析得到的载荷不再是合法 JSON,
/// message_delta 与 message_stop 都识别不出, 记录停留在 incomplete 且用量为空.
///
/// Feeds one translated event frame to the usage observation.
fn capture_frame(observation: &Observation, frame: &[u8]) {
    observation.capture_event(frame_payload(frame));
}

/// 取出 format_sse_event 生成的完整事件帧中的 data 载荷.
///
/// 帧格式固定为 event 行加单行 data, serde_json 产出的载荷不含未转义的换行, 因此第一条
/// data 行的内容就是完整载荷.
///
/// Takes the data payload out of a complete event frame produced by format_sse_event.
fn frame_payload(frame: &[u8]) -> &[u8] {
    for line in frame.split(|byte| *byte == b'\n') {
        if let Some(payload) = line.strip_prefix(b"data: ") {
            return payload;
        }
    }
    b""
}

struct QoderStreamBody {
    prefix: VecDeque<Bytes>,
    receiver: mpsc::Receiver<Bytes>,
    /// 用量观测. 只有响应体完整写出才把请求记为完成; 客户端提前断开时这里被直接丢弃,
    /// Observation 的析构按客户端离开补记一次.
    ///
    /// The usage observation. The request is recorded as completed only when the body has
    /// been written out in full; on an early client disconnect the body is dropped and the
    /// Observation destructor records the request as the client having left.
    observation: Option<Observation>,
    observation_finished: bool,
}

impl QoderStreamBody {
    /// 收尾用量观测: 声明响应体结束, 标记上游传输完成并落盘记录. 全部事件在送达客户端之前
    /// 已经喂入观测, 因此接收端关闭时采集内容已经完整. 重复调用没有效果.
    ///
    /// Tears down the usage observation: declares the body ended, marks the upstream transfer
    /// complete and writes the record. Every event is fed before it reaches the client, so the
    /// captured content is already complete when the receiver closes. Repeated calls do nothing.
    fn finish_observation(&mut self) {
        if self.observation_finished {
            return;
        }
        self.observation_finished = true;
        if let Some(observation) = &self.observation {
            observation.finish_chunks();
            observation.set_transport_complete(true);
            observation.finish(false);
        }
    }
}

impl HttpBody for QoderStreamBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if let Some(chunk) = this.prefix.pop_front() {
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        match this.receiver.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => Poll::Ready(Some(Ok(Frame::data(chunk)))),
            Poll::Ready(None) => {
                this.finish_observation();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aria_core::usage::Store;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    /// 真实格式的网关事件流经转码与采集后, 用量记录必须是 completed 且带令牌与积分.
    ///
    /// 样本按网关实际返回的结构构造: 每行是包了一层 wrapper 的 data 事件, body 是转义的
    /// OpenAI 格式增量, 倒数第二行带 usage 与积分字段. 回归背景: v0.1.11 把完整 SSE 帧再次
    /// 包装后喂入采集器, 载荷不再是合法 JSON, 记录停留在 incomplete 且 usage 为空.
    #[tokio::test]
    async fn qoder_usage_capture_records_completed_with_credits() -> Result<(), String> {
        let lines = [
            r#"data: {"statusCodeValue":200,"body":"{\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"你好\"}}]}"}"#,
            r#"data: {"statusCodeValue":200,"body":"{\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"，世界\"}}]}"}"#,
            r#"data: {"statusCodeValue":200,"body":"{\"id\":\"chatcmpl-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":328,\"completion_tokens\":7,\"total_tokens\":335,\"credits\":0.4137,\"original_credits\":0.5021,\"billable\":true}}"}"#,
            r#"data: {"statusCodeValue":200,"body":"[DONE]"}"#,
        ];
        let mut state = QoderStreamState::new("qoder-claude/qwen3.8-max");
        let mut frames = Vec::new();
        for line in lines {
            frames.extend(state.process_line(line).map_err(|err| err.message)?);
        }
        frames.extend(state.finish());

        let dir = tempfile::tempdir().map_err(|err| err.to_string())?;
        let store = Store::open(dir.path()).map_err(|err| err.to_string())?;
        let observation = Observation::start(
            Arc::new(store),
            Arc::new(AtomicU64::new(0)),
            "qoder",
            "qoder-claude/qwen3.8-max",
        );
        for frame in &frames {
            capture_frame(&observation, frame);
        }
        observation.set_status(200);
        observation.finish_chunks();
        observation.set_transport_complete(true);
        observation.finish(false);

        // 记录在 spawn_blocking 里落盘, 轮询等待日文件出现.
        let mut content = String::new();
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let entries = std::fs::read_dir(dir.path()).map_err(|err| err.to_string())?;
            for entry in entries {
                let path = entry.map_err(|err| err.to_string())?.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                    content = std::fs::read_to_string(&path).map_err(|err| err.to_string())?;
                }
            }
            if !content.is_empty() {
                break;
            }
        }
        assert!(!content.is_empty(), "usage record was not persisted");

        let record: serde_json::Value =
            serde_json::from_str(content.trim()).map_err(|err| err.to_string())?;
        assert_eq!(record["provider"], "qoder");
        assert_eq!(record["model"], "qoder-claude/qwen3.8-max");
        assert_eq!(record["http_status"], 200);
        assert_eq!(record["outcome"], "completed");
        let usage = &record["usage"];
        assert!(usage.is_object(), "usage must be present: {record}");
        assert_eq!(usage["input_tokens"], 328);
        assert_eq!(usage["output_tokens"], 7);
        assert_eq!(usage["total_tokens"], 335);
        let credits = usage["credits"]
            .as_f64()
            .ok_or_else(|| "credits missing".to_owned())?;
        assert!((credits - 0.4137).abs() < 1e-12, "credits: {credits}");
        let original = usage["original_credits"]
            .as_f64()
            .ok_or_else(|| "original_credits missing".to_owned())?;
        assert!(
            (original - 0.5021).abs() < 1e-12,
            "original_credits: {original}"
        );
        assert_eq!(usage["billable"], true);
        Ok(())
    }
}
