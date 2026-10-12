//! Moonshot 搜索编排, 搜索客户端与搜索结果的 JSON 与 SSE 编码.
//!
//! 独立的 Moonshot 搜索服务返回搜索结果而不是模型回复, 被选中的路由仍然负责构造查询
//! 并依据搜索结果作答. 一次请求先执行准备工作, 之后最多运行 max_uses 轮搜索; 每一轮
//! 包含一次模型调用, 对该轮响应的校验与记账, 在模型请求搜索时执行检索, 最后决定回放
//! 对话还是结束.
//!
//! 编排不直接发模型请求, 而是通过 SearchTurn 执行每一轮, 因此 messages 路由与
//! antigravity 路由共用同一套编排, 各自实现自己的轮次.
//!
//! Moonshot search orchestration, the search client, and the JSON and SSE encoding of
//! search results.
//!
//! The standalone Moonshot service returns search results rather than model completions,
//! and the selected route stays responsible for creating queries and answering from those
//! results. One request performs a prepare step and then runs at most max_uses search
//! rounds; each round is a model turn, validation and accounting of that turn, retrieval
//! when the model asked for it, and a replay or finish decision.
//!
//! The orchestration does not send model requests itself but executes every round through
//! SearchTurn, so messages routes and antigravity routes share one orchestration and each
//! implements its own round.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use aria_core::body::{Body, full};
use aria_core::errors;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue, Response, StatusCode, header};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use url::Url;

use crate::forward::{
    MAX_ERROR_BODY_BYTES, MessagesDestination, RawObject, parse_base_url, read_error_prefix,
};

/// 搜索响应体上限, 与 Go 版 readSearchResponse 相同.
///
/// The search response body limit, matching readSearchResponse in the Go version.
const MAX_SEARCH_RESPONSE: usize = 4 << 20;

/// 单次搜索查询的总超时, 与 Go 版 lookup 的 90 秒相同.
///
/// The total timeout of one search lookup, matching the 90 seconds in the Go lookup.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(90);

/// 搜索服务的 User-Agent, 与 Go 版相同.
///
/// The search service User-Agent, unchanged from the Go version.
const USER_AGENT: &str = "self-infra-model-gateway/1";

/// 内部搜索函数名. 部分上游适配器会改写常规的 web_search 函数名, 因此内部调用与公开
/// 的 Anthropic 服务端工具保持不同的名称.
///
/// The internal search function name. Some upstream adapters rewrite the conventional
/// web_search function name, so the internal call stays distinct from the public
/// Anthropic server tool.
const SEARCH_FUNCTION_NAME: &str = "self_infra_search_web";

/// 跨搜索轮次累加的用量字段. 上游省略的字段不参与累加.
///
/// The usage fields accumulated across search rounds. A field the upstream omits
/// contributes nothing.
const TOKEN_COUNTERS: [&str; 4] = [
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
];

/// 单条查询的长度上限, 与 Go 版相同.
///
/// The length limit of one query, unchanged from the Go version.
const MAX_QUERY_BYTES: usize = 4096;

/// 返回给模型的结果条数上限, 与请求中 limit 的取值一致.
///
/// The result limit returned to the model, matching the limit sent in the request.
const MAX_HITS: usize = 5;

/// 搜索服务基址校验失败的信息.
///
/// The message for a failed search service base URL check.
const SEARCH_URL_ERROR: &str =
    "search service requires HTTPS or loopback HTTP without credentials or query";

/// 搜索响应体不完整或超限的信息.
///
/// The message for an incomplete or oversized search response body.
const INCOMPLETE_RESPONSE: &str = "search workflow response incomplete or exceeds limit";

/// 附在固定失败文本时的上游原因字节上限.
///
/// 上游原因用于定位故障, 不需要完整正文; 300 字节足以覆盖常见错误信息, 按字符边界截断,
/// 因此不会把一个多字节字符切成无效序列.
///
/// The byte limit of the upstream reason appended to the fixed failure text.
///
/// The reason exists to locate a fault, so the whole body is unnecessary; 300 bytes cover
/// common error messages and the cut respects character boundaries, so a multibyte character
/// is never split into an invalid sequence.
const MAX_UPSTREAM_REASON: usize = 300;

/// 一次受限搜索请求中调用方可见的搜索次数预算.
///
/// The caller-visible budget for one bounded search request.
#[derive(Debug, Clone, Copy)]
pub struct SearchLimits {
    /// 允许执行的检索次数, 取值 1 到 8.
    ///
    /// The number of lookups allowed, between 1 and 8.
    pub max_uses: usize,
}

/// 一轮非流式模型调用的结果.
///
/// The result of one non-streaming model turn.
#[derive(Debug)]
pub struct TurnResponse {
    /// 上游返回的状态码.
    ///
    /// The status code returned by the upstream.
    pub status: StatusCode,
    /// 上游返回的 Retry-After 值.
    ///
    /// The Retry-After value returned by the upstream.
    pub retry_after: Option<String>,
    /// 上游返回的响应体. 状态码为 200 时是完整响应体; 非 200 时是错误正文的前缀, 编排据此
    /// 生成失败原因.
    ///
    /// The response body returned by the upstream. For a 200 it is the complete body; for a
    /// non-200 it is the prefix of the error body, from which the orchestration builds the
    /// failure reason.
    pub body: Bytes,
}

/// 一轮模型调用的未来.
///
/// The future of one model turn.
pub type TurnFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TurnResponse, SearchFailure>> + Send + 'a>>;

/// 一轮模型调用的执行方式.
///
/// How one model turn is executed.
pub trait SearchTurn: Send + Sync {
    /// headers 是客户端原始请求头, body 是本轮完整的非流式请求体.
    ///
    /// headers are the client's original request headers, and body is this round's
    /// complete non-streaming request body.
    fn turn<'a>(&'a self, headers: &'a HeaderMap, body: Bytes) -> TurnFuture<'a>;
}

/// 必须返回给调用方的 HTTP 结果.
///
/// The HTTP result that must be returned to the caller.
#[derive(Debug)]
pub struct SearchFailure {
    /// 响应状态码.
    ///
    /// The response status code.
    pub status: StatusCode,
    /// 响应体中的错误信息.
    ///
    /// The error message in the response body.
    pub message: String,
    /// 来自上游的 Retry-After 值.
    ///
    /// The Retry-After value taken from the upstream.
    pub retry_after: Option<String>,
}

impl SearchFailure {
    /// 构造一个固定信息与状态的失败.
    ///
    /// Builds a failure with a fixed message and status.
    fn at(status: StatusCode, message: &str) -> SearchFailure {
        SearchFailure {
            status,
            message: message.to_owned(),
            retry_after: None,
        }
    }

    /// 构造一个内部失败, 状态码为 502.
    ///
    /// Builds an internal failure with status 502.
    fn internal(message: &str) -> SearchFailure {
        SearchFailure::at(StatusCode::BAD_GATEWAY, message)
    }

    /// 转换成 Anthropic 错误响应.
    ///
    /// 错误类型固定为 api_error, 与 Go 版 serveSearch 相同; 上游给出的 Retry-After
    /// 原样带上, 取值不是合法头部值时忽略.
    ///
    /// Converts into an Anthropic error response.
    ///
    /// The error type is always api_error, matching serveSearch in the Go version; an
    /// upstream Retry-After value is carried through and ignored when it is not a valid
    /// header value.
    pub fn into_response(self) -> Response<Body> {
        let mut response = errors::anthropic_error(self.status, Some("api_error"), &self.message);
        if let Some(retry_after) = self.retry_after
            && let Ok(value) = HeaderValue::from_str(&retry_after)
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }
}

impl fmt::Display for SearchFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for SearchFailure {}

/// 一条返回给模型的搜索结果. 该服务可能不返回日期.
///
/// One result returned to the model. The service may omit the date.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct SearchHit {
    /// 结果标题. 搜索服务省略该字段时按空字符串处理, 与 Go 版解码进结构体的零值一致;
    /// 取值类型错误仍然报错.
    ///
    /// The result title. A field the service omits counts as an empty string, matching the
    /// zero value the Go version decodes into its struct; a value of the wrong type is
    /// still an error.
    #[serde(default)]
    pub title: String,
    /// 结果链接, 缺省按空字符串处理.
    ///
    /// The result URL; a missing field counts as an empty string.
    #[serde(default)]
    pub url: String,
    /// 结果摘要, 缺省按空字符串处理.
    ///
    /// The result snippet; a missing field counts as an empty string.
    #[serde(default)]
    pub snippet: String,
    /// 结果日期, 缺省时不参与编码, 与 Go 版的 omitempty 相同.
    ///
    /// The result date; when absent it is not encoded, matching the Go omitempty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub date: String,
}

/// 搜索端点的响应结构.
///
/// The response structure of the search endpoint.
#[derive(Deserialize)]
struct SearchResponse {
    #[serde(default)]
    search_results: Option<Vec<SearchHit>>,
}

/// 描述搜索端点以及存放其凭据的环境变量.
///
/// Names the search endpoint and the environment variable holding its credential.
pub struct SearchServiceConfig {
    /// 搜索端点地址.
    ///
    /// The search endpoint URL.
    pub url: String,
    /// 存放搜索服务密钥的环境变量名.
    ///
    /// The environment variable holding the search service key.
    pub key_env: String,
}

/// 携带凭据的搜索客户端. 它复用上游客户端, 因此搜索请求同样复用连接.
///
/// The credential-bearing search client. It reuses the upstream client, so search
/// requests also reuse connections.
pub struct SearchService {
    url: Url,
    authorization: HeaderValue,
    client: reqwest::Client,
}

impl SearchService {
    /// 校验配置并构造搜索客户端.
    ///
    /// 基址必须是 HTTPS, 只有回环地址允许 HTTP, 不能包含用户信息, 查询串或片段;
    /// 密钥必须非空且不含回车, 换行与空字符.
    ///
    /// Validates the configuration and builds the search client.
    ///
    /// The base URL must be HTTPS, HTTP is allowed only for loopback, and it must carry
    /// no credentials, query string or fragment; the key must be non-empty and contain no
    /// carriage return, newline or NUL.
    pub fn new(
        config: &SearchServiceConfig,
        client: reqwest::Client,
        getenv: &dyn Fn(&str) -> Option<String>,
    ) -> Result<SearchService, String> {
        let url = parse_base_url(&config.url).ok_or_else(|| SEARCH_URL_ERROR.to_owned())?;
        let key = getenv(&config.key_env).unwrap_or_default();
        if key.is_empty() || key.contains(['\r', '\n', '\0']) {
            return Err("search service requires a valid key environment variable".to_owned());
        }
        let authorization = HeaderValue::from_bytes(format!("Bearer {key}").as_bytes())
            .map_err(|_| "search service requires a key valid in an HTTP header".to_owned())?;
        Ok(SearchService {
            url,
            authorization,
            client,
        })
    }

    /// 执行一次受限搜索.
    ///
    /// 调用标识会被转发, 因此该服务能够把自己的记录与网关的搜索轮次对应起来. 只保留
    /// 能够作为来源链接使用的结果, 返回数量不超过请求所要求的五条.
    ///
    /// Performs one bounded search.
    ///
    /// The call identifier is forwarded so the service can correlate its records with the
    /// gateway round. Only results usable as source links are kept, and never more than
    /// the five the request asks for.
    pub async fn lookup(
        &self,
        query: &str,
        call_id: &str,
    ) -> Result<Vec<SearchHit>, SearchFailure> {
        let call_header = HeaderValue::from_str(call_id)
            .map_err(|_| SearchFailure::internal("search workflow failed"))?;
        let payload = json!({
            "text_query": query,
            "limit": MAX_HITS,
            "enable_page_crawling": false,
            "timeout_seconds": 30,
        });
        let body = serde_json::to_vec(&payload)
            .map_err(|_| SearchFailure::internal("search workflow failed"))?;
        let response = self
            .client
            .post(self.url.clone())
            .header(header::AUTHORIZATION, self.authorization.clone())
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )
            .header(header::USER_AGENT, HeaderValue::from_static(USER_AGENT))
            .header("x-msh-tool-call-id", call_header)
            .timeout(LOOKUP_TIMEOUT)
            .body(body)
            .send()
            .await
            .map_err(|_| SearchFailure::internal("search workflow connection failed"))?;
        let status = response.status();
        if status != StatusCode::OK {
            return Err(SearchFailure {
                status,
                message: "search workflow upstream returned an error".to_owned(),
                retry_after: retry_after(&response),
            });
        }
        let data = read_bounded_body(response).await?;
        let parsed: SearchResponse = serde_json::from_slice(&data)
            .map_err(|_| SearchFailure::internal("invalid search service response"))?;
        let Some(hits) = parsed.search_results else {
            return Err(SearchFailure::internal("invalid search service response"));
        };
        let mut kept = Vec::with_capacity(MAX_HITS);
        for hit in hits {
            if !usable_source(&hit.url) {
                continue;
            }
            kept.push(hit);
            if kept.len() == MAX_HITS {
                break;
            }
        }
        Ok(kept)
    }
}

/// messages 路由的一轮模型调用: 以非流式方式请求 /v1/messages 并读取至多 4 MiB 的
/// 响应体.
///
/// 轮次使用与常规生成相同的轮换规则, 因此搜索轮次同样按游标在多个上游之间轮换, 并在
/// 上游于生成开始前拒绝时换下一个上游.
///
/// One model turn on a messages route: it requests /v1/messages without streaming and
/// reads at most a 4 MiB response body.
///
/// The turn follows the same rotation rule as ordinary generation, so search rounds also
/// advance the cursor across several upstreams and switch to the next one when an
/// upstream refuses before generation starts.
impl SearchTurn for MessagesDestination {
    fn turn<'a>(&'a self, headers: &'a HeaderMap, body: Bytes) -> TurnFuture<'a> {
        Box::pin(async move {
            // 与常规生成共用同一份路由级不可用窗口: 窗口内的搜索轮次同样立即拒绝, 不转发上游.
            //
            // The round shares the route-level unavailable window with ordinary generation: a
            // search round inside the window is rejected at once without reaching upstream.
            if let Some((reason, remaining)) = self.unavailable_window() {
                let seconds = remaining.as_secs().max(1);
                aria_core::log!(
                    "messages route={} unavailable ({}), search round rejected, {} seconds remaining",
                    self.name(),
                    reason.as_str(),
                    seconds
                );
                return Err(SearchFailure {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message: reason.message(self.name(), seconds),
                    retry_after: Some(seconds.to_string()),
                });
            }
            let mut response = self
                .send_with_rotation(|index| {
                    let request = self.request(index, "/v1/messages", headers, body.clone());
                    async move { request.send().await }
                })
                .await
                .map_err(|_| SearchFailure::internal("search workflow connection failed"))?;
            let status = response.status();
            let retry_after = retry_after(&response);
            if matches!(status.as_u16(), 403 | 429) {
                // 配额类判定需要响应体文本, 因此这里读出前缀用于判断与记录. 前缀同时作为
                // 失败原因交给编排, 避免为 403 与 429 再读一次上游.
                //
                // Deciding whether a rejection is quota-class needs the body text, so the prefix
                // is read to classify and record it. The prefix also becomes the failure reason
                // for the orchestration, so a 403 or 429 is not read from upstream twice.
                let prefix = self.observe_rejection(&mut response, status).await;
                return Ok(TurnResponse {
                    status,
                    retry_after,
                    body: prefix,
                });
            }
            if status.is_success() {
                self.record_success();
            }
            if status != StatusCode::OK {
                // 其余非 200 读出错误正文作为失败原因, 交给编排附在固定文本之后, 因此每个
                // 非 200 都能透出原因. 读取上限与 observe_rejection 相同, 用完整的错误正文,
                // 因为原因需要从 JSON 结构中的 error.message 提取: 上游正文超过 1 KB 时
                // error.message 之后还带 metadata, 按原因长度 300 字节截断会让 JSON 解析
                // 失败, 客户端只能看到正文原文而不是 error.message. 300 字节的上限只在
                // upstream_reason 提取出原因之后用于截断.
                //
                // Every other non-200 reads the error body as the failure reason for the
                // orchestration to append after the fixed text, so each surfaces its cause. The
                // read limit matches observe_rejection and spans the whole error body, because the
                // reason is extracted from error.message in the JSON structure: an upstream body
                // over 1 KB carries metadata after error.message, and truncating to the 300-byte
                // reason length would break JSON parsing so the client would see raw body text
                // instead of error.message. The 300-byte limit only truncates the extracted reason
                // in upstream_reason.
                let reason = read_error_prefix(&mut response, MAX_ERROR_BODY_BYTES).await;
                return Ok(TurnResponse {
                    status,
                    retry_after,
                    body: reason,
                });
            }
            let body = read_bounded_body(response).await?;
            Ok(TurnResponse {
                status,
                retry_after,
                body,
            })
        })
    }
}

/// 判断一个已校验的 Messages 请求是否属于独立的服务端搜索请求.
///
/// 请求必须保持原生行为时返回 None: 该辅助流程只拦截工具列表中恰好包含一个名为
/// web_search 的 web_search_20250305 条目的情况. 对于本服务契约无法落实的约束, 这里
/// 返回错误, 而不是悄悄放宽域名, 位置或者代码执行方面的要求.
///
/// Decides whether an admitted Messages request is the isolated server-search request.
///
/// It returns None when the request must stay native: the helper only intercepts a tool
/// list holding exactly one web_search_20250305 entry named web_search. Constraints this
/// service contract cannot enforce produce an error instead of silently weakening domain,
/// location or code-execution requirements.
pub fn search_options(fields: &RawObject) -> Result<Option<SearchLimits>, String> {
    let tools = match fields.get("tools") {
        Some(raw) => match serde_json::from_str::<Vec<RawObject>>(raw.get()) {
            Ok(tools) => tools,
            Err(_) => return Ok(None),
        },
        None => return Ok(None),
    };
    if tools.len() != 1 {
        return Ok(None);
    }
    let tool = &tools[0];
    if raw_string(tool, "type") != "web_search_20250305" {
        return Ok(None);
    }
    if raw_string(tool, "name") != "web_search" {
        return Err("server search tool must be named web_search".to_owned());
    }
    let choice = match fields.get("tool_choice") {
        Some(raw) => match serde_json::from_str::<Option<ToolChoice>>(raw.get()) {
            Ok(choice) => choice.unwrap_or_default(),
            Err(_) => return Err("invalid tool_choice".to_owned()),
        },
        None => ToolChoice::default(),
    };
    if choice.kind == "none" {
        return Ok(None);
    }
    match choice.kind.as_str() {
        "" | "auto" | "any" => {}
        "tool" => {
            if choice.name != "web_search" {
                return Err("tool_choice must name web_search".to_owned());
            }
        }
        _ => return Err("invalid search tool_choice type".to_owned()),
    }
    for (key, value) in tool {
        match key.as_str() {
            "type" | "name" | "max_uses" | "cache_control" => continue,
            "allowed_callers" => {
                let callers = serde_json::from_str::<Option<Vec<String>>>(value.get())
                    .map_err(|_| "invalid allowed_callers".to_owned())?
                    .unwrap_or_default();
                if callers.iter().any(|caller| caller != "direct") {
                    return Err("search supports only direct calls".to_owned());
                }
                continue;
            }
            _ => {}
        }
        // 未支持的选项只在显式为 null 或空数组时放行, 其余取值一律拒绝.
        //
        // An unsupported option is accepted only when it is explicitly null or an
        // empty array; every other value is rejected.
        if value.get() != "null" && value.get() != "[]" {
            return Err(format!("search service does not support option {key}"));
        }
    }
    let mut uses = 5;
    if let Some(value) = tool.get("max_uses") {
        uses = serde_json::from_str::<Option<i64>>(value.get())
            .map_err(|_| "invalid max_uses".to_owned())?
            .unwrap_or(uses);
    }
    if !(1..=8).contains(&uses) {
        return Err("search max_uses must be between 1 and 8".to_owned());
    }
    Ok(Some(SearchLimits {
        max_uses: uses as usize,
    }))
}

/// 解析客户端请求中的 stream 标志.
///
/// 字段缺失或为 null 时视为非流式, 取值不是布尔时返回错误, 与 Go 版 serveSearch 的
/// 检查相同.
///
/// Parses the stream flag of a client request.
///
/// A missing field or null counts as non-streaming, and a value that is not a boolean is
/// an error, matching the check in the Go serveSearch.
pub fn search_stream(fields: &RawObject) -> Result<bool, String> {
    match fields.get("stream") {
        Some(raw) => match serde_json::from_str::<Option<bool>>(raw.get()) {
            Ok(stream) => Ok(stream.unwrap_or(false)),
            Err(_) => Err("invalid stream flag".to_owned()),
        },
        None => Ok(false),
    }
}

/// 执行一次受限搜索编排, 返回终止轮次的模型消息.
///
/// 轮数比搜索预算多一: 最后一轮依据已经检索到的结果作答. model 是路由发往上游的
/// 模型名, 内部轮次始终使用它; max_body 是网关的请求体上限, 内部轮次超过该上限时
/// 返回 502.
///
/// Runs one bounded search orchestration and returns the terminal model message.
///
/// There is one round more than the search budget: the final round answers from the
/// results already retrieved. model is the name the route sends upstream, and every
/// internal round uses it; max_body is the gateway request body limit, and an internal
/// round exceeding it returns 502.
pub async fn run_search(
    fields: RawObject,
    limits: SearchLimits,
    model: &str,
    max_body: usize,
    turn: &dyn SearchTurn,
    search: &SearchService,
    headers: &HeaderMap,
) -> Result<RawObject, SearchFailure> {
    let rounds = limits.max_uses + 1;
    let mut run = SearchRun {
        max_body,
        turn,
        search,
        headers,
        limits,
        fields,
        model: model.to_owned(),
        budget: 0,
        messages: Vec::new(),
        selection: RawObject::new(),
        records: Vec::new(),
        totals: BTreeMap::new(),
        usage: RawObject::new(),
        uses: 0,
    };
    run.prepare()?;
    for _ in 0..rounds {
        if let Some(message) = run.round().await? {
            return Ok(message);
        }
    }
    Err(SearchFailure::internal(
        "search workflow exceeded round limit",
    ))
}

/// 服务一次搜索请求期间发生变化的所有值, 因此轮次循环可以把每个步骤表达为对该状态
/// 的方法调用.
///
/// Every value that evolves while one search request is served, so the round loop can
/// express each step as a method call on this state.
struct SearchRun<'a> {
    max_body: usize,
    turn: &'a dyn SearchTurn,
    search: &'a SearchService,
    headers: &'a HeaderMap,
    limits: SearchLimits,
    fields: RawObject,
    model: String,
    budget: i64,
    messages: Vec<Box<RawValue>>,
    selection: RawObject,
    records: Vec<Box<RawValue>>,
    totals: BTreeMap<&'static str, i64>,
    usage: RawObject,
    uses: usize,
}

/// 当前轮次中模型发起的一次已通过校验的工具调用.
///
/// One validated tool call from the model in the current round.
struct SearchCall {
    id: String,
    query: String,
}

impl SearchRun<'_> {
    /// 执行一轮: 调用模型, 校验并记账, 必要时检索并回放对话.
    ///
    /// 返回 Some 表示对话已经结束, 其中是终止轮次的模型消息.
    ///
    /// Runs one round: call the model, validate and account for it, and retrieve and
    /// replay the conversation when needed.
    ///
    /// Some means the conversation finished and carries the terminal model message.
    async fn round(&mut self) -> Result<Option<RawObject>, SearchFailure> {
        self.fields
            .insert("messages".to_owned(), raw(&self.messages)?);
        let message = self.model_turn().await?;
        let (content, calls) = self.account(&message)?;
        if calls.is_empty() {
            return self.finish(message, content).map(Some);
        }
        let results = self.retrieve(&calls).await?;
        self.replay(&message, results)?;
        Ok(None)
    }

    /// 校验客户端契约并把请求改写为内部搜索对话, 客户端自带的工具会被替换为唯一的
    /// 内部搜索函数.
    ///
    /// Validates the client contract and rewrites the request into the internal search
    /// conversation, where the client's own tools are replaced by the single internal
    /// search function.
    fn prepare(&mut self) -> Result<(), SearchFailure> {
        let budget = self
            .fields
            .get("max_tokens")
            .and_then(|raw| serde_json::from_str::<Option<i64>>(raw.get()).ok())
            .flatten()
            .filter(|budget| *budget >= 1);
        let Some(budget) = budget else {
            return Err(SearchFailure::at(
                StatusCode::BAD_REQUEST,
                "search requires positive max_tokens",
            ));
        };
        let messages = self
            .fields
            .get("messages")
            .and_then(|raw| serde_json::from_str::<Vec<Box<RawValue>>>(raw.get()).ok())
            .filter(|messages| !messages.is_empty());
        let Some(messages) = messages else {
            return Err(SearchFailure::at(
                StatusCode::BAD_REQUEST,
                "search requires messages",
            ));
        };
        self.budget = budget;
        self.messages = messages;
        crate::forward::cap_thinking_budget(&mut self.fields, budget)
            .map_err(|_| SearchFailure::internal("search workflow failed"))?;
        self.fields.insert("model".to_owned(), raw(&self.model)?);
        self.fields.insert("stream".to_owned(), raw(&false)?);
        self.fields
            .insert("tools".to_owned(), raw(&internal_search_tool())?);
        if let Some(choice) = self.fields.get("tool_choice") {
            self.selection = serde_json::from_str(choice.get()).unwrap_or_default();
        }
        // 强制工具选择改写为 auto. 客户端指向 web_search 的 tool 选择与 any 都会要求模型
        // 必须调用工具, 而部分模型直接拒绝 tool 与 any, 例如 OpenRouter 上的
        // claude-opus-5.5 返回 400 tool_choice: type "tool" and "any" are not supported
        // for this model., 强制选择因此会让搜索请求必然失败. 内部提示已经要求模型搜索,
        // 用 auto 让模型自行决定即可, 其余字段例如 disable_parallel_tool_use 保持不变.
        //
        // A forced tool choice is rewritten to auto. A client tool selection naming
        // web_search and any both require the model to call a tool, and some models reject
        // tool and any outright (claude-opus-5.5 on OpenRouter returns 400
        // tool_choice: type "tool" and "any" are not supported for this model.), so a forced
        // choice would make every search request fail. The internal prompt already asks the
        // model to search, so auto lets it decide, and every other field such as
        // disable_parallel_tool_use stays unchanged.
        let kind = raw_string(&self.selection, "type");
        if kind == "tool" || kind == "any" {
            self.selection.insert("type".to_owned(), raw(&"auto")?);
            self.selection.remove("name");
            self.fields
                .insert("tool_choice".to_owned(), raw(&self.selection)?);
        }
        // 关闭思考的请求在内部搜索请求中省略 thinking 字段, 让上游按模型默认行为处理. 部分
        // 模型不允许关闭推理: 截获的 Claude Code WebSearch 子请求带 thinking 为 type 等于
        // disabled, 经网关转给 OpenRouter 上的 claude-opus-5.5 时返回 400 Reasoning is
        // mandatory for this endpoint and cannot be disabled., 省略该字段时返回 200 并完成
        // 搜索. 省略对不要求推理的模型没有影响: 在带 moonshot_search 的两条普通模型路由上,
        // 省略与关闭两种情况下是否产生思考块完全一致. 其余 thinking 取值原样保留, 让模型按
        // 请求的档位思考; cap_thinking_budget 的预算改写已经在前面执行, 不受这里影响.
        //
        // A request disabling thinking omits the thinking field from the internal search
        // request, letting the upstream apply the model default. Some models forbid disabling
        // reasoning: the captured Claude Code WebSearch sub-request carried thinking with type
        // equal to disabled, and going through the gateway to claude-opus-5.5 on OpenRouter
        // returned 400 Reasoning is mandatory for this endpoint and cannot be disabled., while
        // omitting the field returned 200 and completed the search. Omitting it changes nothing
        // for models that do not require reasoning: on two ordinary moonshot_search routes,
        // thinking blocks appeared identically whether thinking was omitted or disabled. Every
        // other thinking value stays as requested, so the model thinks at the requested level;
        // the budget rewrite of cap_thinking_budget already ran above and is unaffected here.
        if let Some(thinking) = self.fields.get("thinking")
            && let Ok(parsed) = serde_json::from_str::<RawObject>(thinking.get())
            && raw_string(&parsed, "type") == "disabled"
        {
            self.fields.remove("thinking");
        }
        Ok(())
    }

    /// 执行一次带缓冲的模型调用.
    ///
    /// 只有这个受限辅助流程会缓冲模型响应, 并且它只访问与常规生成相同的上游.
    ///
    /// Performs one buffered model call.
    ///
    /// Only this bounded helper buffers a model response, and it reaches only the same
    /// upstream as ordinary generation.
    async fn model_turn(&mut self) -> Result<RawObject, SearchFailure> {
        let body = serde_json::to_vec(&self.fields)
            .map_err(|_| SearchFailure::internal("search conversation exceeds request limit"))?;
        if body.len() > self.max_body {
            return Err(SearchFailure::internal(
                "search conversation exceeds request limit",
            ));
        }
        let response = self.turn.turn(self.headers, Bytes::from(body)).await?;
        if response.status != StatusCode::OK {
            // 上游错误的原因必须透出给客户端: 固定文本无法区分模型拒绝强制工具选择, 配额
            // 耗尽与参数非法, 故障只能另行探测才能定位. 原因附在固定文本之后并记录一行
            // 日志; 日志只包含状态码, 模型名与原因, 不包含请求头, 请求体与密钥.
            //
            // The upstream error reason is surfaced to the client: the fixed text cannot tell
            // a model rejecting a forced tool choice from an exhausted quota or invalid
            // parameters, so a fault could only be located by separate probing. The reason
            // follows the fixed text and one log line records it; the log carries only the
            // status, the model name and the reason, never a header, body or key.
            let reason = upstream_reason(&response.body);
            aria_core::log!(
                "web search upstream error model={} status={} reason={}",
                self.model,
                response.status.as_u16(),
                reason
            );
            let message = if reason.is_empty() {
                "search workflow upstream returned an error".to_owned()
            } else {
                format!("search workflow upstream returned an error: {reason}")
            };
            return Err(SearchFailure {
                status: response.status,
                message,
                retry_after: response.retry_after,
            });
        }
        let message = serde_json::from_slice::<RawObject>(&response.body).ok();
        match message {
            Some(message) if raw_string(&message, "type") == "message" => Ok(message),
            _ => Err(SearchFailure::internal("invalid model search response")),
        }
    }

    /// 校验一条模型消息, 累加 token 用量, 并返回内容块以及模型请求的搜索调用.
    ///
    /// 整轮调用在任何检索执行之前完成校验, 因此被拒绝的轮次不执行搜索, 也不消耗预算.
    ///
    /// Validates one model message, accumulates token usage and returns the content
    /// blocks together with the search calls the model requested.
    ///
    /// The whole round is validated before any lookup runs, so a rejected round performs
    /// no search and spends no budget.
    fn account(
        &mut self,
        message: &RawObject,
    ) -> Result<(Vec<Box<RawValue>>, Vec<SearchCall>), SearchFailure> {
        self.usage = message
            .get("usage")
            .and_then(|raw| serde_json::from_str(raw.get()).ok())
            .unwrap_or_default();
        for name in TOKEN_COUNTERS {
            let Some(value) = self.usage.get(name) else {
                continue;
            };
            let count = serde_json::from_str::<i64>(value.get())
                .ok()
                .filter(|count| *count >= 0);
            let Some(count) = count else {
                return Err(SearchFailure::internal("invalid model token usage"));
            };
            let total = self.totals.entry(name).or_insert(0);
            let Some(sum) = total.checked_add(count) else {
                return Err(SearchFailure::internal("invalid model token usage"));
            };
            *total = sum;
        }
        let content = match message.get("content") {
            Some(raw) => match serde_json::from_str::<Option<Vec<Box<RawValue>>>>(raw.get()) {
                Ok(content) => content.unwrap_or_default(),
                Err(_) => return Err(SearchFailure::internal("invalid model content")),
            },
            None => return Err(SearchFailure::internal("invalid model content")),
        };
        let mut calls = Vec::new();
        let mut seen = BTreeSet::new();
        for block in &content {
            let call: BlockCall = serde_json::from_str(block.get())
                .map_err(|_| SearchFailure::internal("invalid model content block"))?;
            if call.kind != "tool_use" {
                continue;
            }
            if raw_string(message, "stop_reason") != "tool_use" || !seen.insert(call.id.clone()) {
                return Err(SearchFailure::internal(
                    "incomplete or duplicate search tool call",
                ));
            }
            let query = call
                .input
                .as_ref()
                .map(|input| input.query.clone())
                .unwrap_or_default();
            // 本轮已经通过校验的调用计入额度, 因此即使模型在同一轮请求两次搜索, 单轮
            // 也不会超出 max_uses.
            //
            // Calls already validated in this round count against the budget, so one
            // round cannot exceed max_uses even when the model asks twice.
            if call.name != SEARCH_FUNCTION_NAME
                || call.id.is_empty()
                || query.trim().is_empty()
                || query.len() > MAX_QUERY_BYTES
                || self.uses + calls.len() >= self.limits.max_uses
            {
                return Err(SearchFailure::internal(
                    "invalid search call or search budget exhausted",
                ));
            }
            calls.push(SearchCall { id: call.id, query });
        }
        Ok((content, calls))
    }

    /// 为每个已通过校验的调用执行一次检索, 并追加最终消息必须包含的服务端工具记录.
    ///
    /// Performs one lookup per validated call and appends the server-side tool records
    /// that the final message must contain.
    async fn retrieve(&mut self, calls: &[SearchCall]) -> Result<Vec<Value>, SearchFailure> {
        let mut results = Vec::with_capacity(calls.len());
        for call in calls {
            let identifier: [u8; 16] = rand::random();
            let server_id = format!("srvtoolu_{}", hex::encode(identifier));
            let hits = self.search.lookup(&call.query, &server_id).await?;
            self.uses += 1;
            aria_core::log!(
                "web search model={} service=moonshot sources={}",
                self.model,
                hits.len()
            );
            let found: Vec<Value> = hits
                .iter()
                .map(|hit| json!({"type": "web_search_result", "url": hit.url, "title": hit.title}))
                .collect();
            self.records.push(raw(&json!({
                "type": "server_tool_use",
                "id": server_id,
                "name": "web_search",
                "input": {"query": call.query},
            }))?);
            self.records.push(raw(&json!({
                "type": "web_search_tool_result",
                "tool_use_id": server_id,
                "content": found,
            }))?);
            let content = serde_json::to_string(&hits)
                .map_err(|_| SearchFailure::internal("search workflow failed"))?;
            results.push(json!({
                "type": "tool_result",
                "tool_use_id": call.id,
                "content": content,
            }));
        }
        Ok(results)
    }

    /// 使用检索结果继续对话.
    ///
    /// 助手内容按收到的形式转发, 其中包含提供方的思维内容与不透明签名, 并且只发送给
    /// 同一个模型. 客户端的 max_tokens 限制整段对话, 因此第一轮之后的每一轮都使用
    /// 此前轮次剩余的额度.
    ///
    /// Continues the conversation with the retrieved results.
    ///
    /// The assistant content is forwarded as received, including provider thinking and
    /// opaque signatures, and only to the same model. The client's max_tokens caps the
    /// whole conversation, so every round after the first draws on what earlier rounds
    /// left.
    fn replay(&mut self, message: &RawObject, results: Vec<Value>) -> Result<(), SearchFailure> {
        let mut assistant = RawObject::new();
        assistant.insert("role".to_owned(), raw(&"assistant")?);
        assistant.insert(
            "content".to_owned(),
            message
                .get("content")
                .cloned()
                .unwrap_or(raw(&Value::Null)?),
        );
        self.messages.push(raw(&assistant)?);
        self.messages
            .push(raw(&json!({"role": "user", "content": results}))?);
        let choice = if self.uses >= self.limits.max_uses {
            "none"
        } else {
            "auto"
        };
        self.selection.insert("type".to_owned(), raw(&choice)?);
        self.selection.remove("name");
        self.fields
            .insert("tool_choice".to_owned(), raw(&self.selection)?);
        let spent = self
            .totals
            .get("output_tokens")
            .copied()
            .unwrap_or_default();
        let remaining = self.budget - spent;
        if remaining <= 0 {
            return Err(SearchFailure::internal(
                "search output token budget exhausted",
            ));
        }
        self.fields
            .insert("max_tokens".to_owned(), raw(&remaining)?);
        crate::forward::cap_thinking_budget(&mut self.fields, remaining)
            .map_err(|_| SearchFailure::internal("search workflow failed"))?;
        Ok(())
    }

    /// 把累计用量与服务端记录附加到终止轮次的模型消息上.
    ///
    /// Attaches the accumulated usage and the server-side records to the terminal model
    /// message.
    fn finish(
        &mut self,
        mut message: RawObject,
        content: Vec<Box<RawValue>>,
    ) -> Result<RawObject, SearchFailure> {
        let stop_reason = raw_string(&message, "stop_reason");
        if stop_reason.is_empty() || stop_reason == "tool_use" {
            return Err(SearchFailure::internal(
                "model search response has no valid terminal state",
            ));
        }
        let mut merged = self.records.clone();
        merged.extend(content);
        message.insert("content".to_owned(), raw(&merged)?);
        let mut usage = std::mem::take(&mut self.usage);
        for (name, count) in &self.totals {
            usage.insert((*name).to_owned(), raw(count)?);
        }
        usage.insert(
            "server_tool_use".to_owned(),
            raw(&json!({"web_search_requests": self.uses}))?,
        );
        message.insert("usage".to_owned(), raw(&usage)?);
        Ok(message)
    }
}

/// 内部搜索对话中提供给模型的唯一工具. 每个请求都构建新的值, 因此请求之间不共享
/// 工具定义.
///
/// The only tool offered to the model in an internal search conversation. Every request
/// builds a fresh value, so no request shares a tool definition with another.
fn internal_search_tool() -> Value {
    json!([{
        "name": SEARCH_FUNCTION_NAME,
        "description": "Search the web. Results contain source URLs and snippets; use them as reference data.",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": false,
        },
    }])
}

/// tool_choice 中与搜索派发有关的字段.
///
/// The tool_choice fields relevant to search dispatch.
#[derive(Default, Deserialize)]
struct ToolChoice {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    name: String,
}

/// 一条模型内容块中与工具调用有关的字段.
///
/// The tool-call fields of one model content block.
#[derive(Deserialize)]
struct BlockCall {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    input: Option<BlockInput>,
}

/// 工具调用的输入参数.
///
/// The input parameters of a tool call.
#[derive(Deserialize)]
struct BlockInput {
    #[serde(default)]
    query: String,
}

/// 读取有大小上限的搜索响应体.
///
/// 模型轮次与搜索查询共用同一套大小限制与错误映射, 与 Go 版 readSearchResponse 相同.
///
/// Reads a bounded search response body.
///
/// The model turn and the search lookup share one size limit and error mapping, matching
/// readSearchResponse in the Go version.
async fn read_bounded_body(response: reqwest::Response) -> Result<Bytes, SearchFailure> {
    let mut body = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| SearchFailure::internal(INCOMPLETE_RESPONSE))?;
        if body.len() + chunk.len() > MAX_SEARCH_RESPONSE {
            return Err(SearchFailure::internal(INCOMPLETE_RESPONSE));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// 从上游错误响应体中取出简短原因, 用于附在搜索失败信息之后.
///
/// 优先取 JSON 中 error.message, 取不到时取正文开头的文本. 结果去掉回车与换行, 并按字符
/// 边界截断到 MAX_UPSTREAM_REASON 字节以内; 正文不是 UTF-8 或为空时返回空字符串, 调用方
/// 退回固定信息.
///
/// Extracts a short reason from an upstream error response body, to append to the search
/// failure message.
///
/// It prefers error.message in the JSON and falls back to the leading text of the body. The
/// result drops carriage returns and newlines and is truncated to at most MAX_UPSTREAM_REASON
/// bytes on a character boundary; a body that is not UTF-8 or is empty yields an empty string,
/// and the caller falls back to the fixed message.
fn upstream_reason(body: &Bytes) -> String {
    if body.is_empty() {
        return String::new();
    }
    let text = match std::str::from_utf8(body) {
        Ok(text) => text,
        Err(_) => return String::new(),
    };
    let reason = serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| text.to_owned());
    // 去掉回车与换行并按字符边界截断由 single_line 完成, 与 messages 路由的失败记录同一实现.
    //
    // Dropping CR/LF and truncating on a character boundary is done by single_line, the same
    // implementation the messages routes' failure records use.
    aria_core::log::single_line(&reason, MAX_UPSTREAM_REASON)
}

/// 取出响应中的 Retry-After 值.
///
/// Takes the Retry-After value of a response.
fn retry_after(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// 判断一条搜索结果能否作为来源链接使用.
///
/// Reports whether one search result is usable as a source link.
fn usable_source(url: &str) -> bool {
    match Url::parse(url) {
        Ok(parsed) => {
            parsed.username().is_empty()
                && parsed.password().is_none()
                && parsed.host().is_some()
                && (parsed.scheme() == "http" || parsed.scheme() == "https")
        }
        Err(_) => false,
    }
}

/// 返回原始 JSON 值对应的字符串, 取值不是 JSON 字符串时返回空字符串, 与 Go 版
/// stringValue 相同.
///
/// Returns the string form of a raw JSON value, or an empty string when the value is not
/// a JSON string, matching stringValue in the Go version.
fn raw_string(fields: &RawObject, name: &str) -> String {
    fields
        .get(name)
        .and_then(|raw| serde_json::from_str::<String>(raw.get()).ok())
        .unwrap_or_default()
}

/// 编码本模块构造的 JSON 值.
///
/// 这些值来自内存, 编码不会失败; 失败时按 Go 版 serveSearch 的兜底错误映射为 502
/// search workflow failed.
///
/// Encodes a JSON value built inside this module.
///
/// The value comes from memory and encoding cannot fail; a failure maps to the 502 search
/// workflow failed fallback of the Go serveSearch.
fn raw(value: &impl serde::Serialize) -> Result<Box<RawValue>, SearchFailure> {
    serde_json::value::to_raw_value(value)
        .map_err(|_| SearchFailure::internal("search workflow failed"))
}

/// 输出已经缓冲好的消息.
///
/// 搜索结果必须先完整生成才能对外输出, 因此只有这个辅助流程进行缓冲; 流式分支仍然
/// 产生增量事件, 目的是让客户端可见的事件顺序与原生流保持一致. 非流式分支与 Go 版
/// 一样在末尾附加一个换行, 因为 Go 的 json.Encoder 会这样做.
///
/// Emits the buffered message.
///
/// Search results must exist before they can be emitted, so only this helper buffers; the
/// streaming branch still produces delta events to keep the client-visible event order
/// identical to a native stream. Like the Go version, the non-streaming branch appends a
/// trailing newline because Go's json.Encoder does so.
pub fn write_search_message(message: &RawObject, stream: bool) -> Response<Body> {
    let mut response = Response::new(full(Vec::new()));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if !stream {
        let mut body = serde_json::to_vec(message).unwrap_or_default();
        body.push(b'\n');
        *response.body_mut() = full(body);
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        return response;
    }
    let events = encode_stream(message);
    *response.body_mut() = full(events.into_bytes());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
}

/// 把一条已完成的搜索消息编码为原生 Anthropic SSE 事件序列.
///
/// Encodes a finished search message as a native Anthropic SSE event sequence.
fn encode_stream(message: &RawObject) -> String {
    let mut events = String::new();
    // 编码失败时与 Go 版 emit 的行为一致: 停止输出, 已经写出的部分保留.
    //
    // On an encoding failure this matches the Go emit: stop emitting and keep what was
    // already written.
    let _ = encode_events(message, &mut events);
    events
}

/// 逐个写出事件, 任一取值无法编码时提前结束.
///
/// Writes the events one by one, stopping early when a value cannot be encoded.
fn encode_events(message: &RawObject, events: &mut String) -> Result<(), SearchFailure> {
    let usage: RawObject = message
        .get("usage")
        .and_then(|raw| serde_json::from_str(raw.get()).ok())
        .unwrap_or_default();
    // message_start 携带已完成消息中除内容本身与终止停止信息之外的每一个字段, 内容与
    // 终止信息随后到达.
    //
    // message_start carries every field of the finished message except the content
    // itself and the terminal stop information, which arrive later.
    let mut start = message.clone();
    start.insert("content".to_owned(), raw(&Vec::<Value>::new())?);
    start.insert("stop_reason".to_owned(), raw(&Value::Null)?);
    start.insert("stop_sequence".to_owned(), raw(&Value::Null)?);
    let mut initial_usage = usage.clone();
    initial_usage.insert("output_tokens".to_owned(), raw(&0)?);
    start.insert("usage".to_owned(), raw(&initial_usage)?);
    emit(events, "message_start", &[("message", raw(&start)?)])?;

    let blocks: Vec<RawObject> = message
        .get("content")
        .and_then(|raw| serde_json::from_str::<Option<Vec<RawObject>>>(raw.get()).ok())
        .flatten()
        .unwrap_or_default();
    for (index, mut block) in blocks.into_iter().enumerate() {
        let kind = raw_string(&block, "type");
        let text_field = match kind.as_str() {
            "text" => Some("text"),
            "thinking" => Some("thinking"),
            _ => None,
        };
        // 文本与思维内容拆分为开始, 增量与结束三个事件. 其他类型的块整体输出, 因为
        // 它们的内容本身已经是原子单位.
        //
        // Text and thinking are split into start, delta and stop events. Other block
        // types are emitted whole because their content is already atomic.
        let mut text = String::new();
        let mut signature = String::new();
        if let Some(field) = text_field {
            text = raw_string(&block, field);
            block.insert(field.to_owned(), raw(&"")?);
            signature = raw_string(&block, "signature");
            block.remove("signature");
        }
        emit(
            events,
            "content_block_start",
            &[("index", raw(&index)?), ("content_block", raw(&block)?)],
        )?;
        if let Some(field) = text_field
            && !text.is_empty()
        {
            let mut delta = RawObject::new();
            delta.insert("type".to_owned(), raw(&format!("{field}_delta"))?);
            delta.insert(field.to_owned(), raw(&text)?);
            emit(
                events,
                "content_block_delta",
                &[("index", raw(&index)?), ("delta", raw(&delta)?)],
            )?;
        }
        if !signature.is_empty() {
            let delta = json!({"type": "signature_delta", "signature": signature});
            emit(
                events,
                "content_block_delta",
                &[("index", raw(&index)?), ("delta", raw(&delta)?)],
            )?;
        }
        emit(events, "content_block_stop", &[("index", raw(&index)?)])?;
    }
    let mut delta = RawObject::new();
    delta.insert(
        "stop_reason".to_owned(),
        message
            .get("stop_reason")
            .cloned()
            .unwrap_or(raw(&Value::Null)?),
    );
    delta.insert(
        "stop_sequence".to_owned(),
        message
            .get("stop_sequence")
            .cloned()
            .unwrap_or(raw(&Value::Null)?),
    );
    emit(
        events,
        "message_delta",
        &[("delta", raw(&delta)?), ("usage", raw(&usage)?)],
    )?;
    emit(events, "message_stop", &[])?;
    Ok(())
}

/// 追加一个事件: 类型写入载荷后再编码, 与 Go 版 emit 相同.
///
/// Appends one event: the type is written into the payload before encoding, matching the
/// Go emit.
fn emit(
    events: &mut String,
    kind: &str,
    fields: &[(&str, Box<RawValue>)],
) -> Result<(), SearchFailure> {
    let mut payload = RawObject::new();
    payload.insert("type".to_owned(), raw(&kind)?);
    for (name, value) in fields {
        payload.insert((*name).to_owned(), value.clone());
    }
    let data = serde_json::to_string(&payload)
        .map_err(|_| SearchFailure::internal("search workflow failed"))?;
    events.push_str(&format!("event: {kind}\ndata: {data}\n\n"));
    Ok(())
}
