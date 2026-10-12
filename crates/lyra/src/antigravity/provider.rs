//! Antigravity 线路的常量, 会话键, 模型档位改写与上游请求构造.
//!
//! 本模块只负责构造请求, 不发送请求: 账号选择, 401 刷新重试与容量重试预算由 relay 模块承担.
//!
//! The Antigravity route's constants, session key, model tier rewriting and upstream request
//! construction. This module only builds requests and never sends them: account selection, the
//! 401 refresh retry and the capacity retry budget belong to the relay module.
//!
//! 请求头按小写写入. Go 版用 setRawHeader 绕过 http.Header 的首字母大写规范化, 保留 Python
//! 原实现发往上游的小写形态; Rust 的 http 头名本身就规范化为小写, 因此发出的形态与 Go 版一致.
//! HTTP 头名不区分大小写, 这一差异对上游没有影响.
//!
//! Request headers are written in lowercase. Go used setRawHeader to bypass http.Header's
//! capitalization and keep the lowercase form the original Python implementation sent; Rust's
//! header names are normalized to lowercase anyway, so the wire form matches Go's. HTTP header
//! names are case-insensitive, so this difference has no effect on upstream.

use http::HeaderMap;
use rand::TryRng;
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use url::Url;

use super::request::{
    AnthropicOutputConfig, AnthropicRequest, RawField, build_envelope, zero_on_null_list,
};
use super::response::AnthropicTranslator;
use super::search::web_search_options_for;
use crate::upstream::{BuildError, UpstreamRequest, insert_header};

/// 生成接口的路径. 上游只提供流式接口, 非流式响应由本端把事件流聚合成完整报文, 这一点与官方
/// 客户端一致.
///
/// The generation API path. Upstream only offers a streaming API, and a non-streaming response is
/// produced by aggregating the event stream locally, matching the official client.
pub const ANTIGRAVITY_SUFFIX: &str = "/v1internal:streamGenerateContent?alt=sse";

/// 信封里的两个固定值.
///
/// Two fixed values in the envelope.
pub const ANTIGRAVITY_REQUEST_TYPE: &str = "agent";
pub const ANTIGRAVITY_USER_AGENT_FIELD: &str = "antigravity";

/// 请求编号前缀与模型名上可能出现的线路前缀.
///
/// The request id prefix and the route prefix a client may put on a model name.
pub const ANTIGRAVITY_REQUEST_PREFIX: &str = "agent/";
pub const ANTIGRAVITY_MODEL_PREFIX: &str = "antigravity/";

/// 客户端没有给出 max_tokens 或给出非正值时使用的输出预算.
///
/// The output budget used when the client gives no max_tokens or a non-positive value.
pub const ANTIGRAVITY_DEFAULT_MAX_TOKENS: i64 = 8192;

/// 本线路接受的 POST 路径. 入口按去掉查询串的路径比较, 因此带查询串的请求同样命中.
///
/// The POST paths this route accepts. The entry point compares the path with the query string
/// removed, so a request carrying a query string matches as well.
pub const ANTIGRAVITY_POST_PATHS: [&str; 1] = ["/v1/messages"];

/// 触发换号的响应码.
///
/// 401 沿用先刷新并重试一次, 仍失败再换账号的流程, 429 沿用换账号策略. 403 不在表中: Go 版
/// 对 403 换号, Aria 改为冷却该账号且不换号, 见 architecture.md 5.11. 503 先按容量
/// 状态码在同账号有限重试, 仍失败才换号, 因此 503 同时出现在两张表里. 状态码本身不证明账号
/// 被封禁或额度已经用尽. 400 不重试: 缺少工具签名会返回 400, 该对照已对真实上游验证过.
/// 404 也换号: Opus 5.5
/// 分批开放期间, 未获该模型的账号返回 404, 同池其他账号仍可生成; 全部账号均返回 404 时保留
/// 最后的原始错误, 不降级到其他模型.
///
/// The response codes that trigger rotation. A 401 follows the flow of refreshing and retrying
/// once, then rotating if it still fails; 429 follows the rotation policy. 403 is not listed: Go
/// rotated on it, while Aria cools the account down without rotating, see section 5.11 of
/// architecture.md. A 503 first gets
/// a limited same-account retry and rotates only if it still fails, so 503 appears in both tables.
/// The status code alone does not prove an account is banned or its quota exhausted. A 400 is not
/// retried. A 404 also rotates: while Opus 5.5 rolled out in batches, an account without the model
/// returned 404 while other accounts could still generate; when every account returns 404 the last
/// raw error is kept rather than degrading to another model.
pub const ANTIGRAVITY_ROTATE_STATUSES: [u16; 4] = [401, 404, 429, 503];

/// 需要同账号有限重试的响应码.
///
/// 重试次数与等待首个有效候选的总时长都受本端限制, 不能保证上游恢复.
///
/// The response codes that get a limited same-account retry. Both the retry count and the total
/// wait for the first valid candidate are bounded locally and do not guarantee upstream recovery.
pub const ANTIGRAVITY_CAPACITY_STATUSES: [u16; 1] = [503];

/// 容量不足时的额外重试次数. 上游自身的排队与重试不计入.
///
/// The extra retry count for a capacity shortage; upstream's own queueing and retries are not
/// counted.
pub const ANTIGRAVITY_CAPACITY_RETRIES: u32 = 2;

/// 客户端可能用来标识会话的请求头, 按优先级排列.
///
/// The request headers a client may use to identify a session, ordered by priority.
pub const ANTIGRAVITY_SESSION_HEADERS: [&str; 3] =
    ["X-Claude-Code-Session-Id", "session_id", "conversation_id"];

/// 推导请求所属会话的标识, 供按会话固定账号使用.
///
/// 优先使用显式会话头. 取不到时回退为系统提示与首条用户消息的摘要, 这一步是必要的: 客户端经
/// LiteLLM 到达本中继时, 会话头不一定被转发, 而会话前缀在整段对话中保持不变, 足以把同一会话
/// 的请求归到一起. 两种来源用不同前缀区分, 避免取值碰撞. 摘要在原始字节上计算, 与 Go 版对
/// json.RawMessage 直接取摘要的行为一致.
///
/// Derives the identifier of the session a request belongs to, used for session account pinning.
/// Explicit session headers take priority. When none is available it falls back to a digest of the
/// system prompt and first user message, which is necessary because session headers are not always
/// forwarded when a client reaches this relay through LiteLLM, while the conversation prefix stays
/// unchanged across the whole conversation and is enough to group one session's requests together.
/// The two sources use different prefixes so their values cannot collide. The digest is computed
/// over the raw bytes, matching Go digesting json.RawMessage directly.
pub fn session_key(headers: &HeaderMap, body: &[u8]) -> String {
    for name in ANTIGRAVITY_SESSION_HEADERS {
        let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) else {
            continue;
        };
        let value = value.trim();
        if !value.is_empty() {
            return format!("h:{value}");
        }
    }

    /// 会话摘要只需要 system 与 messages 的原始字节.
    ///
    /// The session digest needs only the raw bytes of system and messages.
    #[derive(Deserialize)]
    struct SessionSource {
        #[serde(default)]
        system: RawField,
        #[serde(default, deserialize_with = "zero_on_null_list")]
        messages: Vec<Box<RawValue>>,
    }

    let Ok(source) = serde_json::from_slice::<SessionSource>(body) else {
        return String::new();
    };
    if source.messages.is_empty() {
        return String::new();
    }
    let mut digest = Sha256::new();
    // 字段存在时写入原始字节, 包括显式 null 的四个字节; 字段缺失时不写任何字节. 这与 Go 的
    // json.RawMessage 相同: 它把 null 捕获成文本而不是空值.
    //
    // When the field is present its raw bytes are written, including the four bytes of an explicit
    // null; when it is absent nothing is written. This matches Go's json.RawMessage, which captures
    // null as text rather than as an empty value.
    if let Some(system) = source.system.text() {
        digest.update(system.as_bytes());
    }
    digest.update([0u8]);
    digest.update(source.messages[0].get().as_bytes());
    let hash = digest.finalize();
    // 摘要固定为 32 字节, 取前 16 字节用 get 避免越界 panic.
    //
    // The digest is always 32 bytes; the first 16 are taken with get to avoid an
    // out-of-range panic.
    format!("b:{}", hex::encode(hash.get(..16).unwrap_or(&hash)))
}

/// 去掉线路前缀. 客户端可能发送带前缀的标识, 上游只认裸模型名.
///
/// Strips the route prefix. A client may send a prefixed identifier while upstream only accepts
/// the bare model name.
pub(crate) fn normalize_model(model: &str) -> &str {
    model
        .strip_prefix(ANTIGRAVITY_MODEL_PREFIX)
        .unwrap_or(model)
}

/// 上游模型标识里的档位后缀, 按思考强度从低到高排列.
///
/// The tier suffixes in upstream model identifiers, ordered from low to high thinking effort.
const ANTIGRAVITY_TIERS: [&str; 3] = ["low", "medium", "high"];

/// 把 effort 映射到 ANTIGRAVITY_TIERS 的下标. 上游没有高于 high 的档位, 因此 xhigh 与 max 都
/// 取 high.
///
/// Maps effort to an index of ANTIGRAVITY_TIERS. Upstream has no tier above high, so both xhigh
/// and max use high.
fn effort_tier(effort: &str) -> Option<usize> {
    match effort {
        "low" => Some(0),
        "medium" => Some(1),
        "high" | "xhigh" | "max" => Some(2),
        _ => None,
    }
}

/// 以档位后缀区分思考强度的模型族及其可用档位.
///
/// 上游把档位写进模型标识, 每个档位是独立模型, 默认思考预算不同: Gemini Flash 的 low, medium,
/// high 分别为 1000, 4000 与 -1, Claude 5.5 的三个档位取自其模型目录. 目录中 thinkingLevel
/// 只是模型元数据, 没有请求证据表明 thinkingConfig 接受该字段, 因此只改写模型标识, 不附加上游
/// 参数. Gemini 暂不在表内: 实测 low 档返回 503 "No capacity available", medium 档返回 502,
/// 随后未改写的 high 档也返回同样的容量不足, 低档能否正常生成尚无法确认; high 档的思考预算为
/// -1 即动态不设上限, 已是最高强度, 因此在确认低档可用之前保持请求的 high 档. 只有单一档位的
/// 模型同样不在表内.
///
/// The model families whose thinking effort is distinguished by a tier suffix, with their
/// available tiers. Upstream writes the tier into the model identifier and each tier is a
/// separate model with a different default thinking budget; the tier budgets come from the Gemini
/// Flash and Claude 5.5 model catalogs. thinkingLevel in the catalog is only model metadata and no
/// request evidence shows thinkingConfig accepting that field, so only the model identifier is
/// rewritten. Gemini is not in the table for now: the low tier returned 503 "No capacity
/// available", the medium tier returned 502, and the unrewritten high tier then returned the same
/// capacity error, so whether the lower tiers generate normally is not yet known; the high tier's
/// thinking budget of -1 is already dynamic and unbounded. Single-tier models are not in the table
/// either.
fn tiered_model(family: &str) -> Option<&'static [&'static str]> {
    match family {
        "claude-opus-5-5" | "claude-sonnet-5-5" => Some(&ANTIGRAVITY_TIERS),
        _ => None,
    }
}

/// 按客户端请求的思考强度改写模型标识的档位后缀.
///
/// 目标档位不存在时取该模型族中不高于目标的最高档位. 没有 effort, 取值未知或模型不在档位表中
/// 时原样返回, 保持旧行为. 回给客户端的模型名仍是客户端请求的标识, 只有上游信封使用改写结果.
///
/// Rewrites the tier suffix of a model identifier according to the requested thinking effort. When
/// the target tier does not exist, the highest tier of the family not above the target is used.
/// Without effort, with an unknown value, or for a model outside the table, the model is returned
/// unchanged. The model name returned to the client stays the identifier the client requested;
/// only the upstream envelope uses the rewritten result.
pub(crate) fn model_for_effort(model: &str, output: Option<&AnthropicOutputConfig>) -> String {
    let Some(output) = output else {
        return model.to_owned();
    };
    let Some(target) = effort_tier(&output.effort) else {
        return model.to_owned();
    };
    let Some(cut) = model.rfind('-') else {
        return model.to_owned();
    };
    // cut 是 ASCII 连字符的下标, 两侧的切片边界都落在字符边界上.
    //
    // cut is the index of an ASCII hyphen, so both slice boundaries fall on character
    // boundaries.
    let (family, current) = (&model[..cut], &model[cut + 1..]);
    let Some(available) = tiered_model(family) else {
        return model.to_owned();
    };
    if !available.contains(&current) {
        return model.to_owned();
    }
    for tier in ANTIGRAVITY_TIERS.iter().take(target + 1).rev() {
        if available.contains(tier) {
            return format!("{family}-{tier}");
        }
    }
    model.to_owned()
}

/// 从请求体中提取上游模型名.
///
/// 只解码 model 与 output_config 两个顶层字段, 不反序列化庞大的 messages 与 tools,
/// 避免在选号阶段重复解析完整请求体. 计算规则与 build_envelope 写入上游信封的结果一致.
///
/// Extracts the upstream model name from the request body. Only the top-level model and
/// output_config fields are decoded without deserializing the large messages and tools,
/// avoiding re-parsing the whole request during account selection. The computation matches
/// what build_envelope writes into the upstream envelope.
pub fn upstream_model(body: &[u8]) -> String {
    #[derive(Deserialize)]
    struct Shape {
        #[serde(default)]
        model: String,
        #[serde(default)]
        output_config: Option<AnthropicOutputConfig>,
    }
    match serde_json::from_slice::<Shape>(body) {
        Ok(shape) => model_for_effort(normalize_model(&shape.model), shape.output_config.as_ref()),
        Err(_) => String::new(),
    }
}

/// 返回把上游事件流转换成 Anthropic Messages 的翻译器.
///
/// stream 为真时逐事件回写; 为假时先聚合再一次性输出完整报文. 两种形态共用同一套解析逻辑,
/// 区别只在写出时机. 请求体解析失败或没有搜索工具时不启用搜索记录.
///
/// Returns the translator that converts the upstream event stream into Anthropic Messages. A true
/// stream writes per event; a false stream aggregates first and emits a complete message once.
/// Both forms share the same parsing logic and differ only in write timing. A body that fails to
/// parse or carries no search tool leaves the search record disabled.
pub fn translator_for(model: &str, stream: bool, body: &[u8]) -> AnthropicTranslator {
    let mut translator = AnthropicTranslator::new(model, stream);
    if let Ok(request) = serde_json::from_slice::<AnthropicRequest>(body) {
        // BuildRequest 已完成校验. 响应状态属于单次请求, 不能放入共享状态.
        //
        // BuildRequest has already validated the request; response state belongs to one request
        // and must not live in shared state.
        if let Ok(Some(options)) = web_search_options_for(&request) {
            translator.enable_web_search(options.required);
        }
    }
    translator
}

/// 构造一次上游请求.
///
/// 项目标识取自凭据, 因此需要与鉴权头一起传入的凭据内容; 鉴权头本身只含 Authorization. base
/// 是上游主机前缀, 末尾斜杠在这里去掉, 与 Go 版构造线路时的处理一致. user_agent 由调用方按
/// 3.8 的标识配置渲染, 取值在构造请求时确定.
///
/// Builds one upstream request. The project id comes from the credential, so it is passed
/// alongside the auth headers; the headers themselves contain only Authorization. base is the
/// upstream host prefix and a trailing slash is trimmed here, matching Go trimming it when the
/// route was constructed. user_agent is rendered by the caller from the 3.8 identifier
/// configuration; its value is fixed when the request is built.
pub fn build_request(
    base: &str,
    auth_headers: &[(&str, &str)],
    project_id: &str,
    body: &[u8],
    user_agent: &str,
) -> Result<UpstreamRequest, BuildError> {
    if project_id.is_empty() {
        return Err(BuildError::MissingProjectId);
    }
    let envelope = build_envelope(body, project_id).map_err(BuildError::InvalidRequest)?;
    let base = base.strip_suffix('/').unwrap_or(base);
    let url = Url::parse(&format!("{base}{ANTIGRAVITY_SUFFIX}"))
        .map_err(|_| BuildError::InvalidUpstream)?;
    let mut headers = HeaderMap::new();
    for (name, value) in auth_headers {
        insert_header(&mut headers, name, value)?;
    }
    insert_header(&mut headers, "Content-Type", "application/json")?;
    // 上游据此识别客户端. 取值来自官方客户端的实际请求, 不能替换成猜测值.
    //
    // Upstream identifies the client by this; the value comes from the official client's actual
    // request and must not be replaced with a guess.
    insert_header(&mut headers, "User-Agent", user_agent)?;
    Ok(UpstreamRequest {
        url,
        headers,
        body: envelope,
    })
}

/// 生成随机标识. 只需要在单次会话内不重复, 不要求全局唯一.
///
/// 随机源不可用时退化为全零, 与 Go 的 crypto/rand 失败分支一致, 后果只是请求编号重复, 不影响
/// 正确性. 这里用不会 panic 的 try_fill_bytes, 而不是在随机源失败时终止进程的填充方法.
///
/// Generates a random identifier. It only needs to be unique within one session and not globally.
/// When the random source is unavailable it degrades to all zeros, matching Go's crypto/rand
/// failure branch; the only consequence is a repeated request id. It uses the non-panicking
/// try_fill_bytes rather than the filling method that aborts on an entropy failure.
pub(crate) fn new_identifier() -> String {
    let mut buffer = [0u8; 16];
    if rand::rngs::SysRng.try_fill_bytes(&mut buffer).is_err() {
        return "00000000000000000000000000000000".to_owned();
    }
    hex::encode(buffer)
}
