//! Google 搜索请求校验, grounding 收集与搜索结果编码.
//!
//! 只接受已实测的独立基础搜索. Google 的原生 grounding 不支持与函数声明混用, 也没有可验证的
//! 域名与定位约束, 因此不能兑现的选项一律拒绝, 避免把服务端搜索降级成同名客户端函数. 每次
//! 请求只有一次 grounding 调用; max_uses 限制调用次数, 不限制 Google 在该调用内部生成的查询
//! 变体或结果数量.
//!
//! Validation of Google search requests, grounding collection and encoding of search results.
//! Only the standalone basic search that has been verified is accepted: Google's native
//! grounding supports neither mixing with function declarations nor any verifiable domain or
//! location constraint, so options that cannot be honored are rejected rather than degrading
//! server-side search into a same-named client function. Each request has exactly one grounding
//! call; max_uses bounds the number of calls and does not bound the query variants or result
//! count Google generates inside that call.

use std::collections::HashSet;

use serde::Deserialize;
use serde_json::{Map, Value, json};
use url::Url;

use super::request::{
    AnthropicBlock, AnthropicRequest, AnthropicTool, BLOCK_SERVER_TOOL_USE, BLOCK_TEXT,
    BLOCK_WEB_SEARCH_RESULT, InvalidRequest, ROLE_ASSISTANT, optional_string_field, zero_on_null,
    zero_on_null_list,
};
use super::response::TranslateError;

/// 唯一支持的搜索工具类型与名称.
///
/// The only supported search tool type and name.
pub(crate) const WEB_SEARCH_TOOL_TYPE: &str = "web_search_20250305";
const WEB_SEARCH_TOOL_NAME: &str = "web_search";

/// 搜索正文缓冲的上游事件上限, 与 Go 版相同.
///
/// The upstream event buffer limit for search bodies, the same as Go's.
pub(crate) const MAX_SEARCH_RESPONSE_BYTES: usize = 4 << 20;

/// 一次请求的搜索选项.
///
/// The search options of one request.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WebSearchOptions {
    /// 为真表示客户端要求本轮必须执行搜索.
    ///
    /// True means the client requires this turn to execute a search.
    pub(crate) required: bool,
}

/// 校验搜索工具并给出本次请求的搜索选项. 没有搜索工具时返回 None.
///
/// Validates the search tool and returns this request's search options. Returns None when there
/// is no search tool.
pub(crate) fn web_search_options_for(
    request: &AnthropicRequest,
) -> Result<Option<WebSearchOptions>, InvalidRequest> {
    let mut search: Option<&AnthropicTool> = None;
    for tool in &request.tools {
        if !tool.kind.starts_with("web_search_") {
            continue;
        }
        if tool.kind != WEB_SEARCH_TOOL_TYPE || tool.name != WEB_SEARCH_TOOL_NAME {
            return Err(InvalidRequest::new(
                "Gemini relay supports only web_search_20250305 named web_search",
            ));
        }
        if search.is_some() {
            return Err(InvalidRequest::new("duplicate web search tool"));
        }
        search = Some(tool);
    }
    let Some(search) = search else {
        return Ok(None);
    };
    if search.max_uses.is_some_and(|uses| uses < 1) {
        return Err(InvalidRequest::new(
            "web search max_uses must be a positive integer",
        ));
    }
    if !search.allowed_domains.is_empty() || !search.blocked_domains.is_empty() {
        return Err(InvalidRequest::new(
            "Antigravity cannot enforce allowed_domains or blocked_domains; domain filters are unsupported",
        ));
    }
    if search.user_location.present() {
        return Err(InvalidRequest::new(
            "web search user_location is unsupported by this Gemini relay",
        ));
    }
    if search.search_profile.present() {
        return Err(InvalidRequest::new(
            "web search search_profile is unsupported by this Gemini relay",
        ));
    }
    for caller in &search.allowed_callers {
        if caller != "direct" {
            return Err(InvalidRequest::new(
                "web search supports only direct calls, not code execution",
            ));
        }
    }

    let (choice_type, choice_name) = parse_tool_choice(request.tool_choice.text())?;
    if choice_type == "none" {
        return Ok(None);
    }
    if request.tools.len() != 1 {
        return Err(InvalidRequest::new(
            "Antigravity web search must be requested separately from client tools",
        ));
    }
    let mut options = WebSearchOptions { required: false };
    match choice_type.as_str() {
        "" | "auto" => {}
        "any" => options.required = true,
        "tool" => {
            if choice_name != WEB_SEARCH_TOOL_NAME {
                return Err(InvalidRequest::new(
                    "web search tool_choice names an undeclared tool",
                ));
            }
            options.required = true;
        }
        _ => {
            return Err(InvalidRequest::new("invalid web search tool_choice type"));
        }
    }
    Ok(Some(options))
}

/// 解析 tool_choice 的类型与名称.
///
/// 类型不符时 Go 的解码会失败并报错, 这里保持同样的结果; 缺失与 null 都得到空值.
///
/// Parses the type and name of tool_choice. A type mismatch makes Go's decode fail and error,
/// reproduced here; a missing field and null both give empty values.
fn parse_tool_choice(raw: Option<&str>) -> Result<(String, String), InvalidRequest> {
    let Some(raw) = raw else {
        return Ok((String::new(), String::new()));
    };
    let Ok(parsed) = serde_json::from_str::<Value>(raw) else {
        return Err(InvalidRequest::new("invalid web search tool_choice"));
    };
    let Value::Object(object) = parsed else {
        return if parsed.is_null() {
            Ok((String::new(), String::new()))
        } else {
            Err(InvalidRequest::new("invalid web search tool_choice"))
        };
    };
    let invalid = || InvalidRequest::new("invalid web search tool_choice");
    let kind = optional_string_field(&object, "type")
        .map_err(|_| invalid())?
        .unwrap_or_default();
    let name = optional_string_field(&object, "name")
        .map_err(|_| invalid())?
        .unwrap_or_default();
    Ok((kind, name))
}

/// 一条 grounding 来源.
///
/// One grounding source.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct GroundingChunk {
    #[serde(default)]
    pub(crate) web: Option<GroundingWeb>,
}

/// 来源的网页信息.
///
/// The web information of a source.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct GroundingWeb {
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) uri: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) title: String,
}

/// 一条引用: 正文片段与它引用的来源下标.
///
/// One citation: a body segment and the indices of the sources it cites.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct GroundingSupport {
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) segment: GroundingSegment,
    #[serde(
        rename = "groundingChunkIndices",
        default,
        deserialize_with = "zero_on_null_list"
    )]
    pub(crate) indices: Vec<i64>,
}

/// 被引用的正文片段.
///
/// The cited body segment.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct GroundingSegment {
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) text: String,
}

/// 上游事件里的 grounding 元数据.
///
/// The grounding metadata inside an upstream event.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct GroundingMetadata {
    #[serde(
        rename = "webSearchQueries",
        default,
        deserialize_with = "zero_on_null_list"
    )]
    pub(crate) queries: Vec<String>,
    #[serde(
        rename = "groundingChunks",
        default,
        deserialize_with = "zero_on_null_list"
    )]
    pub(crate) chunks: Vec<GroundingChunk>,
    #[serde(
        rename = "groundingSupports",
        default,
        deserialize_with = "zero_on_null_list"
    )]
    pub(crate) supports: Vec<GroundingSupport>,
}

/// 整段事件流中一次服务端搜索的状态.
///
/// The state of one server-side search accumulated across the whole event stream.
#[derive(Debug, Default)]
pub(crate) struct WebSearchResponse {
    required: bool,
    bytes: usize,
    text: String,
    queries: Vec<String>,
    chunks: Vec<GroundingChunk>,
    supports: Vec<GroundingSupport>,
    executed: bool,
}

impl WebSearchResponse {
    /// 构造一次搜索状态. required 为真时缺少执行证据必须报错.
    ///
    /// Builds one search state. When required is true, missing execution evidence must error.
    pub(crate) fn new(required: bool) -> Self {
        Self {
            required,
            ..Self::default()
        }
    }

    /// 累加一个上游事件的字节数, 返回是否超过缓冲上限.
    ///
    /// Adds one upstream event's byte count and reports whether the buffer limit is exceeded.
    pub(crate) fn add_event_bytes(&mut self, bytes: usize) -> bool {
        self.bytes = self.bytes.saturating_add(bytes);
        self.bytes > MAX_SEARCH_RESPONSE_BYTES
    }

    /// 累积末尾才到达的引用.
    ///
    /// Google 流式 chunks 是增量列表, supports 的索引指向整份候选的来源表, 因此不能在建表时
    /// 去重或重排.
    ///
    /// Accumulates citations that arrive only at the end. Google's streaming chunks form an
    /// incremental list and the indices in supports point into the whole candidate's source
    /// table, so they must not be deduplicated or reordered while building the table.
    pub(crate) fn collect_grounding(&mut self, metadata: Option<&GroundingMetadata>) {
        let Some(metadata) = metadata else {
            return;
        };
        for query in &metadata.queries {
            if !query.is_empty() && !self.queries.contains(query) {
                self.queries.push(query.clone());
            }
        }
        self.chunks.extend(metadata.chunks.iter().cloned());
        self.supports.extend(metadata.supports.iter().cloned());
    }

    /// 追加一段搜索正文. 引用通常只在最后一条事件到达, 因此正文先缓冲.
    ///
    /// Appends a search body segment. Citations usually arrive only in the last event, so the
    /// body is buffered first.
    pub(crate) fn push_text(&mut self, text: &str) {
        self.text.push_str(text);
    }

    /// 是否已经执行过搜索.
    ///
    /// Whether a search was executed.
    pub(crate) fn executed(&self) -> bool {
        self.executed
    }

    /// 在上游正常结束后组装搜索记录.
    ///
    /// 只有走到这里才宣称搜索成功, 保证超时, 截断和普通模型文字不会被当成检索结果. 一个
    /// Google grounding 调用可能报告多个查询变体, 这些查询共同产生一个来源集合, 上游没有提供
    /// 逐查询的结果归属, 因此保持为一个搜索批次.
    ///
    /// Assembles the search record only after upstream ends normally, so a timeout, truncation or
    /// ordinary model text is never claimed as a successful search. One Google grounding call may
    /// report several query variants that jointly produce one source set, and upstream provides
    /// no per-query result attribution, so they stay as one search batch.
    pub(crate) fn content(&mut self, message_id: &str) -> Result<Vec<Value>, TranslateError> {
        let mut results = Vec::with_capacity(self.chunks.len());
        let mut seen: HashSet<String> = HashSet::new();
        for chunk in &self.chunks {
            let Some(web) = chunk.web.as_ref() else {
                continue;
            };
            if !search_url(&web.uri) || !seen.insert(web.uri.clone()) {
                continue;
            }
            results.push(json!({
                "type": "web_search_result",
                "url": web.uri,
                "title": web.title,
            }));
        }
        self.executed = !self.queries.is_empty();
        if !self.executed && (self.required || !results.is_empty()) {
            return Err(TranslateError::new(
                "Google web search did not return executed queries; refusing an ungrounded search response",
            ));
        }
        let mut content = Vec::with_capacity(3);
        if self.executed {
            let id = format!(
                "srvtoolu_{}",
                message_id.strip_prefix("msg_").unwrap_or(message_id)
            );
            content.push(json!({
                "type": BLOCK_SERVER_TOOL_USE,
                "id": id,
                "name": WEB_SEARCH_TOOL_NAME,
                "input": {"query": self.queries.join("\n")},
            }));
            content.push(json!({
                "type": BLOCK_WEB_SEARCH_RESULT,
                "tool_use_id": id,
                "content": results,
            }));
        }
        if !self.text.is_empty() {
            let mut block = Map::new();
            block.insert("type".to_owned(), Value::String(BLOCK_TEXT.to_owned()));
            block.insert("text".to_owned(), Value::String(self.text.clone()));
            let mut citations = Vec::new();
            let mut seen_citations: HashSet<String> = HashSet::new();
            for support in &self.supports {
                // 用上游给出的原文检查引用, 不把 UTF-8 字节索引误当成字符索引.
                //
                // Citations are checked against the original text given by upstream, so a
                // UTF-8 byte index is not mistaken for a character index.
                if support.segment.text.is_empty() || !self.text.contains(&support.segment.text) {
                    continue;
                }
                for index in &support.indices {
                    let Ok(index) = usize::try_from(*index) else {
                        continue;
                    };
                    let Some(chunk) = self.chunks.get(index) else {
                        continue;
                    };
                    let Some(web) = chunk.web.as_ref() else {
                        continue;
                    };
                    if !seen.contains(&web.uri) {
                        continue;
                    }
                    let key = format!("{}\u{0}{}", web.uri, support.segment.text);
                    if !seen_citations.insert(key) {
                        continue;
                    }
                    citations.push(json!({
                        "type": "web_search_result_location",
                        "url": web.uri,
                        "title": web.title,
                        "cited_text": support.segment.text,
                    }));
                }
            }
            if !citations.is_empty() {
                block.insert("citations".to_owned(), Value::Array(citations));
            }
            content.push(Value::Object(block));
        }
        Ok(content)
    }
}

/// 判断来源地址是否可用. 只接受 http 与 https, 且必须有主机名与不含用户信息.
///
/// Decides whether a source URL is usable: only http and https, with a host name and without
/// user information.
fn search_url(raw: &str) -> bool {
    let Ok(parsed) = Url::parse(raw) else {
        return false;
    };
    matches!(parsed.scheme(), "https" | "http")
        && parsed.host_str().is_some_and(|host| !host.is_empty())
        && usable_authority(raw)
}

/// 判断地址文本的权限段是否可用: 必须写出主机, 且不能带任何用户信息.
///
/// Go 的判断是 `parsed.User == nil && parsed.Hostname() != ""`. 两种输入只能在原始文本上判断:
/// 空用户信息 `https://@example.com/x` 会得到非 nil 的 User(""), 而 url crate 把空用户名与空密码
/// 都解析成空值, 解析结果里看不出这个分隔符; 多一条斜杠的 `https:///x` 在 WHATWG 规则下会跳过多
/// 余的斜杠并把 x 当成主机, 而 Go 看到的权限段为空, 主机也为空, 因此该地址被丢弃.
///
/// Decides whether the authority text of an address is usable: a host must be written and no user
/// information may be present. Go checked `parsed.User == nil && parsed.Hostname() != ""`. Two inputs
/// can only be judged on the raw text: an empty userinfo such as `https://@example.com/x` yields a
/// non-nil User(""), while the url crate parses an empty user name and password into empty values and
/// hides the separator; and the extra slash in `https:///x` makes the WHATWG rules skip the surplus
/// slash and treat x as the host, while Go sees an empty authority and an empty host and drops it.
fn usable_authority(raw: &str) -> bool {
    let Some((_, rest)) = raw.split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    !authority.contains('@') && !authority.is_empty()
}

/// 将服务端搜索历史作为有明确标签的数据保留.
///
/// 它不是待执行的 Gemini functionCall, 也不伪造 Anthropic encrypted_content 或 Google 签名.
///
/// Preserves server-side search history as clearly labeled data. It is not a Gemini functionCall
/// to execute and it fabricates neither Anthropic encrypted_content nor a Google signature.
pub(crate) fn replay_web_search_block(
    role: &str,
    block: &AnthropicBlock,
) -> Result<Value, InvalidRequest> {
    if role != ROLE_ASSISTANT {
        return Err(InvalidRequest::new(
            "server web search history requires an assistant message",
        ));
    }
    let (label, raw) = if block.kind == BLOCK_SERVER_TOOL_USE {
        if block.name != WEB_SEARCH_TOOL_NAME {
            return Err(InvalidRequest::new("unsupported server tool in history"));
        }
        ("Previous web search query: ", block.input.text())
    } else {
        ("Previous web search result: ", block.content.text())
    };
    // Go 用 json.Valid 检查原始字节, 缺失的字段没有内容因而视为无效历史.
    //
    // Go checked the raw bytes with json.Valid; a missing field has no content and is therefore
    // invalid history.
    let Some(raw) = raw else {
        return Err(InvalidRequest::new("invalid server web search history"));
    };
    Ok(json!({"text": format!("{label}{raw}")}))
}
