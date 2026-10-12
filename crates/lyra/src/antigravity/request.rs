//! Anthropic Messages 请求到 Gemini GenerateContentRequest 的转换.
//!
//! 两套协议之间有四处真正的语义差异, 实现时必须逐处处理:
//!
//!   1. 角色名不同. Anthropic 用 user 与 assistant, Gemini 用 user 与 model.
//!   2. 工具调用与工具结果的表达不同. Anthropic 用带 id 的 tool_use 与按 id 引用的
//!      tool_result; Gemini 使用函数名及可选的调用 id. 转换时需要记住两种协议的对应
//!      关系, 并把历史载体中的真实签名还原到原 functionCall part.
//!   3. 工具结果必须是对象. Anthropic 允许结果是任意内容, Gemini 要求 response 是
//!      结构化对象, 因此非对象的结果会被包一层.
//!   4. 系统提示的位置不同. Anthropic 把它放在顶层 system, Gemini 放在
//!      systemInstruction, 形态是一个 role 为 user 的 content.
//!
//! 报文类型按 Go 版的结构体逐字段对应声明, 字段类型与可空性保持一致: Go 的 json 解码把
//! null 当作不赋值, 把类型不符当作解码失败, 因此这里用 zero_on_null 复现前者, 用具体类型
//! 复现后者. 需要按客户端写的字面量回传的数字用 serde_json::Number 承载, 避免整数被写成
//! 浮点数.
//!
//! Converts an Anthropic Messages request into a Gemini GenerateContentRequest. Four points
//! are the real semantic differences between the two protocols and each must be handled: role
//! names, the different expression of tool calls and results, the requirement that a tool
//! result be an object, and the different location of the system prompt.
//!
//! The message types mirror the Go structs field by field, including nullability: Go's json
//! decoder treats null as a no-op and a type mismatch as a decode failure, so zero_on_null
//! reproduces the former and concrete field types reproduce the latter. Numbers that must keep
//! the client's literal use serde_json::Number so an integer is never rewritten as a float.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use serde_json::{Map, Number, Value, json};

use super::provider::{
    ANTIGRAVITY_DEFAULT_MAX_TOKENS, ANTIGRAVITY_REQUEST_PREFIX, ANTIGRAVITY_REQUEST_TYPE,
    ANTIGRAVITY_USER_AGENT_FIELD, model_for_effort, new_identifier, normalize_model,
};
use super::search::{WEB_SEARCH_TOOL_TYPE, web_search_options_for};
use super::signatures::google_function_call_bridges;

/// 转换前即可确认的客户端输入错误.
///
/// 账号配置, 鉴权与连接错误不使用此类型, 避免被误报成客户端请求错误. 这一分工与 Go 的
/// ErrInvalidAnthropicRequest 相同: 调用方据此返回 Anthropic 400 而不是 502.
///
/// A client input error that is already certain before the conversion. Account
/// configuration, authentication and connection errors do not use this type, avoiding a
/// misreport as a client request error; the split matches Go's ErrInvalidAnthropicRequest so
/// the caller can answer with an Anthropic 400 instead of a 502.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidRequest {
    message: String,
}

impl InvalidRequest {
    /// 用具体原因构造错误. Display 会补上 Go 版相同的固定前缀.
    ///
    /// Builds the error from a concrete reason; Display adds the same fixed prefix Go used.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// 返回不含固定前缀的具体原因.
    ///
    /// Returns the concrete reason without the fixed prefix.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for InvalidRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid Anthropic request: {}", self.message)
    }
}

impl std::error::Error for InvalidRequest {}

/// 与 Go 一致地把 JSON null 当作不赋值: 解码到结构体字段时得到零值而不是类型错误.
///
/// Matches Go's rule that a JSON null is a no-op: decoding it into a struct field yields the
/// zero value instead of a type error.
pub(crate) fn zero_on_null<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// 与 Go 一致地把 JSON null 当作不赋值: 整个数组字段为 null 时得到空数组, 数组里的 null 元素得到
/// 零值. Go 的 json 解码器把两者都当成空操作, 因此 `"messages": [null]` 得到一个零值轮次而不是
/// 解码失败.
///
/// Matches Go's rule that a JSON null is a no-op: a null list field yields an empty list and a
/// null element yields the zero value. Go's json decoder treats both as no-ops, so
/// `"messages": [null]` gives one zero-value turn instead of a decode failure.
pub(crate) fn zero_on_null_list<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    let items = Option::<Vec<Option<T>>>::deserialize(deserializer)?;
    Ok(items
        .unwrap_or_default()
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect())
}

/// 原始 JSON 文本字段.
///
/// 与 Go 的 json.RawMessage 一致: 字段存在时保留客户端写的原始字节, 显式 null 得到文本 `null`
/// 而不是"缺失", 字段缺失时得到 None. serde 的 Option 会把 null 吸收成 None, 无法区分两者, 因此
/// 这里写自定义反序列化. 原始文本按 serde_json 的取值切片捕获, 前后空白不进入结果, 与 Go 捕获
/// RawMessage 的规则相同.
///
/// A raw JSON text field. Matches Go's json.RawMessage: a present field keeps the raw bytes the
/// client wrote, an explicit null yields the text `null` rather than "absent", and a missing field
/// yields None. Serde's Option would swallow null and lose the distinction, so this uses a custom
/// deserialization. The text is captured as serde_json's value slice, so surrounding whitespace is
/// not included, the same rule Go used when capturing a RawMessage.
#[derive(Debug, Default)]
pub(crate) struct RawField(pub(crate) Option<String>);

impl RawField {
    /// 返回原始文本; 字段缺失时为 None.
    ///
    /// Returns the raw text; None when the field is missing.
    pub(crate) fn text(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// 字段是否存在且不是 JSON null, 对应 Go 的 len(raw) > 0 && string(raw) != "null".
    ///
    /// Whether the field is present and is not JSON null, matching Go's
    /// len(raw) > 0 && string(raw) != "null".
    pub(crate) fn present(&self) -> bool {
        matches!(self.0.as_deref(), Some(text) if text != "null")
    }
}

impl<'de> Deserialize<'de> for RawField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Box<RawValue> 需要 serde_json 在分词阶段给出的原始切片, 因此只能从文本反序列化, 不能从
        // 已经解析成 Value 的结构再反序列化. 本模块的所有 RawField 都来自请求体文本.
        //
        // Box<RawValue> needs the raw slice serde_json produces while tokenizing, so it can only be
        // deserialized from text, never from an already parsed Value. Every RawField in this module
        // comes from the request body text.
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Ok(Self(Some(raw.get().to_owned())))
    }
}

/// 客户端发来的 Anthropic Messages 请求. 字段与 Go 的 AnthropicRequest 一一对应.
///
/// An Anthropic Messages request sent by the client; the fields correspond one to one with
/// Go's AnthropicRequest.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AnthropicRequest {
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) model: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) max_tokens: i64,
    #[serde(default)]
    pub(crate) system: RawField,
    #[serde(default, deserialize_with = "zero_on_null_list")]
    pub(crate) messages: Vec<AnthropicTurn>,
    #[serde(default, deserialize_with = "zero_on_null_list")]
    pub(crate) tools: Vec<AnthropicTool>,
    #[serde(default)]
    pub(crate) tool_choice: RawField,
    /// 采样参数保留客户端写的数字字面量: Go 用 json.Number 解码后再编码, 整数仍写成整数.
    ///
    /// The sampling parameters keep the client's numeric literal: Go decoded them as
    /// json.Number and wrote them back, so an integer stayed an integer.
    #[serde(default)]
    pub(crate) temperature: Option<Number>,
    #[serde(default)]
    pub(crate) top_p: Option<Number>,
    #[serde(default, deserialize_with = "zero_on_null_list")]
    pub(crate) stop_sequences: Vec<String>,
    // 只参与解码的类型校验: Go 的结构体同样声明该字段, 取值不符时整份请求解码失败.
    //
    // Only participates in the decode type check: Go declared the same field and a mismatched
    // value failed the whole request decode.
    #[allow(dead_code)]
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) stream: bool,
    #[serde(default)]
    pub(crate) thinking: Option<AnthropicThink>,
    #[serde(default)]
    pub(crate) output_config: Option<AnthropicOutputConfig>,
}

/// 扩展思考的开关与预算.
///
/// The switch and budget for extended thinking.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AnthropicThink {
    #[serde(rename = "type", default, deserialize_with = "zero_on_null")]
    pub(crate) kind: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) budget_tokens: i64,
}

/// 输出强度. Claude Code 以 thinking.type=adaptive 配合 effort 表达思考强度, 取值为
/// low, medium, high, xhigh 与 max.
///
/// The output effort. Claude Code expresses thinking effort with thinking.type=adaptive plus
/// effort, whose values are low, medium, high, xhigh and max.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AnthropicOutputConfig {
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) effort: String,
}

/// 一轮对话. content 既可以是字符串也可以是内容块数组.
///
/// One conversation turn; content may be a string or an array of content blocks.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AnthropicTurn {
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) role: String,
    #[serde(default)]
    pub(crate) content: RawField,
}

/// 客户端声明的工具. input_schema 就是 JSON Schema.
///
/// A tool declared by the client; input_schema is a JSON Schema.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AnthropicTool {
    #[serde(rename = "type", default, deserialize_with = "zero_on_null")]
    pub(crate) kind: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) name: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) description: String,
    #[serde(default)]
    pub(crate) input_schema: RawField,
    #[serde(default)]
    pub(crate) max_uses: Option<i64>,
    #[serde(default, deserialize_with = "zero_on_null_list")]
    pub(crate) allowed_domains: Vec<String>,
    #[serde(default, deserialize_with = "zero_on_null_list")]
    pub(crate) blocked_domains: Vec<String>,
    #[serde(default)]
    pub(crate) user_location: RawField,
    #[serde(default, deserialize_with = "zero_on_null_list")]
    pub(crate) allowed_callers: Vec<String>,
    #[serde(default)]
    pub(crate) search_profile: RawField,
}

/// 内容块的类型取值. 与 Go 的常量表相同, 供请求, 响应与搜索三个模块共用.
///
/// Content block type values, the same table Go used, shared by the request, response and
/// search modules.
pub(crate) const BLOCK_TEXT: &str = "text";
pub(crate) const BLOCK_IMAGE: &str = "image";
pub(crate) const BLOCK_TOOL_USE: &str = "tool_use";
pub(crate) const BLOCK_TOOL_RESULT: &str = "tool_result";
pub(crate) const BLOCK_THINKING: &str = "thinking";
pub(crate) const BLOCK_REDACTED_THINKING: &str = "redacted_thinking";
pub(crate) const BLOCK_SERVER_TOOL_USE: &str = "server_tool_use";
pub(crate) const BLOCK_WEB_SEARCH_RESULT: &str = "web_search_tool_result";

/// 对话角色. Gemini 只有 user 与 model 两个取值.
///
/// Conversation roles; Gemini has only the two values user and model.
pub(crate) const ROLE_USER: &str = "user";
pub(crate) const ROLE_MODEL: &str = "model";
pub(crate) const ROLE_ASSISTANT: &str = "assistant";

/// 官方为无签名 functionCall 提供的兜底取值. 出处: Vertex AI thought signatures 文档
/// (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/thought-signatures): "There are
/// rare cases where you need to provide functionCall parts that were not generated by the API and
/// therefore don't have an associated thought signature (for example, when transferring history
/// from a model that does not include thought signatures). You can set thought_signature to
/// skip_thought_signature_validator, but, this should be a last resort as it will negatively
/// impact model performance." 本反代只在历史确实由不产生 Google 签名的模型生成、没有载体可还原
/// 时使用它; 有载体却不带签名属于往返缺陷, 必须修复而不是用占位值掩盖, 因为占位值会让上游跳过
/// 校验并降低模型表现.
///
/// The official fallback value for a functionCall without a signature, per the Vertex AI thought
/// signatures guide: it exists for functionCall parts that were not generated by the API (for
/// example, history transferred from a model that does not include thought signatures), and it
/// should be a last resort because it negatively impacts model performance. This relay uses it
/// only when the history was genuinely produced by a model that never had a Google signature and
/// no carrier exists to restore; a carrier that lost its signature is a round-trip defect to fix,
/// never to mask with the placeholder.
const GOOGLE_SKIP_THOUGHT_SIGNATURE_VALIDATOR: &str = "skip_thought_signature_validator";

/// 一个内容块. 各类型用到不同子集, 因此合在一个结构里, 与 Go 的 AnthropicBlock 相同.
///
/// One content block. Each type uses a different subset, so they are combined in one struct,
/// as in Go's AnthropicBlock.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AnthropicBlock {
    #[serde(rename = "type", default, deserialize_with = "zero_on_null")]
    pub(crate) kind: String,

    /// text 与 thinking 共用.
    ///
    /// Shared by text and thinking.
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) text: String,
    // thinking 与 is_error 只参与解码的类型校验, 与 Go 的结构体字段一致.
    //
    // thinking and is_error only participate in the decode type check, matching Go's struct
    // fields.
    #[allow(dead_code)]
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) thinking: String,
    /// redacted_thinking 的不透明数据由客户端原样回传.
    ///
    /// The opaque data of redacted_thinking, passed back by the client unchanged.
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) data: String,

    /// 图片内容的来源.
    ///
    /// Source of image content.
    #[serde(default)]
    pub(crate) source: Option<AnthropicSource>,

    /// 工具调用身份与参数.
    ///
    /// Tool call identity and arguments.
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) id: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) input: RawField,

    /// 工具结果与原调用的关联.
    ///
    /// Tool result and its association with the original call.
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) tool_use_id: String,
    #[serde(default)]
    pub(crate) content: RawField,
    #[allow(dead_code)]
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) is_error: bool,
}

/// 图片的承载方式. base64 是唯一需要支持的一种.
///
/// How an image is carried; base64 is the only form that needs support.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AnthropicSource {
    // type 只参与解码的类型校验, 与 Go 的结构体字段一致.
    //
    // type only participates in the decode type check, matching Go's struct field.
    #[allow(dead_code)]
    #[serde(rename = "type", default, deserialize_with = "zero_on_null")]
    pub(crate) kind: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) media_type: String,
    #[serde(default, deserialize_with = "zero_on_null")]
    pub(crate) data: String,
}

/// 组装上游信封. 信封的字段名与嵌套关系取自官方客户端的实际请求: 项目标识, 请求编号,
/// 客户端标识, 请求类型与模型平级, 真正的 Gemini 载荷全部放在 request 对象内.
///
/// Assembles the upstream envelope. Its field names and nesting come from the official
/// client's actual request: project id, request id, client identifier, request type and model
/// are siblings, and the real Gemini payload sits entirely inside the request object.
pub fn build_envelope(body: &[u8], project_id: &str) -> Result<Vec<u8>, InvalidRequest> {
    let request = decode_request(body)
        .map_err(|err| InvalidRequest::new(format!("解析 Anthropic 请求失败: {err}")))?;
    if request.messages.is_empty() {
        return Err(InvalidRequest::new("请求中没有 messages"));
    }
    let model = model_for_effort(
        normalize_model(&request.model),
        request.output_config.as_ref(),
    );
    let inner = convert_anthropic_request(&request, &model)?;
    let mut envelope = Map::new();
    envelope.insert("project".to_owned(), Value::String(project_id.to_owned()));
    envelope.insert(
        "requestId".to_owned(),
        Value::String(format!("{ANTIGRAVITY_REQUEST_PREFIX}{}", new_identifier())),
    );
    envelope.insert(
        "userAgent".to_owned(),
        Value::String(ANTIGRAVITY_USER_AGENT_FIELD.to_owned()),
    );
    envelope.insert(
        "requestType".to_owned(),
        Value::String(ANTIGRAVITY_REQUEST_TYPE.to_owned()),
    );
    envelope.insert("model".to_owned(), Value::String(model));
    envelope.insert("request".to_owned(), Value::Object(inner));
    // 信封只包含已解析过的 JSON 值, 序列化不会失败; Go 版同样把编码错误直接返回.
    //
    // The envelope only holds already parsed JSON values, so serialization cannot fail; Go
    // likewise returned an encoding error unchanged.
    serde_json::to_vec(&Value::Object(envelope))
        .map_err(|err| InvalidRequest::new(format!("编码上游信封失败: {err}")))
}

/// 解析请求体. Go 的 json.Decoder.Decode 只消费第一个 JSON 值, 其后的内容不参与解析,
/// 这里用流式迭代器保持同样的语义; 顶层为 null 时 Go 得到零值请求, 这里同样处理.
///
/// Parses the request body. Go's json.Decoder.Decode consumes only the first JSON value and
/// ignores anything after it, which the streaming iterator reproduces; a top-level null gives
/// Go a zero-value request, handled the same way here.
fn decode_request(body: &[u8]) -> Result<AnthropicRequest, serde_json::Error> {
    let mut values =
        serde_json::Deserializer::from_slice(body).into_iter::<Option<AnthropicRequest>>();
    match values.next() {
        Some(Ok(request)) => Ok(request.unwrap_or_default()),
        Some(Err(err)) => Err(err),
        // 空输入与纯空白没有可解析的值, 交给标准解析器给出语法错误.
        //
        // Empty or whitespace-only input has no value to parse; the standard parser produces
        // the syntax error.
        None => serde_json::from_slice::<AnthropicRequest>(body),
    }
}

/// 判断上游模型是否属于 Gemini 3 系列.
///
/// 只有 Gemini 3 系列在回放时强制校验 functionCall 的 thought_signature, 其他系列收到不带签名
/// 的 functionCall 不会报错, 因此无签名历史的兜底只对 Gemini 3 系列生效. 依据是 Vertex AI
/// thought signatures 文档: "Gemini 3 models enforce stricter validation on thought signatures
/// than previous Gemini versions" 与 "If a required thought signature is not returned when using
/// Gemini 3 models, the model will return a 400 error." 上游模型目录中的 Gemini 3 系列标识都以
/// gemini-3 开头, 例如 gemini-3-flash 与 gemini-3.8-flash-high.
///
/// Whether the upstream model belongs to the Gemini 3 series. Only that series enforces the
/// functionCall thought_signature on replay; other series accept unsigned functionCall parts, so
/// the fallback for carrier-less history applies to the Gemini 3 series only. The evidence is the
/// Vertex AI thought signatures guide: Gemini 3 models enforce stricter validation and return a
/// 400 error when a required signature is missing. Every Gemini 3 identifier in the upstream model
/// catalog starts with gemini-3, for example gemini-3-flash and gemini-3.8-flash-high.
fn is_gemini_3_series(model: &str) -> bool {
    model.starts_with("gemini-3")
}

/// 把 Anthropic 请求转换成上游信封中 request 字段的内容.
///
/// resolved_model 是已经过归一化与 effort 调整的上游模型名, 只有回放目标属于 Gemini 3 系列时
/// 才允许对无签名的 functionCall 使用官方兜底值.
///
/// Converts the Anthropic request into the content of the envelope's request field. resolved_model
/// is the upstream model name after normalization and effort adjustment; the official fallback for
/// functionCall parts without a signature is allowed only when the replay target belongs to the
/// Gemini 3 series.
fn convert_anthropic_request(
    request: &AnthropicRequest,
    resolved_model: &str,
) -> Result<Map<String, Value>, InvalidRequest> {
    let search = web_search_options_for(request)?;
    let contents = convert_turns(&request.messages, is_gemini_3_series(resolved_model))?;
    let mut inner = Map::new();
    inner.insert("contents".to_owned(), Value::Array(contents));

    if let Some(system) = convert_system(request.system.text()) {
        inner.insert("systemInstruction".to_owned(), system);
    }
    let config = convert_generation_config(request);
    if !config.is_empty() {
        inner.insert("generationConfig".to_owned(), Value::Object(config));
    }
    match search {
        Some(options) => {
            inner.insert("tools".to_owned(), json!([{"googleSearch": {}}]));
            // 搜索与函数调用是两种上游能力. ANY/allowedFunctionNames 会把搜索误当成客户端
            // 函数, 因此不向 Google 搜索请求附加 functionCallingConfig.
            //
            // Search and function calling are two different upstream capabilities.
            // ANY/allowedFunctionNames would mistake search for a client function, so no
            // functionCallingConfig is attached to a Google search request.
            if options.required {
                append_search_instruction(&mut inner);
            }
        }
        None => {
            let declarations = convert_tools(&request.tools);
            if !declarations.is_empty() {
                inner.insert(
                    "tools".to_owned(),
                    json!([{"functionDeclarations": declarations}]),
                );
                if let Some(tool_config) = convert_tool_choice(request.tool_choice.text()) {
                    inner.insert("toolConfig".to_owned(), tool_config);
                }
            }
        }
    }
    Ok(inner)
}

/// 强制搜索时追加的固定指令. 取值与 Go 版逐字相同.
///
/// The fixed instruction appended when search is required; the wording matches Go exactly.
const SEARCH_REQUIRED_INSTRUCTION: &str = "Use Google Search for this request and ground the answer in retrieved sources. Do not answer from memory when search is required.";

/// 把强制搜索的指令追加到系统提示. 已有系统提示时在其后追加, 没有时新建一个空的
/// role=user 内容, 与 Go 版一致.
///
/// Appends the required-search instruction to the system prompt, after an existing one or in a
/// new empty role=user content, matching Go.
fn append_search_instruction(inner: &mut Map<String, Value>) {
    let mut instruction = match inner.remove("systemInstruction") {
        Some(Value::Object(existing)) => existing,
        _ => {
            let mut created = Map::new();
            created.insert("role".to_owned(), json!(ROLE_USER));
            created.insert("parts".to_owned(), Value::Array(Vec::new()));
            created
        }
    };
    match instruction.get_mut("parts") {
        Some(Value::Array(parts)) => parts.push(json!({"text": SEARCH_REQUIRED_INSTRUCTION})),
        _ => {
            instruction.insert(
                "parts".to_owned(),
                Value::Array(vec![json!({"text": SEARCH_REQUIRED_INSTRUCTION})]),
            );
        }
    }
    inner.insert("systemInstruction".to_owned(), Value::Object(instruction));
}

/// 把顶层 system 转成 systemInstruction. 两种输入形态都要接受: 字符串, 或内容块数组.
///
/// Converts the top-level system into systemInstruction. Both input shapes must be accepted: a
/// string or an array of content blocks.
fn convert_system(raw: Option<&str>) -> Option<Value> {
    let raw = raw?;
    let text = match serde_json::from_str::<Value>(raw) {
        // Go 先把字符串形态解码成字符串; 解析失败或不是字符串时按内容块数组处理.
        //
        // Go first decoded the string form; a value that fails to parse or is not a string is
        // handled as an array of content blocks.
        Ok(Value::String(text)) => normalize_antigravity_system_text(&text),
        // Go 把 null 解码成空字符串, 再按空白内容处理.
        //
        // Go decoded null into the empty string and then treated it as blank content.
        Ok(Value::Null) => String::new(),
        Ok(_) => {
            let blocks = parse_blocks(raw).ok()?;
            let parts: Vec<String> = blocks
                .iter()
                .filter(|block| block.kind == BLOCK_TEXT)
                .map(|block| normalize_antigravity_system_text(&block.text))
                .filter(|text| !text.is_empty())
                .collect();
            parts.join("\n\n")
        }
        // 请求体本身是合法 JSON, 因此这里不会出现解析失败; Go 的 decodeBlocks 在非数组形态上报错
        // 并返回空, 这里同样返回 None.
        //
        // The body itself is valid JSON, so parsing cannot fail here; Go's decodeBlocks errored on a
        // non-array shape and returned nothing, which is also None here.
        Err(_) => return None,
    };
    if text.trim().is_empty() {
        return None;
    }
    Some(json!({"role": ROLE_USER, "parts": [{"text": text}]}))
}

/// 客户端元数据行的前缀.
///
/// The prefix of the client metadata line.
const BILLING_HEADER_PREFIX: &str = "x-anthropic-billing-header:";

/// 客户端固定身份句. 必须整句匹配, SDKcustom 之类的延伸词不属于身份声明.
///
/// The client's fixed identity sentences. The whole sentence must match; extensions such as
/// SDKcustom are not part of the identity declaration.
const SDK_IDENTITY: &str = "You are a Claude agent, built on Anthropic's Claude Agent SDK";
const CLI_IDENTITY: &str = "You are Claude Code, Anthropic's official CLI for Claude";

/// 替换后的身份句.
///
/// The replacement identity sentence.
const NEUTRAL_IDENTITY: &str = "You are an AI agent.";

/// 去掉系统块开头的客户端元数据与固定身份句.
///
/// 上游只接受去掉这两项之后的请求: 同一真实请求仅移除其中一项仍返回 429, 同时处理两项才
/// 返回 200, 该结论由对真实上游的对照请求验证. 必须在拼接内容块之前逐块处理, 才能保留每个块
/// 内的其余指令与原始空白. 此兼容处理仅用于系统提示; 用户消息与工具描述中的相同文本仍是
/// 用户内容.
///
/// Removes the client metadata and fixed identity sentence at the start of a system block.
/// Upstream accepts the request only when both are handled: removing just one still returns
/// 429, and handling both returns 200. It must run per block before the blocks are joined so
/// the remaining instructions and original whitespace inside each block survive. This
/// compatibility handling applies only to the system prompt; the same text in user messages and
/// tool descriptions remains user content.
fn normalize_antigravity_system_text(text: &str) -> String {
    let mut text = text;
    let leading = text.len() - trim_leading_ascii_space(text).len();
    if trim_leading_ascii_space(text).starts_with(BILLING_HEADER_PREFIX) {
        let trimmed = &text[leading..];
        let Some(end) = trimmed.find(['\r', '\n']) else {
            // 整块只有元数据行, 没有后续指令.
            //
            // The block holds nothing but the metadata line.
            return String::new();
        };
        // end 是 \r 或 \n 的下标, 两者都是单字节, end + 1 与 end 都落在字符边界上.
        //
        // end is the index of \r or \n, both single-byte, so end and end + 1 fall on
        // character boundaries.
        let mut rest = &trimmed[end + 1..];
        if trimmed.as_bytes()[end] == b'\r' {
            rest = rest.strip_prefix('\n').unwrap_or(rest);
        }
        text = rest;
    }

    let leading = text.len() - trim_leading_ascii_space(text).len();
    let trimmed = &text[leading..];
    for identity in [SDK_IDENTITY, CLI_IDENTITY] {
        let Some(rest) = trimmed.strip_prefix(identity) else {
            continue;
        };
        let rest = match rest.strip_prefix('.') {
            Some(rest) => rest,
            // 身份句后没有标点时也接受, 与 Go 的 rest == "" 分支一致.
            //
            // An identity sentence with no period is also accepted, matching Go's rest == ""
            // branch.
            None if rest.is_empty() => rest,
            None => continue,
        };
        // 保留身份句之前的原始空白, 与 Go 的 text[:len(text)-len(trimmed)] 前缀拼接一致.
        //
        // Keeps the original whitespace before the identity sentence, matching Go's
        // text[:len(text)-len(trimmed)] prefix concatenation.
        // leading 是已去掉的 ASCII 空白长度, 因此是字符边界.
        //
        // leading is the length of the ASCII whitespace already trimmed, so it is a character
        // boundary.
        return format!("{}{NEUTRAL_IDENTITY}{rest}", &text[..leading]);
    }
    text.to_owned()
}

/// 去掉开头的 ASCII 空白, 与 Go 的 strings.TrimLeft(text, " \t\r\n") 相同.
///
/// Trims leading ASCII whitespace, matching Go's strings.TrimLeft(text, " \t\r\n").
fn trim_leading_ascii_space(text: &str) -> &str {
    text.trim_start_matches([' ', '\t', '\r', '\n'])
}

/// 一次工具调用在转换中的对应关系, 用于把 tool_result 还原成函数名与上游调用 id.
///
/// The correspondence of one tool call during conversion, used to restore a tool_result into
/// the function name and upstream call id.
#[derive(Debug, Clone)]
struct GeminiToolReference {
    name: String,
    id: String,
}

/// 把对话轮次转成 contents.
///
/// Gemini 要求相邻内容的角色交替出现, 而 Anthropic 允许连续两条同角色消息, 因此遇到同角色
/// 的相邻轮次时把内容块并入上一轮, 而不是新起一轮. 调用对应表只存在于本次历史转换中, 无需
/// 跨请求缓存. thought_signature_fallback 表示回放目标是否需要无签名 functionCall 的官方兜底.
///
/// Converts conversation turns into contents. Gemini requires adjacent contents to alternate
/// roles while Anthropic allows two consecutive messages of the same role, so adjacent turns of
/// the same role merge their content blocks into the previous turn instead of starting a new
/// one. The call correspondence table exists only for this history conversion and needs no
/// cross-request caching. thought_signature_fallback tells whether the replay target needs the
/// official fallback for functionCall parts without a signature.
fn convert_turns(
    turns: &[AnthropicTurn],
    thought_signature_fallback: bool,
) -> Result<Vec<Value>, InvalidRequest> {
    let mut contents: Vec<Value> = Vec::with_capacity(turns.len());
    let mut tool_calls: HashMap<String, GeminiToolReference> = HashMap::new();

    for turn in turns {
        let role = if turn.role == ROLE_ASSISTANT {
            ROLE_MODEL
        } else {
            ROLE_USER
        };
        let parts = convert_turn_parts(turn, &mut tool_calls, thought_signature_fallback)?;
        if parts.is_empty() {
            continue;
        }
        if let Some(Value::Object(last)) = contents.last_mut() {
            let same_role =
                matches!(last.get("role"), Some(Value::String(existing)) if existing == role);
            if same_role && let Some(Value::Array(existing)) = last.get_mut("parts") {
                existing.extend(parts);
                continue;
            }
        }
        contents.push(json!({"role": role, "parts": parts}));
    }
    Ok(contents)
}

/// 转换一轮里的全部内容块.
///
/// Converts every content block in one turn.
fn convert_turn_parts(
    turn: &AnthropicTurn,
    tool_calls: &mut HashMap<String, GeminiToolReference>,
    thought_signature_fallback: bool,
) -> Result<Vec<Value>, InvalidRequest> {
    let Some(content) = turn.content.text() else {
        return Ok(Vec::new());
    };
    match serde_json::from_str::<Value>(content) {
        // Go 把 null 与缺失都当成空内容, 空字符串同样不产生内容段.
        //
        // Go treats null and a missing field as empty content, and an empty string likewise
        // produces no content segment.
        Ok(Value::Null) => return Ok(Vec::new()),
        Ok(Value::String(text)) => {
            if text.trim().is_empty() {
                return Ok(Vec::new());
            }
            return Ok(vec![json!({"text": text})]);
        }
        Ok(_) => {}
        // 请求体本身是合法 JSON, 解析不会失败; Go 在非字符串且非数组时报错.
        //
        // The body itself is valid JSON so this cannot fail; Go errored when the value was neither a
        // string nor an array.
        Err(err) => return Err(InvalidRequest::new(format!("解析内容块失败: {err}"))),
    }

    let blocks = parse_blocks(content)?;
    let bridges = google_function_call_bridges(turn, &blocks)?;
    // 兜底只补每步的第一个 functionCall part. 官方规则是: 并行调用时只有第一个 part 带签名,
    // 跨多步的连续调用每一步各自带签名, 因此同一步内靠后的 functionCall 不补.
    //
    // The fallback only fills the first functionCall part of each step. The official rule is that
    // for parallel calls only the first part carries a signature and each step of sequential
    // calls carries its own, so later functionCall parts in the same step are left alone.
    let mut function_call_seen = false;
    let mut parts = Vec::with_capacity(blocks.len());
    for block in &blocks {
        match block.kind.as_str() {
            BLOCK_TEXT => {
                if !block.text.is_empty() {
                    parts.push(json!({"text": block.text}));
                }
            }
            BLOCK_IMAGE => {
                let Some(source) = block.source.as_ref() else {
                    continue;
                };
                if source.data.is_empty() {
                    continue;
                }
                let media_type = if source.media_type.is_empty() {
                    "image/png"
                } else {
                    source.media_type.as_str()
                };
                parts.push(json!({
                    "inlineData": {"mimeType": media_type, "data": source.data},
                }));
            }
            BLOCK_TOOL_USE => {
                let bridge = bridges.get(&block.id);
                if !block.name.is_empty() {
                    tool_calls.insert(
                        block.id.clone(),
                        GeminiToolReference {
                            name: block.name.clone(),
                            id: bridge
                                .map(|found| found.function_call_id.clone())
                                .unwrap_or_default(),
                        },
                    );
                }
                // input 缺失时用空对象; 存在时按原始字节解析, 包括 null 与字符串形态. Go 用
                // json.RawMessage 解到 any, 因此显式 null 得到 JSON null.
                //
                // A missing input becomes an empty object; a present one is parsed from its raw
                // bytes, including null and the string form. Go decoded json.RawMessage into any, so
                // an explicit null yields JSON null.
                let args = block
                    .input
                    .text()
                    .and_then(|text| serde_json::from_str::<Value>(text).ok())
                    .unwrap_or_else(|| json!({}));
                let mut call = Map::new();
                call.insert("name".to_owned(), Value::String(block.name.clone()));
                call.insert("args".to_owned(), args);
                let mut part = Map::new();
                if let Some(bridge) = bridge
                    && !bridge.function_call_id.is_empty()
                {
                    call.insert(
                        "id".to_owned(),
                        Value::String(bridge.function_call_id.clone()),
                    );
                }
                part.insert("functionCall".to_owned(), Value::Object(call));
                match bridge {
                    // 有载体的签名原值回传, 不改写.
                    //
                    // A carrier's signature is replayed exactly as captured, never rewritten.
                    Some(bridge) if !bridge.thought_signature.is_empty() => {
                        part.insert(
                            "thoughtSignature".to_owned(),
                            Value::String(bridge.thought_signature.clone()),
                        );
                    }
                    // 没有载体的 functionCall 只可能来自不产生 Google 签名的历史. 回放到 Gemini 3
                    // 系列时为每步的第一个 part 补官方占位值, 让这类历史可以继续会话; 占位值会跳过
                    // 上游校验, 因此不能用于有载体却没有签名的情形.
                    //
                    // A functionCall without a carrier can only come from history that never had a
                    // Google signature. When replaying to the Gemini 3 series the first part of each
                    // step gets the official placeholder so such history can continue the session;
                    // the placeholder skips upstream validation, so it must not be used when a
                    // carrier exists without a signature.
                    None if thought_signature_fallback && !function_call_seen => {
                        part.insert(
                            "thoughtSignature".to_owned(),
                            Value::String(GOOGLE_SKIP_THOUGHT_SIGNATURE_VALIDATOR.to_owned()),
                        );
                    }
                    // 其余情形保持原样: 载体存在但签名缺失时按原样不带签名(有载体却不带签名是
                    // 往返缺陷, 不能用占位值掩盖), 非 Gemini 3 目标以及同一步靠后的
                    // functionCall 同样不带签名.
                    //
                    // Everything else keeps the current shape: a carrier without a signature stays
                    // unsigned (a carrier that lost its signature is a round-trip defect, never
                    // masked by the placeholder), and so do later functionCall parts of a step and
                    // targets other than the Gemini 3 series.
                    _ => {}
                }
                function_call_seen = true;
                parts.push(Value::Object(part));
            }
            BLOCK_TOOL_RESULT => {
                let reference = tool_calls.get(&block.tool_use_id);
                // 找不到对应调用时用 id 占位. 上游按名字匹配, 用占位名会让这一轮关联失效,
                // 但比丢掉整段结果更接近原意.
                //
                // When no matching call is found the id is used as a placeholder. Upstream
                // matches by name, so a placeholder name breaks this turn's association, but it
                // is closer to the intent than dropping the whole result.
                let name = match reference {
                    Some(found) if !found.name.is_empty() => found.name.clone(),
                    _ => block.tool_use_id.clone(),
                };
                let mut response = Map::new();
                response.insert("name".to_owned(), Value::String(name));
                response.insert(
                    "response".to_owned(),
                    Value::Object(tool_result_payload(block)),
                );
                if let Some(found) = reference
                    && !found.id.is_empty()
                {
                    response.insert("id".to_owned(), Value::String(found.id.clone()));
                }
                parts.push(json!({"functionResponse": response}));
            }
            // 本反代的载体已经按工具 id 关联; 其他模型的不透明块与尚未实现无损回放的思考块
            // 都不进入 Gemini.
            //
            // This relay's carrier is already associated by tool id; opaque blocks from other
            // models and thinking blocks whose lossless replay is not implemented do not enter
            // Gemini.
            BLOCK_REDACTED_THINKING | BLOCK_THINKING => continue,
            BLOCK_SERVER_TOOL_USE | BLOCK_WEB_SEARCH_RESULT => {
                parts.push(super::search::replay_web_search_block(&turn.role, block)?);
            }
            _ => {}
        }
    }
    Ok(parts)
}

/// 把工具结果整理成 Gemini 要求的对象.
///
/// Anthropic 的结果可以是字符串, 也可以是内容块数组. Gemini 要求 response 是对象, 因此字符
/// 串会包成 {"result": ...}, 数组会先拼成文本再包一层. 已经是对象的结果原样使用, 这样结构
/// 化工具的结果不会失真.
///
/// Organizes a tool result into the object Gemini requires. An Anthropic result may be a string
/// or an array of content blocks; Gemini requires response to be an object, so a string is
/// wrapped as {"result": ...} and an array is first joined into text and then wrapped. A result
/// that is already an object is used unchanged so structured results do not lose fidelity.
fn tool_result_payload(block: &AnthropicBlock) -> Map<String, Value> {
    let result = |value: &str| {
        let mut payload = Map::new();
        payload.insert("result".to_owned(), Value::String(value.to_owned()));
        payload
    };
    let Some(content) = block.content.text() else {
        return result("");
    };
    // 请求体本身是合法 JSON, 解析不会失败; 无法解析时按原始文本回退, 与 Go 的 string(Content)
    // 一致, 包括原始的空白与键顺序.
    //
    // The body itself is valid JSON so parsing cannot fail; when it does the raw text is kept,
    // matching Go's string(Content) including original whitespace and key order.
    let Ok(parsed) = serde_json::from_str::<Value>(content) else {
        return result(content);
    };
    match parsed {
        Value::Null => result(""),
        Value::String(text) => result(&text),
        Value::Object(object) => object,
        // 数组形态按内容块解析; 其他取值回退原始文本.
        //
        // The array form is parsed as content blocks; any other value falls back to the raw text.
        _ => match parse_blocks(content) {
            Ok(blocks) => {
                let parts: Vec<String> = blocks
                    .iter()
                    .filter_map(|inner| match inner.kind.as_str() {
                        BLOCK_TEXT => Some(inner.text.clone()),
                        BLOCK_IMAGE => Some("[图片结果已省略]".to_owned()),
                        _ => None,
                    })
                    .collect();
                result(&parts.join("\n"))
            }
            // 无法解析成内容块时保留原始文本, 与 Go 的 string(block.Content) 对应.
            //
            // When it cannot be parsed into content blocks the raw text is kept, corresponding to
            // Go's string(block.Content).
            Err(_) => result(content),
        },
    }
}

/// 组装生成参数.
///
/// Assembles the generation parameters.
fn convert_generation_config(request: &AnthropicRequest) -> Map<String, Value> {
    let mut config = Map::new();
    let max_tokens = if request.max_tokens <= 0 {
        ANTIGRAVITY_DEFAULT_MAX_TOKENS
    } else {
        request.max_tokens
    };
    config.insert(
        "maxOutputTokens".to_owned(),
        Value::Number(max_tokens.into()),
    );
    if let Some(temperature) = &request.temperature {
        config.insert("temperature".to_owned(), Value::Number(temperature.clone()));
    }
    if let Some(top_p) = &request.top_p {
        config.insert("topP".to_owned(), Value::Number(top_p.clone()));
    }
    if !request.stop_sequences.is_empty() {
        config.insert(
            "stopSequences".to_owned(),
            Value::Array(
                request
                    .stop_sequences
                    .iter()
                    .map(|sequence| Value::String(sequence.clone()))
                    .collect(),
            ),
        );
    }
    if let Some(thinking) = &request.thinking
        && thinking.kind == "enabled"
        && thinking.budget_tokens > 0
    {
        config.insert(
            "thinkingConfig".to_owned(),
            json!({
                "thinkingBudget": thinking.budget_tokens,
                "includeThoughts": true,
            }),
        );
    }
    config
}

/// Gemini 函数声明所接受的模式关键字. 其他关键字一律剔除.
///
/// 这一点不是可选的美化: 上游对未知字段的处理是直接拒绝整个请求, 而不是忽略它. Claude Code
/// 发送的工具模式里带 $schema, 实测会导致 "Invalid JSON payload received. Unknown name
/// \"$schema\"" 并使整次调用失败. anyOf 也必须剔除: Google Antigravity 将函数声明转给
/// Vertex AI 上的 Claude 5.5 时无法合法转换 anyOf 结构, 会导致 Vertex AI 返回 400
/// "JSON schema is invalid".
///
/// The schema keywords accepted by a Gemini function declaration; all others are removed. This
/// is not optional polish: upstream rejects the entire request on an unknown field instead of
/// ignoring it. Tool schemas sent by Claude Code carry $schema, which in testing produces
/// "Invalid JSON payload received. Unknown name \"$schema\"" and fails the whole call. anyOf
/// must also be removed: Google Antigravity fails to translate anyOf into valid JSON schema for
/// Claude 5.5 on Vertex AI, causing 400 "JSON schema is invalid".
const GEMINI_SCHEMA_KEYS: [&str; 17] = [
    "type",
    "format",
    "title",
    "description",
    "nullable",
    "enum",
    "default",
    "items",
    "properties",
    "required",
    "minimum",
    "maximum",
    "minItems",
    "maxItems",
    "minLength",
    "maxLength",
    "pattern",
];

/// 把工具声明转成 Gemini 的函数声明.
///
/// 两者结构几乎一一对应, 只有字段名不同: Anthropic 用 input_schema, Gemini 用 parameters.
///
/// Converts tool declarations into Gemini function declarations. The two structures map almost
/// one to one, differing only in field names: Anthropic uses input_schema and Gemini uses
/// parameters.
fn convert_tools(tools: &[AnthropicTool]) -> Vec<Value> {
    let mut declarations = Vec::with_capacity(tools.len());
    for tool in tools {
        // tool_choice=none 时搜索工具仍在声明中, 但本轮不能启用它.
        //
        // With tool_choice=none the search tool is still declared, but it must not be enabled
        // for this turn.
        if tool.kind == WEB_SEARCH_TOOL_TYPE || tool.name.is_empty() {
            continue;
        }
        let mut declaration = Map::new();
        declaration.insert("name".to_owned(), Value::String(tool.name.clone()));
        if !tool.description.is_empty() {
            declaration.insert(
                "description".to_owned(),
                Value::String(tool.description.clone()),
            );
        }
        // 模式缺失时不写 parameters; 存在时按原始字节解析, 与 Go 的 len(InputSchema) > 0 一致.
        //
        // A missing schema writes no parameters; a present one is parsed from its raw bytes,
        // matching Go's len(InputSchema) > 0.
        if let Some(schema) = tool
            .input_schema
            .text()
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
        {
            declaration.insert("parameters".to_owned(), sanitize_schema(schema));
        }
        declarations.push(Value::Object(declaration));
    }
    declarations
}

/// 递归剔除 Gemini 不支持的模式关键字.
///
/// 剔除而不是改写: 被去掉的多是校验与文档性质的字段, 不影响模型理解参数的形状. 唯一需要注
/// 意的是 $ref. 上游不支持引用, 去除后该节点会变成空模式, 语义上退化为任意值, 比整次调用
/// 失败要好.
///
/// Recursively removes schema keywords Gemini does not support. Removal rather than rewriting:
/// most dropped fields are validation and documentation oriented and do not affect the model's
/// understanding of the parameter shape. The one point to note is $ref. Upstream does not
/// support references, and removing it turns the node into an empty schema that degrades to any
/// value, which is better than failing the whole call.
fn sanitize_schema(value: Value) -> Value {
    match value {
        Value::Object(node) => {
            let mut cleaned = Map::new();
            for (key, inner) in node {
                if !GEMINI_SCHEMA_KEYS.contains(&key.as_str()) {
                    continue;
                }
                match key.as_str() {
                    // properties 的每个值是子模式, 需要继续清理.
                    //
                    // Each value of properties is a subschema and needs further cleaning.
                    "properties" => {
                        let Value::Object(object) = inner else {
                            continue;
                        };
                        let mut sanitized = Map::new();
                        for (name, sub) in object {
                            sanitized.insert(name, sanitize_schema(sub));
                        }
                        cleaned.insert(key, Value::Object(sanitized));
                    }
                    "items" => {
                        if matches!(inner, Value::Array(_)) {
                            // 元组形式的 items 上游不支持, 退化为通用数组.
                            //
                            // Tuple-form items are unsupported upstream and degrade to a
                            // generic array.
                            cleaned.insert(key, Value::Object(Map::new()));
                        } else {
                            cleaned.insert(key, sanitize_schema(inner));
                        }
                    }
                    _ => {
                        cleaned.insert(key, inner);
                    }
                }
            }
            Value::Object(cleaned)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sanitize_schema).collect()),
        other => other,
    }
}

/// 把工具选择策略转成 Gemini 的 functionCallingConfig.
///
/// Converts the tool selection policy into Gemini's functionCallingConfig. 非对象形态或字段
/// 类型不符时 Go 的解码会失败并返回空, 这里同样返回 None.
///
/// A non-object shape or a field type mismatch makes Go's decode fail and return nothing, which
/// is also None here.
fn convert_tool_choice(raw: Option<&str>) -> Option<Value> {
    let parsed = serde_json::from_str::<Value>(raw?).ok()?;
    let object = parsed.as_object()?;
    let kind = optional_string_field(object, "type").ok()?;
    let name = optional_string_field(object, "name").ok()?;
    let mut calling = Map::new();
    match kind.as_deref() {
        Some("auto") => {
            calling.insert("mode".to_owned(), json!("AUTO"));
        }
        Some("any") => {
            calling.insert("mode".to_owned(), json!("ANY"));
        }
        Some("tool") => {
            calling.insert("mode".to_owned(), json!("ANY"));
            if let Some(name) = name
                && !name.is_empty()
            {
                calling.insert("allowedFunctionNames".to_owned(), json!([name]));
            }
        }
        Some("none") => {
            calling.insert("mode".to_owned(), json!("NONE"));
        }
        _ => return None,
    }
    Some(json!({"functionCallingConfig": calling}))
}

/// 取一个字符串字段: 缺失与 null 都得到 None, 类型不符返回错误.
///
/// Reads a string field: missing and null both give None, a type mismatch returns an error.
pub(crate) fn optional_string_field(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Option<String>, InvalidRequest> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(InvalidRequest::new(format!("字段 {key} 不是字符串"))),
    }
}

/// 解析内容块数组. 既接受字符串形态, 也接受数组形态; null 元素按 Go 的规则得到零值块.
///
/// Parses an array of content blocks, accepting both the string form and the array form; a null
/// element yields a zero-value block as in Go.
fn parse_blocks(text: &str) -> Result<Vec<AnthropicBlock>, InvalidRequest> {
    zero_on_null_list(&mut serde_json::Deserializer::from_str(text))
        .map_err(|err| InvalidRequest::new(format!("解析内容块失败: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::antigravity::test_support::{envelope as build_envelope, inner_request, parts_of};

    #[test]
    fn carrier_less_history_gets_official_fallback_on_gemini_3() {
        assert_eq!(
            GOOGLE_SKIP_THOUGHT_SIGNATURE_VALIDATOR, "skip_thought_signature_validator",
            "兜底占位值必须与官方文档逐字一致"
        );
        let messages = json!([
            {"role": "user", "content": "查两个值"},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "call_00_probe_a", "name": "lookup", "input": {"key": "a"}},
                {"type": "tool_use", "id": "call_01_probe_b", "name": "lookup", "input": {"key": "b"}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call_00_probe_a", "content": "A"},
                {"type": "tool_result", "tool_use_id": "call_01_probe_b", "content": "B"}]},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "call_02_probe_c", "name": "lookup", "input": {"key": "c"}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call_02_probe_c", "content": "C"}]},
        ]);
        let body =
            json!({"model": "gemini-3.8-flash-high", "max_tokens": 16, "messages": messages})
                .to_string();
        let envelope = build_envelope(&body);
        // 信封固定字段断言自 envelope_carries_fixed_fields 并入, 模型名以本测试的请求为准.
        assert_eq!(envelope["project"], json!("proj-1"));
        assert_eq!(envelope["userAgent"], json!("antigravity"));
        assert_eq!(envelope["requestType"], json!("agent"));
        assert_eq!(envelope["model"], json!("gemini-3.8-flash-high"));
        let request_id = envelope["requestId"].as_str().unwrap_or_default();
        assert!(
            request_id.starts_with("agent/"),
            "requestId 前缀错误: {request_id:?}"
        );
        let inner = inner_request(&envelope);
        let parallel = parts_of(inner, 1);
        assert_eq!(parallel.len(), 2, "并行调用应产生两个 part");
        assert_eq!(
            parallel[0]["thoughtSignature"],
            json!(GOOGLE_SKIP_THOUGHT_SIGNATURE_VALIDATOR),
            "每步第一个 functionCall part 应补官方兜底值"
        );
        assert!(
            parallel[1].get("thoughtSignature").is_none(),
            "并行调用的其余 part 不得补签名: {parallel:?}"
        );
        let second_step = parts_of(inner, 3);
        assert_eq!(
            second_step[0]["thoughtSignature"],
            json!(GOOGLE_SKIP_THOUGHT_SIGNATURE_VALIDATOR),
            "多步调用的每一步各自兜底"
        );
        let results = parts_of(inner, 2);
        assert_eq!(
            results[0]["functionResponse"]["name"],
            json!("lookup"),
            "兜底不得影响工具结果的关联"
        );
    }
}
