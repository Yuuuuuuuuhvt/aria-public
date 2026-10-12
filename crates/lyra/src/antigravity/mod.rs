//! Antigravity 线路: 请求转换, 响应转换, 函数调用签名桥与 Google 搜索.
//!
//! The Antigravity line: request translation, response translation, the function-call
//! signature bridge and Google Search.

pub mod provider;
pub mod request;
pub mod response;
pub mod search;
pub mod signatures;

/// 协议转换测试共用的读取助手.
///
/// Go 版的测试同属一个包, 响应, 签名与搜索三组测试共用同一组助手; 这里放在模块根下, 使三组测试
/// 读到的助手行为完全一致, 不必各自复制一份.
///
/// Helpers shared by the protocol conversion tests. The Go tests belonged to one package and the
/// response, signature and search tests shared one set of helpers; putting them at the module root
/// keeps the three test groups reading identical helpers instead of copying them.
#[cfg(test)]
pub(crate) mod test_support {
    use serde_json::{Map, Value};

    use super::request::build_envelope;
    use super::response::AnthropicTranslator;

    /// 用固定项目标识构造信封并解析成 JSON 对象.
    ///
    /// Builds an envelope with the fixed project id and parses it into a JSON object.
    pub(crate) fn envelope(body: &str) -> Value {
        envelope_with_project(body, "proj-1")
    }

    /// 用指定项目标识构造信封并解析成 JSON 对象, 走与生产相同的路径.
    ///
    /// Builds an envelope with the given project id and parses it into a JSON object, following
    /// the same path as production.
    pub(crate) fn envelope_with_project(body: &str, project_id: &str) -> Value {
        let raw = match build_envelope(body.as_bytes(), project_id) {
            Ok(raw) => raw,
            Err(err) => panic!("构造信封失败: {err}"),
        };
        match serde_json::from_slice::<Value>(&raw) {
            Ok(value) => value,
            Err(err) => panic!("解析信封失败: {err}"),
        }
    }

    /// 取出信封里 request 字段的内容.
    ///
    /// Extracts the content of the envelope's request field.
    pub(crate) fn inner_request(envelope: &Value) -> &Map<String, Value> {
        match envelope.get("request").and_then(Value::as_object) {
            Some(inner) => inner,
            None => panic!("信封缺少 request 字段: {envelope}"),
        }
    }

    /// 取出第 index 轮内容的 parts.
    ///
    /// Extracts the parts of the content at index.
    pub(crate) fn parts_of(inner: &Map<String, Value>, index: usize) -> &Vec<Value> {
        let contents = match inner.get("contents").and_then(Value::as_array) {
            Some(contents) => contents,
            None => panic!("contents 结构不符: {}", inner["contents"]),
        };
        let turn = match contents.get(index).and_then(Value::as_object) {
            Some(turn) => turn,
            None => panic!("第 {index} 轮内容不存在: {contents:?}"),
        };
        match turn.get("parts").and_then(Value::as_array) {
            Some(parts) => parts,
            None => panic!("第 {index} 轮没有 parts: {turn:?}"),
        }
    }

    /// 把若干上游事件喂给翻译器并收集全部输出, 调用顺序与 relay 一致: 先处理事件, 首次产生有效
    /// 内容后再写起始消息, 最后收尾.
    ///
    /// Feeds several upstream events to the translator and gathers all output, in the same order as
    /// the relay: handle the event, write the start message once valid content appears, then finish.
    pub(crate) fn collect(translator: &mut AnthropicTranslator, events: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut begun = false;
        for event in events {
            let chunk = match translator.event(event.as_bytes()) {
                Ok(chunk) => chunk,
                Err(err) => panic!("Event 失败: {err}"),
            };
            if !begun && translator.started() {
                let begin = match translator.begin() {
                    Ok(begin) => begin,
                    Err(err) => panic!("Begin 失败: {err}"),
                };
                out.extend(begin);
                begun = true;
            }
            out.extend(chunk);
        }
        let end = match translator.end() {
            Ok(end) => end,
            Err(err) => panic!("End 失败: {err}"),
        };
        out.extend(end);
        out
    }

    /// 把 Anthropic 事件流拆成 事件名 与 data 载荷 的序列.
    ///
    /// Splits an Anthropic event stream into a sequence of event name and data payload pairs.
    pub(crate) fn parse_events(payload: &[u8]) -> Vec<(String, Value)> {
        let Ok(text) = std::str::from_utf8(payload) else {
            panic!("事件流不是 UTF-8");
        };
        let mut events = Vec::new();
        for block in text.split("\n\n") {
            let block = block.trim();
            if block.is_empty() {
                continue;
            }
            let mut name = "";
            let mut data = "";
            for line in block.split('\n') {
                if let Some(rest) = line.strip_prefix("event: ") {
                    name = rest;
                }
                if let Some(rest) = line.strip_prefix("data: ") {
                    data = rest;
                }
            }
            if name.is_empty() || data.is_empty() {
                panic!("事件格式不完整: {block:?}");
            }
            let parsed = match serde_json::from_str::<Value>(data) {
                Ok(parsed) => parsed,
                Err(err) => panic!("事件载荷不是 JSON: {data:?} {err}"),
            };
            events.push((name.to_owned(), parsed));
        }
        events
    }

    /// 取 data 对象里的一个字段.
    ///
    /// Returns one field of the data object.
    pub(crate) fn event_field<'a>(event: &'a (String, Value), key: &str) -> &'a Value {
        match event.1.get(key) {
            Some(value) => value,
            None => panic!("事件缺少 {key} 字段: {}", event.1),
        }
    }

    /// 取 data 对象里某个对象的字段.
    ///
    /// Returns a nested object field of the data object.
    pub(crate) fn event_object<'a>(
        event: &'a (String, Value),
        key: &str,
    ) -> &'a Map<String, Value> {
        match event_field(event, key).as_object() {
            Some(object) => object,
            None => panic!("事件字段 {key} 不是对象: {}", event.1),
        }
    }
}
