//! 客户端密钥文件的加载与常量时间比对.
//!
//! 客户端密钥与上游账号凭据是两类不同的秘密: 客户端密钥是调用方访问本服务的凭据,
//! 泄露只影响本服务的访问权; 上游账号令牌只存在于服务内部, 从不出现在客户端可见的
//! 任何位置.
//!
//! Client key file loading and constant-time verification.
//!
//! Client keys and upstream account credentials are two different kinds of secret: a
//! client key is the credential a caller presents to this service, so leaking it only
//! affects access to this service; upstream account tokens exist only inside the service
//! and never appear anywhere a client can observe.

use std::path::Path;

use http::{HeaderMap, header};
use serde::Deserialize;
use serde_json::{Map, Value};
use subtle::{Choice, ConstantTimeEq};

use crate::jsonval;

/// 客户端密钥文件的顶层结构. keys 缺失时视为空集合, 与 Go 版把缺失字段解码为 nil
/// 切片的行为一致.
///
/// Top level of a client key file. A missing keys field counts as an empty set, matching
/// the Go version, where a missing field decodes into a nil slice.
#[derive(Deserialize)]
struct ClientFile {
    #[serde(default)]
    keys: Vec<Map<String, Value>>,
}

/// 启用状态的客户端密钥集合.
///
/// The set of enabled client keys.
pub struct KeySet {
    keys: Vec<String>,
}

impl KeySet {
    /// 读取客户端密钥文件, 只保留 enabled 为真的条目.
    ///
    /// 真值判断交给 jsonval::truthy, 与改写前的 Python 实现的
    /// entry.get("enabled", True) 一致: 缺失视为启用, 字符串 "false" 也算启用.
    ///
    /// 路径可能指向 /dev/fd/N 这类管道, 因此整份文件只打开并读取一次.
    ///
    /// 解析只读取文件中的第一个 JSON 值, 与 Go 版 json.Decoder.Decode 相同, 文件末尾
    /// 的额外内容不影响加载.
    ///
    /// Reads the client key file and keeps only the entries whose enabled field is
    /// truthy. The truthiness decision is delegated to jsonval::truthy, matching the
    /// entry.get("enabled", True) expression of the Python implementation before the
    /// rewrite: a missing field counts as enabled, and the string "false" also counts
    /// as enabled.
    ///
    /// The path may name a pipe such as /dev/fd/N, so the file is opened and read
    /// exactly once.
    pub fn load(path: &Path) -> Result<KeySet, String> {
        let data = std::fs::read(path).map_err(|error| format!("读取客户端密钥失败: {error}"))?;
        // Go 版的 json.Decoder.Decode 只读取第一个 JSON 值, 文件末尾的额外内容不影响
        // 加载; 这里用流式迭代取第一个值, 保持一致.
        //
        // Go's json.Decoder.Decode reads only the first JSON value and ignores trailing
        // content; the streaming iterator takes the first value the same way.
        let mut values = serde_json::Deserializer::from_slice(&data).into_iter::<ClientFile>();
        let file = match values.next() {
            Some(Ok(file)) => file,
            Some(Err(error)) => return Err(format!("解析客户端密钥失败: {error}")),
            None => return Err("解析客户端密钥失败: EOF".to_owned()),
        };
        let mut keys = Vec::with_capacity(file.keys.len());
        for (position, entry) in file.keys.iter().enumerate() {
            if let Some(enabled) = entry.get("enabled")
                && !jsonval::truthy(enabled)
            {
                continue;
            }
            match entry.get("key").and_then(Value::as_str) {
                Some(key) if !key.is_empty() => keys.push(key.to_owned()),
                _ => return Err(format!("第 {position} 条客户端密钥缺少 key 字段")),
            }
        }
        Ok(KeySet { keys })
    }

    /// 返回启用密钥的数量.
    ///
    /// Returns the number of enabled keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// 报告集合是否为空.
    ///
    /// Reports whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// 判断请求携带的密钥是否命中.
    ///
    /// 与改写前的 Python 实现有一处差异: 这里不提前返回, 而是比较完所有密钥再决定
    /// 结果, 因此耗时不受命中的是第几把密钥影响. Python 的 any(...) 会短路.
    ///
    /// Reports whether a presented key matches an enabled key. One difference from the
    /// Python implementation before the rewrite: it does not return early but compares
    /// every key before deciding, so the time taken does not depend on the position of
    /// the matching key. Python's any(...) short-circuits.
    pub fn authorized(&self, presented: &str) -> bool {
        if presented.is_empty() {
            return false;
        }
        let mut matched = Choice::from(0u8);
        for key in &self.keys {
            matched |= presented.as_bytes().ct_eq(key.as_bytes());
        }
        matched.unwrap_u8() == 1
    }
}

/// 从请求头取出调用方携带的密钥.
///
/// 顺序与改写前的 Python 实现一致: 先看 Authorization 的 Bearer 形式, 不是 Bearer
/// 形式时再看 x-api-key. 两者都没有时返回空字符串.
///
/// 头部取值不是合法 UTF-8 时按缺失处理, 返回空字符串; 这种请求随后会在密钥比对处
/// 被拒绝, 不会绕过鉴权.
///
/// Extracts the caller's key from the request headers. The order matches the Python
/// implementation before the rewrite: it reads the Bearer form of Authorization first,
/// and falls back to x-api-key when the header is not a Bearer value. It returns an
/// empty string when neither is present.
///
/// A header value that is not valid UTF-8 is treated as absent and yields an empty
/// string; such a request is then rejected by the key comparison and cannot bypass
/// authentication.
pub fn presented_key(headers: &HeaderMap) -> String {
    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        && value.to_lowercase().starts_with("bearer ")
    {
        return value[7..].trim().to_owned();
    }
    headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_owned())
        .unwrap_or_default()
}
