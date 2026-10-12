//! aria-core 收纳 canto 与 lyra 共用的基础能力: 客户端鉴权, JSON 取值语义, 日志,
//! 响应体与错误格式, 上游 HTTP 客户端, SSE 分帧, 以及按日 JSONL 的用量统计.
//!
//! aria-core holds what canto and lyra share: client authentication, JSON value
//! semantics, logging, response bodies and error formats, the upstream HTTP client,
//! SSE framing, and daily JSONL usage statistics.

pub mod auth;
pub mod body;
pub mod errors;
pub mod exhaustion;
pub mod jsonval;
pub mod log;
pub mod sse;
pub mod upstream;
pub mod usage;
