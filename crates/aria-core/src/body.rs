//! 所有处理器返回同一种响应体类型, 使服务端可以统一处理缓冲响应与流式响应.
//!
//! Every handler returns one response body type so the server treats buffered and
//! streamed responses alike.

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};

/// 响应体与流式转发中使用的错误类型.
///
/// The error type used by response bodies and streamed forwarding.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// 服务端返回的统一响应体.
///
/// The unified response body returned by the server.
pub type Body = BoxBody<Bytes, BoxError>;

/// 构造一个一次性写出的响应体.
///
/// Builds a body written in one piece.
pub fn full(data: impl Into<Bytes>) -> Body {
    Full::new(data.into())
        .map_err(|never| match never {})
        .boxed()
}

/// 构造空响应体.
///
/// Builds an empty body.
pub fn empty() -> Body {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
