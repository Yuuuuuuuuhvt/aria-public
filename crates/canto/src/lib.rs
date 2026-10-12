//! canto 负责上游已经实现 Anthropic Messages 协议的路由: 只改写模型名并流式转发
//! 原生响应, 以及为声明了 moonshot_search 的路由编排 Kimi 独立搜索.
//!
//! canto serves routes whose upstream already implements Anthropic Messages: it only
//! rewrites the model name and streams the native response, and orchestrates the Kimi
//! standalone search for routes that declare moonshot_search.

pub mod forward;
pub mod search;
mod unavailable;

#[cfg(test)]
mod tests_support;
