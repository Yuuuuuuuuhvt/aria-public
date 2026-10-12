//! relay 模块的测试.
//!
//! 用例按行为分组: passthrough 覆盖 ChatGPT 线路的透传, 账号轮换与刷新重试;
//! antigravity 覆盖协议转换, 容量重试预算与会话固定; usage 覆盖用量记录与转发时序.
//!
//! Tests for the relay module. Cases are grouped by behaviour: passthrough covers the ChatGPT
//! route's pass-through, account rotation and refresh retry; antigravity covers protocol
//! conversion, the capacity retry budget and session pinning; usage covers usage records and
//! forwarding timing.

#[allow(dead_code)]
mod support;

mod antigravity;
