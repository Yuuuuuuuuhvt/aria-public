//! 用量采集与按日 JSONL 存储及查询. Go 来源是 lyra internal/usage, 服务器上已有的
//! 记录必须可以继续读取, 采集规则也必须与 Go 版逐项一致.
//!
//! capture 是与 IO 无关的状态机, 由转发循环喂入响应体字节; store 提供目录锁,
//! 原子追加与按日汇总查询, 全部方法是同步的, 调用方放进 spawn_blocking.
//!
//! Usage capture with daily JSONL storage and queries. The Go source is lyra
//! internal/usage: records already on a server must stay readable and the capture rules
//! must match the Go version item by item.
//!
//! capture is an IO-free state machine fed with response body bytes by the forwarding
//! loop; store provides the directory lock, atomic appends and per-day aggregation. All
//! store methods are synchronous and belong in spawn_blocking.

pub mod capture;
pub mod observe;
pub mod store;

pub use capture::{Capture, CaptureBody, CaptureError, MAX_CAPTURE_BYTES};
pub use observe::Observation;
pub use store::{
    Error, ErrorKind, Filter, Group, Record, Store, Summary, TIMEZONE, Tokens, Totals,
};
