//! lyra 负责订阅账号支持的线路: ChatGPT 账号池透传 Codex Responses 协议, Google
//! 账号池把 Anthropic Messages 转换为 Antigravity 协议, SuperGrok 账号池把 Anthropic
//! Messages 转换为 Grok CLI 渠道的 Responses 协议. 它还提供三种账号的浏览器登录命令.
//!
//! lyra serves subscription-backed lines: a ChatGPT account pool that passes the Codex
//! Responses protocol through, a Google account pool that translates Anthropic Messages
//! into the Antigravity protocol, and a SuperGrok account pool that translates Anthropic
//! Messages into the Responses protocol of the Grok CLI channel. It also provides browser
//! login commands for all three kinds of account.

pub mod antigravity;
pub mod chatgpt;
pub mod client_id;
pub mod commandcode;
pub mod credential;
pub mod login;
pub mod qoder;
pub mod relay;
pub mod upstream;
pub mod xai;
