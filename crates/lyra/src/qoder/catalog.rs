// Copyright (c) 2026 ggbdpq (MIT License)
// Copyright (c) 2026 wangjunyao (MIT License)

//! Qoder 网关模型目录的获取, 缓存与显示名翻译模块.
//!
//! 背景: 网关的 model_config.key 只认内部目录键, 例如 kmodel_latest; 官方 CLI 先把用户选择的
//! 显示名 Kimi-K3 翻译成内部键再发送. 直接把显示名发给网关时, 网关不报错, 而是静默落到免费
//! 默认模型并返回 billable=false 与 credits=0, 表现为模型身份与扣费都与预期不符.
//! 2026-10-11 的抓包对照实验确认, 官方 CLI 经 GET /algo/api/v2/model/list 拉取目录并缓存,
//! 目录条目的 display_name 与 key 就是显示名到内部键的权威映射.
//!
//! 本模块从同一接口获取目录, 在内存中按 TTL 缓存, 把路由配置中的显示名翻译为内部键.
//! 目录完全获取失败且没有任何缓存时返回错误, 不把未翻译的显示名静默发给网关.

use std::time::{Duration, Instant};

use serde::Deserialize;

/// 模型目录接口的相对路径; 网关日志中记录为 /api/v2/model/list, 线上路径带 /algo 前缀.
///
/// 2026-10-11 实测: 不带 /algo 前缀的路径在网关 ALB 层直接返回 503, 带前缀的路径经
/// COSY 签名后返回 200; 签名路径按 signature_path 规则剥掉 /algo 前缀.
pub const MODEL_LIST_ENDPOINT: &str = "/algo/api/v2/model/list?Encode=1";

/// 目录在内存中的缓存时长. 官方 CLI 的磁盘缓存约三分钟过期, 网关侧目录变化以周计;
/// 网关进程长期运行, 一小时刷新的开销可以忽略, 过期后请求路径上同步拉取一次.
pub const CATALOG_TTL: Duration = Duration::from_secs(3600);

/// 显式请求网关默认免费模型的保留键.
///
/// 目录中没有任何条目指向网关的免费默认模型: 2026-10-11 实测全部目录键都计费, 包括目录里
/// 标记 is_free=true 的 qfmodel 与默认键 auto. 网关对不认识的键一律落到免费默认模型, 返回
/// billable=false 与 credits=0; 空键与 default 实测同样落到该模型. 配置中写 default 是
/// 请求该模型的明确方式, 翻译层对它原样放行, 不查目录, 因此目录接口故障不影响免费路由.
/// 该行为依赖网关当前的兜底策略, 网关改为报错时免费路由会随之报错, 需要重新选型.
pub const GATEWAY_DEFAULT_KEY: &str = "default";

/// 目录中一个模型条目的关键字段; 其余字段与翻译无关, 反序列化时忽略.
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogEntry {
    pub key: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub enable: bool,
}

/// 一份模型目录, 只保留翻译所需的条目.
#[derive(Debug, Clone, Default)]
pub struct ModelCatalog {
    entries: Vec<CatalogEntry>,
}

impl ModelCatalog {
    /// 从接口响应体解析目录. 响应按用途分区, chat 分区是 qodercli 会话使用的目录;
    /// 其余分区当前与 chat 同构, 翻译一律以 chat 为准.
    pub fn parse(body: &[u8]) -> Result<ModelCatalog, String> {
        let root: serde_json::Value =
            serde_json::from_slice(body).map_err(|err| format!("解析模型目录 JSON 失败: {err}"))?;
        let chat = root
            .get("chat")
            .and_then(|section| section.as_array())
            .ok_or_else(|| "模型目录响应缺少 chat 分区".to_owned())?;
        let mut entries = Vec::with_capacity(chat.len());
        for item in chat {
            let entry: CatalogEntry = serde_json::from_value(item.clone())
                .map_err(|err| format!("解析模型目录条目失败: {err}"))?;
            if entry.key.is_empty() {
                return Err("模型目录条目缺少 key 字段".to_owned());
            }
            entries.push(entry);
        }
        if entries.is_empty() {
            return Err("模型目录 chat 分区为空".to_owned());
        }
        Ok(ModelCatalog { entries })
    }

    /// 把路由配置中的模型名解析为网关内部键.
    ///
    /// 精确匹配内部键时原样通过, 精确匹配显示名时翻译为内部键; 两者都不命中时返回 None,
    /// 由调用方报错. 大小写与空白不做归一: 配置必须与目录文本完全一致, 避免近似匹配把
    /// 拼错的显示名静默解析到别的模型.
    pub fn resolve(&self, model: &str) -> Option<String> {
        for entry in self.entries.iter().filter(|entry| entry.enable) {
            if entry.key == model {
                return Some(entry.key.clone());
            }
            if !entry.display_name.is_empty() && entry.display_name == model {
                return Some(entry.key.clone());
            }
        }
        None
    }

    /// 列出全部启用条目的显示名与内部键, 供未知模型的错误信息使用.
    pub fn describe_entries(&self) -> String {
        self.entries
            .iter()
            .filter(|entry| entry.enable)
            .map(|entry| format!("{}={}", entry.display_name, entry.key))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// 进程内缓存的一份目录及其获取时间.
pub struct CachedCatalog {
    pub fetched: Instant,
    pub catalog: ModelCatalog,
}
