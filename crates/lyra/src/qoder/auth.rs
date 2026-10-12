// Copyright (c) 2026 ggbdpq (MIT License)
// Copyright (c) 2026 wangjunyao (MIT License)

//! Qoder 网关身份认证、令牌交换与 Bearer 请求签名模块。
//!
//! 协议事实来源为逆向工程所得的公开协议基准实现。
//! 客户端通过个人访问令牌向网关请求交换 jobToken 会话凭据，
//! 并在每次业务请求中动态生成一次性 Bearer 签名，防范重放与篡改。

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use super::catalog::{
    CATALOG_TTL, CachedCatalog, GATEWAY_DEFAULT_KEY, MODEL_LIST_ENDPOINT, ModelCatalog,
};
use super::codec::{
    aes_128_cbc_encrypt, encode, md5_hex, rfc1123_now, rsa_pkcs1_encrypt, sign_auth_date,
};
use super::signatures::{COSY_VERSION, DEFAULT_GATEWAY_URL, DEFAULT_SECRET};

/// 会话提前刷新时间余量，设定为到期前十分钟触发续期。
pub const REFRESH_MARGIN_MS: i64 = 10 * 60 * 1000;

/// 网关用户身份核心字段，对应业务会话中的账户与组织归属信息。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayIdentity {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub aid: String,
    #[serde(default)]
    pub uid: String,
    #[serde(default, alias = "yxUid")]
    pub yx_uid: String,
    #[serde(default, alias = "organizationId")]
    pub organization_id: String,
    #[serde(default, alias = "organizationName")]
    pub organization_name: String,
    #[serde(default = "default_user_type", alias = "userType")]
    pub user_type: String,
    #[serde(default, alias = "securityOauthToken")]
    pub security_oauth_token: String,
    #[serde(default, alias = "refreshToken")]
    pub refresh_token: String,
}

fn default_user_type() -> String {
    "personal_standard".to_string()
}

/// 网关设备指纹标识，用于客户端风控与多端接入识别。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayMachineInfo {
    pub machine_id: String,
    pub machine_token: String,
    pub machine_type: String,
}

impl Default for GatewayMachineInfo {
    fn default() -> Self {
        Self::generate()
    }
}

impl GatewayMachineInfo {
    /// 获取当前操作系统的平台标识字符串。
    pub fn current_platform_type() -> &'static str {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            "linux_amd64"
        }
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        {
            "linux_arm64"
        }
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        {
            "darwin_arm64"
        }
        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        {
            "darwin_amd64"
        }
        #[cfg(not(any(
            all(target_os = "linux", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "aarch64"),
            all(target_os = "macos", target_arch = "aarch64"),
            all(target_os = "macos", target_arch = "x86_64"),
        )))]
        {
            "windows_amd64"
        }
    }

    /// 生成符合网关格式要求的随机机器标识。
    pub fn generate() -> Self {
        Self {
            machine_id: generate_random_uuid(),
            machine_token: generate_random_uuid(),
            machine_type: Self::current_platform_type().to_string(),
        }
    }

    /// 从指定状态文件加载机器标识，若未配置则在内存中临时生成；若文件不存在则生成并安全持久化。
    pub fn load_or_generate(configured_file: Option<&std::path::Path>) -> Self {
        let Some(path) = configured_file else {
            return Self::generate();
        };

        match std::fs::read_to_string(path) {
            Ok(content) => match serde_json::from_str::<Self>(&content) {
                Ok(info) => {
                    if !info.machine_id.is_empty() && !info.machine_token.is_empty() {
                        return info;
                    }
                    aria_core::log!(
                        "warning: Qoder 机器标识文件 {} 字段不完整，将重新生成",
                        path.display()
                    );
                }
                Err(err) => {
                    aria_core::log!(
                        "warning: 解析 Qoder 机器标识文件 {} 失败: {}，将重新生成",
                        path.display(),
                        err
                    );
                }
            },
            Err(err) => {
                if err.kind() != std::io::ErrorKind::NotFound {
                    aria_core::log!(
                        "warning: 读取 Qoder 机器标识文件 {} 失败: {}，将尝试重新写入",
                        path.display(),
                        err
                    );
                }
            }
        }

        let new_info = Self::generate();
        if let Err(err) = Self::persist_machine_info(path, &new_info) {
            aria_core::log!(
                "warning: 持久化 Qoder 机器标识文件 {} 失败: {}",
                path.display(),
                err
            );
        }
        new_info
    }

    /// 将机器标识原子写入文件并设置 0600 权限，确保父目录权限为 0700。
    fn persist_machine_info(path: &std::path::Path, info: &Self) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }

        let json_str =
            serde_json::to_string_pretty(info).map_err(|e| std::io::Error::other(e.to_string()))?;

        let tmp_path_str = format!(
            "{}.tmp.{}.{}",
            path.display(),
            std::process::id(),
            generate_random_uuid()
        );
        let tmp_path = std::path::Path::new(&tmp_path_str);

        std::fs::write(tmp_path, json_str.as_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(tmp_path, std::fs::Permissions::from_mode(0o600));
        }

        std::fs::rename(tmp_path, path)?;
        Ok(())
    }
}

/// 网关 jobToken 响应数据结构，适配服务端动态类型返回。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GatewayJobToken {
    pub name: Option<serde_json::Value>,
    pub id: Option<serde_json::Value>,
    pub user_type: Option<serde_json::Value>,
    pub security_oauth_token: Option<serde_json::Value>,
    pub refresh_token: Option<serde_json::Value>,
    pub expire_time: Option<serde_json::Value>,
}

impl GatewayJobToken {
    fn extract_string(val: &Option<serde_json::Value>) -> String {
        match val {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Number(n)) => n.to_string(),
            _ => String::new(),
        }
    }

    /// 提取用户名文本。
    pub fn name_str(&self) -> String {
        Self::extract_string(&self.name)
    }

    /// 提取唯一用户标识。
    pub fn id_str(&self) -> String {
        Self::extract_string(&self.id)
    }

    /// 提取用户级别类型。
    pub fn user_type_str(&self) -> String {
        let res = Self::extract_string(&self.user_type);
        if res.is_empty() {
            "personal_standard".to_string()
        } else {
            res
        }
    }

    /// 提取安全授权令牌。
    pub fn security_oauth_token_str(&self) -> String {
        Self::extract_string(&self.security_oauth_token)
    }

    /// 提取刷新令牌。
    pub fn refresh_token_str(&self) -> String {
        Self::extract_string(&self.refresh_token)
    }

    /// 提取到期时间毫秒时间戳。
    pub fn expire_time_ms(&self) -> i64 {
        match &self.expire_time {
            Some(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
            Some(serde_json::Value::String(s)) => s.parse::<i64>().unwrap_or(0),
            _ => 0,
        }
    }
}

/// 维护有效会话上下文的持久结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewaySession {
    pub identity: GatewayIdentity,
    pub cosy_key: String,
    pub machine_id: String,
    pub machine_token: String,
    pub machine_type: String,
    pub refresh_token: String,
    pub security_oauth_token: String,
    pub expire_time_ms: i64,
}

/// 根据当前系统随机源生成标准 UUID v4 格式字符串。
fn generate_random_uuid() -> String {
    let mut bytes = [0u8; 16];
    rand::fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex_str = hex::encode(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex_str[0..8],
        &hex_str[8..12],
        &hex_str[12..16],
        &hex_str[16..20],
        &hex_str[20..32]
    )
}

/// 生成 16 字节随机十六进制小写 ASCII 密钥切片。
fn generate_temp_key() -> [u8; 16] {
    let mut random_bytes = [0u8; 8];
    rand::fill(&mut random_bytes);
    let hex_string = hex::encode(random_bytes);
    let mut key = [0u8; 16];
    key.copy_from_slice(hex_string.as_bytes());
    key
}

/// 从服务端响应的 jobToken 与机器信息装配完整可用会话。
pub fn session_from_job_token(
    token: &GatewayJobToken,
    machine: &GatewayMachineInfo,
) -> Result<GatewaySession, String> {
    let identity = GatewayIdentity {
        name: token.name_str(),
        aid: token.id_str(),
        uid: token.id_str(),
        yx_uid: String::new(),
        organization_id: String::new(),
        organization_name: String::new(),
        user_type: token.user_type_str(),
        security_oauth_token: token.security_oauth_token_str(),
        refresh_token: token.refresh_token_str(),
    };
    let temp_key = generate_temp_key();
    let rsa_ciphertext = rsa_pkcs1_encrypt(&temp_key)?;
    let cosy_key = STANDARD.encode(&rsa_ciphertext);
    Ok(GatewaySession {
        identity: identity.clone(),
        cosy_key,
        machine_id: machine.machine_id.clone(),
        machine_token: machine.machine_token.clone(),
        machine_type: machine.machine_type.clone(),
        refresh_token: identity.refresh_token,
        security_oauth_token: identity.security_oauth_token,
        expire_time_ms: token.expire_time_ms(),
    })
}

/// 从请求完整路径中剥离 /algo 前缀，得到网关签名专用的路径部分。
pub fn signature_path(full_url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(full_url) {
        let path = parsed.path();
        if let Some(stripped) = path.strip_prefix("/algo") {
            stripped.to_string()
        } else {
            path.to_string()
        }
    } else if let Some(stripped) = full_url.strip_prefix("/algo") {
        stripped.to_string()
    } else {
        full_url.to_string()
    }
}

/// 承载构建单次请求 Bearer 签名所得产物的返回结构。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerTokenResult {
    pub bearer: String,
    pub cosy_key: String,
}

/// 为业务请求构建一次性 Bearer 授权凭据。
///
/// 算法步骤为生成独立随机临时对称密钥，经 RSA 公钥包裹后得到 cosyKey，
/// 使用对称密钥加密身份信息得到 info 字段，
/// 组织排序键集合的 JSON 负载后编码为 payloadB64，
/// 最后串联各关键要素计算 MD5 校验和构成 Bearer 凭据。
pub fn build_bearer(
    identity: &GatewayIdentity,
    date: &str,
    body: &str,
    path_sig: &str,
    request_id: Option<&str>,
) -> Result<BearerTokenResult, String> {
    let temp_key = generate_temp_key();
    let rsa_ciphertext = rsa_pkcs1_encrypt(&temp_key)?;
    let cosy_key = STANDARD.encode(&rsa_ciphertext);

    let auth_payload = serde_json::to_string(identity)
        .map_err(|err| format!("serialize auth payload error: {err}"))?;
    let encrypted_info = aes_128_cbc_encrypt(auth_payload.as_bytes(), &temp_key)?;
    let info = STANDARD.encode(&encrypted_info);

    let owned_req_id;
    let req_id_str = match request_id {
        Some(id) => id,
        None => {
            owned_req_id = generate_random_uuid();
            &owned_req_id
        }
    };

    let mut payload_map = BTreeMap::new();
    payload_map.insert("cosyVersion", COSY_VERSION);
    payload_map.insert("ideVersion", "");
    payload_map.insert("info", &info);
    payload_map.insert("requestId", req_id_str);
    payload_map.insert("version", "v1");

    let payload_json = serde_json::to_string(&payload_map)
        .map_err(|err| format!("serialize payload map error: {err}"))?;
    let payload_b64 = STANDARD.encode(payload_json.as_bytes());

    let signature_raw = format!("{payload_b64}\n{cosy_key}\n{date}\n{body}\n{path_sig}");
    let sig = md5_hex(&signature_raw);
    let bearer = format!("Bearer COSY.{payload_b64}.{sig}");

    Ok(BearerTokenResult { bearer, cosy_key })
}

/// 组装换取 jobToken 认证请求所需的 HTTP 头部映射集合。
pub fn auth_request_headers(
    machine: &GatewayMachineInfo,
    date: &str,
    signature: &str,
) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        "cosy-machinetoken".to_string(),
        machine.machine_token.clone(),
    );
    headers.insert("cosy-machinetype".to_string(), machine.machine_type.clone());
    headers.insert("login-version".to_string(), "v2".to_string());
    headers.insert("appcode".to_string(), "cosy".to_string());
    headers.insert("accept".to_string(), "application/json".to_string());
    headers.insert("accept-encoding".to_string(), "identity".to_string());
    headers.insert("cosy-version".to_string(), COSY_VERSION.to_string());
    headers.insert("cosy-clienttype".to_string(), "5".to_string());
    headers.insert("date".to_string(), date.to_string());
    headers.insert("signature".to_string(), signature.to_string());
    headers.insert("content-type".to_string(), "application/json".to_string());
    headers.insert("cosy-machineid".to_string(), machine.machine_id.clone());
    headers.insert("user-agent".to_string(), "Go-http-client/2.0".to_string());
    headers
}

/// 组装发送业务请求所需的携带 Bearer 鉴权的 HTTP 头部映射集合。
pub fn bearer_request_headers(
    sess: &GatewaySession,
    date: &str,
    bearer: &str,
    accept: &str,
) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("cosy-data-policy".to_string(), "AGREE".to_string());
    headers.insert("content-type".to_string(), "application/json".to_string());
    headers.insert("cosy-machinetype".to_string(), sess.machine_type.clone());
    headers.insert("cosy-clienttype".to_string(), "5".to_string());
    headers.insert("cosy-date".to_string(), date.to_string());
    headers.insert("cosy-user".to_string(), sess.identity.uid.clone());
    headers.insert("cosy-key".to_string(), sess.cosy_key.clone());
    headers.insert("accept".to_string(), accept.to_string());
    headers.insert("authorization".to_string(), bearer.to_string());
    headers.insert("accept-encoding".to_string(), "identity".to_string());
    headers.insert("cosy-version".to_string(), COSY_VERSION.to_string());
    headers.insert("cosy-machineid".to_string(), sess.machine_id.clone());
    headers.insert("cosy-machinetoken".to_string(), sess.machine_token.clone());
    headers.insert("login-version".to_string(), "v2".to_string());
    headers.insert("user-agent".to_string(), "Go-http-client/2.0".to_string());
    headers
}

/// 将字典头映射安全转换为 reqwest 专用的 HeaderMap。
pub fn to_reqwest_headers(map: &HashMap<String, String>) -> Result<HeaderMap, String> {
    let mut header_map = HeaderMap::new();
    for (key, val) in map {
        let header_name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|err| format!("invalid header name {key}: {err}"))?;
        let header_value = HeaderValue::from_str(val)
            .map_err(|err| format!("invalid header value {val}: {err}"))?;
        header_map.insert(header_name, header_value);
    }
    Ok(header_map)
}

/// 网关认证与会话生命周期管理客户端。
pub struct GatewayAuth {
    pat: String,
    client: reqwest::Client,
    gateway_url: String,
    machine: GatewayMachineInfo,
    request_timeout: Duration,
    session: tokio::sync::Mutex<Option<GatewaySession>>,
    /// 模型目录的进程内缓存; 首次解析模型名时拉取, 之后按 CATALOG_TTL 刷新.
    catalog: tokio::sync::Mutex<Option<CachedCatalog>>,
}

impl GatewayAuth {
    /// 创建新的网关认证客户端实例。
    pub fn new(
        pat: impl Into<String>,
        gateway_url: Option<&str>,
        machine: Option<GatewayMachineInfo>,
        request_timeout_ms: Option<u64>,
    ) -> Result<Self, String> {
        let pat_str = pat.into().trim().to_string();
        if pat_str.is_empty() {
            return Err("personal access token cannot be empty".to_string());
        }
        let timeout_ms = request_timeout_ms.unwrap_or(600_000);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|err| format!("failed to build reqwest client: {err}"))?;

        Ok(Self {
            pat: pat_str,
            client,
            gateway_url: gateway_url.unwrap_or(DEFAULT_GATEWAY_URL).to_string(),
            machine: machine.unwrap_or_default(),
            request_timeout: Duration::from_millis(timeout_ms),
            session: tokio::sync::Mutex::new(None),
            catalog: tokio::sync::Mutex::new(None),
        })
    }

    /// 判断当前会话是否已接近过期需要触发续期。
    pub fn needs_refresh(session: &GatewaySession) -> bool {
        if session.expire_time_ms == 0 {
            return true;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|dur| dur.as_millis() as i64)
            .unwrap_or(0);
        now_ms > session.expire_time_ms - REFRESH_MARGIN_MS
    }

    /// 获取当前缓存的活动会话克隆副本.
    pub async fn current_session(&self) -> Option<GatewaySession> {
        self.session.lock().await.clone()
    }

    /// 获取网关服务接入根地址.
    pub fn gateway_url(&self) -> &str {
        &self.gateway_url
    }

    /// 获取可用有效会话，若不存在或接近过期则自动执行刷新或重新握手。
    pub async fn ensure_session(&self) -> Result<GatewaySession, String> {
        let mut guard = self.session.lock().await;
        if let Some(sess) = guard.as_ref().filter(|s| !Self::needs_refresh(s)) {
            return Ok(sess.clone());
        }
        let new_sess = self.renew_session(&guard).await?;
        *guard = Some(new_sess.clone());
        Ok(new_sess)
    }

    async fn renew_session(
        &self,
        existing: &Option<GatewaySession>,
    ) -> Result<GatewaySession, String> {
        let has_tokens = existing
            .as_ref()
            .map(|s| !s.refresh_token.is_empty())
            .unwrap_or(false);

        if has_tokens {
            let (ref_token, sec_token) = match existing.as_ref() {
                Some(s) => (s.refresh_token.as_str(), s.security_oauth_token.as_str()),
                None => ("", ""),
            };
            match self
                .exchange_job_token(Some(ref_token), Some(sec_token), true)
                .await
            {
                Ok(token) => return session_from_job_token(&token, &self.machine),
                Err(_) => {
                    // 续期遭遇拒绝时回退冷交换重新申请
                    let cold_token = self.exchange_job_token(None, None, false).await?;
                    return session_from_job_token(&cold_token, &self.machine);
                }
            }
        }

        let cold_token = self.exchange_job_token(None, None, false).await?;
        session_from_job_token(&cold_token, &self.machine)
    }

    /// 把路由配置的模型名解析为网关内部目录键.
    ///
    /// 网关只认内部目录键, 配置中写显示名时必须经模型目录翻译; 保留键 default 请求网关的
    /// 免费默认模型, 不查目录直接放行. 目录过期时同步刷新一次; 刷新失败且存在缓存时沿用
    /// 缓存并记录告警, 完全没有缓存时报错, 不把未翻译的显示名发给网关静默兜底.
    pub async fn resolve_model_key(&self, model: &str) -> Result<String, String> {
        if model == GATEWAY_DEFAULT_KEY {
            return Ok(model.to_owned());
        }
        let mut guard = self.catalog.lock().await;
        let stale = match guard.as_ref() {
            Some(cached) => cached.fetched.elapsed() > CATALOG_TTL,
            None => true,
        };
        if stale {
            match self.fetch_model_catalog().await {
                Ok(catalog) => {
                    *guard = Some(CachedCatalog {
                        fetched: std::time::Instant::now(),
                        catalog,
                    });
                }
                Err(err) => {
                    if guard.is_some() {
                        aria_core::log!("warning: 刷新 Qoder 模型目录失败: {err}，沿用缓存目录");
                    } else {
                        return Err(format!("获取 Qoder 模型目录失败: {err}"));
                    }
                }
            }
        }
        let Some(cached) = guard.as_ref() else {
            // 刷新失败且没有任何缓存时已经在上面返回错误, 正常路径不会走到这里.
            return Err("Qoder 模型目录不可用".to_owned());
        };
        cached.catalog.resolve(model).ok_or_else(|| {
            format!(
                "Qoder 模型目录中没有模型 {model:?}，可用条目: {}",
                cached.catalog.describe_entries()
            )
        })
    }

    /// 向网关请求一次模型目录. GET 请求没有请求体, Bearer 签名按空载荷计算.
    async fn fetch_model_catalog(&self) -> Result<ModelCatalog, String> {
        let session = self.ensure_session().await?;
        let endpoint = format!(
            "{}{MODEL_LIST_ENDPOINT}",
            self.gateway_url.trim_end_matches('/')
        );
        let header_map = Self::build_business_headers(&session, "", &endpoint, None)?;
        let headers = to_reqwest_headers(&header_map)?;
        let response = self
            .client
            .get(&endpoint)
            .headers(headers)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|err| format!("请求模型目录失败: {err}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("模型目录接口返回 HTTP {status}"));
        }
        let body = response
            .bytes()
            .await
            .map_err(|err| format!("读取模型目录响应失败: {err}"))?;
        ModelCatalog::parse(&body)
    }

    /// 向网关发送加密认证载荷换取 jobToken 凭据。
    pub async fn exchange_job_token(
        &self,
        refresh_token: Option<&str>,
        security_oauth_token: Option<&str>,
        need_refresh: bool,
    ) -> Result<GatewayJobToken, String> {
        let mut inner_map = serde_json::Map::new();
        inner_map.insert(
            "personalToken".to_string(),
            serde_json::Value::String(self.pat.clone()),
        );
        inner_map.insert(
            "securityOauthToken".to_string(),
            serde_json::Value::String(security_oauth_token.unwrap_or("").to_string()),
        );
        inner_map.insert(
            "refreshToken".to_string(),
            serde_json::Value::String(refresh_token.unwrap_or("").to_string()),
        );
        inner_map.insert(
            "needRefresh".to_string(),
            serde_json::Value::Bool(need_refresh),
        );
        inner_map.insert(
            "authInfo".to_string(),
            serde_json::Value::Object(serde_json::Map::new()),
        );

        let inner_json = serde_json::to_string(&inner_map)
            .map_err(|err| format!("serialize inner auth json error: {err}"))?;

        let mut outer_map = serde_json::Map::new();
        outer_map.insert("payload".to_string(), serde_json::Value::String(inner_json));
        outer_map.insert(
            "encodeVersion".to_string(),
            serde_json::Value::String("1".to_string()),
        );

        let outer_json = serde_json::to_string(&outer_map)
            .map_err(|err| format!("serialize outer auth json error: {err}"))?;

        let encoded_body = encode(outer_json.as_bytes());
        let date = rfc1123_now();
        let signature = sign_auth_date(&date, DEFAULT_SECRET);
        let header_map_raw = auth_request_headers(&self.machine, &date, &signature);
        let headers = to_reqwest_headers(&header_map_raw)?;

        let endpoint = format!(
            "{}/algo/api/v3/user/jobToken?Encode=1",
            self.gateway_url.trim_end_matches('/')
        );

        let response = self
            .client
            .post(&endpoint)
            .headers(headers)
            .body(encoded_body)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|err| format!("post jobToken request failed: {err}"))?;

        let status = response.status();
        if !status.is_success() {
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error body".to_string());
            let truncated = if body_text.len() > 300 {
                &body_text[..300]
            } else {
                &body_text
            };
            return Err(format!(
                "gateway jobToken exchange failed with HTTP {status}: {truncated}"
            ));
        }

        let body_text = response
            .text()
            .await
            .map_err(|err| format!("read jobToken response body error: {err}"))?;

        serde_json::from_str::<GatewayJobToken>(&body_text)
            .map_err(|err| format!("parse jobToken json response error: {err}"))
    }

    /// 针对指定请求构建包含 Bearer 凭据的完整业务请求头集合。
    pub fn build_business_headers(
        session: &GatewaySession,
        body: &str,
        full_url: &str,
        accept: Option<&str>,
    ) -> Result<HashMap<String, String>, String> {
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|dur| dur.as_secs().to_string())
            .unwrap_or_else(|_| "0".to_string());
        let path_sig = signature_path(full_url);
        let bearer_res = build_bearer(&session.identity, &now_sec, body, &path_sig, None)?;
        let mut session_clone = session.clone();
        session_clone.cosy_key = bearer_res.cosy_key;
        Ok(bearer_request_headers(
            &session_clone,
            &now_sec,
            &bearer_res.bearer,
            accept.unwrap_or("application/json"),
        ))
    }
}
