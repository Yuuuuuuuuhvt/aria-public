//! 账号选择, 401 刷新重试与 503 容量重试预算, 与 Go 版 relay/account.go 和
//! relay/retry.go 逐函数对应.
//!
//! 两类失败分开处理: 容量不足在同一账号上有限重试, 令牌失效需要强制刷新后再试一次.
//! 401 可能只是令牌在服务运行期间被撤销, 刷新即可恢复; 刷新后仍然 401 才判定该账号
//! 不可用, 由调用方换号.
//!
//! Account selection, the 401 refresh retry and the 503 capacity retry budget, function by
//! function with Go's relay/account.go and relay/retry.go.
//!
//! Two failure classes are handled separately: capacity shortage is retried a limited number
//! of times on the same account, while token invalidation requires a forced refresh and one
//! retry. A 401 may simply mean the token was revoked while the service was running, and a
//! refresh recovers it; only a 401 after the refresh marks the account unusable, at which
//! point the caller rotates.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, StatusCode};

use super::cooldown::{
    ACCOUNT_COOLDOWN, Cooldown, ExhaustedModels, ExhaustionReason, MODEL_UNAVAILABLE_DURATION,
    TimeoutCooldownTracker, UnavailableModels,
};
use super::forward::anthropic_error_payload;
use super::quota::quota_reset;
use super::translate::{RelayTranslator, TranslatedFailure, TranslatedState};
use crate::antigravity::provider::{
    ANTIGRAVITY_CAPACITY_RETRIES, ANTIGRAVITY_CAPACITY_STATUSES, ANTIGRAVITY_ROTATE_STATUSES,
    translator_for,
};
use crate::chatgpt::CHATGPT_ROTATE_STATUSES;
use crate::client_id::ClientIdentity;
use crate::credential::{
    AuthHeaders, Entry, GoogleSource, Pool, SessionScoped, Source, Status, bearer_headers_of,
    name_of,
};
use crate::relay::request_shape;
use crate::xai::XAI_ROTATE_STATUSES;
use aria_core::usage::Observation;

/// 错误响应体的读取上限, 与 Go 版 logUpstreamError 的 1<<20 相同.
///
/// The read limit of an error response body, matching the 1<<20 of Go's logUpstreamError.
const MAX_ERROR_BODY_BYTES: usize = 1 << 20;

/// 写进日志的错误响应体上限, 与 Go 版 maxLoggedErrorBytes 相同.
///
/// The limit of the error body written into the log, matching Go's maxLoggedErrorBytes.
const MAX_LOGGED_ERROR_BYTES: usize = 1024;

/// 容量重试预算耗尽时的说明, 与 Go 版 errCapacityRetryTimeout 的文本一致.
///
/// The description used when the capacity retry budget is exhausted, matching the text of
/// Go's errCapacityRetryTimeout.
pub(crate) const CAPACITY_RETRY_TIMEOUT: &str = "上游容量重试预算已耗尽";

/// 全部账号都失败且没有更具体错误时的兜底说明, 与 Go 版 errNoUsableAccount 一致.
///
/// The fallback description when every account failed and no more specific error is
/// available, matching Go's errNoUsableAccount.
const NO_USABLE_ACCOUNT: &str = "没有可用账号";

/// 账号池中的账号全部处于 403 冷却时的说明, 见 architecture.md 5.11.
///
/// The description used when every account of the pool is in its 403 cooldown; see section 5.11
/// of architecture.md.
const ALL_ACCOUNTS_COOLING: &str = "全部账号处于 403 冷却";

/// 一次转发请求中来自客户端的两项输入与可选会话标识.
///
/// 三者一起构成账号选择与请求构造的输入, 因此合成一个结构传入, 避免参数列表继续膨胀.
///
/// The two client inputs of one forwarding request plus the optional session identifier.
/// Together they form the input of account selection and request construction, so they are passed
/// as one value instead of growing the argument list further.
pub(crate) struct Inbound<'a> {
    pub(crate) headers: &'a HeaderMap,
    pub(crate) body: &'a Bytes,
    /// 会话标识; 取不到或来源不支持会话固定时为空.
    ///
    /// The session identifier; empty when none can be derived or the source does not support
    /// pinning.
    pub(crate) session: Option<&'a str>,
    pub(crate) stream: bool,
}

/// 按一次快照渲染客户端标识, 返回版本与 User-Agent.
///
/// 标识在构造请求时渲染一次, 线路内的所有请求共用同一取值; 一次获取失败不会改变本次请求. 版本
/// 只从一份快照读取, 使 xai 的 User-Agent 与 x-grok-client-version 不会因为后台更新恰好发生在
/// 两次读取之间而给出不同版本. Command Code 线路使用固定的 User-Agent, 不经过这里.
///
/// Renders the client identifier from one snapshot and returns the version with the User-Agent.
///
/// The identifier is rendered once per request build and shared by every request of the line; one
/// failed fetch does not change this request. The version is read from a single snapshot, so xai's
/// User-Agent and x-grok-client-version cannot disagree because a background update happened
/// between two reads. The Command Code line uses a fixed User-Agent and never comes here.
fn rendered_identity(identity: &ClientIdentity) -> (String, String) {
    let version = identity.snapshot().version;
    let user_agent = identity.config().user_agent(&version);
    (version, user_agent)
}

/// 一条线路在转发阶段需要的能力. 各线路的差异集中在这里, 账号选择与重试逻辑因此共用
/// 一份实现.
///
/// The capabilities one route needs while forwarding. The differences between the routes are
/// collected here, so account selection and the retry logic share one implementation.
pub(crate) enum Route<'a> {
    Antigravity {
        source: &'a GoogleSource,
        base_url: &'a str,
        cooldown: &'a Cooldown,
        user_agent: &'a Arc<ClientIdentity>,
        unavailable: &'a UnavailableModels,
        upstream_model: &'a str,
        timeouts: &'a TimeoutCooldownTracker,
        /// 该模型的额度或容量耗尽记录, 见 architecture.md 5.13.
        ///
        /// The quota or capacity exhaustion records of this model, see section 5.13 of
        /// architecture.md.
        exhausted: &'a ExhaustedModels,
    },
    ChatGpt {
        source: &'a super::ChatGptSource,
        base_url: &'a str,
        cooldown: &'a Cooldown,
        user_agent: &'a Arc<ClientIdentity>,
    },
    /// SuperGrok 账号池. 与 ChatGPT 线路一样只轮转不做会话固定, 也没有容量重试与按模型的
    /// 不可用记录.
    ///
    /// The SuperGrok account pool. Like the ChatGPT line it rotates without session pinning and
    /// has neither capacity retries nor per-model unavailability records.
    Xai {
        source: &'a Pool,
        base_url: &'a str,
        cooldown: &'a Cooldown,
        user_agent: &'a Arc<ClientIdentity>,
    },
    /// Command Code 线路. 只有一把固定密钥, 换号状态码为空, 没有容量重试,
    /// 没有按模型的不可用与耗尽记录, 403 错误直接返回客户端且不进入冷却.
    ///
    /// The Command Code route. It has a single static key, an empty list of rotate status codes,
    /// no capacity retries, no per-model unavailable or exhausted records, and forwards 403 errors
    /// directly to the client without entering cooldown.
    CommandCode {
        source: &'a crate::commandcode::CommandCodeSource,
        base_url: &'a str,
    },
}

impl Route<'_> {
    /// 本线路账号池的冷却记录; 没有冷却的线路为 None, 收到 403 时也不冷却.
    ///
    /// Command Code 线路只有一把固定密钥, 它的 403 表示套餐不含所请求的模型而不是风控,
    /// 冷却会让整条线路 6 小时不可用, 因此这条线路没有冷却记录.
    ///
    /// The cooldown records of this route's account pool; None for a route without a cooldown,
    /// which therefore does not cool down on a 403 either.
    ///
    /// The Command Code line holds a single static key and its 403 means the plan does not
    /// include the requested model rather than a risk-control verdict; a cooldown would disable
    /// the whole line for six hours, so this line keeps no cooldown records.
    fn cooldown(&self) -> Option<&Cooldown> {
        match self {
            Self::Antigravity { cooldown, .. }
            | Self::ChatGpt { cooldown, .. }
            | Self::Xai { cooldown, .. } => Some(cooldown),
            Self::CommandCode { .. } => None,
        }
    }

    /// 触发换号的状态码.
    ///
    /// The status codes that trigger rotation.
    fn rotate_statuses(&self) -> &'static [u16] {
        match self {
            Self::Antigravity { .. } => &ANTIGRAVITY_ROTATE_STATUSES,
            Self::ChatGpt { .. } => &CHATGPT_ROTATE_STATUSES,
            Self::Xai { .. } => &XAI_ROTATE_STATUSES,
            Self::CommandCode { .. } => &[],
        }
    }

    /// 需要在同一账号有限重试的容量状态码; ChatGPT 与 xai 线路没有这一策略.
    ///
    /// The capacity status codes that get a limited same-account retry; the ChatGPT and xai
    /// routes have no such policy.
    fn capacity_statuses(&self) -> &'static [u16] {
        match self {
            Self::Antigravity { .. } => &ANTIGRAVITY_CAPACITY_STATUSES,
            Self::ChatGpt { .. } | Self::Xai { .. } | Self::CommandCode { .. } => &[],
        }
    }

    fn capacity_retries(&self) -> u32 {
        match self {
            Self::Antigravity { .. } => ANTIGRAVITY_CAPACITY_RETRIES,
            Self::ChatGpt { .. } | Self::Xai { .. } | Self::CommandCode { .. } => 0,
        }
    }

    /// 返回本线路的凭据状态, 供健康检查使用.
    ///
    /// Returns this route's credential statuses for the health check.
    pub(crate) async fn status(&self) -> Result<Vec<Status>, crate::credential::Error> {
        match self {
            Self::Antigravity { source, .. } => source.status().await,
            Self::ChatGpt { source, .. } => match source {
                super::ChatGptSource::Pool(pool) => pool.status().await,
                super::ChatGptSource::Broker(broker) => broker.status().await,
            },
            Self::Xai { source, .. } => source.status().await,
            Self::CommandCode { source, .. } => source.status().await,
        }
    }

    /// 按优先级与轮转规则取号; 会话标识可用时使用固定账号的顺序.
    ///
    /// Selects accounts by priority and rotation rules, using the pinned order when a session
    /// identifier is available.
    pub(crate) async fn snapshot(
        &self,
        session: Option<&str>,
    ) -> Result<Vec<Entry>, crate::credential::Error> {
        match (self, session) {
            (Self::Antigravity { source, .. }, Some(session)) => source.snapshot_for(session).await,
            (Self::Antigravity { source, .. }, None) => source.snapshot().await,
            (Self::ChatGpt { source, .. }, _) => match source {
                super::ChatGptSource::Pool(pool) => pool.snapshot().await,
                super::ChatGptSource::Broker(broker) => broker.snapshot().await,
            },
            (Self::Xai { source, .. }, _) => source.snapshot().await,
            (Self::CommandCode { source, .. }, _) => source.snapshot().await,
        }
    }

    /// 把会话固定的位置前移到下一个账号. 只有支持会话固定的来源需要这一动作.
    ///
    /// Moves the session's pinned position to the next account. Only sources supporting
    /// session pinning need this.
    async fn advance(&self, session: &str) {
        if let Self::Antigravity { source, .. } = self {
            source.advance(session).await;
        }
    }

    /// 取一次鉴权头, 必要时先刷新并写回.
    ///
    /// Obtains the auth headers once, refreshing and writing back first when needed.
    async fn authorize(
        &self,
        entry: Entry,
        force_refresh: bool,
    ) -> Result<AuthHeaders, crate::credential::Error> {
        match self {
            Self::Antigravity { source, .. } => source.authorize(entry, force_refresh).await,
            Self::ChatGpt { source, .. } => match source {
                super::ChatGptSource::Pool(pool) => pool.authorize(entry, force_refresh).await,
                super::ChatGptSource::Broker(broker) => {
                    broker.authorize(entry, force_refresh).await
                }
            },
            // 账号池的 authorize 还会生成 ChatGPT 专用的 chatgpt-account-id, Grok 只凭访问令牌
            // 识别账号, 因此这里取刷新后的条目, 只生成 Authorization, 理由见 bearer_headers_of.
            //
            // The pool's authorize also produces the ChatGPT-only chatgpt-account-id, while Grok
            // identifies the account by the access token alone, so the refreshed entry is taken
            // here and only Authorization is produced; see bearer_headers_of for the reason.
            Self::Xai { source, .. } => {
                let authorized = source.authorize_entry(&entry, force_refresh).await?;
                bearer_headers_of(&authorized)
            }
            Self::CommandCode { source, .. } => source.authorize(entry, force_refresh).await,
        }
    }

    /// 构造一次上游请求.
    ///
    /// 只有 antigravity 线路的转换错误属于客户端输入错误, 因此当 Go 版返回
    /// ErrInvalidAnthropicRequest 时映射为 InvalidRequest; 其余构造失败都是本端配置或
    /// 凭据问题.
    ///
    /// Builds one upstream request. Only the antigravity route's conversion errors are client
    /// input errors, so the case Go reports as ErrInvalidAnthropicRequest maps to
    /// InvalidRequest; every other build failure is a local configuration or credential
    /// problem.
    async fn build(
        &self,
        client: &reqwest::Client,
        entry: &Entry,
        auth_headers: &AuthHeaders,
        inbound: &Inbound<'_>,
    ) -> Result<reqwest::Request, AttemptError> {
        let pairs: Vec<(&str, &str)> = auth_headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        let (url, headers, payload) = match self {
            Self::Antigravity {
                base_url,
                user_agent: identity,
                ..
            } => {
                let (_, user_agent) = rendered_identity(identity);
                let project_id = crate::credential::google::project_id_of(entry);
                let built = crate::antigravity::provider::build_request(
                    base_url,
                    &pairs,
                    &project_id,
                    inbound.body.as_ref(),
                    &user_agent,
                )
                .map_err(|error| match error {
                    crate::upstream::BuildError::InvalidRequest(inner) => {
                        AttemptError::InvalidRequest(inner.to_string())
                    }
                    other => AttemptError::Other(other.to_string()),
                })?;
                (built.url, built.headers, built.body)
            }
            Self::ChatGpt {
                base_url,
                user_agent: identity,
                ..
            } => {
                let (_, user_agent) = rendered_identity(identity);
                let built = crate::chatgpt::build_request(
                    base_url,
                    &pairs,
                    inbound.headers,
                    inbound.body.as_ref(),
                    &user_agent,
                )
                .map_err(|error| AttemptError::Other(error.to_string()))?;
                (built.url, built.headers, built.body)
            }
            Self::Xai {
                base_url,
                user_agent: identity,
                ..
            } => {
                let (version, user_agent) = rendered_identity(identity);
                let built = crate::xai::build_request(
                    base_url,
                    &pairs,
                    inbound.body.as_ref(),
                    &user_agent,
                    &version,
                )
                .map_err(|error| AttemptError::Other(error.to_string()))?;
                (built.url, built.headers, built.body)
            }
            Self::CommandCode { base_url, .. } => {
                let built =
                    crate::commandcode::build_request(base_url, &pairs, inbound.body.as_ref())
                        .map_err(|error| AttemptError::Other(error.to_string()))?;
                (built.url, built.headers, built.body)
            }
        };
        client
            .request(reqwest::Method::POST, url)
            .headers(headers)
            .body(payload)
            .build()
            .map_err(|error| AttemptError::Other(format!("构造上游请求失败: {error}")))
    }
}

/// 一次转发请求在账号维度上的结果. 失败用错误值表达, 由调用方按类别决定回写 400, 503
/// 还是 502.
///
/// The result of one forwarding request along the account dimension. Failures are expressed
/// as errors and the caller decides from the class whether to write 400, 503 or 502.
pub(crate) enum AttemptError {
    /// 客户端输入无法转换, 换账号不会修复, 调用方返回 Anthropic 400.
    ///
    /// Client input cannot be converted and rotation would not fix it, so the caller returns
    /// an Anthropic 400.
    InvalidRequest(String),
    /// 容量重试预算耗尽且没有保留的容量响应.
    ///
    /// The capacity retry budget ran out and no capacity response was kept.
    CapacityTimeout,
    /// 上游等待响应头或首个有效事件超时.
    ///
    /// The first-byte timeout expired while waiting for headers or the first event.
    FirstByteTimeout,
    /// 模型在全部账号上都被记录为不可用, 调用方返回 Anthropic 404.
    ///
    /// The model is recorded unavailable on every account, answered with an Anthropic 404.
    ModelUnavailable(String),
    /// 模型在全部账号上都处于额度或容量耗尽记录中, 调用方返回 Anthropic 503 并给出恢复时间.
    ///
    /// The model is recorded as quota or capacity exhausted on every account, answered with an
    /// Anthropic 503 carrying the recovery time.
    Exhausted {
        /// 面向客户端的说明, 已包含模型名与恢复剩余秒数.
        ///
        /// The client-facing description, already carrying the model name and the remaining
        /// seconds until recovery.
        message: String,
        /// 最早恢复的剩余时长, 同时写入 Retry-After.
        ///
        /// The remaining time until the earliest recovery, also written into Retry-After.
        retry_after: Duration,
    },
    /// 输入错误与容量预算之外的失败: 鉴权刷新失败, 上游连接失败, 刷新后仍被拒绝.
    ///
    /// Failures other than invalid input and the capacity budget: a failed authorization
    /// refresh, a failed upstream connection, or a rejection that persists after a refresh.
    Other(String),
}

impl AttemptError {
    /// 返回面向客户端的说明. 输入错误之外的说明会进入 Anthropic 错误报文.
    ///
    /// Returns the client-facing description. Every description except an invalid input
    /// becomes the message of an Anthropic error body.
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::InvalidRequest(message)
            | Self::ModelUnavailable(message)
            | Self::Other(message) => message,
            Self::Exhausted { message, .. } => message,
            Self::CapacityTimeout => CAPACITY_RETRY_TIMEOUT,
            Self::FirstByteTimeout => "上游首字节读取超时",
        }
    }

    pub(crate) fn is_first_byte_timeout(&self) -> bool {
        matches!(self, Self::FirstByteTimeout)
    }
}

/// 一次账号选择的结果: 可供转发的响应与它所属的账号名.
///
/// The outcome of one account selection: a response ready to forward and the account it
/// belongs to.
pub(crate) struct Selected {
    pub(crate) response: AttemptResponse,
    pub(crate) account: String,
}

/// 选择失败时除了错误类别还带最后尝试的账号名, 供日志与错误响应使用.
///
/// On failure the error class is accompanied by the last attempted account name, used by logs
/// and error responses.
pub(crate) struct AttemptFailure {
    pub(crate) account: String,
    pub(crate) error: AttemptError,
}

/// 可转发的上游响应.
///
/// An upstream response ready to forward.
pub(crate) enum AttemptResponse {
    /// 直接来自上游的响应, 响应体尚未读取.
    ///
    /// A response coming straight from upstream with its body unread.
    Upstream(reqwest::Response),
    /// 已成功读取首个有效事件的流式响应.
    ///
    /// A streaming response whose first valid event has been committed.
    StreamPrelude {
        response: reqwest::Response,
        state: Box<TranslatedState>,
        first: Vec<u8>,
    },
    /// 之前收到的容量错误响应, 响应体已经读完.
    ///
    /// A capacity error response received earlier, its body already read.
    Replay(Arc<CapacityResponse>),
    /// 上游事件流携带的直接错误响应.
    ///
    /// A direct error response carried inside the upstream event stream.
    DirectError {
        status: StatusCode,
        headers: HeaderMap,
        body: Vec<u8>,
    },
}

impl AttemptResponse {
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::Upstream(response) | Self::StreamPrelude { response, .. } => response.status(),
            Self::Replay(response) => response.status,
            Self::DirectError { status, .. } => *status,
        }
    }

    pub(crate) fn headers(&self) -> &HeaderMap {
        match self {
            Self::Upstream(response) | Self::StreamPrelude { response, .. } => response.headers(),
            Self::Replay(response) => &response.headers,
            Self::DirectError { headers, .. } => headers,
        }
    }

    /// 取出上游的 Retry-After, 搜索轮次需要把它交给编排.
    ///
    /// Takes upstream's Retry-After, which the search rounds hand to the orchestration.
    pub(crate) fn retry_after(&self) -> Option<String> {
        super::forward::retry_after(self.headers())
    }
}

/// 已经读完的容量错误响应.
///
/// A capacity error response whose body has been read.
pub(crate) struct CapacityResponse {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Vec<u8>,
}

/// 容量重试预算. 与 Go 版 capacityRetryGuard 一致: 只在真实容量错误出现后启用期限, 首次
/// 正常请求继续使用现有的传输超时; 重试成功产生有效事件后解除期限.
///
/// Go 用 context 的取消原因表达"本端主动放弃"与"客户端断开"的区分. Rust 的转发入口没有
/// 客户端取消信号, 只有本端截止时间, 因此这里用截止时刻与两个原子标志表达同一语义:
/// 起算点与 Go 相同, 都取守卫创建的时刻, 也就是转发开始.
///
/// The capacity retry budget. Like Go's capacityRetryGuard: the deadline is armed only after
/// a real capacity error appears, a first normal request keeps the existing transport timeout,
/// and a successful retry that produces a valid event disarms the deadline.
///
/// Go distinguishes a deliberate local give-up from a client disconnect through a context
/// cancellation cause. The Rust forwarding entry point has no client cancellation signal, only
/// the local deadline, so this type uses a deadline and two atomic flags for the same
/// semantics: the start instant matches Go's, taken when the guard is created, which is the
/// start of forwarding.
pub(crate) struct CapacityRetryGuard {
    deadline: Instant,
    first_byte_timeout: Duration,
    active: AtomicBool,
    finished: AtomicBool,
    attempt_deadline: Mutex<Option<Instant>>,
    /// 保留最后一个容量错误响应, 供重试后首个事件挂起时回给客户端.
    ///
    /// Keeps the last capacity error response for when the retried first event hangs.
    capacity_response: Mutex<Option<Arc<CapacityResponse>>>,
}

impl CapacityRetryGuard {
    pub(crate) fn new(budget: Duration, first_byte_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + budget,
            first_byte_timeout,
            active: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            attempt_deadline: Mutex::new(None),
            capacity_response: Mutex::new(None),
        })
    }

    /// 启用期限. 只生效一次; 预算在启用时已经过去时, 后续的过期判定立即为真, 与 Go 版
    /// 剩余时间不大于零时直接取消等价.
    ///
    /// Arms the deadline. It applies once; when the budget has already passed at arming time,
    /// later expiry checks are immediately true, equivalent to Go cancelling right away when
    /// the remaining time is not positive.
    pub(crate) fn activate(&self) {
        let _ = self
            .active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// 为单次尝试启动首字节期限计时.
    ///
    /// Starts the first-byte timeout timer for one attempt.
    pub(crate) fn begin_attempt(&self) {
        let deadline = Instant::now() + self.first_byte_timeout;
        *self
            .attempt_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(deadline);
    }

    /// 解除单次尝试的首字节期限.
    ///
    /// Disarms the first-byte timeout for one attempt.
    pub(crate) fn disarm_first_byte(&self) {
        *self
            .attempt_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// 返回尚在计时的截止时刻; 期限未启用或已经解除时为 None.
    ///
    /// Returns the armed deadline, or None when the deadline is not armed or already
    /// disarmed.
    pub(crate) fn pending_deadline(&self) -> Option<tokio::time::Instant> {
        if self.finished.load(Ordering::SeqCst) {
            return None;
        }
        let capacity = if self.active.load(Ordering::SeqCst) {
            Some(self.deadline)
        } else {
            None
        };
        let attempt = *self
            .attempt_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let earliest = match (capacity, attempt) {
            (Some(c), Some(a)) => Some(c.min(a)),
            (Some(c), None) => Some(c),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        earliest.map(tokio::time::Instant::from_std)
    }

    /// 容量重试预算是否已经耗尽.
    ///
    /// Reports whether the capacity retry budget is exhausted.
    pub(crate) fn capacity_expired(&self) -> bool {
        self.active.load(Ordering::SeqCst)
            && !self.finished.load(Ordering::SeqCst)
            && Instant::now() >= self.deadline
    }

    /// 本次尝试的首字节期限是否已经耗尽.
    ///
    /// Reports whether the first-byte deadline for the current attempt has passed.
    #[allow(dead_code)]
    pub(crate) fn first_byte_expired(&self) -> bool {
        if let Some(deadline) = *self
            .attempt_deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            Instant::now() >= deadline
        } else {
            false
        }
    }

    /// 预算是否已经耗尽.
    ///
    /// Reports whether the budget is exhausted.
    pub(crate) fn expired(&self) -> bool {
        self.capacity_expired()
    }

    /// 解除期限并检查预算. 返回 Err 表示期限已经过去, 调用方按容量预算耗尽处理.
    ///
    /// Disarms the deadline and checks the budget. An Err means the deadline has passed and
    /// the caller treats it as an exhausted capacity budget.
    pub(crate) fn progress(&self) -> Result<(), ()> {
        self.disarm_first_byte();
        let expired = self.capacity_expired();
        self.finished.store(true, Ordering::SeqCst);
        if expired { Err(()) } else { Ok(()) }
    }

    pub(crate) fn set_capacity_response(&self, response: Arc<CapacityResponse>) {
        let mut slot = self.capacity_response();
        *slot = Some(response);
    }

    /// 取出保留的容量错误响应. 取出后不再保留, 与 Go 在 fail 中读完即关闭该响应体一致.
    ///
    /// Takes the kept capacity error response. It is no longer kept afterwards, matching Go
    /// closing that body once read in fail.
    pub(crate) fn take_capacity_response(&self) -> Option<Arc<CapacityResponse>> {
        self.capacity_response().take()
    }

    fn capacity_response(&self) -> std::sync::MutexGuard<'_, Option<Arc<CapacityResponse>>> {
        // 这里只存取一个指针, 不跨越 await, 锁中毒只可能来自无关的 panic.
        //
        // Only a pointer is stored or read here with no await in between, so poisoning can
        // only come from an unrelated panic.
        self.capacity_response
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// 用指定账号发起一次上游请求, 内部完成容量重试与刷新重试.
///
/// Issues one upstream request with the given account, performing the capacity retry and the
/// refresh retry internally.
async fn attempt(
    route: &Route<'_>,
    client: &reqwest::Client,
    entry: &Entry,
    inbound: &Inbound<'_>,
    guard: &Arc<CapacityRetryGuard>,
    observation: Option<&Observation>,
) -> Result<AttemptResponse, AttemptError> {
    let name = name_of(entry);
    let capacity_statuses = route.capacity_statuses();
    let capacity_retries = route.capacity_retries();
    // 已知容量错误在后续等待超时时仍有诊断价值, 因此保留最后一个完整错误响应.
    //
    // A known capacity error still carries diagnostic value when a later wait times out, so
    // the last complete error response is retained.
    let mut last_capacity: Option<Arc<CapacityResponse>> = None;

    for force_refresh in [false, true] {
        let auth_headers = route
            .authorize(entry.clone(), force_refresh)
            .await
            .map_err(|error| AttemptError::Other(error.to_string()))?;
        let mut round = 0u32;
        loop {
            if guard.expired() {
                return match last_capacity.take() {
                    Some(response) => Ok(AttemptResponse::Replay(response)),
                    None => Err(AttemptError::CapacityTimeout),
                };
            }
            guard.begin_attempt();
            let request = route.build(client, entry, &auth_headers, inbound).await?;
            let mut response = match send(client, request, guard).await {
                Ok(response) => response,
                Err(SendFailure::BudgetExpired) => {
                    return match last_capacity.take() {
                        Some(response) => Ok(AttemptResponse::Replay(response)),
                        None => Err(AttemptError::CapacityTimeout),
                    };
                }
                Err(SendFailure::FirstByteTimeout) => {
                    return Err(AttemptError::FirstByteTimeout);
                }
                Err(SendFailure::Upstream(error)) => {
                    if guard.expired()
                        && let Some(response) = last_capacity.take()
                    {
                        return Ok(AttemptResponse::Replay(response));
                    }
                    return Err(AttemptError::Other(format!(
                        "上游连接失败: {}",
                        describe_upstream(error.as_ref())
                    )));
                }
            };
            let status = response.status();
            let is_capacity = capacity_statuses.contains(&status.as_u16());
            if is_capacity {
                // 正常长提示词的首个响应可能超过预算, 因此不能在首次请求前启动计时器.
                // 收到真实 503 后才约束剩余等待, 并保留该错误给客户端.
                //
                // The first response to a normal long prompt may exceed the budget, so the
                // timer must not start before the first request. Only after a real 503 arrives
                // is the remaining wait bounded, and that error is kept for the client.
                guard.activate();
            }
            if is_capacity && round < capacity_retries {
                guard.disarm_first_byte();
                let retained =
                    Arc::new(buffered_error_response(response, &name, Some(guard)).await);
                aria_core::log!(
                    "credential {}: 上游返回 {}, 稍后原样重试",
                    name,
                    status.as_u16()
                );
                guard.set_capacity_response(retained.clone());
                last_capacity = Some(retained);
                let wait = Duration::from_secs(u64::from(round) + 1);
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = sleep_until_deadline(guard) => {
                        return match last_capacity.take() {
                            Some(response) => Ok(AttemptResponse::Replay(response)),
                            None => Err(AttemptError::CapacityTimeout),
                        };
                    }
                }
                round += 1;
                continue;
            }
            if status == StatusCode::UNAUTHORIZED && !force_refresh {
                guard.disarm_first_byte();
                drain_and_close(response).await;
                aria_core::log!("credential {}: 上游拒绝访问令牌, 刷新后重试", name);
                break;
            }
            if status.is_success() && matches!(route, Route::Antigravity { .. }) && inbound.stream {
                let (model, _) = request_shape(inbound.body);
                let translator =
                    RelayTranslator::Antigravity(translator_for(&model, true, inbound.body));
                let mut state = TranslatedState::new(
                    translator,
                    true,
                    observation.cloned(),
                    guard.clone(),
                    name.clone(),
                );
                match state.commit_prelude(&mut response).await {
                    Ok(first) => {
                        return Ok(AttemptResponse::StreamPrelude {
                            response,
                            state: Box::new(state),
                            first,
                        });
                    }
                    Err(failure) => {
                        if matches!(failure, TranslatedFailure::CapacityTimeout) {
                            return match last_capacity.take() {
                                Some(response) => Ok(AttemptResponse::Replay(response)),
                                None => Err(AttemptError::CapacityTimeout),
                            };
                        }
                        if let TranslatedFailure::Event(ref error) = failure {
                            failure.log(&name);
                            guard.disarm_first_byte();
                            let status = failure.status();
                            let payload =
                                anthropic_error_payload(status, error.to_string().as_bytes());
                            return Ok(AttemptResponse::DirectError {
                                status,
                                headers: response.headers().clone(),
                                body: payload,
                            });
                        }
                        if failure.message() == "上游首字节读取超时" {
                            failure.log(&name);
                            return Err(AttemptError::FirstByteTimeout);
                        }
                        failure.log(&name);
                        return Err(AttemptError::Other(format!(
                            "上游首字节读取失败: {}",
                            failure.message()
                        )));
                    }
                }
            }
            guard.disarm_first_byte();
            return Ok(AttemptResponse::Upstream(response));
        }
    }
    Err(AttemptError::Other("刷新后访问令牌仍被拒绝".to_owned()))
}

/// 发送请求时的三类失败: 容量预算耗尽, 首字节超时, 或者上游/传输层失败.
///
/// The three failure classes of sending a request: the capacity budget expired, the
/// first-byte timeout expired, or the upstream or transport layer failed.
enum SendFailure {
    BudgetExpired,
    FirstByteTimeout,
    Upstream(BoxError),
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// 发送一次请求. 预算或首字节期限生效时与截止时刻竞速, 使期限耗尽能够取消正在等待的上游读取, 与 Go
/// 版取消请求 context 的效果一致: 上游观察到连接被关闭, 之后的重试不再发出.
///
/// Sends one request. While a deadline is armed the send races the deadline, so an exhausted
/// deadline cancels an upstream read in progress, matching Go cancelling the request context:
/// upstream observes the closed connection and no later retry is issued.
async fn send(
    client: &reqwest::Client,
    request: reqwest::Request,
    guard: &CapacityRetryGuard,
) -> Result<reqwest::Response, SendFailure> {
    if guard.pending_deadline().is_some() {
        tokio::select! {
            result = client.execute(request) => result.map_err(|error| SendFailure::Upstream(Box::new(error))),
            () = sleep_until_deadline(guard) => {
                if guard.capacity_expired() {
                    Err(SendFailure::BudgetExpired)
                } else {
                    Err(SendFailure::FirstByteTimeout)
                }
            }
        }
    } else {
        client
            .execute(request)
            .await
            .map_err(|error| SendFailure::Upstream(Box::new(error)))
    }
}

/// 等待预算截止时刻. 期限未启用时不会返回, 因为调用方只在期限生效时等待它.
///
/// Waits until the budget deadline. It never returns when the deadline is not armed, because
/// callers wait on it only while the deadline is armed.
async fn sleep_until_deadline(guard: &CapacityRetryGuard) {
    match guard.pending_deadline() {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// 读取并记录上游错误响应体, 返回响应体可重新读取的替身, 与 Go 版 logUpstreamError 相同.
///
/// 响应体与日志分别设上限, 避免日志截断破坏回给客户端的错误对象. 预算生效时读取与截止时刻
/// 竞速: Go 版取消请求 context 会让读取中断并保留已经读到的片段, 这里保持同样的行为, 否则
/// 一个保持打开的 503 响应体会让转发一直等下去.
///
/// Reads and records an upstream error body and returns a stand-in whose body can be read
/// again, matching Go's logUpstreamError. The body and the log line have separate limits, so
/// truncating the log does not corrupt the error object returned to the client. While the budget
/// is armed the read races the deadline: Go cancelling the request context interrupts the read
/// and keeps the bytes already received, and the same behaviour is kept here, because otherwise
/// a 503 body that stays open would leave the forward waiting forever.
pub(crate) async fn buffered_error_response(
    response: reqwest::Response,
    served: &str,
    guard: Option<&CapacityRetryGuard>,
) -> CapacityResponse {
    let status = response.status();
    let headers = response.headers().clone();
    let (payload, failure) = read_error_body(response, guard).await;
    if let Some(reason) = failure {
        aria_core::log!("credential {}: 读取上游错误响应失败: {}", served, reason);
    }
    let logged = &payload[..payload.len().min(MAX_LOGGED_ERROR_BYTES)];
    aria_core::log!(
        "credential {}: 上游返回 {}, 响应体 {}",
        served,
        status.as_u16(),
        String::from_utf8_lossy(logged).trim()
    );
    CapacityResponse {
        status,
        headers,
        body: payload,
    }
}

/// 读取错误响应体, 最多 MAX_ERROR_BODY_BYTES 字节, 返回已读内容与中断原因.
///
/// 读取中断时保留已经收到的片段, 与 Go 的 io.ReadAll 在出错时返回部分数据一致.
///
/// Reads an error body up to MAX_ERROR_BODY_BYTES bytes and returns what was read together with
/// the reason it stopped. An interrupted read keeps the bytes already received, matching Go's
/// io.ReadAll returning partial data on error.
async fn read_error_body(
    mut response: reqwest::Response,
    guard: Option<&CapacityRetryGuard>,
) -> (Vec<u8>, Option<String>) {
    let mut payload = Vec::new();
    loop {
        if payload.len() >= MAX_ERROR_BODY_BYTES {
            return (payload, None);
        }
        let chunk = match guard.and_then(CapacityRetryGuard::pending_deadline) {
            Some(deadline) => {
                tokio::select! {
                    result = response.chunk() => result,
                    () = tokio::time::sleep_until(deadline) => return (payload, Some("容量重试预算已耗尽".to_owned())),
                }
            }
            None => response.chunk().await,
        };
        match chunk {
            Ok(Some(chunk)) => {
                let room = MAX_ERROR_BODY_BYTES - payload.len();
                payload.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) => return (payload, None),
            Err(error) => return (payload, Some(describe_upstream(&error))),
        }
    }
}

/// 丢弃并关闭上游响应, 用于换号前释放连接, 与 Go 版 drainAndClose 相同.
///
/// Discards and closes an upstream response to release the connection before rotating
/// accounts, matching Go's drainAndClose.
async fn drain_and_close(response: reqwest::Response) {
    let _ = crate::credential::read_limited(response, MAX_ERROR_BODY_BYTES).await;
}

/// 把传输层错误描述成不含地址的说明.
///
/// reqwest 的错误文本带有完整请求地址, 而上游地址可能含查询串; 日志与响应体都不允许
/// 出现这种地址, 因此这里只保留错误类别.
///
/// Describes a transport error without the address. reqwest's error text carries the whole
/// request URL, and an upstream URL may contain a query string; neither logs nor response
/// bodies may contain such an address, so only the error class is kept.
fn describe_upstream(error: &(dyn std::error::Error + Send + Sync + 'static)) -> String {
    let Some(error) = error.downcast_ref::<reqwest::Error>() else {
        return "上游请求失败".to_owned();
    };
    if error.is_timeout() {
        "上游读取超时".to_owned()
    } else if error.is_connect() {
        "上游连接失败".to_owned()
    } else if error.is_body() || error.is_decode() {
        "上游响应体读取失败".to_owned()
    } else {
        "上游请求失败".to_owned()
    }
}

/// 按队列顺序尝试账号, 返回可供转发的响应或可分类的失败.
///
/// 换号规则与 Go 版 selectCredential 一致: 上游返回 RotateStatuses 中的状态码且仍有后续
/// 账号时换下一个账号; 已经是最后一个账号时把上游响应原样交给客户端, 让客户端看到真实
/// 错误而不是合成的错误. 会话固定的账号在首个账号失败后前移一次, 之后的请求不再先试
/// 这个账号.
///
/// 与 Go 版不同, 403 不换号: 收到 403 的账号进入冷却, 响应直接交给客户端, 冷却中的账号在
/// 选号时被跳过, 规则见 architecture.md 5.11.
///
/// Go 在每次尝试前检查客户端 context, 客户端取消时释放已选中的响应且不写出响应. Rust 的
/// 转发入口没有客户端取消信号: 调用方丢弃整个请求 future 时, 已经选中的上游响应随语句
/// 作用域一起被丢弃, 连接随之关闭, 效果与 Go 的取消分支相同.
///
/// Tries accounts in queue order and returns a response ready to forward or a classifiable
/// failure. Rotation matches Go's selectCredential: when upstream returns a status listed in
/// RotateStatuses and another account remains, move to the next account; on the last account
/// the upstream response is handed to the client unchanged so it sees the real error rather
/// than a synthesized one. A session-pinned account advances once after the first account
/// fails, so later requests no longer try it first.
///
/// Unlike Go, a 403 does not rotate: the account that received it enters a cooldown, the
/// response goes straight to the client, and cooling accounts are skipped during selection; see
/// section 5.11 of architecture.md.
///
/// Go checks the client context before every attempt and, on cancellation, releases the
/// selected response without writing anything. The Rust entry point has no client
/// cancellation signal: when the caller drops the whole request future, a selected upstream
/// response is dropped with the statement scope and its connection closes, which is the same
/// effect as Go's cancellation branch.
pub(crate) async fn select_credential(
    route: &Route<'_>,
    client: &reqwest::Client,
    queue: &[Entry],
    inbound: &Inbound<'_>,
    guard: &Arc<CapacityRetryGuard>,
    observation: Option<&Observation>,
) -> Result<Selected, AttemptFailure> {
    let rotate_statuses = route.rotate_statuses();
    let now = Instant::now();
    let is_unavailable = |name: &str| -> bool {
        match route {
            Route::Antigravity {
                unavailable,
                upstream_model,
                ..
            } => unavailable.active(name, upstream_model, now),
            Route::ChatGpt { .. } | Route::Xai { .. } | Route::CommandCode { .. } => false,
        }
    };
    // 该账号在该模型上有未到期的额度或容量耗尽记录: 与 404 记录和 403 冷却并列, 选号时跳过.
    //
    // The account has an unexpired quota or capacity exhaustion record for this model: it is
    // skipped in selection alongside the 404 records and the 403 cooldowns.
    let is_exhausted = |name: &str| -> bool {
        match route {
            Route::Antigravity {
                exhausted,
                upstream_model,
                ..
            } => exhausted.active(name, upstream_model, now),
            Route::ChatGpt { .. } | Route::Xai { .. } | Route::CommandCode { .. } => false,
        }
    };
    let eligible: Vec<&Entry> = queue
        .iter()
        .filter(|entry| {
            let name = name_of(entry);
            let in_cooldown = route.cooldown().is_some_and(|c| c.active(&name, now));
            !in_cooldown && !is_unavailable(&name) && !is_exhausted(&name)
        })
        .collect();
    if eligible.is_empty() && !queue.is_empty() {
        if let Route::Antigravity { .. } = route
            && queue.iter().all(|entry| is_unavailable(&name_of(entry)))
        {
            return Err(AttemptFailure {
                account: String::new(),
                error: AttemptError::ModelUnavailable("没有账号能使用该模型".to_owned()),
            });
        }
        if let Route::Antigravity {
            exhausted,
            upstream_model,
            ..
        } = route
        {
            let names: Vec<String> = queue.iter().map(name_of).collect();
            if let Some(remaining) = exhausted.earliest_recovery(&names, upstream_model, now) {
                // 该模型的全部账号都在耗尽记录中: 逐个请求上游只会重复得到同样的拒绝, 因此直接
                // 返回带最早恢复时间的 503. 状态码与报文格式沿用容量预算耗尽那条路径.
                //
                // Every account is recorded as exhausted for this model: requesting upstream one
                // by one would only repeat the same rejection, so an Anthropic 503 carrying the
                // earliest recovery time is returned instead. The status and the payload follow
                // the exhausted capacity budget path.
                let seconds = remaining.as_secs().max(1);
                let message = format!(
                    "模型 {upstream_model} 的全部账号额度或容量耗尽, 最早 {seconds} 秒后恢复"
                );
                aria_core::log!(
                    "credential pool: 模型 {} 全部账号耗尽, 最早 {} 秒后恢复, 不请求上游",
                    upstream_model,
                    seconds
                );
                return Err(AttemptFailure {
                    account: String::new(),
                    error: AttemptError::Exhausted {
                        message,
                        retry_after: remaining,
                    },
                });
            }
        }
        return Err(AttemptFailure {
            account: String::new(),
            error: AttemptError::Other(ALL_ACCOUNTS_COOLING.to_owned()),
        });
    }
    let mut failure = AttemptError::Other(NO_USABLE_ACCOUNT.to_owned());
    let mut account = String::new();
    let mut last_429: Option<(String, Arc<CapacityResponse>)> = None;
    for (position, entry) in eligible.iter().copied().enumerate() {
        if guard.expired() {
            // 预算已经耗尽: 与 Go 版在每次尝试之前检查请求 context 一致, 立即停止, 不再为剩余
            // 账号取号与鉴权. 取号在令牌不新鲜时会向令牌端点发请求并写回凭据文件, 继续尝试会
            // 产生多余的刷新与写回, 并拖长回给客户端的 503.
            //
            // The budget is already exhausted: as in Go, which checks the request context before
            // every attempt, the loop stops at once and takes no number and performs no
            // authorization for the remaining accounts. Selection refreshes a stale token against
            // the token endpoint and writes the credential file back, so continuing would produce
            // needless refreshes and write-backs and would stretch the 503 returned to the client.
            return Err(AttemptFailure {
                account,
                error: AttemptError::CapacityTimeout,
            });
        }
        let name = name_of(entry);
        account = name.clone();
        if let Some(observation) = observation {
            observation.set_account(&name);
        }
        match attempt(route, client, entry, inbound, guard, observation).await {
            Err(AttemptError::InvalidRequest(message)) => {
                return Err(AttemptFailure {
                    account: name,
                    error: AttemptError::InvalidRequest(message),
                });
            }
            Err(error) => {
                aria_core::log!("credential {}: {}", name, error.message());
                advance_pinned(route, inbound, position).await;
                if error.is_first_byte_timeout()
                    && let Route::Antigravity {
                        timeouts, cooldown, ..
                    } = route
                    && timeouts.record_timeout(&name, cooldown, now)
                {
                    aria_core::log!(
                        "credential {}: 连续首字节超时 {} 次, 账号冷却 {} 秒, 换下一个账号",
                        name,
                        timeouts.threshold(),
                        timeouts.duration().as_secs()
                    );
                }
                failure = error;
            }
            Ok(response) => {
                let status = response.status().as_u16();
                if status == StatusCode::FORBIDDEN.as_u16() {
                    // 403 可能是风控判定, 换号会把同一份流量带到其他账号, 因此冷却该账号并把
                    // 响应直接交给客户端. 没有冷却记录的线路只把响应交给客户端, 理由见
                    // Route::cooldown.
                    //
                    // A 403 may be a risk-control verdict and rotating would carry the same
                    // traffic to other accounts, so the account cools down and the response goes
                    // straight to the client. A route without cooldown records only passes the
                    // response on; Route::cooldown gives the reason.
                    if let Some(cooldown) = route.cooldown() {
                        cooldown.start(&name, Instant::now());
                        aria_core::log!(
                            "credential {}: 上游返回 403, 账号冷却 {} 小时, 不换号",
                            name,
                            ACCOUNT_COOLDOWN.as_secs() / 3600
                        );
                    }
                    advance_pinned(route, inbound, position).await;
                    return Ok(Selected {
                        response,
                        account: name,
                    });
                }
                if status == StatusCode::NOT_FOUND.as_u16()
                    && let Route::Antigravity {
                        unavailable,
                        upstream_model,
                        ..
                    } = route
                {
                    unavailable.start(&name, upstream_model, Instant::now());
                    aria_core::log!(
                        "credential {}: 模型 {} 不可用, 记录 {} 小时",
                        name,
                        upstream_model,
                        MODEL_UNAVAILABLE_DURATION.as_secs() / 3600
                    );
                }
                if status == StatusCode::TOO_MANY_REQUESTS.as_u16() {
                    let retained = match response {
                        AttemptResponse::Upstream(response)
                        | AttemptResponse::StreamPrelude { response, .. } => {
                            Arc::new(buffered_error_response(response, &name, Some(guard)).await)
                        }
                        AttemptResponse::Replay(retained) => retained,
                        AttemptResponse::DirectError {
                            status,
                            headers,
                            body,
                        } => Arc::new(CapacityResponse {
                            status,
                            headers,
                            body,
                        }),
                    };
                    // 记录从收到拒绝的时刻起算, 不用本次选号开始时的 now: 503 在记录之前还要经过
                    // 同账号容量重试, 用更早的起点会缩短实际记录的窗口.
                    //
                    // The record starts when the rejection arrives rather than at the selection-start
                    // instant: a 503 passes through the same-account capacity retries first, and an
                    // earlier start would shorten the recorded window.
                    record_exhaustion(
                        route,
                        &name,
                        ExhaustionReason::Quota,
                        quota_reset(&retained.body),
                        Instant::now(),
                    );
                    last_429 = Some((name.clone(), retained.clone()));
                    if position + 1 < eligible.len() {
                        aria_core::log!("credential {}: 上游返回 {}, 换下一个账号", name, status);
                        advance_pinned(route, inbound, position).await;
                        failure = AttemptError::Other(format!("上游返回 {status}"));
                        continue;
                    }
                    return Ok(Selected {
                        response: AttemptResponse::Replay(retained),
                        account: name,
                    });
                }
                if status == StatusCode::SERVICE_UNAVAILABLE.as_u16() {
                    // 503 已经在 attempt 内做过同账号容量重试, 走到这里说明容量仍然不足: 记录
                    // 短时退避, 之后的请求先跳过该账号, 直到记录到期.
                    //
                    // The 503 already went through the same-account capacity retries inside
                    // attempt; reaching here means capacity is still short, so a short backoff is
                    // recorded and later requests skip the account until it expires.
                    record_exhaustion(
                        route,
                        &name,
                        ExhaustionReason::Capacity,
                        None,
                        Instant::now(),
                    );
                }
                if rotate_statuses.contains(&status) && position + 1 < eligible.len() {
                    // 还有账号可试才换号; 已经是最后一个账号时把上游响应原样交给客户端.
                    //
                    // Rotate only when another account remains; on the last account the
                    // upstream response is handed to the client unchanged.
                    aria_core::log!("credential {}: 上游返回 {}, 换下一个账号", name, status);
                    advance_pinned(route, inbound, position).await;
                    match response {
                        AttemptResponse::Upstream(response)
                        | AttemptResponse::StreamPrelude { response, .. } => {
                            drain_and_close(response).await;
                        }
                        AttemptResponse::Replay(_) | AttemptResponse::DirectError { .. } => {}
                    }
                    failure = AttemptError::Other(format!("上游返回 {status}"));
                    continue;
                }
                if rotate_statuses.contains(&status)
                    && let Some((account_429, response_429)) = last_429.take()
                {
                    match response {
                        AttemptResponse::Upstream(response)
                        | AttemptResponse::StreamPrelude { response, .. } => {
                            drain_and_close(response).await;
                        }
                        AttemptResponse::Replay(_) | AttemptResponse::DirectError { .. } => {}
                    }
                    return Ok(Selected {
                        response: AttemptResponse::Replay(response_429),
                        account: account_429,
                    });
                }
                if let Route::Antigravity {
                    timeouts,
                    exhausted,
                    upstream_model,
                    ..
                } = route
                {
                    timeouts.record_success(&name);
                    if (200..300).contains(&status) {
                        // 成功一次即清零: 该账号对该模型的请求已经通过, 之前的耗尽记录与连续
                        // 计数都不再成立.
                        //
                        // One success clears everything: the account just served a request for
                        // this model, so the earlier exhaustion record and the consecutive count
                        // no longer hold.
                        exhausted.record_success(&name, upstream_model);
                    }
                }
                return Ok(Selected {
                    response,
                    account: name,
                });
            }
        }
    }
    if guard.expired() {
        // 队列走完时预算也已耗尽: 与 Go 检查 context 取消原因一致, 容量预算的结论优先于
        // 循环里记住的最后一个错误.
        //
        // The queue ran out and the budget is exhausted too: matching Go checking the context
        // cause, the capacity conclusion takes precedence over the last error the loop kept.
        return Err(AttemptFailure {
            account,
            error: AttemptError::CapacityTimeout,
        });
    }
    if let Some((account_429, response_429)) = last_429.take() {
        return Ok(Selected {
            response: AttemptResponse::Replay(response_429),
            account: account_429,
        });
    }
    Err(AttemptFailure {
        account,
        error: failure,
    })
}

/// 记录一次额度或容量耗尽: 时长取上游给出的恢复时间, 取不到时按连续次数退避.
///
/// 只有 antigravity 线路有按账号与模型区分的记录; ChatGPT 线路的 429 沿用换号策略, 不记录.
/// 日志包含账号名, 模型名, 原因与记录时长, 不包含响应体.
///
/// Records one quota or capacity exhaustion: the duration is the recovery time the upstream gave,
/// or the backoff for the consecutive count when none could be taken.
///
/// Only the antigravity route records per account and model; a 429 on the ChatGPT route keeps the
/// rotation policy and is not recorded. The log line carries the account name, the model name, the
/// reason and the recorded duration, never the response body.
fn record_exhaustion(
    route: &Route<'_>,
    name: &str,
    reason: ExhaustionReason,
    parsed: Option<Duration>,
    now: Instant,
) {
    let Route::Antigravity {
        exhausted,
        upstream_model,
        ..
    } = route
    else {
        return;
    };
    let duration = exhausted.record(name, upstream_model, reason, parsed, now);
    let source = if parsed.is_some() {
        "上游给出的恢复时间"
    } else {
        "退避"
    };
    aria_core::log!(
        "credential {}: 模型 {} {}耗尽, 按{}记录不可用 {} 秒",
        name,
        upstream_model,
        reason.label(),
        source,
        duration.as_secs()
    );
}

/// 该会话首选的账号已不可用时, 把固定位置前移到下一个账号. 只在首个账号上前移一次.
///
/// When the account pinned for the session is unavailable, moves the pinned position to the next
/// account. It advances only once, on the first account.
async fn advance_pinned(route: &Route<'_>, inbound: &Inbound<'_>, position: usize) {
    if position == 0
        && let Some(session) = inbound.session
    {
        route.advance(session).await;
    }
}
