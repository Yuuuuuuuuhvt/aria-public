//! 一条上游线路所需的凭据来源.
//!
//! 两条线路的凭据语义不同: ChatGPT 用账号令牌, 续期走 auth.openai.com; Google 用
//! OAuth 刷新令牌, 续期走 oauth2.googleapis.com, 并且额外需要一个项目标识. 因此这里
//! 只约定两者共有的三件事, 具体语义各自实现.
//!
//! 之所以抽成接口而不是共用一份实现: 取号与刷新逻辑与各自的账号模型绑定, 强行参数化
//! 会让两条线路都变得难读, 而共用部分本来就只有几行.
//!
//! The credential source required by one upstream route.
//!
//! The two routes have different credential semantics: ChatGPT uses account tokens
//! renewed through auth.openai.com, while Google uses OAuth refresh tokens renewed
//! through oauth2.googleapis.com and additionally needs a project id. This interface
//! therefore fixes only the three things both share, and each side implements its own
//! semantics.
//!
//! It is an interface rather than one shared implementation because selection and refresh
//! are tied to each account model; forcing parameterization would make both routes harder
//! to read, and the shared part is only a few lines.
//!
//! 方法返回 impl Future 而不是写成 async fn, 是为了让调用方能够依赖这些 future 是
//! Send: 取号与刷新都要在异步运行时里与请求一起被驱动, 而 async fn 在 trait 中的
//! 返回类型不携带 Send 约束.
//!
//! The methods return impl Future rather than being written as async fn so callers can
//! rely on these futures being Send: selection and refresh are driven together with a
//! request inside the async runtime, and an async fn in a trait does not carry a Send
//! bound in its return type.

use std::future::Future;

use super::{AuthHeaders, Entry, Error, Status};

/// 一条上游线路的凭据来源.
///
/// The credential source of one upstream route.
pub trait Source: Send + Sync {
    /// 返回按优先级与轮转规则排好序的启用凭据.
    ///
    /// Returns enabled credentials ordered by priority and rotation rules.
    fn snapshot(&self) -> impl Future<Output = Result<Vec<Entry>, Error>> + Send;

    /// 返回该凭据的鉴权头, 必要时先刷新并写回.
    ///
    /// Returns the credential's auth headers, refreshing and writing back first when
    /// needed.
    fn authorize(
        &self,
        entry: Entry,
        force_refresh: bool,
    ) -> impl Future<Output = Result<AuthHeaders, Error>> + Send;

    /// 返回全部凭据的公开状态, 含已停用的条目.
    ///
    /// Returns the public status of every credential, including disabled entries.
    fn status(&self) -> impl Future<Output = Result<Vec<Status>, Error>> + Send;
}

/// 支持按会话固定账号的凭据来源实现的扩展接口.
///
/// 固定账号的目的是提高上游提示缓存的命中率: 缓存不跨账号共享, 同一账号连续请求才能
/// 复用. 不支持该能力的来源只实现 Source, 调用方按轮转取号, 行为不变.
///
/// The extension interface implemented by credential sources that support pinning an
/// account per session.
///
/// Pinning raises the upstream prompt cache hit rate: the cache is not shared across
/// accounts and only consecutive requests on one account reuse it. Sources without the
/// capability implement only Source, and callers rotate as before with unchanged
/// behavior.
pub trait SessionScoped: Source {
    /// 返回把 session_key 当前固定的账号排在最前的凭据队列. 该会话首次出现时固定到
    /// 本轮起点账号.
    ///
    /// Returns the credential queue with the account currently pinned for session_key
    /// first. On the session's first appearance it pins the account at the current
    /// rotation start.
    fn snapshot_for(
        &self,
        session_key: &str,
    ) -> impl Future<Output = Result<Vec<Entry>, Error>> + Send;

    /// 把 session_key 的固定位置移到下一个账号, 在固定账号不可用时调用.
    ///
    /// Moves session_key's pinned position to the next account, called when the pinned
    /// account is unavailable.
    fn advance(&self, session_key: &str) -> impl Future<Output = ()> + Send;
}
