# Aria 架构与行为契约

本文是 Aria 的实现规格. 实现, 测试与审查都以本文为准; 本文没有写明的行为, 以 Aria 现有实现与测试的行为为准.

## 1. 目标与背景

Aria 用一个 Rust 可执行文件提供模型网关与订阅账号中继两类能力.

1. Canto 是模型网关. 它把 Claude Code 的 Anthropic Messages 请求按模型名路由到已经实现同一协议的上游, 只改写模型名, 并为部分路由编排 Kimi 的独立搜索.
2. Lyra 是订阅账号中继. ChatGPT 账号池把 Codex 的 Responses 请求透传到 chatgpt.com; Google 账号池把 Anthropic Messages 请求转换为 Antigravity 协议.

同一个 aria 产物按配置文件启用不同能力: 单机可以启用全部能力, 也可以只启用某一个提供方, 例如只启用 ChatGPT 账号池或只启用 Google 账号池. 上游代理只取自进程环境, Aria 不在软件层区分直连与代理.

## 2. 工程结构与模块对应关系

工程是一个 Cargo workspace, 包含四个 crate. 依赖版本只在根目录 Cargo.toml 的 workspace.dependencies 中声明.

### 2.1 aria-core

aria-core 收纳 canto 与 lyra 共用的基础能力.

| 模块 | 职责 | Go 来源 |
|---|---|---|
| jsonval | 凭据文件的取值语义, 已实现 | lyra/internal/jsonval |
| log | log! 日志宏与时间戳, 已实现 | lyra/internal/relaylog |
| body | 统一响应体类型 Body 与 full, empty, 已实现 | 无 |
| errors | Anthropic 与 OpenAI 两种错误格式, 已实现 | canto gateway.go 的 writeError, lyra relay/stream.go 的 anthropicErrorBody |
| auth | 客户端密钥文件加载, 常量时间比对, 从请求头取密钥 | canto/internal/auth, lyra/internal/auth |
| upstream | 构造上游 reqwest 客户端, 代理只取自进程环境 | canto gateway/transport.go, lyra cmd/lyra/main.go 的 newUpstreamTransport |
| sse | 增量 SSE 解析器, 供协议转换读取上游事件 | lyra relay/sse.go |
| usage | 用量采集与按日 JSONL 存储及查询 | lyra internal/usage |
| exhaustion | 额度与容量耗尽的共享规则: 有上限指数退避, Retry-After, Go 时长与错误体恢复字段的解析, 见 5.13 | 无 |

### 2.2 canto

| 模块 | 职责 | Go 来源 |
|---|---|---|
| forward | messages 路由的请求改写与流式转发 | canto gateway/gateway.go, destination.go, transport.go |
| search | Moonshot 搜索编排, 搜索客户端, 搜索结果的 JSON 与 SSE 编码 | canto gateway/search.go, search_client.go, search_workflow.go, search_encode.go |
| unavailable | 单账号 messages 路由的路由级不可用窗口与配额类判定, 见 5.13 | 无 |

### 2.3 lyra

| 模块 | 职责 | Go 来源 |
|---|---|---|
| credential::pool | ChatGPT 与 SuperGrok 账号池: 排序, 轮询, 刷新, 原子写回; 两条线路的 OAuth 客户端参数与提前刷新余量由构造函数传入 | lyra credential/pool.go, source.go |
| credential::google | Google 账号池: 会话固定, 刷新, 项目标识推导 | lyra credential/google.go |
| chatgpt | ChatGPT 提供方: Responses 透传的请求构造; 子模块 request, response 与 signatures 的 Messages 转换同时供 xai 使用 | lyra upstream/chatgpt.go |
| xai | xai 提供方: Grok CLI 订阅后端的请求构造, 指纹头与 OAuth 常量, 见 3.10 | 无 |
| antigravity::provider | Antigravity 提供方: 请求封装, 会话键, 思考强度档位 | lyra upstream/antigravity.go, provider.go |
| antigravity::request | Anthropic 请求转为 Gemini 请求 | lyra upstream/antigravity_request.go, anthropic.go |
| antigravity::response | Gemini 事件转为 Anthropic 流式或非流式响应 | lyra upstream/antigravity_response.go |
| antigravity::signatures | 函数调用签名桥的编码与校验 | lyra upstream/antigravity_signatures.go |
| antigravity::search | Google 搜索请求校验与结果编码 | lyra upstream/antigravity_search.go |
| relay | 账号选择, 401 刷新重试, 容量重试预算, 透传与转换转发, 用量观测 | lyra relay 目录除 sse.go 以外的全部文件 |
| login::google | Google 浏览器登录, 输出候选凭据文件 | lyra cmd/agy-login |
| login::chatgpt | ChatGPT 浏览器登录, 新增功能, 见第 6.2 节 | 无 |
| login::xai | xai 浏览器登录, 新增功能, 见第 6.3 节 | 无 |

### 2.4 aria

aria 是可执行文件 crate, 负责命令行, 统一配置解析与校验, HTTP 服务, 请求分派, 模型列表, 健康检查与用量查询端点, 以及优雅停机. Go 来源是 canto cmd/model-gateway, lyra cmd/lyra, lyra relay/handler.go 中的分派部分.

### 2.5 搜索轮次接口

Moonshot 搜索编排需要多次以非流式方式调用原模型. messages 路由的轮次是一次 HTTP 请求, antigravity, chatgpt 与 xai 路由的轮次要经过 lyra 的账号选择与协议转换, qoder 路由的轮次要经过 lyra 的 Qoder 网关缓冲入口. 因此 canto::search 不直接发请求, 而是通过下面的接口执行每一轮, 由 aria crate 为各种路由分别实现. canto 与 lyra 之间没有直接依赖.

```rust
pub struct TurnResponse {
    pub status: http::StatusCode,
    pub retry_after: Option<String>,
    pub body: bytes::Bytes,
}

pub type TurnFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<TurnResponse, SearchFailure>> + Send + 'a>>;

pub trait SearchTurn: Send + Sync {
    /// headers 是客户端原始请求头, body 是本轮完整的非流式请求体.
    fn turn<'a>(&'a self, headers: &'a http::HeaderMap, body: bytes::Bytes) -> TurnFuture<'a>;
}
```

状态码不是 200 时, 编排返回 SearchFailure, 状态码与 Retry-After 取自 TurnResponse, 信息在固定文本 search workflow upstream returned an error 之后附上 TurnResponse 中携带的上游原因; 响应体超过 4 MiB 时返回 502. 每个路由的轮次都在非 200 时读出错误正文作为失败原因, 读取上限与配额类判定相同, 都是 forward.rs 中的 MAX_ERROR_BODY_BYTES, 因此任何一条线路的故障都能把上游原因带回客户端; 原因从错误正文的 JSON 中提取, 读取到 300 字节就截断会让 JSON 解析失败, 所以正文需要完整读出, 300 字节的上限只用于截断提取出的原因. 这与 Go 版 readSearchResponse 的大小限制规则相同, 附加上游原因是 Aria 新增的行为, 见第 7 节. lyra::relay 为此提供一个缓冲执行入口, 以非流式方式完成一次 antigravity, chatgpt 或 xai 请求并返回状态码与完整响应体; lyra::qoder 的缓冲入口把网关事件流汇总成一条非流式 Messages 消息, 走 qoder 路由的搜索轮次使用它.

### 2.6 Antigravity 工具模式的已知修复

Antigravity 线路对工具参数模式做一处修正: 白名单 geminiSchemaKeys 删除 anyOf, sanitizeSchema 不再递归处理 anyOf, anyOf 与其他未知关键字一样被剔除. 原因是 Google Antigravity 把函数声明转给 Vertex AI 上的 Claude 5.5 时无法合法转换 anyOf, 会返回 400 JSON schema is invalid.

## 3. 统一配置文件

aria serve 读取一个 JSON 配置文件. 解析时拒绝未知字段, 文件只能包含一个 JSON 对象. 任何校验失败都必须在开始监听前报错退出, 错误信息说明出错的字段与路由名, 不输出密钥.

### 3.1 顶层字段

| 字段 | 类型 | 默认值 | 含义 |
|---|---|---|---|
| listen | 字符串 | 127.0.0.1:20182 | 监听地址, 命令行 --listen 优先 |
| clients | 字符串 | 无 | 客户端密钥文件路径, 命令行 --clients 优先, 两者至少有一个 |
| max_concurrency | 正整数 | 100 | 同时处理的生成请求上限 |
| max_body_bytes | 正整数 | 33554432 | 请求体上限 |
| upstream_idle_timeout_secs | 正整数 | 600 | 等待上游响应头与响应体任意字节的最长空闲时间 |
| upstream_first_byte_timeout_secs | 正整数 | 120 | 等待上游响应头与首个有效事件的最长首字节时间, 必须小于 upstream_idle_timeout_secs |
| timeout_cooldown_threshold | 正整数 | 2 | Antigravity 账号连续首字节超时触发冷却的次数上限 |
| timeout_cooldown_secs | 正整数 | 300 | Antigravity 账号因连续首字节超时进入冷却的时长 |
| usage_dir | 字符串 | 无 | 设置后启用用量采集与 GET /v1/usage |
| moonshot_search | 对象 | 无 | Kimi 搜索服务, 字段 url 与 key_env, 校验规则同 Canto |
| providers | 对象 | 空 | 订阅账号提供方, 见 3.2 |
| routes | 数组 | 空 | Messages 路由, 见 3.3 |

routes 为空且没有配置 chatgpt 提供方时, 配置无效. 只配置 xai 提供方而没有路由同样无效, 因为 xai 提供方只能经路由使用.

### 3.2 providers

providers.antigravity 启用 Google 账号池.

| 字段 | 默认值 | 含义 |
|---|---|---|
| credentials | 必填 | Google 账号池文件路径, 格式同 Lyra 的 google.json |
| base_url | https://daily-cloudcode-pa.googleapis.com | Antigravity 后端 |
| client_id, client_secret | 见右侧 | OAuth 客户端. client_id 有公开默认值; client_secret 没有默认值, 必须由配置提供或取自环境变量 ARIA_ANTIGRAVITY_CLIENT_SECRET, 两者都没有时配置校验失败 |

providers.chatgpt 启用 ChatGPT 账号池与 Responses 端点.

| 字段 | 默认值 | 含义 |
|---|---|---|
| credentials | 与 broker 二选一 | ChatGPT 账号池文件路径, 格式同 Lyra 的 credentials.json; 本实例持有 refresh token 并负责刷新 |
| broker | 与 credentials 二选一 | 令牌分发来源, 字段 url 与 key_env, 见 3.6 |
| base_url | https://chatgpt.com/backend-api/codex | Codex 后端 |
| models | Aria 内置的 Codex 模型清单 | 出现在模型列表中的 Codex 模型名 |
| broker_clients | 无 | 只在使用 credentials 写法时允许. 分发密钥文件路径, 格式同 3.5 的客户端密钥文件; 设置后启用 3.6 的分发端点 |

providers.xai 启用 SuperGrok 账号池与 xai 路由, 见 3.10. 它不提供 Responses 透传端点, 也没有令牌分发写法.

| 字段 | 默认值 | 含义 |
|---|---|---|
| credentials | 必填 | SuperGrok 账号池文件路径, 格式同 ChatGPT 账号池; 本实例持有 refresh token 并负责刷新, 候选条目由 aria login xai 生成, 见 6.3 |
| base_url | https://cli-chat-proxy.grok.com/v1 | 官方 Grok CLI 使用的订阅后端 |
| token_url | https://auth.x.ai/oauth2/token | 令牌刷新端点 |
| client_id | b1a00492-073a-47ea-816f-4c329264a828 | 官方 Grok CLI 的公开 OAuth 客户端 |
| scope | openid profile email offline_access grok-cli:access api:access | 刷新申请的授权范围, 与 aria login xai 申请的范围相同 |
| models | grok-4.7, grok-4.7-build-fast, grok-4.6, grok-4.5 | 出现在模型列表中的 Grok 模型名, 取自 2026-10-09 探针的模型清单 |

providers.qoder 启用 Qoder 官方网关直接上游.

| 字段 | 默认值 | 含义 |
|---|---|---|
| base_url | https://gateway.qoder.com.cn | Qoder 网关接入根地址 |
| token_env | QODER_CN_PAT | 个人访问令牌所在的环境变量名 |
| machine_file | 无 | 设备持久化机器标识存储文件路径, 未设置时在内存中临时生成 |
| max_concurrency | 2 | Qoder 上游并发请求上限 |
| models | 空 | 该提供方支持的模型列表 |

providers.commandcode 启用 Command Code Provider API 接入, 见 3.11.

| 字段 | 默认值 | 含义 |
|---|---|---|
| base_url | https://api.commandcode.ai/provider/v1 | Command Code Provider API 接入根地址 |
| key_env | COMMAND_CODE_API_KEY | 上游 API 密钥所在的环境变量名 |
| models | zai-org/GLM-5.3 | 出现在模型列表中的模型名 |

### 3.3 routes

每条路由的字段如下. upstream 决定路由类型, 两种类型允许的字段不同, 多余字段视为错误.

| 字段 | messages 类型 | antigravity 类型 | 含义 |
|---|---|---|---|
| name | 必填 | 必填 | 路由名, 也是可请求的模型名 |
| display_name | 可选 | 可选 | 模型列表中的展示名 |
| aliases | 可选 | 可选 | 其他可请求的模型名 |
| model | 必填 | 必填 | 发给上游的模型名 |
| upstream | messages | antigravity | 路由类型; 第三种类型 chatgpt 见 3.7, 第四种类型 qoder 见 3.9, 第五种类型 xai 见 3.10, 第六种类型 commandcode 见 3.11 |
| base_url | 必填 | 不允许 | 上游 Messages 基址, 网关追加 /v1/messages |
| base_url_env | 可选 | 不允许 | 环境变量非空时覆盖 base_url |
| key_env | 必填 | 不允许 | 上游密钥所在的环境变量 |
| moonshot_search | 可选 | 可选 | 为真时独立网页搜索走 Kimi 搜索服务 |
| max_output_tokens | 可选 | 可选 | 正整数, 该路由允许的最大输出 token 数, 见本节末尾 |

校验规则沿用 Canto: 路由名与别名全局唯一且非空; 基址必须是 HTTPS, 只有回环地址允许 HTTP, 不能包含用户信息, 查询串或片段; 密钥必须非空且不含回车, 换行与空字符; moonshot_search 为真时必须配置顶层 moonshot_search. antigravity 类型的路由要求配置 providers.antigravity. qoder 类型的路由要求配置 providers.qoder, 字段规则见 3.9. xai 类型的路由要求配置 providers.xai, 字段规则见 3.10. commandcode 类型的路由要求配置 providers.commandcode, 字段规则见 3.11.

antigravity 路由的 moonshot_search 为真时, 独立网页搜索走 Kimi 搜索编排, 搜索轮次发往同一条 antigravity 路由. 为假时, 网页搜索请求交给 Antigravity 的 Google 搜索, 行为同 Lyra.

max_output_tokens 的规则: 用户于 2026-10-06 决定由网关按路由限制输出上限, 起因是部分上游在最高思考强度下单次思考就用满 Claude Code 的默认输出上限. 设置后, 请求的 max_tokens 大于该值时改写为该值; 请求没有 max_tokens 时不添加. 思考预算规则独立于上限: 任何一轮请求中 thinking.budget_tokens 存在且不小于该轮最终的 max_tokens 时, 改写为 max_tokens 减 1, 结果低于 1024 时仍然保留, 交由上游判断. 预算规则对每一轮都适用, 包括 Moonshot 搜索编排把 max_tokens 改为剩余额度的后续轮次, 这一点与是否设置 max_output_tokens 无关. 两项都不需要改写时请求体逐字节不变; 需要改写时只修改这两个字段, 其余字段按 5.5 第 1 条保持原始取值. 上限规则对生成请求, 搜索编排的每一轮与 antigravity 路由都适用, count_tokens 不改写. 取值由各路由的配置决定.

### 3.4 messages 路由的多上游轮换

同一服务商可能有多把密钥或多个渠道, 用户要求轮换使用. messages 类型的路由可以用 upstreams 数组代替 base_url, base_url_env 与 key_env 三个字段, 两种写法只能选一种. upstreams 的每一项是 {"base_url": ..., "base_url_env": ..., "key_env": ...}, 至少一项, 每项的校验规则同单上游写法.

1. 每个请求从路由内部的轮询游标指向的上游开始, 游标在每个请求后前进一位, 使多把密钥的用量大致均衡.
2. 上游返回 401, 402, 403 或 429 时, 读取并丢弃该响应体, 换下一个上游重新发送同一请求体, 直到全部上游都试过; 最后一个上游的响应原样返回. 这些状态码表示上游在生成开始前明确拒绝, 换上游重试不违反不重放生成请求的原则. 连接失败, 超时和其他状态码不换上游. 常规生成的连接建立阶段失败在同一上游上重试一次, 见 5.5 第 4 条; 连接超时与请求已发出后的失败不重试, 与不重放规则一致.
3. 每次换上游写一行日志, 包含路由名, 上游序号与状态码, 不包含密钥与地址.
4. 搜索轮次与 count_tokens 使用同样的轮换规则.

### 3.5 客户端密钥文件

格式同 Canto 与 Lyra: {"keys": [{"name": ..., "key": ..., "enabled": ...}]}. enabled 缺失视为启用, 真值规则用 jsonval::truthy. 启用的密钥数为零时启动失败.

### 3.6 ChatGPT 令牌分发

ChatGPT 的 refresh token 每次刷新都会轮换并作废旧值, 同一份凭据不能由多个实例同时刷新. 因此只让一个实例持有 refresh token, 其他实例只向它索取 access token.

1. 分发端: 配置了 providers.chatgpt.credentials 与 broker_clients 的实例提供 GET /v1/broker/chatgpt. 鉴权只接受 broker_clients 中的密钥, 普通客户端密钥返回 401; broker_clients 中的密钥也不能访问其他端点. 查询参数 account 可选, 取账号的 account_id; force 可选, 取 1 时强制刷新指定账号或全部启用账号. 处理时对每个目标账号调用凭据池的 authorize, 不新鲜时刷新并写回, 规则同 Responses 透传. 响应为 {"credentials": [...]}, 每项只包含 name, account_id, plan, enabled, priority, access_token, expires_at, 不包含 refresh_token 与其他字段. 刷新失败的账号不出现在结果中, 并写日志. 全部失败时返回 502.
2. 索取端: providers.chatgpt.broker 的 url 必须是 HTTPS 或回环 HTTP, 不能包含用户信息, 查询串或片段; key_env 指定分发密钥所在的环境变量. 索取端不读写任何凭据文件, 账号列表只保存在内存中. 启动时不联网; 第一次需要账号时向分发端索取. 账号的 access_token 距过期不足 3600 秒时, 或上游对该账号返回 401 时, 以 account 与 force 参数重新索取该账号, 不自行刷新. 分发端不可达时该次请求按刷新失败处理, 与 Lyra 刷新失败时的轮换规则相同. 账号排序, 轮询游标与轮换状态码与本地凭据池相同.
3. 分发请求使用令牌刷新客户端, 总超时 60 秒. 日志不得出现 access_token 与分发密钥.
4. 分发端返回非 2xx 时, 错误信息只包含状态码, 以及按第 7 节规则从响应体中取出的错误代码, 不包含响应体或其片段. 该错误会写进日志, 并经 relay 返回给客户端.

### 3.7 chatgpt 路由: 经 Codex 渠道使用 GPT 模型

用户于 2026-10-06 要求 Claude Code 经 aria 同时使用 Antigravity 渠道的 Gemini 与 Opus 以及 Codex 渠道的 GPT. chatgpt 路由把 Anthropic Messages 请求转换为 Codex 后端的 OpenAI Responses 请求, 用 providers.chatgpt 的账号池或令牌分发发送, 再把 Responses 事件流转换回 Anthropic 格式. 协议依据为 Codex CLI 源码 openai/codex 提交 3f1ccb7 的 codex-rs/codex-api/src/common.rs, codex-rs/protocol/src/models.rs, codex-rs/core/src/client.rs 与 codex-rs/codex-api/src/sse/responses.rs, 以及对该后端的实测记录.

1. 配置: upstream 为 chatgpt 的路由允许字段 name, display_name, aliases, model, moonshot_search 与 reasoning_effort, 要求配置 providers.chatgpt. reasoning_effort 可选, 取 low, medium, high 或 xhigh, 默认 high. 不允许 base_url, base_url_env, key_env 与 max_output_tokens, 因为 Codex 后端拒绝 max_output_tokens 参数.
2. 请求体: model 取路由的 model; stream 恒为 true; store 为 false; include 为 ["reasoning.encrypted_content"]; reasoning 为 {"effort": E, "summary": "auto"}, E 取 output_config.effort, 其中 low, medium, high, xhigh 原样使用, max 映射为 xhigh, 缺失或其他取值时用路由的 reasoning_effort; parallel_tool_calls 为 true, 除非 tool_choice.disable_parallel_tool_use 为真; 请求头存在 X-Claude-Code-Session-Id 时 prompt_cache_key 取其值. max_tokens, temperature, top_p, top_k, stop_sequences 与 thinking 不发送, 原因是 Codex 后端拒绝 max_output_tokens, 推理模型不支持采样参数; 这些差异写入完成日志之外的说明文档即可, 不报错.
3. instructions: system 为字符串时原样使用; 为块数组时取全部 text 块的 text 以两个换行连接; 为空时省略.
4. input 条目按消息顺序生成, 每个内容块生成一个条目或并入相邻的同角色消息条目:
   - user 的 text 生成 {"type":"message","role":"user","content":[{"type":"input_text","text":...}]}; image 的 base64 来源生成 {"type":"input_image","image_url":"data:<media_type>;base64,<data>"}, url 来源生成 {"type":"input_image","image_url":<url>}.
   - user 的 tool_result 生成 {"type":"function_call_output","call_id":<tool_use_id>,"output":...}; 内容只有文本时 output 为文本字符串, 含图片时为 input_text 与 input_image 组成的数组; is_error 为真时在第一个文本前加上 "Error: ".
   - assistant 的 text 生成 {"type":"message","role":"assistant","content":[{"type":"output_text","text":...}]}.
   - assistant 的 tool_use 生成 {"type":"function_call","call_id":<id>,"name":<name>,"arguments":<input 的紧凑 JSON 文本>}.
   - assistant 的 thinking 块, 其 signature 是本节第 7 条的推理桥时, 生成 {"type":"reasoning","id":<桥中的 id>,"summary":[{"type":"summary_text","text":<thinking>}],"encrypted_content":<桥中的值>}, thinking 为空时 summary 为空数组. Codex CLI 在 store 为 false 时同样发送带服务端前缀的条目 id, 见 client.rs 的 prepare_response_items_for_request. 其他来源的 thinking 与 redacted_thinking 块丢弃; 已识别为本协议但校验失败的推理桥返回 400 invalid_request_error.
   - 消息中途出现的 role 为 system 的消息生成 role 为 developer 的 message 条目.
5. tools: 客户端工具生成 {"type":"function","name":...,"description":...,"strict":false,"parameters":<input_schema 原样>}. tool_choice 的 auto 映射为 "auto", any 映射为 "required", tool 映射为 {"type":"function","name":...}, none 映射为 "none", 缺失时为 "auto". web_search_20250305 工具在路由声明 moonshot_search 时走 Kimi 搜索编排, 搜索轮次通过 chatgpt 路由的缓冲入口执行; 未声明时返回 400 invalid_request_error, 信息说明该路由不支持网页搜索.
6. 响应: 收到第一个 response.created 或第一个内容事件时开始, 流式请求按 Antigravity 转换器的提交时机写出 message_start, 消息标识为 msg_ 加 32 位十六进制随机数, model 取路由的 model.
   - output_item.added 的 reasoning 条目开启 thinking 块; reasoning_summary_text.delta 产生 thinking_delta; 该条目的 output_item.done 先产生 signature_delta, 值为推理桥, 再结束块.
   - message 条目开启 text 块; output_text.delta 产生 text_delta; output_item.done 结束块.
   - function_call 的增量按 item_id 与 output_index 关联到独立缓冲区, 不提前发送参数. output_item.done 给出完整 JSON 对象参数后一次性发送 tool_use 块, id 取 call_id, name 取 name. 如果文本或思考块仍打开, 工具块等其结束后发送, 避免复用内容块序号. 完成参数与增量不一致时以完成参数为准, 警告只记录字节长度与 SHA-256, 不记录参数原文. 缺少完整参数, 参数不是 JSON 对象或响应结束时仍有未完成工具调用都视为未完成流. 此规则由 chatgpt, xai 与 commandcode 共用; 代价是工具参数需等到条目完成才对客户端可见, 文本增量不变. 身份关联依据 [Responses 事件定义](https://developers.openai.com/api/reference/resources/responses/streaming-events).
   - response.completed: usage 的 input_tokens 取 input_tokens 减 input_tokens_details.cached_tokens, 不小于 0; cache_read_input_tokens 取 cached_tokens; output_tokens 取 output_tokens, 已包含推理; stop_reason 在出现过 function_call 时为 tool_use, 否则为 end_turn; 随后写出 message_delta 与 message_stop.
   - response.incomplete 的 incomplete_details.reason 为 max_output_tokens (Codex 与 Grok 的取值) 或 length (Command Code Provider API 的取值) 时按 stop_reason max_tokens 正常结束, 其他原因与 response.failed, error 事件一样视为失败: 提交前返回 Anthropic 错误响应, 状态码取上游错误中的 HTTP 语义, 无法判断时为 502; 提交后写出 event: error, 不写 message_stop. 流在 response.completed 之前结束视为未完成.
   - 非流式客户端请求同样以流式访问上游, 聚合为一条 Anthropic 消息后返回.
7. 推理桥: signature 取 base64 编码的 JSON {"type":"aria.chatgpt.reasoning","version":1,"id":<reasoning 条目 id>,"encrypted_content":<值>}, 校验规则与 Antigravity 的函数调用签名桥相同: 类型匹配后字段必须完整, 不允许未知字段, 版本必须为 1. encrypted_content 缺失时不产生 signature_delta.
8. 账号选择, 401 刷新重试, 轮换状态码 401, 402, 429, 403 冷却与令牌分发规则同 Responses 透传. 用量按 chatgpt 采集规则从上游事件读取, provider 为 chatgpt, account 为账号名.

### 3.8 客户端标识自动跟进

把订阅线路的客户端标识写死在代码里会让标识落后于官方 CLI, 使这批请求显得陈旧, 跟进版本需要重新编译发布. Aria 改为运行时跟进: 每条线路维护一份共享标识, 后台任务定时获取官方 CLI 的最新发布号, Codex 线路跟 npm 上的 @openai/codex 发布, Antigravity 线路跟 agy 仓库 google-antigravity/antigravity-cli 的 GitHub release, 请求按模板渲染 User-Agent, 失败时沿用最近一次成功的值, 从未成功时用配置的后备值. 例如 Codex 线路的历史写死值是 codex_cli_rs/0.157.1, Antigravity 线路的历史写死值是 antigravity/cli/1.2.12 (aidev_client; os_type=darwin; arch=arm64; cl=989022288; auth_method=consumer). 依据是 agy 的 GitHub release 与 npm 上的 @openai/codex 发布.

1. 配置: providers.chatgpt, providers.antigravity 与 providers.xai 各有可选对象 client, 字段为 user_agent_template, version_url 与 fallback_version, 三个字段都可单独缺省, 缺省时逐字段取线路默认值, 因此 "client": {} 与不写等价. user_agent_template 中的 {version} 替换为当前版本; 模板不含 {version} 时视为固定标识, 不发起版本请求.
2. Codex 默认值: version_url 为 https://registry.npmjs.org/@openai/codex/latest, 从响应 JSON 的 version 字段取版本; user_agent_template 为 codex_cli_rs/{version}; fallback_version 为 0.160.1, 这是 2026-10-06 的最新发布, 与 GitHub 标签 rust-v0.160.1 一致. originator 请求头保持 codex_cli_rs 不变. 真实客户端的完整 User-Agent 还带系统与终端信息, 用户可用模板补全, 默认不编造系统信息.
3. Antigravity 默认值: version_url 为 agy 仓库的 GitHub latest release https://api.github.com/repos/google-antigravity/antigravity-cli/releases/latest, 从响应 JSON 的 tag_name 字段取版本; user_agent_template 为 antigravity/cli/{version} (aidev_client; os_type=darwin; arch=arm64; cl=989022288; auth_method=consumer), 形态与 agy 的实测抓取一致, 其中 darwin/arm64 是 agy 二进制自带的系统标识, 模板可配; fallback_version 为 1.2.17, 这是 2026-10-05 的发布. 生成请求与 loadCodeAssist 推导项目标识使用 serve 进程的共享标识; Google 登录命令是建号的一次性动作, 不发版本请求, 直接渲染后备版本, 它的账号初始化请求用与生成请求相同的基础标识, 取邮箱的 userinfo 请求用 Go-http-client/1.1, 两者都与 agy 的实测抓取一致.
4. 获取规则: 版本必须是至少三段的点分数字, 每段纯数字, 否则视为获取失败; 四段的 0.160.1 与三段的 1.2.17 都通过, 带字母前缀, 连字符后缀或不足三段的 1.0 都失败. 获取是 serve 进程的职责: 服务启动后立即获取一次, 之后每 3 小时一次; 单次超时 10 秒; 用令牌刷新客户端, 因此遵守进程环境的代理设置. 清单的解析种类由 version_url 决定: 以 .yml 或 .yaml 结尾按 YAML 顶层 version: 行解析, 其余按 JSON 解析, JSON 取值先读 version 字段再回退 tag_name, 因此 npm 发布与 GitHub release 两个形态都支持, 不引入 YAML 依赖. 获取请求的 User-Agent 用 aria, 说明版本获取是 Aria 自身的行为, 不与官方 CLI 发往业务接口的标识混同. 版本变化时写一行日志; 获取失败不影响请求处理. 模板不含 {version} 的固定标识不发版本请求.
5. 渲染时机: User-Agent 在每次构造上游请求时从共享标识读取, 因此每条线路同一时刻只有一个取值, 一次获取失败不会改变正在处理的请求. 令牌刷新请求继续使用 Go-http-client/1.1, 与 Go 版和 agy 在这一步的行为一致, 不受本节影响.
6. aria check 输出各线路的模板与后备版本; GET /healthz 增加 client 字段, 给出 antigravity, chatgpt 与 xai 三条线路当前使用的版本与来源, 来源取 fetched, cached 或 fallback, 未配置的线路该条目为 null.
7. xai 默认值: 2026-10-09 加入. version_url 为 https://registry.npmjs.org/@xai-official/grok/latest, 从响应 JSON 的 version 字段取版本; user_agent_template 为 grok-cli/{version}, 形态取自官方 Grok CLI 的请求; fallback_version 为 1.0.50, 这是探针当日的最新发布. 同一版本还写入 x-grok-client-version 请求头, 两个请求头从同一份标识快照渲染, 因此不会因为后台更新恰好发生在两次读取之间而给出不同版本, 见 3.10 第 3 条. xai 登录命令不使用客户端标识, 它的请求标识见 6.3.

### 3.9 Qoder 直接 HTTP 上游

Qoder 直接 HTTP 上游为 Claude Code 等客户端提供经由 Qoder 官方网关访问通义千问等大语言模型的标准 Anthropic Messages 协议兼容通道.

1. 无状态 HTTP 架构设计:
该上游采用完全无状态的 HTTP 设计, 区别于需要本地维护多账号轮换状态池的机制, 本模块直接基于单用户个人访问令牌向网关发起动态会话交换. 运行时在内存中维护会话凭据与到期时间戳, 在会话即将过期前十分钟触发自动续期或者冷重新握手. 客户端发起的每一个 Anthropic Messages 请求体都在无状态内存流中经过结构转换, 映射为网关所需的 chatTemplate 报文, 并利用自定义 Base64 置换字母表与自逆重排算法进行编码, 最终通过单次短连接发往网关 chatSse 流式端点.

2. 签名算法与鉴权体系:
网关通信包含两层签名机制. 第一层为换取 jobToken 会话凭据时的签名, 客户端获取当前标准时间的 RFC 1123 格式文本, 与协议静态机密串拼接后计算 MD5 哈希摘要, 放置于 signature 请求头中. 第二层为业务请求的 Bearer 签名, 客户端为单次请求生成随机对称加密密钥, 经网关服务端固定 RSA 公钥通过 PKCS1 v1.5 标准加密后编码为 cosyKey 字段; 接着使用该对称密钥以 AES-128-CBC 算法加密用户身份载荷得到密文 info; 随后将客户端版本, 请求标识与密文组织为紧凑 JSON 并执行标准 Base64 编码生成 payloadB64; 最后将 payloadB64, cosyKey, 时间戳, 编码后的请求体字符串以及剥离 /algo 前缀的请求路径以换行符串联, 计算 MD5 得到 32 位十六进制摘要, 组合为 Bearer 凭据放入 Authorization 请求头中.

3. 流式转码与事件映射:
网关通过数据行返回 Server-Sent Events 事件流, 报文格式为外层状态包裹与内部 JSON 负载. 流式转码状态机逐行解析数据, 在首个有效数据到达时生成 Anthropic 的 message_start 起始事件, 分配以 msg_ 为前缀的随机消息标识. 当网关数据中出现 reasoning_content 思考字段时, 状态机输出 thinking 类型的内容块起始, 增量与结束事件; 出现 content 正文字段时, 输出 text 类型的内容块起始, 增量与结束事件; 出现 tool_calls 工具调用时, 输出 tool_use 内容块以及 input_json_delta 参数增量事件. 当事件流结束时, 状态机输出带有 stop_reason 与完成用量信息的 message_delta 以及 message_stop 终止事件. 上游没有给出任何内容块与停止原因时不再合成终止事件: 流式入口与缓冲入口都返回 502 api_error, 报文说明上游返回空响应, 并记录一行日志含 stage=empty 与截断到 300 字节的上游原文摘要. 判定依据是上游是否给出过思考, 正文或工具调用增量, 以及 choices 中是否出现过非空 finish_reason; 只发 message_start 或完全无有效数据的流都属于这一类, 完全无有效数据的流仍返回 Qoder 网关未返回有效事件. 原因是 2026-10-12 07:00:54Z 至 07:00:56Z 出现六条 200 且 completed 的空记录: 上游没有内容与用量, 状态机合成的 end_turn 与 message_stop 让客户端看到一个立即结束且没有输出的空回合, 用量记录为零 token, 客户端与运维两侧都无法知道上游实际返回了什么. 若网关返回 401 或 403 状态码, 或者在响应体中返回代表登录过期的错误代码 105, 状态机映射为 UNAUTHORIZED 认证错误; 遇到网关 429 频控时映射为 TOO_MANY_REQUESTS; 遇到上游连接失败或解析异常时统一映射为 BAD_GATEWAY.

4. 缓冲入口, 非流式请求与搜索编排:
声明 moonshot_search 的 qoder 路由会拦截 /v1/messages 上的独立网页搜索请求, 与 antigravity, chatgpt 与 xai 路由一样交给 canto::search 的 Moonshot 编排, 由 QoderTurn 以非流式方式执行每一轮; count_tokens 一律透传. 搜索编排的每一轮都需要非流式的完整 Anthropic 消息, 而网关只提供事件流, 因此 lyra::qoder 另外提供一个缓冲入口. 它复用同一份状态机的转换结果, 把事件流汇总成一条与非流式 Messages 响应同形的消息, 顶层包含 type, role, model, content, stop_reason, stop_sequence 与 usage, 文本, 思考与工具调用分别回填为 text, thinking 与带 input 的 tool_use 内容块, message_start 与 message_delta 的用量合并到同一条 usage. 请求构造, 会话维持, Bearer 签名与状态码校验与流式入口共用同一份实现, 发往网关的请求完全相同, Accept 同样取 text/event-stream, 区别只在于读完整个响应体后再汇总, 因此两个入口的错误映射完全一致. 2026-10-09 对真实网关实测, 同一请求以 Accept: application/json 发送时网关返回 500 Internal Server Error. 缓冲响应体上限为 4 MiB, 与搜索轮次的读取上限相同. 普通 qoder 请求的 stream 为 false 时也走这个缓冲入口, 返回 application/json 的非流式报文; stream 缺失或为 null 时按 Anthropic 接口的默认值视为非流式, 判定规则与搜索编排的 search_stream 相同; stream 为 true 时保持原来的流式转码.

5. total_credits 积分采集机制:
Qoder 网关在流式响应的 usage 字段中除了给出 prompt_tokens 与 completion_tokens 之外, 还附带本次请求扣减的 total_credits 实际积分数值. 状态机在流式解析过程中提取该数值, 并在流结束时更新用量状态. 采集模块将该数值无缝接入用量统计持久化机制, 使管理端不仅能够掌握 token 消耗量, 还能准确追踪 Qoder 平台账户的额度消耗进度.

6. 设备标识持久化与平台类型选型:
为防范多设备共用同一令牌或进程重启导致网关识别为频繁更换新机器而触发风控, Aria 采用持久化设备身份机制. 每台设备在首次初始化时生成唯一的 machine_id 与 machine_token 并持久化存储至状态目录 ~/.local/state/aria/qoder_machine.json, 后续重启时直接读取复用. 核对参考项目与 Qoder 官方客户端的实际上报行为: 官方客户端在 umid-cache.json 中记录硬件指纹与平台元数据, 参考实现 ggbdpq/qoder-proxy-api 则硬编码 windows_amd64; Aria 优先探测并上报本机真实系统的规范化平台名称, 例如 Linux x86_64 平台采用 linux_amd64, macOS Apple Silicon 平台采用 darwin_arm64, 在未知架构下才后备使用参考实现的 windows_amd64, 确保既能被网关平稳接受, 又符合真实主机环境.

7. 并发限制与错误码对齐:
Aria 为 Qoder 提供方引入专用的 Semaphore 信号量控制, 并发槽位上限由 providers.qoder.max_concurrency 指定, 默认为 2. 活跃请求超出上限时立即返回 429 状态码并附加 Retry-After: 1 响应头, 行为与网关全局并发限制保持一致. 一次搜索编排全程只持有一个许可, 编排的每一轮都复用同一个槽位, 因此一次搜索不会因为多轮调用而重复占用并发额度; 许可不足时的 429 同样按 5.8 第 3 条记入用量与完成日志. 当未在环境中配置 QODER_CN_PAT 等访问令牌时, 网关统一返回 503 状态码与英文错误信息 Qoder access token is not configured in environment variable, 避免返回 401 导致 Claude Code 等客户端误判为客户端密钥失效而提示重新登录; 同时在服务启动阶段打印告警日志.

8. 合规依据与协议参考:
Qoder 线路的协议实现严格以开源社区逆向工程梳理公开的官方网关协议基准为依据, 涵盖固定协议版本号, 请求端点规范, 字母表置换表, 会话生命周期管理与签名组装标准. 全部相关逻辑均遵循开源许可, 仅使用标准加密算法与公开协议数据结构, 保障协议实现的清晰与合规.

### 3.10 xai 路由: 经 SuperGrok 订阅使用 Grok 模型

用户于 2026-10-09 要求把 SuperGrok 订阅接入 aria, 客户端把 xai 路由当作普通模型名使用, 消耗订阅额度而不是 api.x.ai 的按量余额. xai 路由把 Anthropic Messages 请求转换为官方 Grok CLI 订阅后端 cli-chat-proxy.grok.com 上的 OpenAI Responses 请求, 用 providers.xai 的账号池发送, 再把 Responses 事件流转换回 Anthropic 格式. 协议依据为对该订阅后端的探针, 默认基址与回调形态另外核对过官方 grok CLI 1.0.50 二进制中的默认取值.

1. 配置: upstream 为 xai 的路由允许的字段与 chatgpt 路由相同, 即 name, display_name, aliases, model, moonshot_search 与 reasoning_effort, 要求配置 providers.xai. reasoning_effort 可选, 取 low, medium, high 或 xhigh, 默认 high. 不允许 base_url, base_url_env, key_env, upstreams 与 max_output_tokens: 上游与凭据来自 providers.xai, 请求转换不发送 max_tokens, 设置了也不会生效.
2. 请求体与响应转换: 事件流的事件名, 条目形态与用量字段与 Codex 一致, 已由探针验证. 请求体与 Codex 只有一处不同, 由 2026-10-09 的真实上游冒烟发现: 请求没有工具时 Grok 对 tool_choice 返回 400 invalid-argument, 文本为 A tool_choice was set on the request but no tools were specified, 而没有工具时的 parallel_tool_calls 被接受; 因此 xai 路由在没有工具时省略 tool_choice, chatgpt 路由的请求体保持 3.7 第 2 条不变. 除此之外 3.7 第 2 至 6 条原样适用, 包括网页搜索工具的规则, 推理桥的类型标识见第 4 条. 该账号的模型清单为 grok-4.7, grok-4.7-build-fast, grok-4.6 与 grok-4.5, 上下文均为 256000, 推理档位为 xhigh, high, medium 与 low, 默认 high.
3. 上游请求: 地址为 providers.xai.base_url 去掉末尾斜杠后拼接 /responses, 默认即 https://cli-chat-proxy.grok.com/v1/responses. 同一访问令牌访问 api.x.ai 返回 personal-team-blocked:spending-limit, 说明 api.x.ai 是按量余额池, 不能作为默认值. 请求头依次为 Authorization: Bearer 访问令牌, User-Agent, x-grok-client-version, x-grok-client-identifier: grok-shell, X-XAI-Token-Auth: xai-grok-cli, Content-Type: application/json 与 Accept: text/event-stream; 缺少任何一个指纹头时推理端点返回 Your Grok CLI version (none) is outdated. User-Agent 按 3.8 的模板渲染, x-grok-client-version 取同一份快照中的版本. 与 chatgpt 路由有三处不同: 不发送 chatgpt-account-id 与 originator, 它们是 Codex 的标识, 官方 Grok CLI 不发送; 不透传 session_id, conversation_id 与 openai-beta, 会话缓存键已经写进请求体的 prompt_cache_key; Content-Type 与 Accept 是固定值而不取客户端请求头, 因为本路由只服务 Messages 转换, 客户端的 Accept 描述的是它期望的 Anthropic 响应, 发往上游的请求体恒为 stream 为 true 的 JSON.
4. 推理桥: 格式与校验规则同 3.7 第 7 条, type 取 aria.xai.reasoning. encrypted_content 只能由签发它的后端解密, 因此 chatgpt 路由签发的推理桥在 xai 路由按其他来源丢弃, 反之亦然; 客户端在同一会话中切换两种路由时, 一条线路的推理内容不会被发往另一条线路.
5. 账号与刷新: 凭据文件格式同 ChatGPT 账号池. 取号, 轮询, 原子写回, 401 刷新重试, 轮换状态码 401, 402, 429 与 403 冷却都与 chatgpt 路由相同, 不做会话固定, 也没有容量重试与按模型的不可用记录. 刷新向 providers.xai.token_url 发送表单 client_id, grant_type=refresh_token, refresh_token 与 scope, 取值来自 providers.xai, User-Agent 为 Go-http-client/1.1; 刷新令牌每次刷新都会轮换, 响应没有新值时才沿用旧值. 访问令牌的有效期约 6 小时, 令牌响应的 expires_in 为 21600, 因此提前刷新的余量取 3600 秒, 与 3.6 第 2 条的索取余量相同; 沿用 ChatGPT 线路的 24 小时余量会让令牌永远不够新鲜, 每个请求都刷新一次并轮换刷新令牌.
6. 用量: 按 chatgpt 采集规则从上游事件读取, provider 为 xai, account 为账号名; 走 Moonshot 搜索编排时按 5.8 第 3 条记为 messages.

### 3.11 commandcode 路由: 经 Command Code Provider API 使用 GLM-5.3 模型

用户于 2026-10-09 决定给 aria 网关新增第六条线路 commandcode, 单独发布为 v0.1.10. 该线路用固定的 API 密钥访问 Command Code 的 Provider API, 把 Anthropic Messages 请求转换成 OpenAI Responses 请求发往 https://api.commandcode.ai/provider/v1/responses, 流式与非流式都转回 Anthropic 格式. 第一批只启用模型 zai-org/GLM-5.3. 协议依据为 2026-10-09 派发会话用真实密钥实测的数据.

1. 配置: upstream 为 commandcode 的路由允许 name, display_name, aliases, model, moonshot_search 与 reasoning_effort, 禁止 base_url, base_url_env, key_env, upstreams 与 max_output_tokens, 并且要求配置 providers.commandcode. 上游基址与密钥环境变量来自 providers.commandcode, 密钥环境变量默认取 COMMAND_CODE_API_KEY.
2. 协议事实: 鉴权请求头为 Authorization: Bearer 密钥, 错误密钥返回 401 且正文带有 UNAUTHORIZED, 套餐不含的模型返回 403 且错误信息以 MODEL_NOT_IN_PLAN 开头. 请求体接受 model, stream 为 true, store 为 false, include 为 [reasoning.encrypted_content], reasoning 为 effort 加 summary auto, instructions, input 条目, function 形式的 tools, tool_choice, parallel_tool_calls 与 prompt_cache_key. 没有工具时的 tool_choice 同样被接受. 上游接受 max_output_tokens, 实测 128000 被接受, 更高的上限没有测试; 较小的值会以 incomplete_details 的 reason 为 length 截断输出, 网关把 length 与 Codex 的 max_output_tokens 一样按达到输出上限处理, 转换为 stop_reason max_tokens 的正常结束; 2026-10-11 之前只识别 max_output_tokens, 该线路的截断被误判为上游响应不完整并返回 502, 见 3.7 第 6 条. 事件流依次出现 response.created, response.in_progress, response.output_item.added, response.output_item.done, response.output_text.delta 与 response.output_text.done, response.content_part.added 与 response.content_part.done, response.function_call_arguments.delta 与 response.function_call_arguments.done, 以及 response.completed. 工具调用与第二轮的工具结果回放都正常. GLM-5.3 的上下文长度为 1000000.
3. 思考事件处理: GLM-5.3 不发送 response.reasoning_summary_text.delta, 而是发送 response.reasoning.delta, 事件形如 {"type":"response.reasoning.delta","item_id":"rs_...","output_index":0,"content_index":0,"delta":"17"}; 结束时发送 response.reasoning.done 且携带完整思考文本; 推理条目的 output_item.added 包含空 summary 数组, output_item.done 包含 reasoning_text 内容块且没有 encrypted_content. ChatGptTranslator 把 response.reasoning.delta 与 response.reasoning_summary_text.delta 一样处理为思考增量; 推理条目缺失 encrypted_content 时不写出签名. Codex 与 Grok 不发送该事件, 因此对既有线路无影响.
4. 与 xai 线路的两处差异及原因: 第一处为 max_output_tokens, commandcode 线路在 ResponsesDialect 中启用 emit_max_output_tokens, 将客户端的 max_tokens 写为请求体的 max_output_tokens, 原因是上游接受该字段并按指定上限截断; Codex 后端拒绝该字段, 见 3.7, xai 线路沿用 Codex 的请求体且没有验证 Grok 对该字段的处理, 因此 chatgpt 与 xai 线路保持不写, 两者的请求体与 v0.1.9 逐字节一致. 第二处为 403 不进入冷却, commandcode 收到上游 403 时原样把错误交给客户端而不冷却账号, 原因是 Command Code 的 403 表示套餐不含该模型而非风控, 并且本线路只有一把固定密钥, 冷却会导致整条线路在 6 小时内完全不可用.
5. 上游请求头: 只写 Authorization: Bearer 密钥, User-Agent, Content-Type: application/json 与 Accept: text/event-stream. User-Agent 采用固定值 aria, 不做客户端版本跟进, 也不发送任何其他客户端的指纹头.
6. 推理桥: 格式与校验规则同 3.7 第 7 条, 类型标识取 aria.commandcode.reasoning. 本线路签发的推理桥在 chatgpt 与 xai 路由按其他来源丢弃, 保证三条线路互不回放对方的推理内容.
7. 凭据与中继: 本线路只有一把固定密钥, 不是账号池, 不需要刷新与换号. 凭据来源提供单个条目, 在健康检查中可见且不暴露密钥. Route 变体为 CommandCode, 换号状态码为空, 没有容量重试, 没有按模型的不可用与耗尽记录.
8. 用量: provider 为 commandcode 时沿用 Codex 的采集规则, 从 response.completed 读取用量字段, account 记录路由名或 commandcode. 走 Moonshot 搜索编排时按 5.8 第 3 条记为 messages.

## 4. 命令行

| 命令 | 作用 |
|---|---|
| aria serve --config FILE [--clients FILE] [--listen ADDR] | 启动服务 |
| aria check --config FILE [--clients FILE] | 只做配置与环境变量校验, 不联网, 成功时输出路由摘要并以 0 退出 |
| aria login google --out FILE [--port 51121] [--timeout-secs 900] [--priority N] [--note TEXT] [--config FILE] | Google 浏览器登录, 见 6.1 |
| aria login chatgpt --out FILE [--port 1455] [--timeout-secs 900] [--priority N] [--note TEXT] [--config FILE] | ChatGPT 浏览器登录, 见 6.2 |
| aria login xai --out FILE [--port 56121] [--timeout-secs 900] [--priority N] [--note TEXT] [--config FILE] | SuperGrok 浏览器登录, 见 6.3 |
| aria version | 输出版本号与构建提交 |

版本号取 CARGO_PKG_VERSION, 构建提交取编译期环境变量 ARIA_GIT_SHA, 未设置时输出 unknown. 命令行解析只用标准库, 不引入 clap. 未知参数, 缺少取值和重复参数都报错并以 2 退出.

## 5. HTTP 服务行为

### 5.1 端点

| 方法与路径 | 鉴权 | 行为 |
|---|---|---|
| GET /health/liveliness | 否 | 返回 {"status":"ok"} |
| GET /healthz, GET /v1/healthz | 是 | 返回 {"status":"ok","credentials":[...],"cooldowns":[...],"unavailable_models":[...],"unavailable_routes":[...]}, 账号状态格式同 Lyra credential.Status, cooldowns 见 5.11, unavailable_models 见 5.12 与 5.13, unavailable_routes 见 5.13 |
| GET /v1/models | 是 | 模型列表, 见 5.4 |
| GET /v1/usage | 是 | 用量查询, 参数与响应同 Lyra relay/usage.go, 未配置 usage_dir 时返回 503 |
| POST /v1/messages | 是 | 按模型名路由 |
| POST /v1/messages/count_tokens | 是 | messages 路由透传到上游; antigravity, chatgpt, xai, commandcode 与 qoder 路由返回 404 not_found_error |
| POST /v1/responses, /responses, /backend-api/codex/responses | 是 | 配置了 chatgpt 提供方时透传, 否则 404 |

其他路径返回 404. 路径比较不包含查询串, 但 Responses 透传时 Lyra 按 RequestURI 匹配, Aria 改为按路径匹配并丢弃查询串. xai 与 commandcode 提供方不增加端点: 客户端经 /v1/messages 使用其路由, Responses 透传端点只服务 chatgpt 提供方.

### 5.2 鉴权

客户端密钥先取 Authorization 的 Bearer 形式, 不是 Bearer 时取 x-api-key, 与 Canto 相同. 比对必须比较完所有启用密钥, 不能在命中后提前返回. 失败返回 401.

### 5.3 生成请求的公共处理

以下规则适用于全部 POST 生成端点.

1. 并发槽位在读取请求体之前获取. 没有空闲槽位时立即返回 429, 带 Retry-After: 1. 槽位一直持有到响应体发送完毕或客户端断开.
2. 请求体在 60 秒内读完, 超时或读取失败返回 400. 超过 max_body_bytes 返回 413. 允许分块传输, 不要求 Content-Length.
3. Messages 请求体必须是含非空字符串 model 字段的 JSON 对象, 否则 400; 模型名不在路由表中返回 400 unknown model.
4. 生成请求不得在部分失败后隐式重放. 只有第 5.6 节列出的账号轮换与刷新重试允许再次发送.

### 5.4 模型列表

返回 {"object":"list","data":[...],"has_more":false,"first_id":...,"last_id":...}. data 先按配置顺序列出路由, 每项为 {"id","type":"model","object":"model","display_name","owned_by":"aria"}; id 取第一个别名, 没有别名时取路由名; display_name 缺省时取路由名. 随后依次列出 chatgpt, xai 与 commandcode 提供方的 models, display_name 取模型名. 列表为空时 first_id 与 last_id 为 null.

### 5.5 messages 路由

行为与 Canto 的 destination.forward 相同.

1. 模型名与路由的 model 不同时, 只改写 model 字段并重新编码请求体; 其余字段以原始 JSON 保留, 不经过中间数据结构. 键顺序可以按字典序重排.
2. 上游请求头: 删除 Authorization, X-Api-Key, Proxy-Authorization, Cookie, Accept-Encoding 与逐跳头, 设置 Authorization: Bearer 密钥, X-Api-Key: 密钥, Content-Type: application/json, Host 取上游主机. 不添加 Forwarded 与 X-Forwarded 系列请求头. anthropic-version, anthropic-beta 与会话头原样保留.
3. 上游响应: 状态码与响应头原样转发, 删除 Set-Cookie 与逐跳头, 设置 Cache-Control: no-store. 响应体按块流式转发, 依靠拉取式读取形成背压, 不建立无界队列. 客户端断开后上游请求随之取消. 上游返回非成功状态时, 先读取错误响应体的前缀, 记录状态码与去掉回车换行并截断到 300 字节的错误体, 再把前缀与剩余响应体原样回给客户端; 响应体在传输中断开时记录一行日志, 含失败阶段 stream 与错误原因.
4. 连接上游失败返回 502 api_error upstream connection failed, 并记录一行日志含失败阶段与错误原因, 不记录可能带凭据的地址. 失败阶段取 connect (连接建立: TCP, DNS, TLS 或代理握手), connect_timeout (连接建立超时), first_byte_timeout (请求已发出后等待响应头超时) 与 before_first_byte (请求已发出后在收到任何响应字节前断开). 连接建立阶段的失败发生在请求字节送达上游之前, 上游没有计费也没有生成内容, 因此在同一上游上重试一次; 连接超时与请求已经发出之后的失败不重试, 保持 3.4 节的不重放规则.
5. 上游空闲超过 upstream_idle_timeout_secs 时中止该流.
6. 声明了 moonshot_search 的路由收到独立网页搜索请求时, 执行 Canto 的搜索编排, 判定条件与全部限制同 search.go 与 search_workflow.go, 与 Go 版不同的是第一轮内部请求不强制工具选择. Go 版把客户端指向 web_search 的强制选择改写为指向内部搜索函数的强制选择, any 原样转发; Aria 在客户端强制选择 web_search 或传 any 时, 把内部第一轮请求的 tool_choice 改写为 auto 并保留其余字段, 客户端传 auto 或不传时行为不变, 回放之后各轮的 auto 与 none 规则不变. 原因是部分模型拒绝强制工具选择: 2026-10-09 经本机 aria 探测, 路由 openrouter-claude/claude-opus-5.5 对 tool 或 any 返回 400 tool_choice: type "tool" and "any" are not supported for this model., 对 auto 返回 200, 因此强制选择会让该模型上的搜索请求必然失败, 而内部提示已经要求模型搜索, 用 auto 即可. 若模型第一轮没有调用搜索而直接作答, 编排按现有 finish 的规则返回该回答. 与 Go 版不同, 客户端关闭思考时内部搜索请求省略 thinking 字段, 让上游按模型默认行为处理, 其余 thinking 取值原样保留, cap_thinking_budget 的规则不变. 原因是部分模型不允许关闭推理: 2026-10-09 截获到的 Claude Code WebSearch 子请求带 thinking 为 type 等于 disabled, 经本机网关转给 OpenRouter 上的 claude-opus-5.5 时返回 400 Reasoning is mandatory for this endpoint and cannot be disabled., 省略该字段时返回 200 并完成搜索. 省略对不要求推理的模型没有影响, 在带 moonshot_search 的普通模型路由上, 省略与关闭两种情况下是否产生思考块完全一致.
7. 搜索编排的上游非 200 时, 客户端可见的错误在固定文本 search workflow upstream returned an error 之后附上上游原因, 并记录一行日志含状态码, 模型名与原因. 原因优先取响应 JSON 中 error.message, 取不到时取正文开头的文本, 去掉回车与换行并按字符边界截断到 300 字节以内. 日志不含请求头, 请求体, 响应体与密钥.

### 5.6 antigravity 路由与 Responses 透传

行为与 Lyra 的 relay.Handler 相同, 区别只有以下几点.

1. antigravity 路由先把请求体的 model 改写为路由的 model, 再交给 Lyra 的转换逻辑. 原 Lyra 对 antigravity/ 前缀的剥离保留.
2. 账号选择, 会话固定, 401 刷新重试, 503 容量重试与 25 秒预算, 轮换状态码, Advance 规则, 错误映射与转换流的提交时机全部沿用 Lyra 源码, 403 例外, 见 5.11.
3. Responses 透传沿用 Lyra 的 ChatGPT 提供方, 透传头部, 轮换状态码与日志格式, 403 例外, 见 5.11.
4. 单次账号尝试受 upstream_first_byte_timeout_secs 约束. 从请求发往上游起算, 覆盖到收到响应头并读到首个有效事件为止. 超时后取消本次上游请求, 视同轮换状态码换到下一个账号重试, 并在首选账号上前移会话固定. 传输错误发生时同样在首选账号上前移会话固定并换号重试.

### 5.7 上游 HTTP 客户端

1. 代理只来自进程环境的 HTTPS_PROXY 或 https_proxy, 以及 NO_PROXY 或 no_proxy, 支持 http, https 与 socks5 代理地址. 必须关闭 reqwest 的系统代理读取. 回环地址的上游从不经过代理. 未设置时直连.
2. messages 路由与搜索服务共用一个客户端, 允许 HTTP/2, 连接超时 15 秒, 每个主机最多保留 16 条空闲连接, 空闲连接 90 秒后关闭, TCP keepalive 30 秒, 不做自动解压.
3. Lyra 的订阅账号提供方共用另一个客户端, 包括 xai, 只用 HTTP/1.1, 连接超时 30 秒, 其余同上. Go 版 Lyra 关闭了 HTTP/2, Aria 保持这一行为.
4. 两个客户端的读空闲超时都取 upstream_idle_timeout_secs. 每次账号尝试等待响应头与首个有效事件的最长首字节时间取 upstream_first_byte_timeout_secs. 令牌刷新与登录请求使用单独的 60 秒总超时.

### 5.8 用量统计

1. 存储格式, 目录锁, 文件权限, fsync 规则, 损坏检测与查询语义完全沿用 Lyra internal/usage/store.go, 服务器上已有的记录必须可以继续读取. 时间字段按 RFC 3339 读写, 可带纳秒与时区.
2. 采集沿用 Lyra internal/usage/capture.go 的 chatgpt 与 antigravity 规则; xai 线路的上游事件与 Codex 逐字段一致, 按 chatgpt 规则采集. 新增 messages 规则: 从 Anthropic 流式事件 message_start 的 message.usage 与 message_delta 的 usage, 或非流式响应的 usage, 读取 input_tokens, output_tokens, cache_read_input_tokens 与 cache_creation_input_tokens. cache_creation_input_tokens 记为 cache_write_input_tokens; input_tokens 不含缓存, 记录时把两项缓存加回 input_tokens, 使口径与 Lyra 的输入包含缓存一致; total_tokens 为输入加输出. message_stop 视为 completed, error 事件视为 error; 非流式响应的 type 为 message 时视为 completed, type 为 error 时视为 error. 缓存加回在汇总时统一进行, 不在单个事件内进行, 使后到事件重复给出 input_tokens 时结果不变.
3. 记录字段 provider 取 messages, antigravity, chatgpt, xai, commandcode 或 qoder; account 对 messages 路由取路由名, 对账号池取账号名, 对 commandcode 路由取路由名, 对 qoder 路由取路由名. antigravity, chatgpt, xai, commandcode 与 qoder 路由走 moonshot 搜索编排时, 客户端收到的是 Anthropic 格式的聚合响应, 因此按 messages 规则采集, provider 记为 messages, account 取路由名. count_tokens 不是生成请求, 不记录用量. 每个响应带 X-Relay-Request-Id 头.

### 5.9 日志

1. 每个请求写一行访问日志, 格式同 Lyra 的 accessLog.
2. 生成请求结束时写一行完成日志: POST 路径, route, credential, upstream 状态, 请求与响应字节数, 耗时, 未完成时追加 incomplete.
3. 日志中不得出现请求体, 密钥, 令牌和带查询串的上游地址, 也不得出现成功响应的响应体. 上游错误响应体只允许去掉回车换行并按 300 字节截断后记录, 见 5.5 第 3 条与 Lyra 的 buffered_error_response; 失败原因只记录原因链上的说明, 不记录带地址的外层错误文本.

### 5.10 停机

收到 SIGTERM 或 SIGINT 后停止接受新连接, 已有请求最多继续 60 秒, 之后取消剩余请求并退出. 监听失败时以非零状态退出.

### 5.11 账号冷却

用户于 2026-10-06 决定: 账号池中的账号收到上游 403 时进入冷却, 不再立即换下一个账号重试. Go 版 Lyra 把 403 当作轮换状态码, 同一个请求会依次发给多个账号; 如果 403 是服务商的风控判定, 例如 2026-09-29 两个 Google 账号被要求扫码验证时返回的 403, 换号会把同一份流量扩散到整个账号池. 403 与风控的关联属于推断, 本节是降低风险的保守策略.

1. 范围: Antigravity, ChatGPT 与 SuperGrok 三个账号池, 包括 antigravity 路由, chatgpt 路由, xai 路由, Responses 透传与搜索轮次; ChatGPT 使用令牌分发索取端时, 冷却记录在发出请求的实例上. messages 路由的多上游轮换使用服务商 API 密钥, 不属于账号池, 仍按 3.4. commandcode 线路使用固定 API 密钥, 403 表示套餐不含该模型而非风控, 冷却会导致整条线路不可用, 因此不进入冷却, 见 3.11.
2. 进入冷却: 上游对某个账号返回 403 时, 该账号冷却 6 小时. 本次请求不换号, 403 响应按原有错误映射返回客户端; 会话固定的首选账号同时前移, 规则同轮换. 写一行日志, 包含账号名, 状态码与冷却时长.
3. 选号: 取号顺序不变, 冷却中的账号被跳过; 轮换判定中的最后一个账号指未冷却账号中的最后一个. 全部账号都在冷却时返回 502, 信息说明全部账号处于冷却.
4. 状态只保存在内存中, 进程重启后清空. 账号完成人工验证后, 重启 aria 即可恢复使用.
5. 轮换状态码中去掉 403: Antigravity 为 401, 404, 429, 503, ChatGPT 与 xai 为 401, 402, 429. 首字节超时与传输错误同样触发换号并在首选账号上前移会话固定. 其余重试规则不变.
6. GET /healthz 增加 cooldowns 数组, 每项为 {"provider","name","remaining_seconds"}, provider 取 antigravity, chatgpt 或 xai, 没有冷却账号时为空数组.
7. 用户于 2026-10-07 确认: 同一个 Antigravity 账号连续两次首字节超时, 进入 5 分钟冷却. 冷却期间选号跳过该账号, 期满自动恢复. 中间只要有一次成功, 就把连续超时计数清零. 冷却记录与 GET /healthz 中的展示沿用现有冷却机制, 不另起一套. 连续超时触发次数与冷却时长作为配置项 timeout_cooldown_threshold 与 timeout_cooldown_secs, 默认值分别为 2 次与 300 秒.

### 5.12 不可用模型记录与轮换失败响应

用户于 2026-10-06 决定: 自动记住没有模型权限的账号, 并且在全部账号失败时返回更有意义的响应.

1. 模型不可用记录范围: 仅用于 Antigravity 账号池. 当某个账号对特定上游模型返回 404 时, 记录该账号与该模型的组合不可用, 有效期 6 小时. 上游模型名称取写入上游请求信封的 model 字段取值, 与发往上游的名称保持一致. 状态只保存在内存中, 进程重启后清空.
2. 选号与快速失败: 选号时, 对当前请求的模型跳过被记录为不可用的账号, 规则与 403 冷却并列. 某个模型在全部账号上都被记录为不可用时, 不再请求上游, 直接返回 Anthropic 格式的 404, type 为 not_found_error, 信息说明没有账号能使用该模型.
3. 记录日志: 记录不可用模型时输出一行日志, 包含账号名, 模型名与有效期, 不包含响应体.
4. 健康检查: GET /healthz 增加 unavailable_models 数组, 每项为 {"provider","name","model","remaining_seconds"}, 没有记录时为空数组. provider 取 antigravity. 5.13 的额度与容量耗尽记录加入后, 该数组的每项还带 reason 字段, 见 5.13 第 6 条.
5. 全部账号失败响应优化: 适用于 Antigravity, ChatGPT 与 SuperGrok 三个账号池. 当一次选号中全部账号都返回轮换状态码时, 如果本次尝试中出现过 429 状态码, 返回最后一次收到的 429 响应, 包括其状态码, 响应头中的 Retry-After 与响应体; 没有出现过 429 状态码时保持返回最后一个账号响应的原有规则. 选号轮换遇到 429 状态码时读取并保留其完整响应体, 读取上限沿用 1048576 字节.

### 5.13 额度与容量耗尽记录

用户于 2026-10-08 决定: 上游明确表示额度或容量耗尽的账号与路由自动记为不可用, 到期自动恢复, 不需要人工干预. 背景是同日观测到的两类实例: Antigravity 线路的 429 QUOTA_EXHAUSTED 与 503 MODEL_CAPACITY_EXHAUSTED 只在本次请求内换号, 新请求仍会先撞上已经耗尽的账号; 第三方单账号路由, 例如 Kimi 与 coding-plan, 的配额类 403 与 429 直接透传, 新请求继续被转发到上游.

1. Antigravity 账号记录: 某个账号对某个上游模型返回 429 时, 解析错误体中的恢复时间并记录该账号与该模型的组合不可用. 解析优先级是 metadata.quotaResetTimeStamp, 即 RFC3339 绝对时间; 其次是 metadata.quotaResetDelay 或 RetryInfo.retryDelay, 即 Go 时长文本, 例如 1h14m5.453370951s. 解析不到, 时间戳已经过去或取值不合理时按连续耗尽次数退避: 起始 60 秒, 每次记录加倍, 上限 30 分钟. 账号对该模型返回 503 时按同一退避记录, 成功一次即清零连续计数. 单次记录的有效期不超过 7 天, 时间戳已经过去时不采用.
2. 恢复与选号: 记录只保存在内存中, 进程重启后清空, 由后续请求重新建立, 与 5.11 的账号冷却和 5.12 的模型不可用记录一致. 记录期间选号跳过该账号; 该账号对该模型有一次成功请求时记录立即清除, 不需要人工干预. 记录按账号与上游模型区分: 同一账号 Opus 配额耗尽不影响它的 Gemini 请求.
3. 全部账号耗尽: 某个模型的全部账号都在未到期的记录中时, 不再逐个请求上游, 直接返回 Anthropic 格式的 503, type 为 api_error, 报文说明模型名与最早恢复的剩余秒数, 并在 Retry-After 响应头中给出同一秒数. 其余全部不可用的组合沿用 5.11 的 502 (全部账号处于 403 冷却) 与 5.12 的 404 (没有账号能使用该模型).
4. 第三方单账号路由记录: 429 一律按请求受限记录; 403 只在其错误消息命中 usage limit 时按配额耗尽记录. 判断依据是上游的实际文本: Kimi 官方错误参考在 403 下同时列出 "You've reached your 5-hour usage limit.", "You've reached your monthly usage limit for this billing cycle." 与 "You've reached your weekly (7-day) usage limit." 三条用量窗口耗尽文本, 以及 "(security_risk) We consider the current URL poses a security risk and are unable to provide fetch service at this time." 一类请求级拒绝与 "You've reached your concurrent request limit." 这类并发限制, 因此状态码本身无法区分, 只有含 usage limit 的消息说明用量窗口已经耗尽. 恢复时间优先取 Retry-After 响应头的秒数, 其次取错误体中的恢复字段, 都取不到时按第 1 条的退避. 窗口内该路由的请求直接返回 Anthropic 格式的 503, 报文与 Retry-After 给出恢复剩余秒数, 不再转发上游; 一次成功的请求清除窗口与连续计数. 搜索轮次与该路由的常规生成共用同一份窗口.
5. 日志: 记录耗尽与直接拒绝各写一行日志, 包含账号名或路由名, 模型, 原因与记录时长, 不包含响应体.
6. 查询方法: GET /healthz 或 /v1/healthz 需要客户端密钥, 响应中的 unavailable_models 数组每项为 {"provider","name","model","reason","remaining_seconds"}, reason 取 model_not_found (5.12 的 404 记录), quota_exhausted 或 capacity_exhausted; unavailable_routes 数组每项为 {"route","reason","remaining_seconds"}, reason 取 quota_exhausted (403 用量窗口耗尽) 或 rate_limited (429). 两个数组只列出仍在记录中的条目, 没有记录时为空数组. 派发子会话前查询这两个数组, 即可知道哪些账号或路由当前不可用以及各自何时恢复.

## 6. 登录命令

### 6.1 Google 登录

完整移植 lyra cmd/agy-login 的行为: PKCE, state 校验, 回调只监听 127.0.0.1, 授权地址参数, 令牌交换, 读取邮箱, loadCodeAssist 推导项目标识与档位, 候选文件格式与字段, 原子发布且不覆盖已有文件. 受保护路径改为: 传入 --config 时, 配置中 antigravity, chatgpt 与 xai 提供方的 credentials 路径都不能作为输出, 包括通过符号链接指向它们的情况. 删除原实现中对 /etc/codex-relay 与 /etc/gemini-relay 两个固定目录和环境变量的保护.

### 6.2 ChatGPT 登录

新增命令, 流程与 Codex CLI 的浏览器登录相同.

1. 授权地址为 https://auth.openai.com/oauth/authorize, 参数 response_type=code, client_id=app_EMoamEEZ73f0CkXaXp7hrann, redirect_uri=http://localhost:1455/auth/callback, scope=openid profile email offline_access, code_challenge 与 code_challenge_method=S256, id_token_add_organizations=true, codex_cli_simplified_flow=true, state.
2. 回调路径 /auth/callback, 只监听 127.0.0.1. 令牌交换向 https://auth.openai.com/oauth/token 发送表单 grant_type=authorization_code, code, redirect_uri, client_id, code_verifier.
3. 从 id_token 的载荷解析 https://api.openai.com/auth 声明中的 chatgpt_account_id 与 chatgpt_plan_type, 以及 email. 只解码载荷, 不校验签名, 令牌来自刚建立的 TLS 连接.
4. 候选文件格式同 Lyra credentials.json 的单个条目: name 取邮箱, account_id, plan, access_token, refresh_token, expires_at, enabled, priority, note, source, updated_at. 写入规则同 6.1.

每台设备各自登录一次即可取得独立的 refresh token 族, 不与其他设备的令牌轮换冲突.

### 6.3 xai 登录

新增命令, 流程与官方 Grok CLI 的浏览器登录相同, 协议依据见 3.10. 每台设备同样各自登录一次.

1. 授权地址为 https://auth.x.ai/oauth2/authorize, 参数按键排序: client_id=b1a00492-073a-47ea-816f-4c329264a828, code_challenge, code_challenge_method=S256, nonce, redirect_uri=http://127.0.0.1:端口/callback, referrer=grok-build, response_type=code, scope=openid profile email offline_access grok-cli:access api:access, state. 官方 CLI 还申请 conversations 与 workspaces 的读写权限, 网关只调用推理接口, 不申请这四项.
2. 回调路径为 /callback, 只监听 127.0.0.1. 官方 CLI 注册的回调地址是 http://127.0.0.1/callback, 每次登录绑定随机端口, 授权服务按 RFC 8252 对回环回调不比较端口, 因此默认端口取社区实现常用的 56121, 可用 --port 改变. 令牌交换向 https://auth.x.ai/oauth2/token 发送表单 grant_type=authorization_code, code, redirect_uri, client_id, code_verifier, 没有 client_secret.
3. 访问令牌的 JWT 载荷只有 sub 而没有邮箱, 因此交换成功后用访问令牌请求 https://auth.x.ai/oauth2/userinfo, 取 sub 与 email; sub 缺失时登录失败, 不写出候选文件. nonce 随授权请求发出, 身份不取自 id_token, 因此不回读 nonce. 令牌交换与 userinfo 请求的 User-Agent 为 Go-http-client/1.1, 与账号池刷新同一端点时一致.
4. 候选文件格式同 6.2 第 4 条: name 取邮箱, account_id 取 sub, plan 取 superGrok, 只用于展示; source 为 xai-login on 主机名, 其余字段与写入规则同 6.2.

## 7. 与 Go 版不同的行为

以下差异是有意的, 实现必须遵守, 测试应覆盖.

| 对象 | Go 版 | Aria |
|---|---|---|
| Antigravity 可请求的模型 | Lyra 接受任意模型名 | 只接受配置中的路由 |
| 并发满 | Lyra 排队等待 | 立即 429 |
| 请求体超限 | Lyra 返回 400 且要求 Content-Length | 413, 允许分块传输 |
| Antigravity 实例的鉴权失败等错误 | Lyra 使用 OpenAI 风格错误 | Anthropic 错误格式 |
| 上游代理 | Lyra 不使用代理 | 两种路由都读取进程环境代理 |
| 配置方式 | RELAY_ 环境变量与命令行参数 | 统一 JSON 配置文件 |
| Server 响应头 | Lyra 返回 codex-relay/1.0 | 不设置 |
| 工具模式 anyOf | Lyra 当前保留 | 剔除, 见 2.6 |
| messages 路由用量 | Canto 不记录 | 记录, 见 5.8 |
| 上游密钥的合法性 | Canto 在请求阶段由 net/http 拒绝不能放入请求头的密钥 | 启动阶段校验并拒绝 |
| 搜索轮次的查询串 | Canto 复制客户端请求, 包括查询串 | 只携带请求头与请求体, 见 2.5 |
| 搜索编排第一轮的工具选择 | search_workflow.go 把客户端指向 web_search 的强制选择改写为指向内部搜索函数的强制选择, any 原样转发 | 客户端强制选择 web_search 或传 any 时, 内部请求改写为 auto 并保留其余字段, 见 5.5 |
| 搜索编排的思考字段 | search_workflow.go 原样转发客户端的 thinking | 客户端关闭思考时内部请求省略 thinking, 其余取值原样保留, 见 5.5; 原因是 Opus 5.5 拒绝关闭推理, 2026-10-09 截获的 Claude Code 子请求因此返回 400 |
| 搜索编排的上游错误信息 | 只返回固定文本 search workflow upstream returned an error | 固定文本之后附上上游原因, 并记录含状态码与模型名的日志, 见 2.5 与 5.5 |
| 浮点数的输出文本 | Go 把 1.0 写成 1 | 保留客户端原样, 例如 1.0, 数值相同 |
| 凭据文件中的非规范数字 | Go 以 json.Number 原样保留字面量 | 指数写法与超出 u64 的整数按 serde_json 规范化, 数值相同 |
| 文件关闭错误 | Go 检查 Close 错误并视为写入不确定 | Rust 无法观测关闭错误, 以写入后的 sync_all 保证持久性 |
| 请求体中的重复 JSON 键 | Go 取最后一个取值 | serde 报 duplicate field, 请求以 400 拒绝 |
| 账号池收到 403 | Lyra 换下一个账号 | 该账号冷却 6 小时, 本次请求不换号, 见 5.11 |
| 凭据刷新与令牌分发失败时的错误信息 | 带出上游响应体前 200 字节 | 只保留状态码与错误代码. 错误代码依次取 JSON 响应体中字符串形式的 error, error.code, error.type, 只允许 ASCII 字母, 数字, 下划线, 点与连字符, 长度 1 到 64, 不合格或非 JSON 时省略. 适用于 ChatGPT 令牌刷新, Google 令牌刷新, loadCodeAssist 项目标识推导与令牌分发 |
| 客户端标识 | Lyra 把两条线路的 User-Agent 写死在代码里 | 运行时自动跟进各自官方 CLI 的最新发布, 可配置, 见 3.8 |
| Antigravity 的 User-Agent | antigravity/cli/1.2.12, 9 月 29 日抓取时写死 | antigravity/cli/{版本}, 形态不变, 版本跟进 agy 的 GitHub release, 见 3.8 |
| 登录命令的 Antigravity 标识 | 取邮箱与账号初始化同用一份写死值 | 取邮箱用 Go-http-client/1.1, 账号初始化用与生成请求相同的基础标识, 见 3.8 |
| Antigravity 账号收到 404 | 每次请求都重新尝试该账号 | 记录该账号与模型不可用 6 小时并在选号时跳过, 全部账号不可用时直接返回 404, 见 5.12 |
| Antigravity 账号收到 429 或 503 | Lyra 只在本次请求内换号, 不留记录 | 按账号与模型记录不可用到期时间, 记录期间选号跳过该账号, 见 5.13 |
| Antigravity 全部账号额度或容量耗尽 | Lyra 返回最后一个账号的响应 | 不请求上游, 直接返回 503 与最早恢复时间, 见 5.13 |
| messages 路由收到配额类 403 或 429 | Canto 原样透传, 每个请求都转发上游 | 记录路由级不可用窗口, 窗口内直接返回 503 与恢复时间, 见 5.13 |
| 全部账号失败时的响应 | 原样返回最后一个账号的响应 | 若尝试中出现过 429 则返回最后一个 429 响应, 包含 Retry-After 与响应体, 见 5.12 |

## 8. 测试要求

1. Go 版每个测试描述的行为都要有对应的 Rust 测试, 测试名可以不同. 不需要对应的测试必须在交付说明中列出并说明原因.
2. 涉及网络的测试只能使用本机回环地址上的桩服务, 不得访问真实上游, 不得读取本机真实凭据.
3. 每个 crate 的测试放在模块内的 tests 子模块, 跨模块的端到端测试放在 crates/aria/tests.
4. cargo fmt --check, cargo clippy --all-targets -- -D warnings 与 cargo test 必须全部通过.

## 9. 模块接口

lyra::relay, 用量观测与登录命令, 以及 aria 可执行文件之间的接口如下. 签名已经以占位实现的形式写入仓库, 实现时不得修改签名; 确需修改时应同步更新本文.

### 9.1 用量观测

aria_core::usage::Observation 记录一次客户端请求的用量, 实现位于 crates/aria-core/src/usage/observe.rs. 它是可以克隆的句柄, 内部用 std::sync::Mutex 保护状态, 持锁期间不得 await.

1. Observation::start(store, write_failures, provider, model) 生成请求编号. store 的类型为 Arc<Store>, write_failures 的类型为 Arc<AtomicU64>, 用于 /v1/usage 返回的 write_failures_since_start.
2. id() 返回请求编号, 调用方把它写入 X-Relay-Request-Id 响应头.
3. set_account, capture_event, begin_body, observe_chunk, finish_chunks, set_status, set_transport_complete, mark_write_failed 分别对应 Go relay/usage.go 与 usage/capture.go 中的同名行为. capture_event 用于转换线路的上游事件; 透传线路先以响应的 Content-Type 调用 begin_body, 再用 observe_chunk 与 finish_chunks 喂入响应体字节, 内部使用 CaptureBody.
4. finish(client_cancelled) 按 Go usageObservation.finish 的规则计算 outcome 与状态码, 在 spawn_blocking 中写入 Store. 句柄最后一个副本被丢弃而 finish 从未调用时, 视为客户端已断开并完成同样的记录, 保证每个请求恰好记录一次.

### 9.2 lyra::relay

```rust
pub struct AntigravityConfig {
    pub credentials: PathBuf,
    pub base_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub user_agent: Arc<lyra::client_id::ClientIdentity>,
}
pub struct ChatGptConfig {
    pub source: ChatGptSourceConfig,
    pub base_url: String,
    pub models: Vec<String>,
    pub user_agent: Arc<lyra::client_id::ClientIdentity>,
}
pub enum ChatGptSourceConfig {
    Credentials(PathBuf),
    Broker(BrokerConfig),
}
pub struct XaiConfig {
    pub credentials: PathBuf,
    pub base_url: String,
    pub token_url: String,
    pub client_id: String,
    pub scope: String,
    pub models: Vec<String>,
    pub user_agent: Arc<lyra::client_id::ClientIdentity>,
}
pub struct CommandCodeConfig {
    pub base_url: String,
    pub api_key: String,
    pub models: Vec<String>,
}
pub struct RelayConfig {
    pub antigravity: Option<AntigravityConfig>,
    pub chatgpt: Option<ChatGptConfig>,
    pub xai: Option<XaiConfig>,
    pub commandcode: Option<CommandCodeConfig>,
    pub capacity_budget: Duration,
}
pub struct BufferedResponse { pub status: StatusCode, pub retry_after: Option<String>, pub body: Bytes }
pub struct CooldownStatus { pub provider: &'static str, pub name: String, pub remaining_seconds: u64 }
pub struct UnavailableModelStatus { pub provider: &'static str, pub name: String, pub model: String, pub reason: &'static str, pub remaining_seconds: u64 }

impl Relay {
    pub fn new(config: RelayConfig, client: reqwest::Client, refresh_client: reqwest::Client) -> Result<Relay, String>;
    pub fn has_antigravity(&self) -> bool;
    pub fn has_chatgpt(&self) -> bool;
    pub fn chatgpt_models(&self) -> &[String];
    pub fn has_xai(&self) -> bool;
    pub fn xai_models(&self) -> &[String];
    pub fn has_commandcode(&self) -> bool;
    pub fn commandcode_models(&self) -> &[String];
    pub async fn statuses(&self) -> Result<Vec<Status>, String>;
    pub fn cooldowns(&self) -> Vec<CooldownStatus>;
    pub fn unavailable_models(&self) -> Vec<UnavailableModelStatus>;
    pub async fn messages(&self, headers: &HeaderMap, body: Bytes, route: &str, observation: Option<Observation>) -> Response<Body>;
    pub async fn messages_buffered(&self, headers: &HeaderMap, body: Bytes) -> BufferedResponse;
    pub async fn chatgpt_messages(&self, headers: &HeaderMap, body: Bytes, route: &str, observation: Option<Observation>, reasoning_effort: &str, upstream_model: &str) -> Response<Body>;
    pub async fn chatgpt_messages_buffered(&self, headers: &HeaderMap, body: Bytes, reasoning_effort: &str, upstream_model: &str) -> BufferedResponse;
    pub async fn xai_messages(&self, headers: &HeaderMap, body: Bytes, route: &str, observation: Option<Observation>, reasoning_effort: &str, upstream_model: &str) -> Response<Body>;
    pub async fn xai_messages_buffered(&self, headers: &HeaderMap, body: Bytes, reasoning_effort: &str, upstream_model: &str) -> BufferedResponse;
    pub async fn commandcode_messages(&self, headers: &HeaderMap, body: Bytes, route: &str, observation: Option<Observation>, reasoning_effort: &str, upstream_model: &str) -> Response<Body>;
    pub async fn commandcode_messages_buffered(&self, headers: &HeaderMap, body: Bytes, reasoning_effort: &str, upstream_model: &str) -> BufferedResponse;
    pub async fn responses(&self, headers: &HeaderMap, path: &str, body: Bytes, observation: Option<Observation>) -> Response<Body>;
}
```

1. messages 的 body 已经由调用方改写为上游模型名. 它完成 Lyra 的账号选择, 重试与转换, 返回流式或非流式的 Anthropic 响应, 错误使用 Anthropic 格式. route 只用于日志.
2. messages_buffered 用于搜索轮次: 强制非流式, 读取完整响应体, 至多 4 MiB.
3. responses 完成 ChatGPT 账号池的透传, 错误使用 OpenAI 风格格式. path 是客户端请求的路径.
4. client 是只用 HTTP/1.1 的上游客户端, refresh_client 用于令牌刷新与项目标识推导.
5. user_agent 是各线路的共享客户端标识, 见 3.8. 调用方构造一次, 克隆进线路配置并保留一份驱动后台更新; 每次构造上游请求时从中渲染 User-Agent.
6. xai_messages 与 xai_messages_buffered 的行为与 chatgpt 的两个入口相同, 两组入口共用同一份转换转发实现, 线路差异见 3.10. xai 的令牌端点由 XaiConfig.token_url 携带, 测试直接在配置中把它指向桩服务.

### 9.3 登录命令

```rust
pub struct LoginOptions { pub out: PathBuf, pub port: u16, pub timeout: Duration, pub priority: i64, pub note: Option<String>, pub protected: Vec<PathBuf> }
pub async fn lyra::login::google::run(options: LoginOptions) -> Result<(), String>;
pub async fn lyra::login::chatgpt::run(options: LoginOptions) -> Result<(), String>;
pub async fn lyra::login::xai::run(options: LoginOptions) -> Result<(), String>;
```

protected 是不能作为输出的账号池路径, 由 aria 从 --config 中各提供方的 credentials 取得. 三个命令把授权地址打印到标准输出, 等待浏览器回调, 写出候选文件后打印摘要, 摘要中不包含令牌.

### 9.4 aria 可执行文件

模块划分: main.rs 负责命令行; config.rs 负责第 3 节的配置解析与校验; server.rs 负责监听, 连接处理, 60 秒写超时与优雅停机; dispatch.rs 负责鉴权, 并发槽位, 请求体读取, 模型路由, 模型列表, 健康检查与用量端点; search_turn.rs 为 antigravity, chatgpt 与 xai 路由实现 canto::search::SearchTurn. 端到端测试放在 crates/aria/tests, 用桩服务模拟 Messages 上游, Google 令牌端点, Antigravity 后端, Codex 后端, xAI 令牌端点与 Grok 订阅后端.
