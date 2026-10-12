# Aria

Aria 是一个用 Rust 编写的模型网关与订阅账号中继. 它对外提供 Anthropic Messages 协议, 把 Claude Code 等客户端的请求按模型名路由到不同的上游: 已经实现同一协议的服务商 API, 或者使用者自己的订阅账号.

## 项目做什么

Aria 的单个可执行文件承担两类能力, 由配置文件决定启用哪些.

1. 模型网关. 它把 Anthropic Messages 请求按配置的路由名转发到实现同一协议的上游, 只改写模型名, 并为声明了搜索的路由编排 Kimi 搜索服务.
2. 订阅账号中继. 它把 Anthropic Messages 请求转换为上游订阅后端使用的协议, 用账号池发送, 再把上游的事件流转换回 Anthropic 格式. 目前支持三条订阅线路: ChatGPT (Codex 后端), Google Antigravity 与 SuperGrok (Grok CLI 订阅后端).

同一个产物按配置文件启用不同能力: 单机可以启用全部能力, 也可以只启用某一个提供方.

## 支持的上游线路

路由由配置文件中的 routes 数组声明, 每条路由的类型由 upstream 字段决定.

| upstream | 说明 |
|---|---|
| messages | 上游已经实现 Anthropic Messages 协议, 网关只改写模型名. 支持单上游与多上游轮换. |
| antigravity | 经 Google Antigravity 订阅账号访问 Gemini 与 Claude 模型. |
| chatgpt | 经 ChatGPT 账号池使用 Codex 后端的 GPT 模型. |
| xai | 经 SuperGrok 订阅使用 Grok 模型. |
| qoder | 经 Qoder 官方网关访问其支持的模型. |
| commandcode | 经 Command Code Provider API 按量访问其模型. |

## 构建

工具链版本由仓库的 rust-toolchain.toml 固定, rustup 会自动选用.

```bash
git clone <本仓库地址>
cd aria-public
cargo build --release --locked
```

产物为 `target/release/aria`. 构建与测试只依赖标准 Rust 工具链, 不需要额外的系统包.

## 运行与配置

Aria 读取一个 JSON 配置文件. 解析时拒绝未知字段, 文件只能包含一个 JSON 对象; 任何校验失败都在开始监听前报错退出, 错误信息说明出错的字段与路由名, 不输出密钥.

```bash
# 只做配置与环境变量校验, 不联网
aria check --config /path/to/config.json --clients /path/to/clients.json

# 启动服务
aria serve --config /path/to/config.json --clients /path/to/clients.json
```

最小配置需要一份客户端密钥文件和至少一条路由或一个提供方.

```json
{
  "listen": "127.0.0.1:20182",
  "clients": "/path/to/clients.json",
  "routes": [
    {
      "name": "example-route",
      "model": "upstream-model-name",
      "upstream": "messages",
      "base_url": "https://upstream.example.com",
      "key_env": "UPSTREAM_API_KEY"
    }
  ]
}
```

客户端密钥文件格式为 `{"keys": [{"name": ..., "key": ..., "enabled": ...}]}`, 启用的密钥数为零时启动失败.

### 订阅读取线路的凭据

三条订阅线路各自需要一个浏览器登录流程来生成候选凭据文件.

```bash
aria login google  --out /path/to/google.json
aria login chatgpt --out /path/to/chatgpt.json
aria login xai     --out /path/to/xai.json
```

登录命令会打印一个浏览器授权地址, 在本机回环端口等待回调, 完成后写出候选文件; 候选文件不会覆盖已存在的文件, 也不会写入正在使用的账号池.

Antigravity 线路的 OAuth 客户端密钥没有内置默认值, 因为该密钥属于 Antigravity 官方客户端, 不随本仓库分发. 使用 Google 线路时, 需要在配置文件的 providers.antigravity.client_secret 中提供, 或者把它放进环境变量 `ARIA_ANTIGRAVITY_CLIENT_SECRET`. 两者都没有而配置了 providers.antigravity 时, 校验会直接失败.

完整的配置字段, HTTP 端点与行为契约见 [docs/architecture.md](docs/architecture.md).

## 使用风险说明

1. 部分线路依赖使用者自己的个人订阅服务, 并且请求会模仿对应官方客户端的请求标识. 这类用法可能与相应服务的服务条款不一致. 使用者需要自行确认并遵守各服务的条款, 自行承担使用后果.
2. 使用者需要自行准备订阅账号与访问凭据, 本仓库不提供任何账号, 令牌或密钥.
3. 上游服务的协议与接口可能随时变化, 模仿客户端标识的做法也可能被上游识别或限制. 上游策略变化导致的不可用不在本项目的保证范围内.

## 测试与检查

```bash
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

测试只访问本机回环地址上的桩服务, 不访问任何真实上游, 也不读取本机真实凭据.

## 许可证

本项目暂未授予任何许可证. 在许可证确定之前, 默认保留所有权利, 请勿在未获授权的情况下复制, 修改或分发本仓库内容.

## 文档

1. [docs/architecture.md](docs/architecture.md): 实现规格, 配置格式, HTTP 行为与各线路契约.
2. [docs/review-standard.md](docs/review-standard.md): 代码审查的逐条核对标准.
