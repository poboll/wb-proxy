# wb-proxy + lb-proxy — WorkBuddy 免费大模型反代 · 多渠道负载均衡网关

两个单文件、零依赖的 Node.js(≥ 18)代理，解决两件事：

| 文件 | 作用 |
|---|---|
| `wb-proxy.mjs` | 把 WorkBuddy(腾讯)账号里限时免费的大模型反代成 **OpenAI 兼容接口**；支持**账号池**多账号负载均衡、国际版/国内版自动路由 |
| `lb-proxy.mjs` | 统一前端代理：把上游聚合接口的全部模型按「家族」自动分组，**多渠道轮转 + 故障转移 + 熔断冷却 + 会话粘性保缓存**，对客户端隐藏一切渠道错误 |

## 免费模型

| 模型 | 说明 | 免费截止 |
|---|---|---|
| `deepseek-v4.1-flash` | DeepSeek 旗舰,1M 上下文,多模态,支持工具调用 | 2026-11-01 |
| `hy3` | 腾讯混元思考模型 | 2026-11-01 |
| `step-5-preview` | 阶跃星辰旗舰 | 2026-10-26 |
| `deepseek-v4.1-flash-sg` | 付费通道(x0.03 积分)。**默认被本代理禁用**(返回 403 防误扣费),设 `WB_ALLOW_PAID=1` 后重启可放行 | — |

> 以 WorkBuddy 账号内的实际促销为准,免费窗口可能变化。

## 快速开始

### wb-proxy(单账号)

```bash
# 1. 登录(一次性,浏览器授权,token 存 ~/.wb-proxy/auth.json,有效期约 1 年)
#    默认不再自动打开浏览器 —— 会打印链接,自行复制到「目标账号」的浏览器/无痕窗口完成授权
#    (多账号场景非常有用:一个浏览器只登一个账号,不会串号;WB_OPEN=1 可恢复自动打开)
node wb-proxy.mjs login

# 2. 启动反代
node wb-proxy.mjs serve --port 8377
```

### wb-proxy 账号池(多账号负载均衡)

```bash
# 每个账号登录到各自的池文件(绝不覆盖当前在用的 auth.json)
WB_AUTH_FILE=~/.wb-proxy/pool/账号A.json node wb-proxy.mjs login
WB_AUTH_FILE=~/.wb-proxy/pool/账号B.json node wb-proxy.mjs login

# 之后正常 serve 即可:自动加载 ~/.wb-proxy/pool/*.json,轮询使用,某个账号 429/额度耗尽就冷却并切换
# 钉选单个账号:WB_ACCOUNT=账号A 启动,或 echo 账号A > ~/.wb-proxy/active(写 auto 取消钉选)
# 国际版(www.workbuddy.ai)与国内版(www.workbuddy.cn)账号可混放,按各自 token 里的 domain 自动路由
```

### lb-proxy(多渠道网关,推荐直接给客户端用)

```bash
node lb-proxy.mjs --port 8399
# 上游数据源:任意 OpenAI 兼容聚合接口,默认 http://127.0.0.1:18900/v1(LB_AGG_BASE / LB_AGG_KEY 可改)
```

## API 使用

### 端点(两个代理一致)

- `POST /v1/chat/completions` — OpenAI Chat Completions(流式/非流式)
- `POST /v1/responses` — OpenAI **Responses API**(新版 Codex 等只支持 Responses 的客户端,自动双向翻译)
- `GET /v1/models` — 模型列表
- `GET /health` — 健康检查(lb-proxy 还返回每渠道的请求数/token 用量/冷却状态)

### 请求示例

```bash
# 非流式
curl http://127.0.0.1:8399/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer 任意' \
  -d '{"model":"deepseek-flash","messages":[{"role":"user","content":"用一句话介绍北京"}]}'

# 流式(SSE,末尾带 usage 与 data: [DONE])
curl -N http://127.0.0.1:8399/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer 任意' \
  -d '{"model":"deepseek-flash","stream":true,"messages":[{"role":"user","content":"从1数到5"}]}'
```

### 模型名怎么填

- baseURL:`http://127.0.0.1:8399/v1`(lb-proxy)或 `http://127.0.0.1:8377/v1`(wb-proxy)
- apiKey:**任意值**(本机不校验;wb-proxy 启动时加了 `--key` 则填该值)
- model:填**组名**——一个名字背后是同一模型的多家渠道,网关自动轮转:

| 组名 | 渠道数 | 选中渠道(策略:同厂商只取一个·中档·避开 Max/高速) |
|---|---|---|
| `deepseek-flash` | 13 | buddy / workbuddy / lobsterai / codearts / opencode / qoder 等全系 flash(含 vision-exp) |
| `deepseek-pro` | 5 | buddy / lobsterai / codearts / qoder 等 deepseek-v4-pro |
| `glm` / `glm-flash` | 5 / 7 | buddy·glm-5.2、z-ai·glm-5.3、codearts·GLM-5.2 … |
| `kimi` | 4 | buddy·kimi-k2.7、workbuddy·kimi-k2.6(避开 k3 最新) |
| `gpt` | 2 | workbuddy·gpt-5.5(中档)、openai·gpt-oss-20b |
| `qwen` | 1 | lobsterai·qwen3.6-plus(中档) |
| `hy` | 2 | buddy·hy3、workbuddy·hy3(避开 hy4-preview) |
| `step` | 2 | buddy/workbuddy·step-5-preview |
| `minimax` | 2 | buddy·minimax-m3、lobsterai·MiniMax-M2.7 |
| `gemini` `grok` `doubao` `openpangu` `nemotron` | 各 1~2 | workbuddy·gemini-3.5-flash、grok-4.7… |
| `mimo` `longcat` `space-bunny` `cohere` `lfm` `dots` `inkling` `laguna` `xingchen` | 各 1~2 | 新收录的小尺寸免费模型 |
| `multimodal` | 3 | glm-5v-turbo、qwen3.8-omni、nemotron-omni 等视觉/omni 模型 |

- 也支持**精确渠道 id 直连**(如 `buddy/deepseek-v4.1-flash`),原样打到该渠道,不轮转;
- `deepseek-flash` 与 `deepseek-pro` 是**两个独立集合,互不归纳**;
- 响应头 `X-LB-Channel` 标注实际命中的渠道,便于观测轮转/降级。

### 环境变量

lb-proxy(全部可选,均有默认值):

| 变量 | 默认 | 说明 |
|---|---|---|
| `LB_AGG_BASE` / `LB_AGG_KEY` | `http://127.0.0.1:18900/v1` / 内置 | 上游聚合接口地址与密钥 |
| `LB_CHANNEL_BUDGET` | `1000000` | 每渠道 token 预算,达到后退出轮换(雨露均沾) |
| `LB_COOLDOWN_MS` / `LB_COOLDOWN_MS_MAX` | `3000` / `120000` | 失败冷却:3s 起步指数退避,封顶 2 分钟 |
| `LB_CONNECT_COOLDOWN_MS` | `3000` | 上游整体连接失败(ECONNREFUSED 等)的短冷却起点 |
| `LB_ROUTE_TIMEOUT_MS` | `25000` | 单渠道连接/首包超时 |
| `LB_PRESTREAM_IDLE_MS` | `60000` | 开流后 60s 见不到真实内容即判失败换渠道(无视 ping 保活) |
| `LB_POSTSTREAM_IDLE_MS` | `60000` | 已见到内容后 60s 无新内容即截断换渠道 |
| `LB_AFFINITY` | `1` | 会话粘性(同一对话稳定命中同一渠道,保 KV 缓存);`0` 关 |
| `LB_FALLBACK` / `LB_FALLBACK_MAX` | `1` / `8` | 原组全不可用时,跨模型免费兜底(最多试几条) |
| `LB_PAID_RESORT` | `workbuddy/deepseek-v4.1-flash-sg` | 付费底牌渠道;全免费顶不上才用;置空=关闭 |
| `LB_PAID_BEFORE_FALLBACK` | `0` | 置 `1` 则付费底牌先于跨模型兜底 |
| `LB_RETRY` / `LB_DEADLINE_MS` | `1` / `50000` | 连接类失败当场重试次数 / 单请求软时限 |
| `LB_LOW_PREFIX` | 空 | 要降到最低优先级的厂商前缀(逗号分隔) |
| `LB_REFRESH_MS` | `600000` | 重新拉取上游模型列表的周期 |
| `LB_SYSTEM` | `You are a helpful assistant.` | 首条非 system 时补的中性 system(部分渠道强制要求) |
| `LB_STREAM_LIVE` | `0` | 置 `1` 退回「见首字即提交」实时流式(首字快,但提交后出错无法重试) |
| `LB_DEFAULT_MAX_TOKENS` | `8192` | 客户端未指定 max_tokens 时注入的输出上限;`0` 关闭 |

wb-proxy:`WB_ALLOW_PAID=1` 放行付费模型;`WB_AUTH_FILE=<路径>` 指定登录落地文件(账号池);`WB_ACCOUNT=<名>` 钉选账号;`WB_OPEN=1` 登录时自动打开浏览器;`WB_SYSTEM_PROMPT` 覆盖补的 system;`WB_DEBUG=1` 把最近一次上游请求体写到 `~/.wb-proxy/last-request.json`。

## 负载均衡设计讲解(lb-proxy)

设计目标:**客户端永远拿到完整回答,永远看不到失败原因**;同时把免费额度「雨露均沾」地摊到所有渠道上。

1. **家族自动分组**:启动时(之后每 10 分钟)从上游拉取全部模型,按 deepseek/glm/kimi/qwen/gpt/gemini… 分成若干组对外只暴露一个组名;客户端不用关心背后是哪家厂商。
2. **同厂商只取一个 + 中档策略**:同一厂商的每日额度是共用的(glm-5.1/5.2/5.3 都抢同一家配额),所以每组每个 provider 只留一个;取**版本中位数**当中档(避开最新最高版),同档多变体优先 flash/lite 等轻量档;`Max / high-speed / ultra / super` 等高配直接剔除(带边界匹配,不会误杀 minimax)。
3. **会话粘性(保缓存)**:亲和键 = system + 首个 user 消息(同一任务全程恒定),每个对话锁定一条 home 渠道并记住——**同一对话每轮都打同一条渠道,KV/前缀缓存持续命中**(实测 `cached_tokens` 稳定 > 0);不同任务自然分散,雨露均沾不丢。渠道坏了才换,恢复后回到它。
4. **每渠道 1M token 预算**:每渠道累计到 `LB_CHANNEL_BUDGET`(默认 1M)即退出轮换,换下一家;全部到预算也退而用「烧得最少」的渠道继续服务,绝不停摆。
5. **三级故障转移**:原组免费多渠道 → 其它免费模型顶上(跨模型兜底)→ 付费底牌(同家族 sg 通道)。任何一层能用就不返回错误。
6. **熔断与冷却(指数退避)**:失败从 **3s 起步**逐次翻倍(封顶 2 分钟,连接类封顶 60s),±20% 抖动避免整组同步恢复;成功一次立即清零。上游聚合接口瞬态抖动几秒即自愈,不会一锁十几分钟。
7. **零失败外传(流式缓存模式)**:流式请求**整段缓存、完整成功才提交**——渠道错误(识别 `<厂商>:` 前缀、`param invalid`/`quota`/`idle timeout` 等短语,支持拆片与换行变体)、只发 ping 不开工、卡死、无完成事件、空内容,一律**一个字节都不发客户端**,网关自己冷却换渠道重试;客户端最坏看到的是「多等了几秒」,而不是错误。
8. **双墙钟看护**:开流 60s 见不到内容即切、见到内容后 60s 无新内容即切(ping 不续期)——上游 300s idle 之前就切走,失败转移耗时从 5 分钟压到 1 分钟内。
9. **长输出兜底**:客户端没设 `max_tokens` 时注入默认 8192,避免渠道默认值偏小把长回答截断;客户端显式设的值永远尊重。

高可用形态:`wb-proxy` 侧还有**账号级**负载均衡(池内 round-robin,401 自动刷新重试、429/额度耗尽按上游重置时间冷却切换、钉选账号),两层 LB 可叠加使用。

## 客户端接入

- baseURL: `http://127.0.0.1:8399/v1`(lb-proxy)或 `http://127.0.0.1:8377/v1`(wb-proxy)
- apiKey: 任意值
- model: 组名(见上表)或精确渠道 id

### Codex(~/.codex/config.toml)

```toml
model_provider = "custom"
model = "deepseek-flash"          # 走 lb-proxy 时用组名; 直连 wb-proxy 用 deepseek-v4.1-flash

[model_providers.custom]
name = "custom"
wire_api = "responses"            # 新版 Codex 只支持 responses, wb-proxy 已兼容; lb-proxy 请用 chat/completions
requires_openai_auth = false
base_url = "http://127.0.0.1:8399/v1"
experimental_bearer_token = "1"
```

其他支持自定义 OpenAI baseURL 的工具(Cherry Studio / NextChat / LobeChat / CC Switch 等)同理。

## 实现要点

**wb-proxy**
- 上游 `POST https://www.workbuddy.ai/v2/chat/completions`,OpenAI SSE 格式,仅支持流式;非流式由代理聚合 SSE 后返回标准 JSON。
- 网关安全策略:首条消息必须是 system(代理自动补一条,`WB_SYSTEM_PROMPT` 可覆盖),不认 `developer` 角色(归一为 system);并行工具调用合并成标准 OpenAI 单条 assistant 多 tool_calls 形态。
- 认证头 `Authorization: Bearer <accessToken>` + `X-User-Id`(从 JWT sub 解析);token 快过期自动 `/v2/plugin/auth/token/refresh` 续期(账号池按各自 domain 续期、写回各自文件)。
- 完整模拟官方 WorkBuddy CLI 客户端头,否则裸请求会被分到更严格的外部限流桶,容易 429。
- 弹性 DNS:系统 DNS 被 fake-ip VPN 劫持时自动改用阿里 DoH 解析真实 IP 直连。
- 本 fork 相对上游的改动:
  - 修复 Responses API 的 `reasoning.effort`/`reasoning_effort` **推理强度被静默丢弃**的缺陷;
  - 付费模型默认禁用(403),防误扣费(`WB_ALLOW_PAID=1` 放行);
  - 账号池负载均衡(`~/.wb-proxy/pool/*.json`,round-robin/random,401 刷新重试,429 按上游重置时间冷却切换,支持钉选);
  - 按账号 `domain` 路由,国际版/国内版通吃;
  - `login` 默认不再自动打开浏览器(多账号防串号),`WB_OPEN=1` 恢复。

**lb-proxy**
- 单文件只用 node 内置模块,实测常驻内存约 50MB。
- 渠道可靠度分层(buddy/opencode/workbuddy/lobsterai 排前),冷启动优先命中稳渠道。
- 全部策略见上一节,环境变量见上表。

## 部署到服务器

```bash
scp wb-proxy.mjs lb-proxy.mjs user@server:~/
# 在本机 login 后把 token 传上去(账号池则传整个 pool 目录)
scp ~/.wb-proxy/auth.json user@server:~/.wb-proxy/auth.json
```

systemd 单元示例:

```ini
[Service]
ExecStart=/usr/bin/node /home/user/lb-proxy.mjs --port 8399
Restart=always
```

别忘了防火墙放行端口。macOS 上 `~/Downloads` 受 TCC 限制无法用 launchd 常驻,用 `nohup ... & disown` 即可(会话结束不掉)。

## 注意与免责声明

- 免费额度是 WorkBuddy 官方限时促销活动,活动结束后按正常积分计费;每日签到赠送的额度按账号/渠道各自计算。
- 429 限流按账号计算,多节点部署不能突破总额度,只是备用接入点。
- 本项目通过官方登录接口获取本人账号 token 并调用其网关,仅供学习研究;高频/商用调用可能触发风控或违反服务条款,使用者自行承担风险。
- token 文件在 `~/.wb-proxy/auth.json` 与 `~/.wb-proxy/pool/*.json`(权限 600),勿泄露、勿提交(`.gitignore` 已排除)。
- lb-proxy 的上游密钥默认值仅对应作者本机聚合服务,请用 `LB_AGG_BASE` / `LB_AGG_KEY` 指向你自己的服务。

## License

MIT
