# opencode-free-autogate

Go 实现的 **OpenCode 免费模型本地网关**：把 opencode.ai 免费模型包装成 OpenAI / Anthropic / Codex 兼容接口，Windows 单文件运行。

```
opencode 客户端
    ↓  http://localhost:13339/openai/v1
opencode-free-autogate.exe
    ↓  两轮健康检测 ＋ 对冲竞速 ＋ 会话粘性 ＋ 多镜像轮转
opencode.ai/zen ＋ 3 个公共 CDN 镜像
```

## 工作原理

**启动后约 60 秒完成两轮体检，之后你只跟验证过的节点说话：**

```
拉取节点池（订阅/源链接，99 个）
  ↓ 第一轮：GET 探活（零配额）——全部节点测真实上游连通性＋延迟     → 幸存 32 个
  ↓ 第二轮：chat 深检（每 IP 一个 max_tokens=1 迷你对话）           → 验证 28 个真健康
  ↓ 按"真实对话级延迟"排序，随时待命；4 个额度枯竭的假健康坐板凳
```

- 第一轮证明"网络通"，第二轮穿透上游额度门证明"配额没死"——只过第一轮的是假健康。两轮数量都随池子规模动态决定，不设固定上限。
- **对话时**：同会话优先复用上次胜出的出口（粘性，吃上游提示缓存）；首发未决则按延迟排序对冲竞速——多路同时出发，最快吐数据的赢，输家立即取消且不记惩罚；全军覆没落直连兜底。
- **限流记账分级**：上游亲口给的恢复时间（Retry-After / 响应体）＝权威板凳，到点才回归；自己推断的（如 FreeUsageLimitError 默认 2 小时）允许每小时深检提前推翻。计费类错误（402）直接坐 24 小时。
- **板凳到期不踩踏**：刚恢复的出口进入 60 秒观察窗，期间只放一路在途，防止对冲并发把刚回血的额度瞬间压爆。

## 功能

### 调度与加速

| 功能 | 说明 |
|---|---|
| **对冲竞速** | 出口按近期表现评分排序、分批出发（首批少量、迟迟无人交付首块再加发）；出口轮流分散到主上游与各镜像；最快交付者胜出，其余立即取消 |
| **会话粘性** | 同会话钉住上次胜出的出口（30 分钟 TTL），上游提示缓存命中率实测 99.8% vs 竞速轮换 0%；钉住失败自动照常升级竞速，可用性不受影响 |
| **缓存字段注入** | 自动附加 `prompt_cache_retention=24h` ＋ `cache_control`（GLM/Zhipu 系自动跳过），延长上游提示缓存 TTL |
| **SSE 保活** | 竞速超过 5 秒未决时提前提交 SSE 响应头并发心跳注释行，客户端不会误判断线 |

### 健康管理

| 功能 | 说明 |
|---|---|
| **两轮检测** | GET 全量探活（零配额）＋ chat 全量深检（动态数量、8 路并行），启动即跑、每小时复检 ±20% 抖动 |
| **空流拦截** | 返回 200 却无数据即断的流在网关内被拦下换出口；中途夭折的出口记账降权 |
| **板凳阶梯** | 连续失败 30s→1m→2m→5m 逐级暂停；额度枯竭按上游声明或推断长停；探活通过/竞速胜出即回归 |
| **唤醒恢复** | 检测 Windows 休眠恢复（5 秒心跳），清死连接池＋补槽，睡眠后第一波请求不撞陈旧套接字 |

### 协议与防护

| 功能 | 说明 |
|---|---|
| **协议兼容** | OpenAI / Anthropic / Codex 三种路由，客户端零改造接入 |
| **请求体卫生** | 自动剔除缺失 function.name 的工具条目、tools 超 128 截断、剥离 client_metadata——畸形请求体不再烧掉出口尝试 |
| **管家流量拦截** | 客户端的配额探测类请求（极保守匹配＋日志可见）本地直接应答，不消耗上游额度 |
| **UA 版本同步** | 每天从 npm 拉官方 CLI 最新版本号，固定版本号不会日久成为识别特征 |

### 节点接入

| 功能 | 说明 |
|---|---|
| **高级节点** | 内嵌 sing-box v1.13：`vless` `vmess` `trojan` `ss` `hysteria2(hy2)` `tuic` 分享链接直接粘贴，自动转为内部 SOCKS5 参与探活和竞速 |
| **在线节点池** | 定时拉取源链接 → 真实探活 → 健康入池 / 失效自删；支持文本列表、JSON、base64 订阅 |
| **手动节点保护** | 手动填写的节点无条件保留、永不自动移除（坐板凳≠删除） |
| **模型清单** | 启动拉取免费模型长期使用；短名自动重定向 `-free`；models.dev 每日校准仅作下线预警，不影响深检模型选择 |

程序不内置任何节点或订阅；不收集任何数据。

## 快速开始

1. 从 [Releases](https://github.com/krisxu23/opencode-free-autogate/releases/tag/exe-latest) 下载 `opencode-free-autogate-gui.exe`，任意目录双击运行。
2. 编辑 opencode 配置文件（`~/.config/opencode/opencode.jsonc`），添加 provider：

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "freegate": {
      "npm": "@ai-sdk/openai-compatible",
      "options": {
        "baseURL": "http://localhost:13339/openai/v1",
        "apiKey": "sk-local-freegate"
      },
      "models": {
        "mimo-v2.5-free": { "name": "MiMo v2.5" },
        "big-pickle": { "name": "Big Pickle" }
      }
    }
  }
}
```

3. 在 opencode 里切换到 FreeGate 模型即可。完整模型列表以网关界面为准。

## API 路由

| 协议 | 路由 |
|---|---|
| OpenAI | `/openai/v1/models` · `/openai/v1/chat/completions` |
| Anthropic | `/anthropic/v1/messages` |
| Codex | `/codex/v1/responses` |
| 健康检查 | `/healthz` |

## 日志速查

| 标签 | 含义 |
|---|---|
| `[高级] 探活通过 X/Y` | 第一轮 GET 探活结果（Y=全量节点） |
| `[深检] 本轮开始：N 个候选…模型 X` | 第二轮真实对话检测开跑（N=全部幸存者） |
| `[深检] 本轮 N 个出口：通过 X / 失败 Y` | 深检战报；失败的已坐板凳 |
| `[粘性] ses_xxx 钉住 …` | 该会话后续请求优先走同一出口 |
| `[竞速] 胜出: … 已发 N 路（含直连）` | 本笔请求的交付出口与并发路数 |
| `[限流] … 暂停 …（上游声明/推断）` | 额度受限板凳；括号内为依据等级 |
| `[校准]` | models.dev 下线预警（借道出口拉取） |
| `[管家]` | 本地拦截了客户端的配额探测请求 |
| `[唤醒]` | 检测到系统休眠恢复并已自愈 |
| `[指纹]` | UA 版本同步结果 |

## 节点来源

| 来源 | 用法 |
|---|---|
| 手动节点 | 设置 → 代理节点框粘贴，一行一个；支持 socks5/http URL 与各协议分享链接 |
| 订阅 / 节点源 | 打开「在线节点池」开关并填源链接；支持 base64 订阅、socks5 文本列表、amux JSON、明文分享链接 |
| mihomo 本地 | 填 `socks5://127.0.0.1:7890` 即可复用本机 Clash 的全部节点 |

<details>
<summary>推荐的公共免费代理源（可选，可用率低，仅作备胎）</summary>

```
https://proxy.amux.ai/api/proxies
https://raw.githubusercontent.com/watchttvv/free-proxy-list/refs/heads/main/proxy.txt
https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt
https://bestcf.pages.dev/s5gy/all.txt
https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt
https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5.txt
https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt
```
</details>

<details>
<summary>配合 mihomo 按进程分流（可选）</summary>

```yaml
# proxy-groups 追加
- name: 'OpenCode网关'
  type: select
  proxies: ['OpenCode轮询']
- name: 'OpenCode轮询'
  type: load-balance
  strategy: round-robin
  include-all: true
  exclude-type: 'DIRECT'
  url: 'https://g.cn/generate_204'
  interval: 600

# rules 顶部追加
- PROCESS-NAME,opencode-free-autogate.exe,OpenCode网关
```

网关里填 `socks5://127.0.0.1:7890`，切到「走代理」保存重启。
</details>

## 配置

设置在界面修改后「保存并重启」生效，持久化到 exe 同目录 `config.json`（控制台版同样读取该文件）。环境变量优先级更高：

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `13339` | 监听端口 |
| `LISTEN_ADDR` | `127.0.0.1` | 监听地址；设为 `0.0.0.0` 可供局域网设备访问 |
| `CUSTOM_PROXIES` | 空 | 逗号分隔的代理 URL / 分享链接 |
| `MIRROR_URLS` | 空 | 上游镜像基址 |
| `PROXY_LIST_URLS` | 空 | 节点池源链接 |
| `PROXY_RACE` / `PROXY_RACE_WIDTH` | `1` / `8` | 对冲竞速开关 / 自动节点最大参与路数 |
| `PROXY_HEDGE_DELAY` | `1500` | 对冲竞速：首批无首字节后加发下一批的延迟（毫秒） |
| `PROXY_STICKY` | `1` | 会话粘性开关 |
| `PROXY_DEEP_PROBE_INTERVAL` | `3600000` | chat 深检间隔（毫秒） |
| `PROXY_PROBE_MODEL` | `big-pickle` | 深检固定模型：长期在售型号，其他名字随时下线不参与选择 |
| `PROXY_CACHE_FIELDS` | `1` | prompt 缓存字段注入开关 |
| `PROXY_LOCAL_MOCKS` | `1` | 管家流量本地应答开关 |
| `PROXY_FIRST_BYTE_TIMEOUT` | `30000` | 流式首字节超时（毫秒） |
| `HARD_TIMEOUT` | `180000` | 流式总预算（毫秒） |
| `STREAM_IDLE_TIMEOUT` | `300000` | 流式开始后上游静默多久算断流（毫秒），长思考模型可调大 |
| `NON_STREAM_TIMEOUT` | `300000` | 非流式请求总预算（毫秒） |
| `PROXY_ORDER` | 空 | 回退顺序；由 config.json 自动设为 `custom` 或 `direct` |
| `INSECURE_TLS` | `0` | 置 `1` 跳过上游 TLS 证书校验（自签证书的镜像/代理环境用） |

> 默认仅监听本机（127.0.0.1）。上游证书默认严格校验，仅自签证书环境需要 `INSECURE_TLS=1`。
> 「保存并重启」会剔除上表由 config.json 派生的变量后重启进程；`LISTEN_ADDR`、`INSECURE_TLS` 等显式设置的环境变量不受影响。

## 构建

推送后 GitHub Actions 自动构建并发布到 [exe-latest](https://github.com/krisxu23/opencode-free-autogate/releases/tag/exe-latest)：`-gui.exe`（图形版）/ `-console.exe`（排障版）。本地构建需 Go 1.24+：

```bash
go test ./...
go build -trimpath -ldflags "-s -w -H windowsgui" -o opencode-free-autogate-gui.exe .   # GUI 版先用 go-winres 嵌图标
go build -trimpath -ldflags "-s -w" -o opencode-free-autogate-console.exe .
```

## 免责声明

本项目仅为个人学习研究用途的本地网络工具，不提供任何节点资源；使用公共免费代理产生的风险（稳定性、隐私、第三方节点可信度）自行评估。请遵守 opencode.ai 服务条款与当地法律法规。
