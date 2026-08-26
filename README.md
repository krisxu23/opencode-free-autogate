# opencode-free-autogate

Go 实现的 **OpenCode 免费模型本地网关**：把 opencode.ai 免费模型包装成 OpenAI / Anthropic / Codex 兼容接口，Windows 单文件运行，原生现代化界面。

```
opencode 客户端（DSH / ZCode / opencode CLI…）
    ↓  http://localhost:13339/openai/v1
opencode-free-autogate.exe
    ↓  两轮健康检测 ＋ 对冲竞速 ＋ 会话粘性 ＋ 吸收模式 ＋ 全局熔断
opencode.ai/zen ＋ 3 个公共 CDN 镜像
```

## 工作原理

**启动后约 60 秒完成两轮体检，之后你只跟验证过的节点说话：**

```
拉取节点池（订阅源 + 裸代理源，如 GitHub IPOcate 免费列表）
  ↓ 源拉取：直连失败时自动经现有健康出口兜底重试（源获取与上游请求同一哲学）
  ↓ 第一轮：GET 探活（零配额）——全部节点测真实上游连通性＋延迟     → 幸存
  ↓ 第二轮：chat 深检（每 IP 一个 max_tokens=1 迷你对话）           → 验证真健康
  ↓ 按"真实对话级延迟"排序，随时待命；额度枯竭的假健康坐板凳
```

- 第一轮证明"网络通"，第二轮穿透上游额度门证明"配额没死"——只过第一轮的是假健康。两轮数量都随池子规模动态决定，不设固定上限。**所有节点类型**（socks5/vless/vmess/ss/trojan/hysteria2）统一走两轮检测，标准一致。
- **对话时**：同会话优先复用上次胜出的出口（粘性，吃上游提示缓存）；首发未决则按延迟排序对冲竞速——多路同时出发，最快吐数据的赢，输家立即取消且不记惩罚；全军覆没落直连兜底。
- **流式请求双保险**：出口中途夭折 → **吸收模式**在网关内换道重试直到拿到完整回复，客户端只见保活心跳；上游整体抖动 → **全局熔断器**识别风暴、冻结惩罚转指数退避，池子不自相残杀。
- **限流记账分级**：上游亲口给的恢复时间（Retry-After / 响应体）＝权威板凳，到点才回归；自己推断的（如 FreeUsageLimitError 默认 2 小时）允许每小时深检提前推翻。计费类错误（402）直接坐 24 小时。
- **板凳到期不踩踏**：刚恢复的出口进入 60 秒观察窗，期间只放一路在途，防止对冲并发把刚回血的额度瞬间压爆。

## 功能

### 调度与加速

| 功能 | 说明 |
|---|---|
| **对冲竞速** | 出口按近期表现评分排序、分批出发（首批少量、迟迟无人交付首块再加发）；出口轮流分散到主上游与各镜像；最快交付者胜出，其余立即取消 |
| **会话粘性** | 同会话钉住上次胜出的出口（30 分钟 TTL），上游提示缓存命中率实测 99.8% vs 竞速轮换 0%；钉住失败自动照常升级竞速，可用性不受影响 |
| **缓存字段注入** | 自动附加 `prompt_cache_retention=24h` ＋ `cache_control`（GLM/Zhipu 系自动跳过；客户端自带断点达 4 个上限时不再叠加，防上游判非法），延长上游提示缓存 TTL |
| **SSE 保活** | 竞速超过 5 秒未决时提前提交 SSE 响应头并发心跳注释行，客户端不会误判断线 |
| **吸收模式** | 流式回复在网关内完整接收并校验，中途截断自动换道重试（默认最多 10 次），客户端全程只看保活心跳；拿到验证完整的原文后一次性交付 |

### 健康管理

| 功能 | 说明 |
|---|---|
| **两轮检测** | 初检（GET 零配额）与复检（chat 真实对话）两条并行流水线：普通代理与 sing-box 高级映射节点合并一条队列、按「检测并发」**连轴转逐个测完即拉下一轮**；初检通过先进内部候选池（不参与竞速、界面不显示），复检通过立即转正——正式节点永远双重验证过。复检另按「深检间隔」±20% 抖动循环兜底全池 |
| **空流拦截** | 返回 200 却无数据即断的流在网关内被拦下换出口；中途夭折的出口记账降权 |
| **板凳阶梯** | 连续失败 30s→1m→2m→5m 逐级暂停；额度枯竭按上游声明或推断长停；探活通过/竞速胜出即回归 |

### 韧性与熔断

| 功能 | 说明 |
|---|---|
| **全局熔断器** | 45 秒滑动窗口内 ≥6 次失败横跨 ≥4 个出口且 ≥2 个镜像 → 判定上游/链路级故障：冻结一切出口惩罚、吸收模式转指数退避（300ms→9.6s 封顶）、静默 60 秒自动解除——源站抖动不再引发全池雪崩。静默满 30 秒额外放行一次「半开探针」立即重试，成功即提前解除（借鉴 cc-switch HalfOpen 态），最坏恢复等待从 60 秒压到约 30 秒 |
| **失败归因修正** | ctx 取消/预算到期的竞速腿不再被误记为镜像失败；只有真实上游错误才参与记账与熔断判定 |
| **唤醒恢复** | 检测 Windows 休眠恢复（5 秒心跳），清死连接池＋补槽，睡眠后第一波请求不撞陈旧套接字；与熔断器互补——一个管"节点死了"，一个管"大家都被冤枉了" |

### 协议与防护

| 功能 | 说明 |
|---|---|
| **协议兼容** | OpenAI / Anthropic / Codex 三种路由，客户端零改造接入 |
| **请求体卫生** | 自动剔除缺失 function.name 的工具条目、tools 超 128 截断、剥离 client_metadata、空 `tool_calls.arguments` 补 `"{}"`（Minimax 系严格上游会对空参数拒收整个请求）——畸形请求体不再烧掉出口尝试 |
| **请求指纹整形** | 出站 body 顶层键序统一对齐原生 CLI 构造序（chat 形态取自 @ai-sdk/openai-compatible 源码、responses 形态取自 OmniRoute 抓包），消灭"改写过=字母序、没改过=原序"的双指纹特征；Anthropic `/v1/messages` 无可靠依据，刻意不整形 |
| **SSE 分块卫生** | 直通流与吸收流双路径：丢弃解析失败的 `data:` 行（上游夹带的错误页不再喂给客户端）、删除空 `tool_calls:[]`、补缺失的 object/created 字段；>1MB 整行透传、截断维持静默干净关闭语义 |
| **思考模型兼容** | deepseek/kimi/minimax 系模型多轮对话自动补 `reasoning_content` 占位符，防止 OpenAI 格式客户端缺字段导致上游 400 |
| **管家流量拦截** | 客户端的配额探测类请求（极保守匹配＋日志可见）本地直接应答，不消耗上游额度 |
| **UA 版本同步** | 每天从 npm 拉官方 CLI 最新版本号，固定版本号不会日久成为识别特征 |
| **回显取证**（默认关） | `PROXY_ECHO_DEBUG=1` 时把胜者响应头脱敏落日志（auth/token/key 类值替换为 `<已脱敏 len=N>`），用于排查上游是否下发 turn-scoped 状态头 |

### 节点接入

| 功能 | 说明 |
|---|---|
| **高级节点** | 内嵌 sing-box v1.13：`vless` `vmess` `trojan` `ss` `hysteria2(hy2)` `tuic` 分享链接直接粘贴，自动转为内部 SOCKS5 参与探活和竞速 |
| **在线节点池** | 后台连轴转：拉源 → 初检 → 复检 → 转正，循环往复无轮间间隔；测试与入池规模完全由源列表决定；源直连失败自动经健康出口兜底重试；支持文本列表、JSON、base64 订阅、裸 socks5/ip:port 列表 |
| **手动节点保护** | 手动填写的节点无条件保留、永不自动移除（坐板凳≠删除） |
| **模型清单** | 启动拉取免费模型长期使用；短名自动重定向 `-free`；models.dev 每日校准仅作下线预警，不影响深检模型选择 |

程序不内置任何节点或订阅；不收集任何数据。

### 界面

| 特性 | 说明 |
|---|---|
| **深色标题栏** | 跟随系统暗色开关（Win10 20H1+）；Win11 额外标题栏染色＋Mica 材质，老系统静默降级 |
| **自绘信息卡** | 顶部横幅：应用名 ＋ 运行时长秒级跳动 ＋ 大号健康状态灯（蓝=待命 绿=正常 橙=有限流/截断 红=出现失败），颜色与状态行联动 |
| **今日用量** | 状态区实时显示当日请求数与 token 进出量（按模型聚合，SSE 流式与非流式双路径解析 usage 块），落盘 exe 同目录 `usage_stats.json`，跨天自动清零 |
| **栅格化排版** | 统一间距层级；等宽字体展示地址/日志便于对齐 |

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
| `[吸收] 第 N 次截断…换道重试` | 吸收模式发现截断，正在换出口重试 |
| `[吸收] … 取得完整回复` | 吸收模式成功，完整原文即将一次性交付 |
| `[全局熔断] … 冻结惩罚并转入指数退避` | 判定上游/链路级故障，进入全局保护态（静默 60 秒自动解除） |
| `[全局熔断] … 放行半开探针 / 探针成功，提前解除` | 静默满 30 秒后的单次立即重试及其结果：成功则不必等满 60 秒 |
| `[SSE卫生] 丢弃 N 条无效行` | 直通流清洗掉了解析失败的 data 行 |
| `[限流] … 暂停 …（上游声明/推断）` | 额度受限板凳；括号内为依据等级 |
| `[池] 经出口 xxx 拉取成功` | 节点源直连失败后经出口兜底重试成功 |
| `[池] 拉取失败 … / 出口兜底也失败` | 源直连失败，正在/已用出口兜底重试 |
| `[校准]` | models.dev 下线预警（借道出口拉取） |
| `[管家]` | 本地拦截了客户端的配额探测请求 |
| `[唤醒]` | 检测到系统休眠恢复并已自愈 |
| `[指纹]` | UA 版本同步结果 |

## 节点来源

| 来源 | 用法 |
|---|---|
| 手动节点 | 设置 → 代理节点框粘贴，一行一个；支持 socks5/http URL 与各协议分享链接 |
| 订阅 / 节点源 | 打开「在线节点池」开关并填源链接；支持 base64 订阅、socks5 文本列表、amux JSON、明文分享链接 |
| 裸代理源 | GitHub iplocate 免费 socks5 列表等直填 `ip:port` 文本；直连不通时网关自动走出口兜底 |
| mihomo 本地 | 填 `socks5://127.0.0.1:7890` 即可复用本机 Clash 的全部节点 |

<details>
<summary>推荐的公共免费代理源（可选，可用率低，仅作备胎）</summary>

```
https://github.cmliussss.net/https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt
https://gh-proxy.com/https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt
https://ghfast.top/https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt
https://bestcf.pages.dev/s5gy/all.txt
https://proxy.amux.ai/api/proxies
```

> 镜像地址（gh-proxy.com / ghfast.top / gh.llkk.cc 等）随时段波动，多贴几个做对冲。死掉的源只是日志一条 `拉取失败`，零成本；不同镜像拉到相同 ip:port 自动去重。
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
| `PROXY_ABSORB_STREAMING` | `false` | 吸收模式开关（GUI 勾选框写入 config.json `absorb_streaming`） |
| `PROXY_ABSORB_ATTEMPTS` | `10` | 吸收模式最大换道尝试次数（config.json `absorb_attempts`） |
| `PROXY_BODY_FINGERPRINT` | `1` | 出站 body 键序指纹整形开关 |
| `PROXY_SSE_HYGIENE` | `1` | SSE 分块卫生开关（丢无效行/删空 tool_calls/补缺失字段） |
| `PROXY_ECHO_DEBUG` | `0` | 胜者响应头脱敏回显日志（取证用，日常不开） |
| `PROXY_DEEP_PROBE_INTERVAL` | `3600000` | chat 深检间隔（毫秒） |
| `PROXY_PROBE_CONCURRENCY` | `32` | 检测并发路数，GET 初检与 chat 深检/复检共用（1-128；节点上千时调高，GUI「检测并发」输入框同款） |
| `PROXY_PROBE_MODEL` | `big-pickle` | 深检模型：GUI 下拉可选（实时上游模型列表），环境变量优先；长期在售型号最稳 |
| `PROXY_CACHE_FIELDS` | `1` | prompt 缓存字段注入开关 |

## 构建

官方发布一律走 GitHub Actions：自动嵌入图标与 GUI 清单、冒烟测试拦截残缺包，产物发布到 [exe-latest](https://github.com/krisxu23/opencode-free-autogate/releases/tag/exe-latest)（显式 `--draft=false`，保证访客与直链可见可下）。**日常使用请直接下载 Release，不要用本地裸 `go build` 的产物**——缺资源嵌入步骤的 exe 没有图标和 Common-Controls 清单，walk 界面会退化成旧版控件样式。

本地仅作代码验证：

```sh
go vet ./...
go test -tags "with_quic,with_utls,with_gvisor" -count=1 ./...
```

本地如需出验证包，必须先跑 winres 资源嵌入再构建（与 CI 完全同款）：

```sh
$env:GOPROXY="https://goproxy.cn,direct"  # proxy.golang.org 直连超时时的备用源
go run github.com/tc-hib/go-winres@v0.3.3 simply --icon app.ico --manifest gui --arch amd64
go build -trimpath -tags "with_quic,with_utls,with_gvisor" \
  -ldflags "-s -w -H windowsgui -X main.uiMode=gui" -o opencode-free-autogate-gui.exe .
Remove-Item *.syso  # 构建后清理资源文件，不入库
```

无 cgo、单文件产物。
