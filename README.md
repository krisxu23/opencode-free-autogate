# opencode-free-autogate

Go 实现的免费模型本地网关：把 **opencode.ai 免费模型 + Cline 免费模型**包装成 OpenAI / Anthropic / Codex 兼容接口，Windows 单文件运行，无需任何账号即可使用 opencode 免费模型（Cline 模型需 OAuth 账号）。

```
Codex / Cline / DSH / opencode CLI / 任何 OpenAI 兼容客户端
    ↓  http://localhost:13339/v1
opencode-free-autogate.exe
    ↓
opencode.ai/zen ＋ Cline API ＋ 你配置的代理节点/直连
```

## 核心特性

- **两轮健康检测**：GET 探活（网络通）+ chat 深检（额度没死），只跟验证过的出口说话
- **波次竞速**：多出口并发，最快交付者胜出；会话粘性钉住胜出出口，提示缓存命中率实测 99.8%
- **吸收模式**：流式回复在网关内换道重试直到完整，客户端全程只见保活心跳，不感知上游抖动
- **全局熔断**：上游/链路级故障自动识别，冻结惩罚转指数退避，节点池不自相残杀
- **模型级容错**：模型被限流时自动换候选模型（fallback 链）；下线模型自动从列表剔除
- **IP 信誉**：正式池节点体检（Blackbox + iprisk.top + 地区识别），按纯净度排序出口
- **高级协议节点**：vless / vmess / trojan / ss / hysteria2 / tuic 分享链接直接粘贴，内嵌 sing-box 转本地 socks
- **稳定性细节**：SSE 保活心跳、中流续写（assistant prefill 补尾）、慢流看门狗、提交前静默重试、Retry-After 封顶、流首块限流错误识别
- **请求体卫生**：自动修复畸形请求（缺字段补全、tools 截断、孤儿 tool_result 修复、载荷超限裁剪历史）
- **Windows 图形界面**：供应商/节点池/设置/运行状态/日志 分页管理，今日用量实时展示

## 快速开始

1. 从 [Releases](https://github.com/krisxu23/opencode-free-autogate/releases/tag/exe-latest) 下载 `opencode-free-autogate-gui.exe`，双击运行。
2. 在 opencode 配置（`~/.config/opencode/opencode.jsonc`）添加 provider：

```jsonc
{
  "provider": {
    "freegate": {
      "npm": "@ai-sdk/openai-compatible",
      "options": {
        "baseURL": "http://localhost:13339/v1",
        "apiKey": "sk-local-freegate"
      },
      "models": {
        "opencode/big-pickle": { "name": "Big Pickle (opencode)" }
      }
    }
  }
}
```

3. 在客户端里切换到网关模型即可。完整模型列表见网关「运行状态」页。

## API 路由

| 协议 | 路由 |
|---|---|
| OpenAI | `/v1/models` · `/v1/chat/completions` |
| Anthropic | `/v1/messages` |
| Codex | `/v1/responses` |
| 健康检查 | `/healthz` |

## 主要配置

设置在界面修改后「保存并重启」生效，持久化到 exe 同目录 `config.json`；环境变量优先级更高。

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `13339` | 监听端口 |
| `CUSTOM_PROXIES` | 空 | 手动代理（socks5/http URL 或 vless/vmess 等分享链接） |
| `PROXY_LIST_URLS` | 空 | 在线节点池源链接（订阅/文本/JSON/base64） |
| `PROXY_RACE_WIDTH` | `8` | 竞速并发路数 |
| `PROXY_ABSORB_STREAMING` | `0` | 吸收模式开关 |
| `PROXY_POOL_OPENCODE` | `1` | OpenCode 走节点池出口（0 = 直连） |
| `PROXY_POOL_CLINE` | `0` | Cline 走节点池出口 |
| `PROXY_MODEL_FALLBACKS` | 空 | 模型级 fallback 链（逗号分隔，如 `deepseek-v4-flash-free,big-pickle`） |
| `PROXY_MODEL_ALIASES` | 空 | 客户端模型名别名（如 `claude-sonnet-4.5=big-pickle`） |
| `PROXY_PREFERRED_REGIONS` | 空 | 地区偏好（`US,JP,SG,KR,HK,TW,CN,EU,OTHER`） |
| `CF_FALLBACK_URL` / `CF_FALLBACK_KEY` | 空 | 稳定锚点：全池耗尽后借道自部署 Cloudflare Worker 兜底 |
| `GATEWAY_KEY` | 自动生成 | API 访问密钥 |

完整配置见界面「设置」页。

## 构建

```sh
go run github.com/tc-hib/go-winres@v0.3.3 simply --icon app.ico --manifest gui --arch amd64
go build -trimpath -tags "with_quic,with_utls,with_gvisor" \
  -ldflags "-s -w -H windowsgui -X main.uiMode=gui" -o opencode-free-autogate-gui.exe .
```

无 cgo，单文件产物。日常使用请直接下载 Release（含图标与 GUI 清单）。

## 声明

程序不内置任何节点或订阅，不收集任何数据。
