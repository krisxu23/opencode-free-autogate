package main

import (
	"encoding/json"
	"log"
	"regexp"
	"strings"
	"time"
)

// enhanceRequestBody 请求体卫生 + prompt 缓存字段，一次解析完成：
//
// 卫生（防上游 400 白白烧掉一次出口尝试）：
//   - 丢弃缺失 function.name 的工具条目（opencode CLI 的已知序列化产物，
//     上游以 tools[n].function: missing field "name" 拒绝整个请求）
//   - tools 超过 128 条时截断（上游上限）
//   - 剥离顶层 client_metadata（隐私收窄，上游不消费）
//
// 缓存（同类项目实测可把上游提示缓存 TTL 从 ~5 分钟延到 24h）：
//   - prompt_cache_retention:"24h"
//   - cache_control:{type:"ephemeral",ttl:"1h"}（GLM/Zhipu 系模型拒绝
//     该字段，按前缀黑名单跳过）
//
// 仅在确有改动时重新序列化；无改动原样返回，零额外成本。
// injectCache 仅对 OpenAI 协议路径开启（Anthropic 原生体不接受这两个
// 顶层字段），并受 PROXY_CACHE_FIELDS 开关控制。
// enhanceRequestBody 是字节接口的薄包装（测试与外部调用用）；主链路在
// handlePost 中直接操作已解析的 payload，避免重复反序列化。
func enhanceRequestBody(body []byte, injectCache bool) []byte {
	payload := parseJSONObject(body)
	if payload == nil {
		return body
	}
	if !enhanceRequestBodyPayload(payload, injectCache) {
		return body
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return out
}

// enhanceRequestBodyPayload 请求体卫生 + prompt 缓存字段，就地改写 payload，
// 返回是否有改动。改动项与判定规则见 enhanceRequestBody 上方注释。
func enhanceRequestBodyPayload(payload map[string]any, injectCache bool) bool {
	if payload == nil {
		return false
	}
	model, _ := payload["model"].(string)
	changed := false

	if _, ok := payload["client_metadata"]; ok {
		delete(payload, "client_metadata")
		changed = true
	}

	// 思考系模型补丁（OCFreeRelay/9router/OmniRoute 三家共识）：
	// deepseek/kimi/minimax 系多轮对话要求 assistant 历史消息携带
	// reasoning_content，OpenAI 格式客户端不发该字段 → 上游 400 拒绝
	// 整个请求、白烧出口尝试。缺字段时注入单空格占位符。
	if msgs, ok := payload["messages"].([]any); ok && model != "" {
		injected := false
		for _, m := range msgs {
			entry, ok := m.(map[string]any)
			if !ok {
				continue
			}
			role, _ := entry["role"].(string)
			_, hasToolCalls := entry["tool_calls"]
			if !needsReasoningContent(model, role, hasToolCalls) {
				continue
			}
			if rc, ok := entry["reasoning_content"].(string); ok && rc != "" {
				continue // 已有非空内容不覆盖
			}
			entry["reasoning_content"] = " "
			injected = true
		}
		if injected {
			changed = true
		}
	}

	if raw, ok := payload["tools"].([]any); ok {
		cleaned := make([]any, 0, len(raw))
		for _, item := range raw {
			if hasFunctionName(item) {
				cleaned = append(cleaned, item)
			}
		}
		if len(cleaned) != len(raw) {
			changed = true
		}
		const maxTools = 128
		truncated := false
		if len(cleaned) > maxTools {
			cleaned = cleaned[:maxTools]
			truncated = true
		}
		switch {
		case len(cleaned) == 0:
			delete(payload, "tools")
		default:
			payload["tools"] = cleaned
		}
		if truncated {
			changed = true
		}
	}

	// 工具调用参数兜底（借鉴 cc-switch json_canonical 规则）：
	// 空/纯空白 arguments 会被 Minimax 等严格上游以 400
	// invalid function arguments json string 拒绝整个请求；
	// 宽松上游（OpenAI/Kimi）虽容忍，但统一补 "{}" 零风险。
	if msgs, ok := payload["messages"].([]any); ok {
		fixed := false
		for _, m := range msgs {
			entry, ok := m.(map[string]any)
			if !ok {
				continue
			}
			calls, ok := entry["tool_calls"].([]any)
			if !ok {
				continue
			}
			for _, c := range calls {
				call, ok := c.(map[string]any)
				if !ok {
					continue
				}
				fn, ok := call["function"].(map[string]any)
				if !ok {
					continue
				}
				if a, ok := fn["arguments"].(string); ok && strings.TrimSpace(a) == "" {
					fn["arguments"] = "{}"
					fixed = true
				}
			}
		}
		if fixed {
			changed = true
		}
	}

	if injectCache && model != "" {
		// retention 是上游网关字段，各模型通吃；cache_control 仅 Anthropic
		// 系翻译层接受，GLM/Zhipu 系模型会拒收，按前缀黑名单跳过。
		if payload["prompt_cache_retention"] != "24h" {
			payload["prompt_cache_retention"] = "24h"
			changed = true
		}
		if cc, ok := payload["cache_control"].(map[string]any); (!ok || cc["type"] != "ephemeral") && !rejectsCacheControl(model) {
			// 断点预算管理（借鉴 cc-switch cache_injector）：Anthropic 系
			// 上游最多接受 4 个 cache_control 断点，客户端自带标记超限时
			// 不再叠加注入，避免整个请求被上游判非法。
			if countCacheControl(payload) < maxCacheControlBreakpoints {
				payload["cache_control"] = map[string]any{"type": "ephemeral", "ttl": "1h"}
				changed = true
			}
		}
	}
	return changed
}

// maxCacheControlBreakpoints Anthropic 系上游允许的 cache_control 断点上限。
const maxCacheControlBreakpoints = 4

// countCacheControl 递归统计 payload 中已存在的 cache_control 标记数，
// 供断点预算管理判断是否还能叠加注入（借鉴 cc-switch cache_injector）。
func countCacheControl(v any) int {
	switch t := v.(type) {
	case map[string]any:
		n := 0
		for k, child := range t {
			if k == "cache_control" {
				n++
			}
			n += countCacheControl(child)
		}
		return n
	case []any:
		n := 0
		for _, child := range t {
			n += countCacheControl(child)
		}
		return n
	default:
		return 0
	}
}

func hasFunctionName(item any) bool {
	obj, ok := item.(map[string]any)
	if !ok {
		return true // 形状不明的条目不误删
	}
	typ, _ := obj["type"].(string)
	if typ != "" && typ != "function" {
		return true // 非 function 型工具（自定义工具等）不做检查
	}
	// Responses API（Codex /v1/responses）的 function 工具是扁平形态：
	// name/description/parameters 直接在顶层，没有 function 包装对象。
	// 这里必须先认扁平 name，否则 Codex 的全部工具会被当成畸形剥光。
	if name, ok := obj["name"].(string); ok && strings.TrimSpace(name) != "" {
		return true
	}
	fn, ok := obj["function"].(map[string]any)
	if !ok {
		return false // chat 形态却缺 function 对象：上游必拒
	}
	name, _ := fn["name"].(string)
	return strings.TrimSpace(name) != ""
}

// glmZhipuRe 是拒绝 cache_control 字段的模型前缀黑名单。
var glmZhipuRe = regexp.MustCompile(`(?i)^(glm|zhipu|z-ai|zai)`)

// needsReasoningContent 判断该消息是否需要补 reasoning_content 占位：
//   - kimi 系：只要求带 tool_calls 的 assistant 消息（9router 实测规则）
//   - deepseek / minimax 系：所有 assistant 消息
func needsReasoningContent(model, role string, hasToolCalls bool) bool {
	if role != "assistant" {
		return false
	}
	lower := strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.HasPrefix(lower, "kimi"):
		return hasToolCalls
	case strings.Contains(lower, "deepseek"), strings.Contains(lower, "minimax"):
		return true
	}
	return false
}

func rejectsCacheControl(model string) bool {
	return glmZhipuRe.MatchString(strings.TrimSpace(model))
}

// housekeepingRe 匹配代理管家流量的特征词：配额探测类请求。
var housekeepingRe = regexp.MustCompile(`(?i)(quota|配额|usage check|额度检查|rate.?limit check)`)

// tryLocalHousekeeping 本地应答代理管家流量（如客户端的配额探测）：
// 这类请求不产生用户价值却烧真实免费配额。识别条件刻意保守——非流式、
// max_tokens≤48、消息极短、命中特征词三者同时满足才拦截，宁可漏放不可
// 误杀真实请求。enabled 由 PROXY_LOCAL_MOCKS 控制（默认开），每次拦截都有日志。
func tryLocalHousekeeping(enabled bool, path string, stream bool, payload map[string]any) ([]byte, bool) {
	if !enabled || stream || !strings.HasSuffix(path, "/v1/chat/completions") {
		return nil, false
	}
	mt := 0
	if v, ok := payload["max_tokens"].(float64); ok {
		mt = int(v)
	} else if v, ok := payload["max_completion_tokens"].(float64); ok {
		mt = int(v)
	}
	if mt > 48 {
		return nil, false
	}
	messages, ok := payload["messages"].([]any)
	if !ok || len(messages) == 0 || len(messages) > 2 {
		return nil, false
	}
	var text strings.Builder
	for _, m := range messages {
		entry, ok := m.(map[string]any)
		if !ok {
			return nil, false
		}
		if role, _ := entry["role"].(string); role != "user" && role != "system" {
			return nil, false
		}
		switch content := entry["content"].(type) {
		case string:
			text.WriteString(content)
			text.WriteString("\n")
		case []any: // 多模态内容块：出现即放弃判定
			return nil, false
		default:
			return nil, false
		}
	}
	joined := text.String()
	if len(joined) > 400 || !housekeepingRe.MatchString(joined) {
		return nil, false
	}
	log.Printf("[管家] 识别为配额探测请求，本地应答（节省一次上游调用）")
	model, _ := payload["model"].(string)
	response := map[string]any{
		"id":      "chatcmpl-local-housekeeping",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": "Quota check passed."},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{
			"prompt_tokens":     0,
			"completion_tokens": 4,
			"total_tokens":      4,
		},
	}
	raw, err := json.Marshal(response)
	if err != nil {
		return nil, false
	}
	return raw, true
}
