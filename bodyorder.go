package main

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
)

// 出站请求体指纹整形：把顶层 JSON 键统一重排成原生客户端的构造序。
//
// 动机：卫生改写过的请求体会走 Go map 序列化（键名字母序），未改写的保留
// 客户端自己的键序——同一个网关对外呈现两种指纹，本身就是可识别特征；
// 且字母序与任何真实 CLI 的构造序都不一致。
//
// 键序依据（均为公开可查的真实来源，非猜测）：
//   - OpenAI chat 形态：@ai-sdk/openai-compatible v2.0.41 的 getArgs/doStream
//     对象字面量插入序（opencode CLI 即用该 SDK 访问 zen 端点）；
//   - Responses 形态：OmniRoute 项目 mitmproxy 抓包记录的 Codex CLI 字段序
//     （open-sse/config/cliFingerprints.ts）。
//
// 排列规则：命中前缀表的键按表序输出 → 未命中键按字母序追加 → 网关自己
// 注入的两个缓存字段垫底（中间件追加语义）。JSON 键序无语义，纯整形零风险。
// Anthropic /v1/messages 形态暂无可靠来源依据，维持透传不整形。

var chatBodyFieldOrder = []string{
	"model", "user",
	"max_tokens", "temperature", "top_p", "frequency_penalty", "presence_penalty",
	"response_format", "stop", "seed",
	"reasoning_effort", "verbosity",
	"messages",
	"tools", "tool_choice",
	"stream", "stream_options",
}

var responsesBodyFieldOrder = []string{
	"model", "stream",
	"input", "instructions",
	"store", "reasoning", "prompt_cache_key",
	"tools", "tool_choice",
	"include", "service_tier", "parallel_tool_calls", "metadata",
}

// bodyFieldTrailer 网关注入字段：永远排在最后，模拟"中间件事后附加"的位置。
var bodyFieldTrailer = []string{"prompt_cache_retention", "cache_control"}

// pickBodyOrder 按端点路径返回字段序表；nil 表示该形态不整形。
func pickBodyOrder(path string) []string {
	switch {
	case strings.HasSuffix(path, "/chat/completions"):
		return chatBodyFieldOrder
	case strings.Contains(path, "/responses"):
		return responsesBodyFieldOrder
	default:
		return nil
	}
}

// marshalOrderedBody 按给定顺序序列化顶层键。任何一个值无法序列化时整体
// 放弃（ok=false），调用方沿用原有字节——绝不因整形丢请求。
func marshalOrderedBody(payload map[string]any, order []string) ([]byte, bool) {
	if payload == nil {
		return nil, false
	}
	ordered := make([]string, 0, len(payload))
	seen := make(map[string]struct{}, len(payload))
	for _, key := range order {
		if _, ok := payload[key]; ok {
			ordered = append(ordered, key)
			seen[key] = struct{}{}
		}
	}
	extras := make([]string, 0, len(payload))
	for key := range payload {
		if _, ok := seen[key]; ok {
			continue
		}
		skip := false
		for _, trailer := range bodyFieldTrailer {
			if key == trailer {
				skip = true
				break
			}
		}
		if !skip {
			extras = append(extras, key)
		}
	}
	sort.Strings(extras)
	ordered = append(ordered, extras...)
	for _, key := range bodyFieldTrailer {
		if _, ok := payload[key]; ok {
			ordered = append(ordered, key)
		}
	}

	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	buf.WriteByte('{')
	first := true
	for _, key := range ordered {
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return nil, false
		}
		valJSON, err := json.Marshal(payload[key])
		if err != nil {
			return nil, false
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		buf.Write(keyJSON)
		buf.WriteByte(':')
		buf.Write(valJSON)
	}
	buf.WriteByte('}')
	return buf.Bytes(), true
}
