package main

import (
	"encoding/json"
	"log"
	"strings"
)

// 载荷预检 + 历史自动裁剪（借鉴 kiro payload_guards）：上游对请求体大小有
// 隐式上限（Kiro 实测 ~615KB，超限返回误导性的 400 "Improperly formed
// request"）。发送前先序列化量大小，超限时从最旧消息成对裁剪历史
// （user/assistant 各一条），对齐到 user 边界，保留最近的上下文。
//
// 裁剪纪律（kiro 同款）：
//   - 至少保留最新一条 user 消息（请求本身不可丢）；
//   - 成对裁剪（user+assistant 一次），避免破坏消息交替结构；
//   - 裁剪后修复孤儿 tool_result（无前驱 assistant 的 tool_result 会被
//     严格上游 400 拒绝）——把孤儿 tool_result 转成文本并入相邻 user，
//     保留上下文的同时不破坏消息结构。

const (
	payloadGuardLimit = 512 << 10 // 默认载荷上限 512KB（PROXY_PAYLOAD_LIMIT 可覆盖）
	payloadGuardMin   = 8 << 10   // 裁剪后不得低于的最小载荷（防止极端裁剪）
)

// trimPayloadToLimit 就地裁剪 payload 的 messages 历史直到序列化大小
// 不超过 limit。返回是否发生了裁剪。
func trimPayloadToLimit(payload map[string]any, limit int) bool {
	if payload == nil || limit <= 0 {
		return false
	}
	messages, ok := payload["messages"].([]any)
	if !ok || len(messages) <= 1 {
		return false
	}
	changed := false
	for {
		size, err := json.Marshal(payload)
		if err != nil {
			return changed
		}
		if len(size) <= limit || len(messages) <= 1 {
			break
		}
		// 从最旧开始成对裁剪；只剩一条时不裁（请求本体不可丢）。
		if len(messages) > 1 {
			messages = messages[1:]
			changed = true
		}
		if len(messages) > 1 {
			messages = messages[1:]
		}
		payload["messages"] = messages
	}
	if !changed {
		return false
	}
	payload["messages"] = messages
	repairOrphanedToolResults(payload)
	log.Printf("[载荷] 历史超出 %d 字节，已裁剪至 %d 条消息（保留最新上下文）", limit, len(messages))
	return true
}

// repairOrphanedToolResults 修复裁剪后可能出现的孤儿 tool_result：
// 消息列表以 tool_result 开头（无前驱 assistant 的 tool_use 配对）时，
// 把它并入紧随其后的 user 消息（转为文本），避免上游 400。
func repairOrphanedToolResults(payload map[string]any) {
	messages, ok := payload["messages"].([]any)
	if !ok || len(messages) == 0 {
		return
	}
	first, ok := messages[0].(map[string]any)
	if !ok {
		return
	}
	role, _ := first["role"].(string)
	if role != "tool" {
		return
	}
	// 找后续第一条 user 消息，把孤儿 tool 内容并进去后删除这条 tool。
	orphanText := toolResultText(first)
	for i := 1; i < len(messages); i++ {
		entry, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}
		if r, _ := entry["role"].(string); r != "user" {
			continue
		}
		if orphanText != "" {
			switch content := entry["content"].(type) {
			case string:
				entry["content"] = "[trimmed tool result]\n" + orphanText + "\n\n" + content
			case []any:
				entry["content"] = append([]any{map[string]any{
					"type": "text", "text": "[trimmed tool result]\n" + orphanText,
				}}, content...)
			}
		}
		messages = append(messages[:0], messages[1:]...) // 删除孤儿 tool 消息
		payload["messages"] = messages
		return
	}
	// 没有后续 user：孤儿 tool 直接删除（无可并入处）。
	payload["messages"] = messages[1:]
}

// toolResultText 提取 tool 消息的文本内容（content 或 tool_result 字段）。
func toolResultText(msg map[string]any) string {
	switch content := msg["content"].(type) {
	case string:
		return strings.TrimSpace(content)
	case []any:
		var parts []string
		for _, block := range content {
			if b, ok := block.(map[string]any); ok {
				if text, ok := b["text"].(string); ok && text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n"))
	}
	if result, ok := msg["tool_result"].(string); ok {
		return strings.TrimSpace(result)
	}
	return ""
}
