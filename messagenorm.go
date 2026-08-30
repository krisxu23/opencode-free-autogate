package main

import (
	"encoding/json"
	"strings"
)

// 消息归一化/修复管线（借鉴 kiro converters_core.build_kiro_payload）：
// 不同客户端（Codex/Cline/DSH）发送的会话历史格式各有特点，部分上游对
// 历史格式要求严格（必须 user 开头、必须交替、无孤儿 toolResult、无相邻
// 同角色），不修复会 400。此处实现 6 步管线，在请求转发前修复：
//
//  1. ensure_first_message_is_user  — 首个非 user 消息前补合成 user；
//  2. normalize_message_roles       — 未知角色（developer/system）归一化；
//  3. merge_adjacent_messages       — 合并相邻同角色消息；
//  4. ensure_alternating_roles      — 连续 user 间补空 assistant 占位；
//  5. ensure_assistant_before_tool_results — 孤儿 toolResult 转文本并入相邻 user；
//  6. strip_all_tool_content        — 无工具定义时 tool_calls 转文本保留上下文。
//
// 开关：PROXY_NORMALIZE_MESSAGES=1 开启（默认关——opencode 上游对格式
// 宽松，仅在遇到严格上游/客户端格式怪异时按需打开）。
// 仅在确有改动时重新序列化；无改动零成本。

// normalizeMessages 就地归一化 payload["messages"]，返回是否有改动。
func normalizeMessages(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	raw, ok := payload["messages"].([]any)
	if !ok || len(raw) == 0 {
		return false
	}
	messages := make([]*normMsg, 0, len(raw))
	for _, m := range raw {
		entry, ok := m.(map[string]any)
		if !ok {
			continue
		}
		messages = append(messages, &normMsg{entry: entry, role: roleOf(entry)})
	}
	if len(messages) == 0 {
		return false
	}
	changed := false

	// 6. 无工具定义时 tool_calls 转文本（保留上下文而非丢内容）。
	if _, hasTools := payload["tools"]; !hasTools {
		for _, m := range messages {
			if _, hasCalls := m.entry["tool_calls"]; hasCalls && m.role == "assistant" {
				m.entry["content"] = toolCallsText(m.entry)
				delete(m.entry, "tool_calls")
				changed = true
			}
		}
	}

	// 2. 未知角色归一化到 user。
	for _, m := range messages {
		if m.role != "user" && m.role != "assistant" && m.role != "tool" {
			m.entry["role"] = "user"
			m.role = "user"
			changed = true
		}
	}

	// 1. 首个非 user 消息前补合成 user。
	if messages[0].role != "user" {
		first := map[string]any{"role": "user", "content": "(empty placeholder)"}
		messages = append([]*normMsg{{entry: first, role: "user"}}, messages...)
		changed = true
	}

	// 5. 孤儿 toolResult（无前驱 assistant 的 tool_use 配对）转文本并入相邻 user。
	changed = normalizeOrphanToolResults(messages) || changed

	// 3. 合并相邻同角色（tool 并入前一条 user，其余按 role 合并）。
	var merged []*normMsg
	for _, m := range messages {
		if len(merged) > 0 && merged[len(merged)-1].role == m.role {
			last := merged[len(merged)-1]
			lastText := messageText(last.entry)
			curText := messageText(m.entry)
			if lastText != "" || curText != "" {
				last.entry["content"] = strings.TrimSpace(lastText + "\n" + curText)
				if len(curText) > 0 {
					changed = true
				}
			}
			continue
		}
		merged = append(merged, m)
	}
	messages = merged

	// 4. 连续 user 间补空 assistant 占位（交替结构）。
	var alternating []*normMsg
	prevRole := ""
	for _, m := range messages {
		if m.role == "user" && prevRole == "user" {
			placeholder := map[string]any{"role": "assistant", "content": "(empty placeholder)"}
			alternating = append(alternating, &normMsg{entry: placeholder, role: "assistant"})
			changed = true
		}
		alternating = append(alternating, m)
		prevRole = m.role
	}
	messages = alternating

	if !changed {
		return false
	}
	out := make([]any, 0, len(messages))
	for _, m := range messages {
		out = append(out, m.entry)
	}
	payload["messages"] = out
	return true
}

type normMsg struct {
	entry map[string]any
	role  string
}

func roleOf(entry map[string]any) string {
	role, _ := entry["role"].(string)
	return strings.ToLower(strings.TrimSpace(role))
}

func messageText(entry map[string]any) string {
	switch content := entry["content"].(type) {
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
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func toolCallsText(entry map[string]any) string {
	calls, ok := entry["tool_calls"].([]any)
	if !ok {
		return ""
	}
	var parts []string
	for _, c := range calls {
		call, ok := c.(map[string]any)
		if !ok {
			continue
		}
		fn, _ := call["function"].(map[string]any)
		if fn == nil {
			continue
		}
		name, _ := fn["name"].(string)
		args, _ := fn["arguments"].(string)
		if name != "" {
			parts = append(parts, name+": "+args)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "[tool call]\n" + strings.Join(parts, "\n")
}

// normalizeOrphanToolResults 修复列表开头的孤儿 toolResult（裁剪或客户端
// 怪异历史产生）：并入紧随其后的 user 消息，避免上游 400。返回新切片
// （开头孤儿已移除，内容并入后继 user）。
func normalizeOrphanToolResults(messages []*normMsg) bool {
	if len(messages) == 0 || messages[0].role != "tool" {
		return false
	}
	text := toolResultText(messages[0].entry)
	rest := messages[1:]
	for i := 0; i < len(rest); i++ {
		if rest[i].role != "user" {
			continue
		}
		if text != "" {
			switch content := rest[i].entry["content"].(type) {
			case string:
				rest[i].entry["content"] = "[trimmed tool result]\n" + text + "\n\n" + content
			case []any:
				rest[i].entry["content"] = append([]any{map[string]any{
					"type": "text", "text": "[trimmed tool result]\n" + text,
				}}, content...)
			}
		}
		break
	}
	copy(messages, rest)
	messages = messages[:len(rest)]
	_ = messages
	return true
}

// normalizeMessagesBytes 字节接口的薄包装（测试/外部调用用）。
func normalizeMessagesBytes(body []byte) ([]byte, bool) {
	payload := parseJSONObject(body)
	if payload == nil {
		return body, false
	}
	if !normalizeMessages(payload) {
		return body, false
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return body, false
	}
	return out, true
}
