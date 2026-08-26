package main

import (
	"bytes"
	"encoding/json"
	"time"
)

// SSE 分块卫生：直通不再裸转发。
//
// 上游的 SSE 流并不总是干净的（9router 项目多年兼容性经验的移植）：
//   - 会在 data 行里夹带 HTML 错误页片段——裸转发会把它们喂给客户端造成
//     诡异解析错误；
//   - 会发出 delta.tool_calls 空数组——@ai-sdk 系客户端会误判为真实工具
//     触发、提前终止 reasoning；
//   - chat chunk 可能缺 object/created 字段，严格客户端会拒收。
//
// 处理原则：
//   - 只清洗 data: 行；注释/event:/id: 等行原样透传；
//   - [DONE] 哨兵原样透传；超长行走安全阀原样透传；
//   - 解析失败的 data 行整行静默丢弃并计数入日志；
//   - 不动终止语义：缺 [DONE] 时维持既有"静默干净关闭"设计——那是截断
//     信号，补发反而会让客户端把残次品当完整回复。

const (
	sseMaxLineBytes    = 1 << 20   // 单行超过 1MB：放弃逐行处理，整体透传（安全阀）
	ssePatchValveBytes = 256 << 10 // 超过 256KB 的合法 data 行不做结构修补（CPU 阀）
)

type sseSanitizer struct {
	pending   []byte // 跨读取边界的半行缓冲
	dropped   int    // 已丢弃的无效 data 行数（供日志）
	oversized bool   // 正处于超长行透传态
}

func newSSESanitizer() *sseSanitizer { return &sseSanitizer{} }

// feed 输入一个上游读取分块，返回应当转发给客户端的字节。
// 半行会缓存到下一个分块拼齐再处理；SSE 事件以 \n 收尾，延迟不超过一个事件。
func (s *sseSanitizer) feed(chunk []byte) []byte {
	var out []byte
	data := chunk
	for {
		idx := bytes.IndexByte(data, '\n')
		if idx < 0 {
			break
		}
		line := data[:idx]
		data = data[idx+1:]
		if len(s.pending) > 0 {
			line = append(s.pending, line...)
			s.pending = s.pending[:0]
		}
		if s.oversized {
			// 超长行在此前的 feed 中已按原始字节放行，这里只收它的结尾。
			s.oversized = false
			out = appendSSELine(out, line)
			continue
		}
		if len(line) > sseMaxLineBytes {
			out = appendSSELine(out, line)
			continue
		}
		emitted, keep := s.sanitizeLine(line)
		if !keep {
			continue
		}
		out = appendSSELine(out, emitted)
	}
	if len(data) > 0 {
		s.pending = append(s.pending, data...)
		if len(s.pending) > sseMaxLineBytes && !s.oversized {
			// 半行本身已超限：整体放行进入超长行透传态，直到遇到换行。
			s.oversized = true
			out = append(out, s.pending...)
			s.pending = s.pending[:0]
		}
	}
	return out
}

// flush 在流结束时调用：把残留半行原样交还（上游最后一行可能不带换行），
// 并汇报丢弃统计。
func (s *sseSanitizer) flush() []byte {
	if len(s.pending) == 0 {
		return nil
	}
	tail := s.pending
	s.pending = nil
	return tail
}

func appendSSELine(dst, line []byte) []byte {
	dst = append(dst, line...)
	return append(dst, '\n')
}

// sanitizeLine 清洗单行。keep=false 表示该行应被静默丢弃。
// 返回的 emitted 不含换行符，由调用方补齐。
func (s *sseSanitizer) sanitizeLine(line []byte) ([]byte, bool) {
	trimmed := bytes.TrimRight(line, "\r")
	if !bytes.HasPrefix(trimmed, []byte("data:")) {
		return trimmed, true // 注释心跳 / event: / id: / 空行：原样
	}
	payload := bytes.TrimSpace(trimmed[len("data:"):])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return trimmed, true
	}
	if len(payload) > ssePatchValveBytes {
		return trimmed, true // CPU 安全阀：超大行只透传不修补
	}
	if !json.Valid(payload) {
		s.dropped++
		return nil, false // 上游夹带的 HTML 错误页等垃圾：整行丢弃
	}
	needsPatch := !bytes.Contains(payload, []byte(`"object"`)) ||
		bytes.Contains(payload, []byte(`"tool_calls"`))
	if !needsPatch {
		return trimmed, true
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		s.dropped++
		return nil, false
	}
	patched := false
	if _, hasChoices := obj["choices"]; hasChoices {
		if _, ok := obj["object"]; !ok {
			obj["object"] = "chat.completion.chunk"
			patched = true
		}
		if _, ok := obj["created"]; !ok {
			obj["created"] = time.Now().Unix()
			patched = true
		}
	}
	if rawChoices, ok := obj["choices"].([]any); ok {
		for _, rawChoice := range rawChoices {
			choice, ok := rawChoice.(map[string]any)
			if !ok {
				continue
			}
			delta, ok := choice["delta"].(map[string]any)
			if !ok {
				continue
			}
			if toolCalls, ok := delta["tool_calls"].([]any); ok && len(toolCalls) == 0 {
				delete(delta, "tool_calls")
				patched = true
			}
		}
	}
	if !patched {
		return trimmed, true
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return trimmed, true // 重编码失败：宁原样透传不丢数据
	}
	return append([]byte("data: "), out...), true
}

// sanitizeSSEBytes 对一段完整 SSE 缓冲做一次性清洗（吸收模式路径用）。
func sanitizeSSEBytes(body []byte) []byte {
	s := newSSESanitizer()
	out := s.feed(body)
	if tail := s.flush(); len(tail) > 0 {
		out = append(out, tail...)
	}
	return out
}
