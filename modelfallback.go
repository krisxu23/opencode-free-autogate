package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
)

// 模型级 fallback 链（借鉴 zen-proxy / freellmapi 的候选模型循环）：
// 竞速只换「出口」，不换「模型」——当限流是模型级（deepseek-v4-flash-free
// 在所有出口都被 FreeUsageLimitError）时，换出口毫无意义，必须换模型。
//
// dispatchModelChain 包装 g.dispatch：请求模型解析成候选链
// [原模型, ...cfg.modelFallbacks]，对每个候选重新跑完整竞速（出口级去重
// 在换模型时清空——换模型是新的上游行为，同一批出口值得重试）。换模型
// 成功后，把响应里的 model 字段回写为客户端请求的原始名，客户端无感知。
//
// 判定触发：dispatch 返回 errAllExitsFailed/errNoProxy（整池出口都败），
// 或最终可重试响应（429/5xx）——这正是"限流风暴"形态。普通非重试 4xx
// （模型名写错/参数错）不换模型直接透传，避免把客户端错误烧到每个模型。

// dispatchModelChain 是模型级 fallback 的调度入口；无 fallback 配置时
// 等价于直接调用 g.dispatch（零开销）。
func (g *gateway) dispatchModelChain(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	chain := g.resolveModelChain(request)
	if len(chain) <= 1 {
		return g.dispatch(ctx, request, trace)
	}
	originalModel := requestModelName(request)
	var lastResp *gatewayResponse
	var lastErr error
	for i, model := range chain {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		req := request
		if i > 0 {
			body, ok := rewriteBodyModel(request.body, model)
			if !ok {
				break
			}
			req.body = body
			trace.clearTried()
			trace.noteModelFallback(originalModel, model)
		}
		resp, err := g.dispatch(ctx, req, trace)
		if err != nil {
			if errors.Is(err, errAllExitsFailed) || errors.Is(err, errNoProxy) || errors.Is(err, errStreamTruncated) {
				lastErr = err
				continue // 换下一个模型重试
			}
			return nil, err // 请求取消/预算到期等终端错误，不再换模型
		}
		if retryableStatus(resp.status) && i < len(chain)-1 {
			// 模型级限流（429）或 5xx：还有候选模型，继续尝试。
			lastResp = resp
			log.Printf("[模型fallback]%s %s 返回 %d，切换下一个模型 %s", trace.tagString(), originalModel, resp.status, chain[i+1])
			continue
		}
		if i > 0 {
			resp = rewriteResponseModel(resp, originalModel, model)
		}
		return resp, nil
	}
	if lastResp != nil {
		return lastResp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errNoProxy
}

// resolveModelChain 从请求体模型 + 配置的 fallback 列表构造候选链，去重、
// 排除与当前模型重复的项、排除健康探测判定的 dead 模型。请求体解析失败时
// 返回单元素链（原样透传）。
func (g *gateway) resolveModelChain(request upstreamRequest) []string {
	current := requestModelName(request)
	if current == "" {
		return []string{current}
	}
	chain := []string{current}
	seen := map[string]struct{}{current: {}}
	for _, m := range g.cfg.modelFallbacks {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		if _, dup := seen[m]; dup {
			continue
		}
		if g.modelHealth != nil && !g.modelHealth.isAlive(m) {
			continue // 健康探测判定已下线的模型不进 fallback 链
		}
		seen[m] = struct{}{}
		chain = append(chain, m)
	}
	return chain
}

// requestModelName 从请求体提取 model 字段（空串表示无法解析）。
func requestModelName(request upstreamRequest) string {
	payload := parseJSONObject(request.body)
	if payload == nil {
		return ""
	}
	model, _ := payload["model"].(string)
	return strings.TrimSpace(model)
}

// rewriteBodyModel 把请求体 JSON 里的 model 字段改写为目标模型名。
// 返回 ok=false 表示请求体不是可改写的 JSON（原样交还）。
func rewriteBodyModel(body []byte, model string) ([]byte, bool) {
	payload := parseJSONObject(body)
	if payload == nil {
		return body, false
	}
	payload["model"] = model
	out, err := json.Marshal(payload)
	if err != nil {
		return body, false
	}
	return out, true
}

// rewriteResponseModel 把响应里的 model 字段回写为客户端请求的原始模型名：
// 非流式响应体直接 JSON 改字段；流式响应在 SSE 数据行里做精确替换。
// from 是上游实际使用的模型名（fallback 目标），只替换该值，避免误伤
// 正文里恰好出现的相同字段。
func rewriteResponseModel(resp *gatewayResponse, original, from string) *gatewayResponse {
	if resp == nil || original == "" {
		return resp
	}
	if resp.live == nil && len(resp.body) > 0 {
		payload := parseJSONObject(resp.body)
		if payload != nil {
			if model, _ := payload["model"].(string); model != "" {
				payload["model"] = original
				if out, err := json.Marshal(payload); err == nil {
					resp.body = out
				}
			}
		}
		return resp
	}
	if resp.live != nil {
		// 流式：包一层改写器，在转发时替换 model 字段。
		resp.live.response.Body = &modelRewriteBody{
			src:      resp.live.response.Body,
			original: original,
			from:     from,
		}
	}
	return resp
}

// modelRewriteBody 流式响应体包装器：把 SSE 数据行里的
// "model":"<from>" 替换为 "model":"<original>"。行缓冲保证跨 chunk 拆分
// 的 model 字段也能被完整识别；输出按需缓存，改写后的行长于调用方缓冲
// 时也不会溢出。
type modelRewriteBody struct {
	src      io.ReadCloser
	original string
	from     string
	lineBuf  []byte // 正在拼接的未完成行
	pending  []byte // 已改写待消费的输出
}

// maxModelRewriteLine 是单行缓冲上限：超过视为异常行，原样透传不清零
// 行缓冲（防上游无换行流撑爆内存）。
const maxModelRewriteLine = 1 << 20

func (m *modelRewriteBody) Read(p []byte) (int, error) {
	for len(m.pending) == 0 {
		chunk := make([]byte, 32<<10)
		n, err := m.src.Read(chunk)
		if n > 0 {
			m.lineBuf = append(m.lineBuf, chunk[:n]...)
			if len(m.lineBuf) > maxModelRewriteLine {
				// 异常超长行：原样放行并清空，避免永久卡在拼接。
				m.pending = append(m.pending, m.lineBuf...)
				m.lineBuf = nil
			}
			m.flushLines()
		}
		if err != nil {
			if len(m.lineBuf) > 0 {
				// 流结束：剩余半行也改写后交付。
				m.pending = append(m.pending, m.rewriteLine(m.lineBuf)...)
				m.lineBuf = nil
			}
			if len(m.pending) == 0 {
				return 0, err
			}
			break
		}
		if len(m.pending) > 0 {
			break
		}
	}
	n := copy(p, m.pending)
	m.pending = m.pending[n:]
	return n, nil
}

func (m *modelRewriteBody) Close() error {
	return m.src.Close()
}

// flushLines 把行缓冲里完整的行（含换行符）改写后移入输出缓冲，
// 保留最后一个不完整的行继续拼接。
func (m *modelRewriteBody) flushLines() {
	for {
		idx := bytes.IndexByte(m.lineBuf, '\n')
		if idx < 0 {
			return
		}
		line := m.lineBuf[:idx+1]
		if bytes.Contains(line, []byte(`"model"`)) {
			m.pending = append(m.pending, m.rewriteLine(line)...)
		} else {
			m.pending = append(m.pending, line...)
		}
		m.lineBuf = m.lineBuf[idx+1:]
	}
}

// rewriteLine 对单个 SSE 行做 model 字段替换：匹配 "model":"<from>" 模式，
// 把值替换为客户端原始名。找不到可替换的值时原样返回。
func (m *modelRewriteBody) rewriteLine(line []byte) []byte {
	marker := []byte(`"model":`)
	idx := bytes.Index(line, marker)
	if idx < 0 {
		return line
	}
	rest := line[idx+len(marker):]
	rest = bytes.TrimSpace(rest)
	if len(rest) == 0 || rest[0] != '"' {
		return line
	}
	// 找到闭合引号
	end := -1
	for i := 1; i < len(rest); i++ {
		if rest[i] == '"' && rest[i-1] != '\\' {
			end = i
			break
		}
	}
	if end < 0 {
		return line
	}
	if m.from != "" && string(rest[1:end]) != m.from {
		return line // 与目标上游名不符，不替换
	}
	replacement := []byte(`"model":"` + m.original + `"`)
	out := make([]byte, 0, len(line))
	out = append(out, line[:idx]...)
	out = append(out, replacement...)
	out = append(out, rest[end+1:]...)
	return out
}
