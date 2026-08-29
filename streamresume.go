package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// 中流续写：透传流在中途夭折（未见终止标记）时，把已发给客户端的文本
// 作为 assistant prefill 重新请求一次，把补上的尾段无缝接在原流后面。
// 相比"静默干净关闭等客户端整体重试"（既有语义），续写不用重烧整段
// prompt——免费上游按会话计额度，重试一轮的成本远大于补尾。
// 设计借鉴 OmniRoute streamRecovery，按本网关形态做了裁剪：
//   - 仅 OpenAI chat 形态（/v1/chat/completions）：Responses/Anthropic 形态
//     的续写需要重建推理链，风险大于收益，维持静默关闭语义；
//   - 流中出现过 tool_calls 增量时绝不续写（半截工具调用无法拼接）；
//   - 补尾流整段缓冲后验完整、离线去重再转发，不做流式缝合——
//     续写只发生在断流之后，多一次缓冲的延迟客户端感知不到；
//   - 至多一次；预算不足 45 秒放弃；全部失败回落到静默干净关闭。

const (
	resumeSeamMax       = 2048 // 缝合去重的最大比对长度
	resumeSeamMin       = 8    // 尾部重叠达到该长度才按重叠剔除
	resumeRestartMin    = 64   // 模型无视 prefill 重启时，重复前缀至少有这么长才剔除
	resumeMinBudget     = 45 * time.Second
	resumeMaxBuffer     = 8 << 20 // 补尾流缓冲上限（8MB），超出按失败处理
	resumeTextHardLimit = 2 << 20 // 已发文本超过该长度不再续写（prefill 成本失控）
)

// streamResumer 持有一次流式响应的续写上下文。
type streamResumer struct {
	gateway  *gateway
	origin   upstreamRequest // 产生原流的请求（body 可改写后重发）
	trace    *requestTrace
	sawTools bool // 已发内容里出现过 tool_calls 增量：禁用续写
}

func newStreamResumer(g *gateway, origin *upstreamRequest, trace *requestTrace) *streamResumer {
	if g == nil || origin == nil {
		return nil
	}
	return &streamResumer{gateway: g, origin: *origin, trace: trace}
}

// eligible 判定当前流能否续写：chat 形态、开关开启、未见过工具调用、
// 已有可续的文本、总预算还够。
func (r *streamResumer) eligible(text string) bool {
	if r == nil || !r.gateway.cfg.streamResume || r.sawTools || text == "" {
		return false
	}
	if !strings.HasSuffix(r.origin.path, "/v1/chat/completions") {
		return false
	}
	if len(text) > resumeTextHardLimit {
		return false
	}
	if time.Until(r.origin.deadline) < resumeMinBudget {
		return false
	}
	return true
}

// resume 执行一次补尾并把结果直接写给客户端。返回是否成功交付了
// 带终止标记的完整补尾（调用方据此决定跳过静默关闭收尾）。
func (r *streamResumer) resume(w http.ResponseWriter, ctx context.Context, sentText string) bool {
	payload := parseJSONObject(r.origin.body)
	if payload == nil {
		return false
	}
	messages, ok := payload["messages"].([]any)
	if !ok {
		return false
	}
	model, _ := payload["model"].(string)
	// prefill 接缝：把已发文本作为 assistant 消息追加，上游模型大多会
	// 接着写；无视 prefill 重启的由 dedupeResumeText 剔除重复段。
	appended := append(append([]any(nil), messages...), map[string]any{
		"role":    "assistant",
		"content": sentText,
	})
	payload["messages"] = appended
	body, err := json.Marshal(payload)
	if err != nil {
		return false
	}

	deadline := time.Now().Add(resumeMinBudget)
	if r.origin.deadline.Before(deadline) {
		deadline = r.origin.deadline
	}
	request := r.origin
	request.body = body
	request.deadline = deadline
	log.Printf("[续写]%s 流中断于 %d 字节文本，追加 assistant prefill 补尾（出口:%s）",
		r.trace.tagString(), len(sentText), r.trace.finalProxy)
	resp, err := r.gateway.dispatch(ctx, request, r.trace)
	if err != nil {
		log.Printf("[续写]%s 补尾请求失败: %v", r.trace.tagString(), err)
		return false
	}
	if resp.live == nil {
		log.Printf("[续写]%s 补尾请求得到非流式响应 status=%d，放弃", r.trace.tagString(), resp.status)
		return false
	}
	data, rerr := resp.live.readAll(deadline)
	if rerr != nil {
		log.Printf("[续写]%s 补尾流读取失败: %v", r.trace.tagString(), rerr)
		return false
	}
	if r.gateway.cfg.sseHygiene {
		data = sanitizeSSEBytes(data)
	}
	if !streamComplete(data) {
		log.Printf("[续写]%s 补尾流仍不完整，放弃并回落静默关闭", r.trace.tagString())
		return false
	}
	newText, finishReason := extractStreamText(data)
	if usageObserver != nil {
		if m, us, ok := ExtractLastUsage(data); ok {
			usageObserver(m, us.PromptTokens, us.CompletionTokens, us.CachedTokens)
		}
	}
	remaining := dedupeResumeText(sentText, newText)
	if finishReason == "" {
		finishReason = "stop"
	}
	if err := writeResumeTail(w, model, remaining, finishReason); err != nil {
		return false
	}
	log.Printf("[续写]%s 补尾完成：剔除接缝 %d 字节，交付 %d 字节 + 终止标记 %q",
		r.trace.tagString(), len(newText)-len(remaining), len(remaining), finishReason)
	return true
}

// dedupeResumeText 从补尾文本中剔除与已发文本重复的接缝段。
// 两种形态（OmniRoute 同款经验）：
//   - 接缝重叠：模型从断点附近继续，开头重复了已发文本的尾部；
//   - 无视 prefill 重启：模型从头重写，补尾文本以已发文本的前缀开头。
//
// 其余情况（全新内容）原样返回。
func dedupeResumeText(sent, next string) string {
	if next == "" {
		return next
	}
	// 接缝重叠：找最大的 L ≤ resumeSeamMax 使 sent 的尾部等于 next 的前缀。
	maxSeam := len(sent)
	if maxSeam > resumeSeamMax {
		maxSeam = resumeSeamMax
	}
	if maxSeam > len(next) {
		maxSeam = len(next)
	}
	best := 0
	for l := maxSeam; l >= resumeSeamMin; l-- {
		if sent[len(sent)-l:] == next[:l] {
			best = l
			break
		}
	}
	if best > 0 {
		return next[best:]
	}
	// 重启形态：已发文本的前缀在补尾文本开头整段重现。
	limit := len(sent)
	if limit > resumeSeamMax {
		limit = resumeSeamMax
	}
	if limit > len(next) {
		limit = len(next)
	}
	if limit >= resumeRestartMin && sent[:limit] == next[:limit] {
		return next[limit:]
	}
	return next
}

// extractStreamText 从一段 chat 形态 SSE 原文中提取 choices[0].delta.content
// 拼接出的全文，并返回最后一个非空 finish_reason。
func extractStreamText(data []byte) (string, string) {
	var text strings.Builder
	finishReason := ""
	for _, raw := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(raw)
		if !strings.HasPrefix(trimmed, "data:") {
			continue
		}
		payload := strings.TrimSpace(trimmed[len("data:"):])
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason any `json:"finish_reason"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(payload), &chunk) != nil {
			continue
		}
		if len(chunk.Choices) > 0 {
			text.WriteString(chunk.Choices[0].Delta.Content)
			if chunk.Choices[0].FinishReason != nil {
				if s, ok := chunk.Choices[0].FinishReason.(string); ok && s != "" {
					finishReason = s
				}
			}
		}
	}
	return text.String(), finishReason
}

// writeResumeTail 把补尾文本合成一条 chat chunk、附上终止 chunk 与 [DONE]
// 写给客户端。首块之后客户端照常累计 delta.content，语义与自然收尾一致。
func writeResumeTail(w http.ResponseWriter, model, remaining, finishReason string) error {
	if remaining != "" {
		chunk, err := json.Marshal(map[string]any{
			"id":      "chatcmpl-resume",
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   model,
			"choices": []any{map[string]any{
				"index": 0,
				"delta": map[string]any{"content": remaining},
			}},
		})
		if err != nil {
			return err
		}
		if _, err := w.Write(append(append([]byte("data: "), chunk...), '\n', '\n')); err != nil {
			return err
		}
	}
	terminal, err := json.Marshal(map[string]any{
		"id":      "chatcmpl-resume",
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"delta":         map[string]any{},
			"finish_reason": finishReason,
		}},
	})
	if err != nil {
		return err
	}
	if _, err := w.Write(append(append([]byte("data: "), terminal...), '\n', '\n')); err != nil {
		return err
	}
	_, err = w.Write([]byte("data: [DONE]\n\n"))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return err
}
