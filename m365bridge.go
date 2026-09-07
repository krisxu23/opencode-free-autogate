package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	m365pkg "github.com/krisxu23/opencode-free-autogate/m365"
)

// M365 内置供应商：账号库与 ChatHub 客户端常驻网关进程，不再需要单独
// 启动 M365-Copilot2API 进程并把 127.0.0.1:4141 当外部供应商挂进来。
var (
	m365Mu      sync.Mutex
	m365Engine  *m365pkg.Engine
	m365OpenErr error
	m365Path    string
)

// m365AccountsFile 是账号库文件名，落在 data/ 目录下（与 Cline 账号同级）。
const m365AccountsFile = ".m365-accounts.json"

// ensureM365 惰性打开 M365 引擎（首次调用时才建库/读盘）。
func ensureM365() (*m365pkg.Engine, error) {
	m365Mu.Lock()
	defer m365Mu.Unlock()
	if m365Engine != nil {
		return m365Engine, nil
	}
	if m365OpenErr != nil {
		return nil, m365OpenErr
	}
	path := clineResolveDataPath(m365AccountsFile)
	eng, err := m365pkg.Open(path)
	if err != nil {
		m365OpenErr = err
		log.Printf("[M365] 初始化失败: %v", err)
		return nil, err
	}
	m365Engine = eng
	m365Path = path
	log.Printf("[M365] 内置供应商已就绪，账号库 %s（%d 个账号）", path, len(eng.Accounts()))
	if !m365pkg.MasterKeyOK() {
		log.Printf("[M365] 警告：未设置 M365_MASTER_KEY，刷新令牌将明文落盘")
	}
	return eng, nil
}

// m365StorePath 返回账号库路径（未初始化时返回空串），供界面展示。
func m365StorePath() string {
	m365Mu.Lock()
	defer m365Mu.Unlock()
	return m365Path
}

// m365ModelIDs 返回 "m365/模型" 形式的对外模型名列表。
// 无可用账号时返回 nil：/v1/models 里不出现调不动的模型，
// 客户端（Codex/Cline）不会误选。
func m365ModelIDs() []string {
	eng, err := ensureM365()
	if err != nil || eng == nil {
		return nil
	}
	if len(eng.Accounts()) == 0 {
		return nil
	}
	models := eng.Models()
	out := make([]string, 0, len(models))
	for _, m := range models {
		out = append(out, m365SupplierID+"/"+m)
	}
	return out
}

// m365SupplierID 是内置 M365 供应商的保留前缀。
const m365SupplierID = "m365"

// m365BuiltinSupplier 构造内置供应商描述符：不进 config.json，
// 是否可用取决于账号库里有没有有效账号。
func m365BuiltinSupplier() (Supplier, bool) {
	eng, err := ensureM365()
	if err != nil || eng == nil {
		return Supplier{}, false
	}
	if len(eng.Accounts()) == 0 {
		return Supplier{}, false
	}
	return Supplier{
		ID:      m365SupplierID,
		Name:    "M365 Copilot",
		BaseURL: "internal://m365",
		Models:  eng.Models(),
		Enabled: true,
	}, true
}

// m365ChatRequest 是从 OpenAI 请求体里解出的最小必要字段。
type m365ChatRequest struct {
	Model    string            `json:"model"`
	Messages []m365ChatMessage `json:"messages"`
	Stream   bool              `json:"stream"`
}

type m365ChatMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

// m365MessageText 兼容 content 为字符串或多模态数组两种写法。
func (m m365ChatMessage) text() string {
	switch v := m.Content.(type) {
	case string:
		return v
	case []any:
		var parts []string
		for _, item := range v {
			if part, ok := item.(map[string]any); ok {
				if t, ok := part["text"].(string); ok {
					parts = append(parts, t)
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

// dispatchM365 把 OpenAI 请求交给内置 M365 引擎：流式返回 SSE 管道，
// 非流式直接返回完整 JSON。respModel 是回写给客户端的模型名（带前缀）。
func (g *gateway) dispatchM365(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	eng, err := ensureM365()
	if err != nil || eng == nil {
		return nil, errors.New("m365 内置供应商不可用")
	}
	var chat m365ChatRequest
	if err := json.Unmarshal(request.body, &chat); err != nil {
		return nil, errors.New("m365: 请求体解析失败")
	}
	if len(chat.Messages) == 0 {
		return nil, errors.New("m365: 请求体缺少 messages")
	}
	// 客户端看到的名字原样回写（保证 Codex/Cline 不错乱），
	// 剥掉前缀后的真名才是引擎认识的模型。
	respModel := chat.Model
	realModel := chat.Model
	if _, real, ok := SplitSupplierPrefix(respModel); ok {
		realModel = real
	}
	msgs := make([]m365pkg.ChatMessage, 0, len(chat.Messages))
	for _, m := range chat.Messages {
		msgs = append(msgs, m365pkg.ChatMessage{Role: m.Role, Content: m.text()})
	}
	req := m365pkg.ChatRequest{
		Model:      realModel,
		Messages:   msgs,
		SessionKey: request.session,
	}
	if trace != nil {
		trace.finalProxy = "m365"
		trace.addAttempt("m365")
	}

	// 单次请求上限：ChatHub 一轮长响应可能拖很久，用非流式超时兜底，
	// 避免客户端挂着等一个卡死的上游。
	timeout := g.cfg.nonStreamTimeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if !request.stream {
		res, err := eng.Chat(runCtx, req, nil, nil)
		if err != nil {
			return nil, err
		}
		body := m365FinishPayload(respModel, res.Text)
		return &gatewayResponse{
			status: http.StatusOK,
			header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
			body:   body,
		}, nil
	}
	return m365StreamResponse(runCtx, eng, req, respModel)
}

// m365StreamResponse 把 M365 增量文本转成 OpenAI SSE 流，包成 live 响应
// 交回既有写出管线（与上游透传流走同一条路径）。
func m365StreamResponse(ctx context.Context, eng *m365pkg.Engine, req m365pkg.ChatRequest, respModel string) (*gatewayResponse, error) {
	pr, pw := io.Pipe()
	created := time.Now().Unix()
	first := true
	send := func(text string, finish bool) error {
		if first {
			first = false
			if _, err := pw.Write(m365SSEChunk(created, respModel, "", false)); err != nil {
				return err
			}
		}
		return m365WriteChunk(pw, created, respModel, text, finish)
	}

	go func() {
		var err error
		defer func() {
			if err != nil {
				// 出错也要按 SSE 语义收尾：先发错误块再关闭，
				// 否则客户端会一直挂在半开的流上。
				_ = m365WriteErrorChunk(pw, err)
			}
			_ = pw.Close()
		}()
		_, err = eng.Chat(ctx, req, func(delta string) error {
			if delta == "" {
				return nil
			}
			return send(delta, false)
		}, nil)
		if err != nil {
			return
		}
		if first {
			// 上游一个增量都没给：补发一个空块，保证流有内容可交付。
			err = send("", false)
			if err != nil {
				return
			}
		}
		err = send("", true)
	}()

	res := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":  []string{"text/event-stream"},
			"Cache-Control": []string{"no-cache"},
		},
		Body: pr,
	}
	return &gatewayResponse{
		status: http.StatusOK,
		header: http.Header{"Content-Type": []string{"text/event-stream"}},
		live:   &liveResponse{response: res, cancel: func() { _ = pr.Close() }, headerAt: time.Now()},
	}, nil
}

func m365SSEChunk(created int64, model, text string, finish bool) []byte {
	chunk := map[string]any{
		"id":      "chatcmpl-m365",
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []any{map[string]any{
			"index": 0,
			"delta": map[string]any{},
		}},
	}
	if finish {
		chunk["choices"] = []any{map[string]any{
			"index":         0,
			"delta":         map[string]any{},
			"finish_reason": "stop",
		}}
	}
	body, _ := json.Marshal(chunk)
	return append(append([]byte("data: "), body...), '\n', '\n')
}

func m365WriteChunk(w io.Writer, created int64, model, text string, finish bool) error {
	delta := map[string]any{}
	if text != "" {
		delta["content"] = text
	}
	choice := map[string]any{"index": 0, "delta": delta}
	if finish {
		choice["finish_reason"] = "stop"
	}
	chunk := map[string]any{
		"id":      "chatcmpl-m365",
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []any{choice},
	}
	body, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	_, err = w.Write(append(append([]byte("data: "), body...), '\n', '\n'))
	if err != nil {
		return err
	}
	if finish {
		_, err = w.Write([]byte("data: [DONE]\n\n"))
	}
	return err
}

func m365WriteErrorChunk(w io.Writer, err error) error {
	payload, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": err.Error(),
			"type":    "upstream_error",
			"code":    "m365_upstream_error",
		},
	})
	_, werr := w.Write(append(append([]byte("data: "), payload...), '\n', '\n'))
	if werr != nil {
		return werr
	}
	_, werr = w.Write([]byte("data: [DONE]\n\n"))
	return werr
}

// m365FinishPayload 构造非流式响应体。
func m365FinishPayload(model, text string) []byte {
	body, _ := json.Marshal(map[string]any{
		"id":      "chatcmpl-m365",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": text},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{
			"prompt_tokens":     0,
			"completion_tokens": 0,
			"total_tokens":      0,
		},
	})
	return body
}
