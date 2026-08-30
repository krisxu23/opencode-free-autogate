package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDedupeResumeText(t *testing.T) {
	cases := []struct {
		name string
		sent string
		next string
		want string
	}{
		{"无重叠的全新内容", "Hello wor", "ld! done", "ld! done"},
		{"短于最小重叠阈值的接缝不剔除", "hello wor", "world peace", "world peace"},
		{"接缝重叠达到阈值即剔除", "hello world, this is a", "this is a test", " test"},
		{"长接缝重叠", strings.Repeat("a", 100), strings.Repeat("a", 40) + "tail", "tail"},
		{"无视 prefill 重启", strings.Repeat("x", 100), strings.Repeat("x", 100) + "rest", "rest"},
		{"重启不足最短重复长度不剔除", "short", "short continuation", "short continuation"},
		{"空补尾", "abc", "", ""},
	}
	for _, c := range cases {
		if got := dedupeResumeText(c.sent, c.next); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestExtractStreamText(t *testing.T) {
	data := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"B\"},\"finish_reason\":\"length\"}]}\n\n" +
		"data: [DONE]\n\n")
	text, finish := extractStreamText(data)
	if text != "AB" {
		t.Fatalf("text=%q", text)
	}
	if finish != "length" {
		t.Fatalf("finish=%q", finish)
	}
}

func TestStreamResumerEligibility(t *testing.T) {
	g := newGateway(config{streamResume: true})
	origin := &upstreamRequest{
		path:     "/v1/chat/completions",
		deadline: time.Now().Add(time.Minute),
		body:     []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
	}
	r := newStreamResumer(g, origin, newRequestTrace())
	if r == nil || !r.eligible("some text") {
		t.Fatal("合法 chat 请求应可续写")
	}
	if r.eligible("") {
		t.Fatal("无已发文本不应续写")
	}
	r.sawTools = true
	if r.eligible("some text") {
		t.Fatal("流中出现 tool_calls 后不应续写")
	}
	r.sawTools = false
	// newStreamResumer 会复制 origin，预算不足要重新构造才能反映到被测对象。
	origin.deadline = time.Now().Add(5 * time.Second)
	late := newStreamResumer(g, origin, newRequestTrace())
	if late.eligible("some text") {
		t.Fatal("预算不足不应续写")
	}
	origin.deadline = time.Now().Add(time.Minute)
	// 非 chat 形态不续写（resumer 内部持有 origin 副本，直接改它的副本）。
	r.origin.path = "/v1/responses"
	if r.eligible("some text") {
		t.Fatal("非 chat 形态不应续写")
	}
	r.origin.path = "/v1/chat/completions"
	g2 := newGateway(config{streamResume: false})
	r2 := newStreamResumer(g2, origin, newRequestTrace())
	if r2.eligible("some text") {
		t.Fatal("开关关闭不应续写")
	}
}

// 端到端：上游第一段流中途断掉（无终止标记），续写请求应携带 assistant
// prefill，且客户端最终收到拼接后的完整回复 + 终止标记。
func TestStreamResumeEndToEnd(t *testing.T) {
	var calls atomic.Int32
	var secondBody []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"Hello wor\"}}]}\n\n"))
			flusher := w.(http.Flusher)
			flusher.Flush()
			// 模拟上游中途掐断：不带任何终止标记直接断流。
			panic(http.ErrAbortHandler)
		}
		secondBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ld!\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 10 * time.Second,
		hardTimeout:      90 * time.Second,
		nonStreamTimeout: 90 * time.Second,
		streamIdle:       10 * time.Second,
		streamResume:     true,
		sseHygiene:       true,
	}
	application := &app{gateway: newGateway(cfg)}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	trace := newRequestTrace()
	recorder := httptest.NewRecorder()

	response, guard, err := application.handlePost(recorder, request, "/v1/chat/completions", trace.start.Add(cfg.hardTimeout), trace)
	if err != nil {
		t.Fatalf("handlePost: %v", err)
	}
	application.finish(recorder, request, trace, response, nil, guard)

	body := recorder.Body.String()
	if !strings.Contains(body, "Hello wor") {
		t.Fatalf("应包含第一段文本: %q", body)
	}
	if !strings.Contains(body, "ld!") {
		t.Fatalf("应包含续写补尾: %q", body)
	}
	if !strings.Contains(body, "\"finish_reason\":\"stop\"") || !strings.Contains(body, "[DONE]") {
		t.Fatalf("续写后应有终止标记: %q", body)
	}
	if calls.Load() != 2 {
		t.Fatalf("上游应被请求两次，实际 %d", calls.Load())
	}
	var payload map[string]any
	if json.Unmarshal(secondBody, &payload) != nil {
		t.Fatalf("续写请求体非法: %q", secondBody)
	}
	messages, _ := payload["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("续写请求应带 assistant prefill: %q", secondBody)
	}
	prefill, _ := messages[1].(map[string]any)
	if role, _ := prefill["role"].(string); role != "assistant" {
		t.Fatalf("prefill 角色应为 assistant: %v", prefill)
	}
	if content, _ := prefill["content"].(string); content != "Hello wor" {
		t.Fatalf("prefill 内容应为已发文本: %v", prefill)
	}
}

// 慢流看门狗：单窗口违规后恢复流量——不应掐断（首句流得慢的合法流）。
func TestStallWatchdogSparesRecoveringStream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"go\"}}]}\n\n"))
		flusher := w.(http.Flusher)
		flusher.Flush()
		// 第一窗口：涓流（违规 1）
		for i := 0; i < 8; i++ {
			time.Sleep(40 * time.Millisecond)
			_, _ = w.Write([]byte("x"))
			flusher.Flush()
		}
		// 恢复：正常速度补足健康窗口的字节量
		for i := 0; i < 10; i++ {
			time.Sleep(20 * time.Millisecond)
			_, _ = w.Write([]byte("0123456789012345678901234567890123456789"))
			flusher.Flush()
		}
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
		flusher.Flush()
		time.Sleep(200 * time.Millisecond)
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamIdle:       30 * time.Second,
		stallWindow:      300 * time.Millisecond,
		stallMinBytes:    64,
	}
	application := &app{gateway: newGateway(cfg)}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	trace := newRequestTrace()
	recorder := httptest.NewRecorder()

	response, guard, err := application.handlePost(recorder, request, "/v1/chat/completions", trace.start.Add(cfg.hardTimeout), trace)
	if err != nil {
		t.Fatalf("handlePost: %v", err)
	}
	application.finish(recorder, request, trace, response, nil, guard)
	body := recorder.Body.String()
	if !strings.Contains(body, "[DONE]") {
		t.Fatalf("恢复流不应被看门狗掐断: %q", body)
	}
	if !strings.Contains(body, "go") {
		t.Fatalf("首段数据应已透传: %q", body)
	}
}

// 慢流看门狗：还在滴但基本卡死的流应被掐断，而不是等到空闲超时。
func TestStallWatchdogKillsDribblingStream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"go\"}}]}\n\n"))
		flusher := w.(http.Flusher)
		flusher.Flush()
		for i := 0; i < 400; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			_, _ = w.Write([]byte("x")) // 每滴 1 字节，远低于窗口阈值
			flusher.Flush()
			time.Sleep(40 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamIdle:       30 * time.Second, // 空闲超时远大于看门狗窗口
		stallWindow:      300 * time.Millisecond,
		stallMinBytes:    64,
		streamResume:     false,
	}
	application := &app{gateway: newGateway(cfg)}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	trace := newRequestTrace()
	recorder := httptest.NewRecorder()

	started := time.Now()
	response, guard, err := application.handlePost(recorder, request, "/v1/chat/completions", trace.start.Add(cfg.hardTimeout), trace)
	if err != nil {
		t.Fatalf("handlePost: %v", err)
	}
	application.finish(recorder, request, trace, response, nil, guard)
	if elapsed := time.Since(started); elapsed > 15*time.Second {
		t.Fatalf("看门狗应在窗口期掐断慢流，实际耗时 %v", elapsed)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "go") {
		t.Fatalf("首段数据应已透传: %q", body)
	}
	if strings.Contains(body, "[DONE]") {
		t.Fatalf("被掐断的流不应出现终止标记: %q", body)
	}
}

// 非 SSE Content-Type 拦截：流式请求拿到 HTML 错误页时应换出口重试，
// 而不是把 HTML 喂给客户端。
func TestGarbageContentTypeRetriedOnNextExit(t *testing.T) {
	html := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html><body>oops</body></html>"))
	}))
	defer html.Close()

	sse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// 含真实内容块的完整流：空 delta+终止标记会被空响应验证门拦下。
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer sse.Close()

	// HTML 端点伪装成一个自定义出口节点：第一个出口返回 HTML 垃圾，
	// 直连兜底（真正的 SSE 端点）交付完整流。
	htmlURL := strings.TrimPrefix(html.URL, "http://")
	cfg := config{
		project:          projectSpec{upstream: sse.URL, directFallback: true},
		proxyMode:        "custom",
		customProxies:    htmlURL,
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
	}
	application := &app{gateway: newGateway(cfg)}
	application.gateway.markManual(htmlURL)
	slot, err := slotFromRawURL("http://" + htmlURL)
	if err != nil {
		t.Fatalf("slot: %v", err)
	}
	application.gateway.addSlot(slot, true)

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	trace := newRequestTrace()
	recorder := httptest.NewRecorder()
	response, guard, err := application.handlePost(recorder, request, "/v1/chat/completions", trace.start.Add(cfg.hardTimeout), trace)
	if err != nil {
		t.Fatalf("handlePost: %v", err)
	}
	application.finish(recorder, request, trace, response, nil, guard)
	body := recorder.Body.String()
	if strings.Contains(body, "<html>") {
		t.Fatalf("HTML 垃圾不应到达客户端: %q", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatalf("应从直连兜底拿到完整流: %q", body)
	}
}
