package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestHoldbackBufferBuffersUntilCommit 验证：窗口未到期且无终止标记时，
// push 不返回任何待转发字节（仍在缓冲期）。
func TestHoldbackBufferBuffersUntilCommit(t *testing.T) {
	hb := newHoldbackBuffer(500*time.Millisecond, 65536)
	toFlush, complete := hb.push([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
	if complete {
		t.Fatal("无终止标记不应判定完整")
	}
	if len(toFlush) != 0 {
		t.Fatalf("窗口未到期不应提交，得到 %d 字节", len(toFlush))
	}
}

// TestHoldbackBufferCompleteMarker 验证：缓冲内出现终止标记时立即完整交付。
func TestHoldbackBufferCompleteMarker(t *testing.T) {
	hb := newHoldbackBuffer(10*time.Second, 65536)
	toFlush, complete := hb.push([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"))
	if !complete {
		t.Fatal("含 finish_reason 终止标记应判定完整")
	}
	if len(toFlush) == 0 {
		t.Fatal("完整时应交付全部缓冲")
	}
}

// TestHoldbackBufferWindowExpiry 验证：超过时间窗口后提交缓冲。
// 窗口从第一次 push 开始计时，因此先推一段启动计时，睡过窗口再推。
func TestHoldbackBufferWindowExpiry(t *testing.T) {
	hb := newHoldbackBuffer(10*time.Millisecond, 65536)
	toFlush, complete := hb.push([]byte("data: a\n\n"))
	if complete || len(toFlush) != 0 {
		t.Fatal("首次 push 仅启动计时，不应提交")
	}
	time.Sleep(15 * time.Millisecond)
	toFlush, complete = hb.push([]byte("data: b\n\n"))
	if complete {
		t.Fatal("无终止标记不应完整")
	}
	if len(toFlush) == 0 {
		t.Fatal("窗口到期后应提交缓冲")
	}
	if !hb.committed {
		t.Fatal("提交后 committed 应为 true")
	}
}

// TestHoldbackBufferMaxBytes 验证：缓冲超过字节上限后提交。
func TestHoldbackBufferMaxBytes(t *testing.T) {
	hb := newHoldbackBuffer(time.Second, 64)
	toFlush, complete := hb.push([]byte(strings.Repeat("x", 32)))
	if len(toFlush) != 0 {
		t.Fatal("未达上限不应提交")
	}
	toFlush, complete = hb.push([]byte(strings.Repeat("y", 32)))
	if complete {
		t.Fatal("无终止标记不应完整")
	}
	if len(toFlush) < 64 {
		t.Fatalf("达到字节上限应提交全部缓冲，得到 %d 字节", len(toFlush))
	}
}

// TestWriteWithHoldbackSilentRetry 端到端：上游第一段流提交前截断（无终止
// 标记），holdback 静默换道重发，客户端最终收到完整回复且无中间残留。
func TestWriteWithHoldbackSilentRetry(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if calls.Add(1) == 1 {
			// 首次：发了几个真实数据块后立即掐断（无终止标记）。
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))
			flusher := w.(http.Flusher)
			flusher.Flush()
			panic(http.ErrAbortHandler)
		}
		// 第二次：完整回复。
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"full\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamIdle:       30 * time.Second,
		holdbackWindow:   500 * time.Millisecond,
		holdbackBytes:    65536,
		holdbackRetries:  2,
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
	if !strings.Contains(body, "full") {
		t.Fatalf("应收到重发后的完整回复: %q", body)
	}
	if strings.Contains(body, "partial") {
		t.Fatalf("首次截断的内容不应到达客户端（静默重发）: %q", body)
	}
	if !strings.Contains(body, "\"finish_reason\":\"stop\"") || !strings.Contains(body, "[DONE]") {
		t.Fatalf("应有终止标记: %q", body)
	}
	if calls.Load() != 2 {
		t.Fatalf("上游应被请求两次，实际 %d", calls.Load())
	}
}

// TestWriteWithHoldbackDisabledPassthrough 验证：holdback 关闭时走原透传路径，
// 截断内容直接到达客户端（既有语义不变）。
func TestWriteWithHoldbackDisabledPassthrough(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))
		flusher := w.(http.Flusher)
		flusher.Flush()
		panic(http.ErrAbortHandler)
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamIdle:       30 * time.Second,
		holdbackWindow:   0, // 关闭
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
	if !strings.Contains(body, "partial") {
		t.Fatalf("holdback 关闭时截断内容应直接透传: %q", body)
	}
}

// TestHoldbackConfigGating 验证 holdbackConfig 的启用条件。
func TestHoldbackConfigGating(t *testing.T) {
	off := config{holdbackWindow: 0, holdbackBytes: 65536}.holdbackConfig()
	if off.enabled {
		t.Fatal("窗口为 0 时不应启用 holdback")
	}
	on := config{holdbackWindow: time.Second, holdbackBytes: 65536}.holdbackConfig()
	if !on.enabled {
		t.Fatal("窗口与字节上限都为正时应启用")
	}
	zeroBytes := config{holdbackWindow: time.Second, holdbackBytes: 0}.holdbackConfig()
	if zeroBytes.enabled {
		t.Fatal("字节上限为 0 时不应启用 holdback")
	}
}

// TestHoldbackNilReceiverFallsBack 验证：nil holdback 直接走 streamResponse，
// 不 panic（writeCommittedStream / writeGatewayResponse 都靠这个兜底）。
func TestHoldbackNilReceiverFallsBack(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamIdle:       30 * time.Second,
		holdbackWindow:   0,
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
	if !strings.Contains(recorder.Body.String(), "[DONE]") {
		t.Fatalf("nil holdback 应走原透传路径拿到完整流: %q", recorder.Body.String())
	}
}

// TestWriteWithHoldbackRetriesExhaustedFallback 验证：静默重发次数耗尽后
// 把已缓冲内容交给 streamResponse 走既有截断收尾（客户端看到已发部分 + 干净关闭）。
func TestWriteWithHoldbackRetriesExhaustedFallback(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		calls.Add(1)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))
		flusher := w.(http.Flusher)
		flusher.Flush()
		panic(http.ErrAbortHandler)
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamIdle:       30 * time.Second,
		holdbackWindow:   500 * time.Millisecond,
		holdbackBytes:    65536,
		holdbackRetries:  1,
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
	if calls.Load() != 2 {
		t.Fatalf("重试耗尽应请求两次（首次+1 次重发），实际 %d", calls.Load())
	}
}

// TestOverloadMarkers 验证过载标记识别（FCC failure_policy _OVERLOAD_MARKERS）。
func TestOverloadMarkers(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"显式 overloaded", `{"error":"provider is overloaded"}`, true},
		{"overload", `{"error":"overload"}`, true},
		{"resource exhausted", `{"error":"Resource Exhausted"}`, true},
		{"resourceexhausted 连写", `{"error":"RESOURCEEXHAUSTED"}`, true},
		{"capacity", `{"error":"model at capacity"}`, true},
		{"limit reached", `{"error":"limit reached"}`, true},
		{"busy", `{"error":"server busy"}`, true},
		{"普通 5xx 无标记", `{"error":"internal server error"}`, false},
		{"额度错误不算过载", `{"error":"FreeUsageLimitError"}`, false},
	}
	for _, c := range cases {
		if got := overloadedUpstream([]byte(c.body)); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// TestClassifyUpstreamFailureOverloadBench 验证：5xx+过载标记 → 推断短板凳；
// 普通 5xx 不坐板凳（交给竞速换出口）；429 不带恢复提示时按原有限流逻辑。
func TestClassifyUpstreamFailureOverloadBench(t *testing.T) {
	d, src := classifyUpstreamFailure(http.StatusServiceUnavailable, []byte(`{"error":"model overloaded"}`), "")
	if src != benchHeuristic || d <= 0 {
		t.Fatalf("过载 5xx 应坐推断短板凳，得到 d=%v src=%v", d, src)
	}
	d, src = classifyUpstreamFailure(http.StatusInternalServerError, []byte(`{"error":"internal error"}`), "")
	if src != benchNone || d != 0 {
		t.Fatalf("普通 5xx 不应坐板凳，得到 d=%v src=%v", d, src)
	}
	// 529 是典型的过载状态码，应坐板凳。
	d, src = classifyUpstreamFailure(529, []byte(`{"error":"provider overloaded"}`), "")
	if src != benchHeuristic || d <= 0 {
		t.Fatalf("529 过载应坐推断短板凳，得到 d=%v src=%v", d, src)
	}
	// 429 不带 Retry-After/额度标记：不属于过载分类，走原逻辑（不坐板凳）。
	d, src = classifyUpstreamFailure(http.StatusTooManyRequests, []byte(`{"error":"overloaded"}`), "")
	if src != benchNone || d != 0 {
		t.Fatalf("429 无恢复提示不应坐板凳，得到 d=%v src=%v", d, src)
	}
}

// TestCfFallbackAttemptModelPrefix 验证稳定锚点：模型名补 opencode/ 前缀、
// 认证头走 cfFallbackKey、流式响应正常返回。
func TestCfFallbackAttemptModelPrefix(t *testing.T) {
	var gotModel string
	var gotAuth string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		payload := parseJSONObject([]byte(mustReadBody(r)))
		if payload != nil {
			gotModel, _ = payload["model"].(string)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer worker.Close()

	g := newGateway(config{
		cfFallbackURL:    worker.URL,
		cfFallbackKey:    "sk_cf_test",
		firstByteTimeout: 5 * time.Second,
	})
	request := upstreamRequest{
		method:   http.MethodPost,
		path:     "/v1/chat/completions",
		headers:  http.Header{},
		body:     []byte(`{"model":"big-pickle","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
		stream:   true,
		deadline: time.Now().Add(10 * time.Second),
	}
	resp, ok := g.cfFallbackAttempt(context.Background(), request)
	if !ok {
		t.Fatal("CF Worker 应成功交付")
	}
	if gotModel != "opencode/big-pickle" {
		t.Fatalf("模型名应补 opencode/ 前缀，得到 %q", gotModel)
	}
	if gotAuth != "Bearer sk_cf_test" {
		t.Fatalf("认证头应为 Bearer sk_cf_test，得到 %q", gotAuth)
	}
	if resp.live == nil {
		t.Fatal("流式请求应返回 live 响应")
	}
}

// TestCfFallbackFailOpen 验证 fail-open：CF Worker 不可达/出错时返回 ok=false，
// 调用方按既有兜底语义处理。
func TestCfFallbackFailOpen(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer worker.Close()

	g := newGateway(config{
		cfFallbackURL:    worker.URL,
		cfFallbackKey:    "sk_cf_test",
		firstByteTimeout: 5 * time.Second,
	})
	request := upstreamRequest{
		method:   http.MethodPost,
		path:     "/v1/chat/completions",
		headers:  http.Header{},
		body:     []byte(`{"model":"big-pickle","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
		stream:   true,
		deadline: time.Now().Add(10 * time.Second),
	}
	if _, ok := g.cfFallbackAttempt(context.Background(), request); ok {
		t.Fatal("5xx 应 fail-open 返回 false")
	}
	// 未配置时也 fail-open。
	g2 := newGateway(config{firstByteTimeout: 5 * time.Second})
	if _, ok := g2.cfFallbackAttempt(context.Background(), request); ok {
		t.Fatal("未配置 CF URL 应 fail-open 返回 false")
	}
}

// TestCfFallbackEndToEnd 端到端：直连失败时借道 CF Worker 兜底交付完整流。
func TestCfFallbackEndToEnd(t *testing.T) {
	// 主上游：永远 5xx（直连失败场景）。
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal"}`))
	}))
	defer dead.Close()

	// CF Worker：完整回复。
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk_cf_test" {
			t.Errorf("缺少 CF 认证头")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"via cf\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: [DONE]\n\n"))
	}))
	defer worker.Close()

	cfg := config{
		project:          projectSpec{upstream: dead.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamIdle:       30 * time.Second,
		cfFallbackURL:    worker.URL,
		cfFallbackKey:    "sk_cf_test",
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
	if !strings.Contains(body, "via cf") || !strings.Contains(body, "[DONE]") {
		t.Fatalf("直连失败后应借道 CF Worker 交付完整流: %q", body)
	}
}

func mustReadBody(r *http.Request) string {
	data := make([]byte, 0, 4096)
	buf := make([]byte, 512)
	for {
		n, err := r.Body.Read(buf)
		data = append(data, buf[:n]...)
		if err != nil {
			break
		}
	}
	return string(data)
}
