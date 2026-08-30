package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ── P0-1: 流首块 JSON 错误拦截 ─────────────────────────────────────────────

func TestFirstDataError(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		want   bool
		status int
	}{
		{"200+内嵌 quota", `data: {"error":{"message":"FreeUsageLimitError: monthly limit"}}`, true, http.StatusTooManyRequests},
		{"小写", `data: {"error":{"message":"freeusagelimiterror"}}`, true, http.StatusTooManyRequests},
		{"普通错误非 quota", `data: {"error":{"message":"rate limit exceeded"}}`, true, http.StatusBadGateway},
		{"无 error 键不算", `data: {"message":"FreeUsageLimitError"}`, false, 0},
		{"正常内容", `data: {"choices":[{"delta":{"content":"hi"}}]}`, false, 0},
		{"空白 data", `data: [DONE]`, false, 0},
		{"空", ``, false, 0},
	}
	for _, c := range cases {
		status, isErr := firstDataError([]byte(c.in))
		if isErr != c.want || status != c.status {
			t.Errorf("%s: isErr=%v (want %v) status=%d (want %d)", c.name, isErr, c.want, status, c.status)
		}
	}
}

// ── 空响应拦截（empty_model_response 修复）───────────────────────────────

func TestPrefixEmptyResponse(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"首行即 [DONE]", `data: [DONE]` + "\n\n", true},
		{"空 choices 后 [DONE]", `data: {"choices":[]}` + "\n\ndata: [DONE]\n\n", true},
		{"仅 role 块后 [DONE]（真实空响应形态）", `data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\ndata: [DONE]\n\n", true},
		{"空 delta 无终止后 [DONE]", `data: {"choices":[{"delta":{}}]}` + "\n\ndata: [DONE]\n\n", true},
		{"有内容后 [DONE] 正常", `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\ndata: [DONE]\n\n", false},
		{"正常 role 块（慢流首段无 [DONE] 放行）", `data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\n", false},
		{"正常内容块", `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n", false},
		{"先 usage 后内容", `data: {"usage":{"prompt_tokens":1}}` + "\n\n", false},
		{"首行是注释后 [DONE]", `: keepalive` + "\n" + `data: [DONE]` + "\n\n", true},
		{"空 data 行后 [DONE]", `data: ` + "\n\ndata: [DONE]\n\n", true},
	}
	for _, c := range cases {
		if got := prefixEmptyResponse([]byte(c.in)); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// ── P0-2: Retry-After 封顶 ────────────────────────────────────────────────

func TestRetryAfterCap(t *testing.T) {
	old := maxRetryAfterOverride
	defer func() { maxRetryAfterOverride = old }()
	maxRetryAfterOverride = 1 * time.Hour

	// 超 1h 的 Retry-After 封顶为 1h 且降为推断值。
	d, src := classifyUpstreamFailure(http.StatusTooManyRequests, []byte(`{}`), "999999")
	if d != maxOpenCodeRetryAfter || src != benchHeuristic {
		t.Fatalf("超长 Retry-After 应封顶 1h 推断值，got (%v,%v)", d, src)
	}
	// 1h 内保留权威值。
	d, src = classifyUpstreamFailure(http.StatusTooManyRequests, []byte(`{}`), "120")
	if d != 120*time.Second || src != benchAuthoritative {
		t.Fatalf("1h 内 Retry-After 应保留权威值，got (%v,%v)", d, src)
	}
	// 可覆盖封顶值。
	maxRetryAfterOverride = 30 * time.Minute
	d, src = classifyUpstreamFailure(http.StatusTooManyRequests, []byte(`{}`), "7200")
	if d != 30*time.Minute || src != benchHeuristic {
		t.Fatalf("自定义封顶应生效，got (%v,%v)", d, src)
	}
}

// ── P0-3: 模型 fallback 链 ────────────────────────────────────────────────

func TestResolveModelChain(t *testing.T) {
	g := newGateway(config{modelFallbacks: []string{"b", "c", "b", ""}})
	req := upstreamRequest{body: []byte(`{"model":"a"}`)}
	chain := g.resolveModelChain(req)
	if len(chain) != 3 || chain[0] != "a" || chain[1] != "b" || chain[2] != "c" {
		t.Fatalf("chain=%v", chain)
	}
	// 无 fallback 配置时单元素。
	g2 := newGateway(config{})
	if got := g2.resolveModelChain(req); len(got) != 1 {
		t.Fatalf("无 fallback 应单元素，got %v", got)
	}
	// 请求体不可解析时空串链。
	if got := g2.resolveModelChain(upstreamRequest{}); len(got) != 1 {
		t.Fatalf("空请求应单元素，got %v", got)
	}
	// dead 模型不进链。
	g3 := newGateway(config{modelFallbacks: []string{"dead-model", "good"}})
	g3.modelHealth.record("dead-model", "dead", false, time.Now())
	g3.modelHealth.record("good", "working", true, time.Now())
	chain3 := g3.resolveModelChain(req)
	found := false
	for _, m := range chain3 {
		if m == "dead-model" {
			found = true
		}
	}
	if found {
		t.Fatalf("dead 模型不应进 fallback 链: %v", chain3)
	}
	if len(chain3) != 2 {
		t.Fatalf("chain3=%v", chain3)
	}
}

func TestRewriteBodyModel(t *testing.T) {
	body := []byte(`{"model":"a","messages":[{"role":"user","content":"hi"}]}`)
	out, ok := rewriteBodyModel(body, "b")
	if !ok {
		t.Fatal("应可改写")
	}
	var payload map[string]any
	if json.Unmarshal(out, &payload) != nil {
		t.Fatal("输出必须是合法 JSON")
	}
	if payload["model"] != "b" {
		t.Fatalf("model 应为 b, got %v", payload["model"])
	}
}

func TestModelRewriteBodyStream(t *testing.T) {
	src := io.NopCloser(strings.NewReader(`data: {"model":"big-pickle","choices":[{"delta":{"content":"hi"}}]}

data: [DONE]

`))
	w := &modelRewriteBody{src: src, original: "deepseek-v4-flash-free", from: "big-pickle"}
	out := make([]byte, 512)
	n, err := w.Read(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	text := string(out[:n])
	if !strings.Contains(text, `"model":"deepseek-v4-flash-free"`) {
		t.Fatalf("model 字段应回写: %q", text)
	}
	if strings.Contains(text, `"model":"big-pickle"`) {
		t.Fatalf("原模型名不应残留: %q", text)
	}
}

// 端到端：主模型 429，fallback 模型成功，客户端收到完整流且 model 字段回写。
func TestModelFallbackEndToEnd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := parseJSONObject([]byte(mustReadBody(r)))
		model, _ := payload["model"].(string)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if model == "primary" {
			// 主模型被限流（流首块 JSON 错误形态）。
			_, _ = w.Write([]byte(`data: {"error":{"message":"FreeUsageLimitError: primary limit"}}` + "\n\n"))
			return
		}
		// fallback 模型成功，响应 model 字段带 fallback 名。
		_, _ = w.Write([]byte(`data: {"model":"fallback","choices":[{"delta":{"content":"via fallback"}}]}` + "\n\n" +
			`data: {"model":"fallback","choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
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
		modelFallbacks:   []string{"fallback"},
	}
	application := &app{gateway: newGateway(cfg)}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"primary","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	trace := newRequestTrace()
	recorder := httptest.NewRecorder()

	response, guard, err := application.handlePost(recorder, request, "/v1/chat/completions", trace.start.Add(cfg.hardTimeout), trace)
	if err != nil {
		t.Fatalf("handlePost: %v", err)
	}
	application.finish(recorder, request, trace, response, nil, guard)

	body := recorder.Body.String()
	if !strings.Contains(body, "via fallback") {
		t.Fatalf("应通过 fallback 模型交付: %q", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatalf("应有终止标记: %q", body)
	}
	if !trace.modelSwitched {
		t.Fatal("trace 应记录模型切换")
	}
}

// ── P0-4: 模型健康探测 ────────────────────────────────────────────────────

func TestModelHealthTracker(t *testing.T) {
	h := newModelHealthTracker()
	if !h.isAlive("unknown") {
		t.Fatal("未知模型应视为存活（不误杀）")
	}
	now := time.Now()
	h.record("m1", "dead", false, now)
	h.record("m1", "dead", false, now.Add(time.Second))
	if h.isAlive("m1") {
		t.Fatal("连续 2 次失败应判 dead")
	}
	h.record("m2", "rateLimited", false, now)
	if !h.isAlive("m2") {
		t.Fatal("限流不应判 dead（会恢复）")
	}
	h.record("m3", "working", true, now)
	h.record("m3", "flaky", false, now.Add(time.Second))
	if !h.isAlive("m3") {
		t.Fatal("单次失败不应判 dead")
	}
}

// ── P1-5: 熔断错误归因（取消不计） ───────────────────────────────────────

func TestOutageIgnoresClientCancel(t *testing.T) {
	// 竞速输家被取消（context.Canceled）不应计入全局熔断证据。
	// 直接验证 dispatchRace 的错误分支已跳过 punishExit。
	g := newGateway(config{
		project:          projectSpec{upstream: "http://127.0.0.1:1", directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 100 * time.Millisecond,
		hardTimeout:      time.Second,
	})
	trace := newRequestTrace()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 已取消的上下文
	_, err := g.dispatch(ctx, upstreamRequest{
		method:   http.MethodPost,
		path:     "/v1/chat/completions",
		headers:  http.Header{},
		body:     []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
		stream:   true,
		deadline: time.Now().Add(time.Second),
	}, trace)
	if err == nil {
		t.Fatal("已取消的上下文应返回错误")
	}
	// 熔断不应因客户端取消而触发。
	if g.outage.Tripped() {
		t.Fatal("客户端取消不应触发全局熔断")
	}
}

// ── P2-9: 载荷预检 + 历史裁剪 ────────────────────────────────────────────

func TestTrimPayloadToLimit(t *testing.T) {
	messages := make([]any, 0, 20)
	for i := 0; i < 20; i++ {
		messages = append(messages, map[string]any{
			"role": "user", "content": strings.Repeat("x", 1000),
		})
		messages = append(messages, map[string]any{
			"role": "assistant", "content": strings.Repeat("y", 1000),
		})
	}
	payload := map[string]any{"model": "m", "messages": messages}
	if !trimPayloadToLimit(payload, 4096) {
		t.Fatal("超限应裁剪")
	}
	trimmed, _ := payload["messages"].([]any)
	if len(trimmed) >= len(messages) {
		t.Fatalf("应裁剪历史: before=%d after=%d", len(messages), len(trimmed))
	}
	// 裁剪后序列化必须不超限。
	if out, err := json.Marshal(payload); err != nil || len(out) > 4096+4096 {
		t.Fatalf("裁剪后应不超限: len=%d err=%v", len(out), err)
	}
	// 不超限不裁剪。
	small := map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}
	if trimPayloadToLimit(small, 1<<20) {
		t.Fatal("小请求不应裁剪")
	}
}

// ── P2-10: 吸收产物缓存 ──────────────────────────────────────────────────

func TestAbsorbCacheRoundtrip(t *testing.T) {
	c := newAbsorbCache(time.Minute)
	header := http.Header{"Content-Type": []string{"application/json"}}
	c.store("key1", http.StatusOK, header, []byte(`{"ok":true}`))

	entry, ok := c.lookup("key1")
	if !ok || string(entry.body) != `{"ok":true}` {
		t.Fatal("缓存命中应返回存储内容")
	}
	if _, ok := c.lookup("missing"); ok {
		t.Fatal("未命中应返回 false")
	}
	// TTL 过期。
	c.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, ok := c.lookup("key1"); ok {
		t.Fatal("TTL 过期应失效")
	}
}

func TestCacheKeyCanonical(t *testing.T) {
	a := map[string]any{"model": "m", "messages": []any{map[string]any{"content": "hi", "role": "user"}}}
	b := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hi"}}, "model": "m"}
	if cacheKeyForRequest(a) != cacheKeyForRequest(b) {
		t.Fatal("键序/字段序不同的相同请求应同键")
	}
}

// ── P2-11: 消息归一化 ────────────────────────────────────────────────────

func TestNormalizeMessages(t *testing.T) {
	payload := map[string]any{
		"messages": []any{
			map[string]any{"role": "assistant", "content": "hello"},
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{"role": "user", "content": "again"},
		},
	}
	if !normalizeMessages(payload) {
		t.Fatal("应发生归一化")
	}
	messages, _ := payload["messages"].([]any)
	// 首个非 user 前补 user → 现在第一条是 user。
	first, _ := messages[0].(map[string]any)
	if role, _ := first["role"].(string); role != "user" {
		t.Fatalf("首条应为 user: %v", first)
	}
	// 连续 user 间应补空 assistant。
	roles := make([]string, 0, len(messages))
	for _, m := range messages {
		entry, _ := m.(map[string]any)
		roles = append(roles, entry["role"].(string))
	}
	joined := strings.Join(roles, ",")
	if !strings.Contains(joined, "user,assistant,user") {
		t.Fatalf("连续 user 间应补 assistant 占位: %v", roles)
	}
}

// ── P2-12: 头部透传 ──────────────────────────────────────────────────────

func TestCollectHeadersPassthrough(t *testing.T) {
	cfg := config{project: projectSpec{forwardHeaders: []string{"X-Test"}}}
	a := &app{gateway: newGateway(cfg)}
	source := http.Header{}
	source.Set("X-Test", "v1")
	source.Set("X-Real-IP", "1.2.3.4")
	out := a.collectHeaders(source)
	if out.Get("X-Test") != "v1" {
		t.Fatal("forwardHeaders 应透传")
	}
	if out.Get("X-Real-IP") != "1.2.3.4" {
		t.Fatal("x-real-ip 应透传")
	}
}

// 熔断恢复探针：未熔断时不触发（安静休息）。
func TestRecoveryProbeIdle(t *testing.T) {
	g := newGateway(config{
		project:      projectSpec{probePath: "/v1/models", probeHeaders: http.Header{}},
		probeTimeout: time.Second,
	})
	if g.recoveryProbeOnce(context.Background()) {
		t.Fatal("无上游时探针应失败（不误报恢复）")
	}
}
