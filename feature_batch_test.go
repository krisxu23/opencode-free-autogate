package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	if d := parseRetryAfter("120"); d != 120*time.Second {
		t.Fatalf("秒数形式应解析为 120s，实际 %v", d)
	}
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if d := parseRetryAfter(future); d <= 0 || d > 2*time.Minute {
		t.Fatalf("HTTP 日期形式应解析为未来时长，实际 %v", d)
	}
	for _, bad := range []string{"", "  ", "abc", "-5", "1970-01-01T00:00:00Z"} {
		if d := parseRetryAfter(bad); d != 0 {
			t.Fatalf("非法值 %q 应返回 0，实际 %v", bad, d)
		}
	}
}

func TestClassifyUpstreamFailure(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		retryAfter string
		wantDur    time.Duration
		wantSrc    benchSource
	}{
		{"计费错误按天板凳", http.StatusPaymentRequired, `{"error":"billing"}`, "", creditBenchDur, benchAuthoritative},
		{"Retry-After 秒数", http.StatusTooManyRequests, `{}`, "120", 120 * time.Second, benchAuthoritative},
		{"响应体恢复时间", http.StatusTooManyRequests, `{"message":"quota resets in 13 hours"}`, "", 13 * time.Hour, benchAuthoritative},
		{"无时间额度枯竭走推断", http.StatusTooManyRequests, `FreeUsageLimitError`, "", quotaBenchDuration, benchHeuristic},
		{"普通 429 不坐板凳", http.StatusTooManyRequests, `{"error":"slow down"}`, "", 0, benchNone},
		{"5xx 不坐额度板凳", http.StatusBadGateway, `{}`, "", 0, benchNone},
		{"过短 Retry-After 抬到下限", http.StatusTooManyRequests, `{}`, "3", minQuotaBench, benchAuthoritative},
		{"超长 Retry-After 封顶", http.StatusTooManyRequests, `{}`, "999999", maxQuotaBench, benchAuthoritative},
	}
	for _, tc := range cases {
		d, src := classifyUpstreamFailure(tc.status, []byte(tc.body), tc.retryAfter)
		if d != tc.wantDur || src != tc.wantSrc {
			t.Fatalf("%s: got (%v,%v) want (%v,%v)", tc.name, d, src, tc.wantDur, tc.wantSrc)
		}
	}
}

func TestParseResetHint(t *testing.T) {
	cases := map[string]time.Duration{
		`Rate limit exceeded. Resets in 13 hours.`:      13 * time.Hour,
		`please try again in 20 minutes`:                20 * time.Minute,
		`quota resets in 45 secs`:                       45 * time.Second,
		`try again after 2 days`:                        48 * time.Hour,
		`no hint here`:                                  0,
		`resets in zero hours`:                          0,
	}
	for body, want := range cases {
		if got := parseResetHint([]byte(body)); got != want {
			t.Fatalf("parseResetHint(%q) = %v, want %v", body, got, want)
		}
	}
}

func TestEnhanceRequestBody(t *testing.T) {
	body := []byte(`{"model":"big-pickle","client_metadata":{"ua":"x"},"tools":[{"type":"function","function":{"name":"good","parameters":{}}},{"type":"function"},{"type":"custom"}],"messages":[]}`)
	out := enhanceRequestBody(body, true)
	var payload map[string]any
	if json.Unmarshal(out, &payload) != nil {
		t.Fatal("输出必须是合法 JSON")
	}
	if _, ok := payload["client_metadata"]; ok {
		t.Fatal("client_metadata 应被剥离")
	}
	tools, _ := payload["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("缺失 function.name 的条目应被剔除，保留 2 条，实际 %d", len(tools))
	}
	if payload["prompt_cache_retention"] != "24h" {
		t.Fatal("应注入 prompt_cache_retention")
	}
	cc, _ := payload["cache_control"].(map[string]any)
	if cc == nil || cc["type"] != "ephemeral" || cc["ttl"] != "1h" {
		t.Fatalf("应注入 cache_control，实际 %v", payload["cache_control"])
	}

	// GLM 系：retention 注入、cache_control 跳过。
	glm := enhanceRequestBody([]byte(`{"model":"glm-4.7-free"}`), true)
	var glmPayload map[string]any
	json.Unmarshal(glm, &glmPayload)
	if glmPayload["prompt_cache_retention"] != "24h" {
		t.Fatal("GLM 也应注入 retention")
	}
	if _, ok := glmPayload["cache_control"]; ok {
		t.Fatal("GLM 不应注入 cache_control")
	}

	// Anthropic 路径（injectCache=false）：只做卫生不注缓存。
	an := enhanceRequestBody(body, false)
	if strings.Contains(string(an), "prompt_cache_retention") {
		t.Fatal("injectCache=false 不应注入缓存字段")
	}

	// 无改动时原样返回（字节级不变）。
	clean := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	if got := enhanceRequestBody(clean, false); string(got) != string(clean) {
		t.Fatal("无改动应原样返回")
	}

	// 工具超限截断。
	many := make([]string, 0, 140)
	for i := 0; i < 140; i++ {
		many = append(many, `{"type":"function","function":{"name":"f"}}`)
	}
	big := []byte(`{"model":"m","tools":[` + strings.Join(many, ",") + `]}`)
	var bigPayload map[string]any
	json.Unmarshal(enhanceRequestBody(big, false), &bigPayload)
	if tools, _ := bigPayload["tools"].([]any); len(tools) != 128 {
		t.Fatalf("工具超限应截断到 128，实际 %d", len(tools))
	}
}

func TestTryLocalHousekeeping(t *testing.T) {
	localMocksEnabled = true
	defer func() { localMocksEnabled = true }()

	hit := map[string]any{
		"model":      "big-pickle",
		"max_tokens": float64(16),
		"messages":   []any{map[string]any{"role": "user", "content": "check my quota status"}},
	}
	canned, ok := tryLocalHousekeeping("/v1/chat/completions", false, hit)
	if !ok || canned == nil {
		t.Fatal("配额探测请求应被本地拦截")
	}
	var response map[string]any
	if json.Unmarshal(canned, &response) != nil {
		t.Fatal("本地应答必须是合法 JSON")
	}

	misses := []struct {
		name string
		path string
		stream bool
		payload map[string]any
	}{
		{"流式请求放行", "/v1/chat/completions", true, hit},
		{"路径不符放行", "/v1/messages", false, hit},
		{"大 max_tokens 放行", "/v1/chat/completions", false, map[string]any{
			"max_tokens": float64(4096),
			"messages":   []any{map[string]any{"role": "user", "content": "check my quota status"}},
		}},
		{"长文本放行", "/v1/chat/completions", false, map[string]any{
			"max_tokens": float64(16),
			"messages":   []any{map[string]any{"role": "user", "content": strings.Repeat("explain quota mechanics in depth ", 40)}},
		}},
		{"普通提问放行", "/v1/chat/completions", false, map[string]any{
			"max_tokens": float64(16),
			"messages":   []any{map[string]any{"role": "user", "content": "write a fibonacci function"}},
		}},
		{"助手消息在场放行", "/v1/chat/completions", false, map[string]any{
			"max_tokens": float64(16),
			"messages": []any{
				map[string]any{"role": "assistant", "content": "hi"},
				map[string]any{"role": "user", "content": "quota?"},
			},
		}},
	}
	for _, mc := range misses {
		if _, ok := tryLocalHousekeeping(mc.path, mc.stream, mc.payload); ok {
			t.Fatalf("%s 不应被拦截", mc.name)
		}
	}

	localMocksEnabled = false
	if _, ok := tryLocalHousekeeping("/v1/chat/completions", false, hit); ok {
		t.Fatal("开关关闭时应整体放行")
	}
}

func TestSoftLimitedLeaseFlow(t *testing.T) {
	tracker := newExitTracker()
	addr := "10.9.9.9:1080"

	tracker.observeQuotaBurn(addr, time.Hour, benchHeuristic)
	if !tracker.benched(addr) {
		t.Fatal("烧额后应坐板凳")
	}
	if tracker.authoritativelyBenched(addr) {
		t.Fatal("推断板凳不应标记为权威")
	}
	tracker.observeQuotaBurn(addr, 3*time.Hour, benchAuthoritative)
	if !tracker.authoritativelyBenched(addr) {
		t.Fatal("上游声明板凳应标记为权威")
	}

	// 手动把板凳拨到已过期，filterBenched 应放行并进入观察窗。
	tracker.mu.Lock()
	tracker.stats[addr].benchUntil = time.Now().Add(-time.Minute)
	tracker.mu.Unlock()
	kept := tracker.filterBenched([]slot{{addr: addr}})
	if len(kept) != 1 {
		t.Fatal("过期板凳应放行")
	}
	if !tracker.softLimited(addr) {
		t.Fatal("过期出口应进入观察窗")
	}
	if tracker.benched(addr) {
		t.Fatal("过期板凳应清零")
	}

	// 观察窗内已有在途路数时，launchWave 会跳过该出口（这里验证租约计数）。
	tracker.enterExit(addr)
	if tracker.inFlightCount(addr) != 1 {
		t.Fatal("enterExit 应记一路在途")
	}
	tracker.exitExit(addr)
	if tracker.inFlightCount(addr) != 0 {
		t.Fatal("exitExit 应释放租约")
	}
	tracker.exitExit(addr) // 多放不越界
	if tracker.inFlightCount(addr) != 0 {
		t.Fatal("租约不应为负")
	}
}

func TestStickySessionPin(t *testing.T) {
	gw := newGateway(config{})
	gw.stickyRemember("sess-1", "10.1.1.1:1080")
	addr, ok := gw.stickyLookup("sess-1")
	if !ok || addr != "10.1.1.1:1080" {
		t.Fatalf("粘性记录应命中，got (%q,%v)", addr, ok)
	}
	if _, ok := gw.stickyLookup(""); ok {
		t.Fatal("空会话不应命中")
	}
	gw.stickyRemember("sess-1", "直连") // 直连不覆盖已有粘性
	if addr, _ := gw.stickyLookup("sess-1"); addr != "10.1.1.1:1080" {
		t.Fatal("直连胜出不应改写粘性出口")
	}

	// TTL 过期后不再命中。
	gw.stickyMu.Lock()
	entry := gw.sticky["sess-1"]
	entry.seen = time.Now().Add(-stickyTTL - time.Minute)
	gw.sticky["sess-1"] = entry
	gw.stickyMu.Unlock()
	if _, ok := gw.stickyLookup("sess-1"); ok {
		t.Fatal("超过 TTL 的粘性记录不应命中")
	}
}

func TestProbeModelFixed(t *testing.T) {
	// 深检模型固定 big-pickle（长期在售）；列表里的其他名字随时下线，
	// 且"列表有"不代表 chat 门放行——不参与探活模型选择。
	gw := newGateway(config{})
	if got := gw.probeModelID(); got != "big-pickle" {
		t.Fatalf("默认深检模型应为 big-pickle，got %s", got)
	}
	custom := newGateway(config{probeModel: "my-model"})
	if got := custom.probeModelID(); got != "my-model" {
		t.Fatalf("PROXY_PROBE_MODEL 覆盖应生效，want my-model got %s", got)
	}
}
