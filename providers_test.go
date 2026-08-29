package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// OpenCode 供应商关闭节点池出口：即使池里有自定义节点，请求也必须直连。
func TestProviderPoolSwitchOpenCodeOff(t *testing.T) {
	var proxyHits atomic.Int32
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyHits.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer proxySrv.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		opencodePoolOff:  true, // 关闭 OpenCode 节点池出口
	}
	g := newGateway(cfg)
	slot, err := slotFromRawURL(strings.TrimPrefix(proxySrv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	g.addSlot(slot, true)

	trace := newRequestTrace()
	request := upstreamRequest{
		method: http.MethodPost, path: "/v1/chat/completions", nonStream: true,
		body: []byte(`{}`), deadline: time.Now().Add(30 * time.Second),
	}
	resp, err := g.dispatchOnce(context.Background(), request, trace)
	if err != nil {
		t.Fatal(err)
	}
	if resp.status != http.StatusOK {
		t.Fatalf("池关闭时应直连上游拿到 200，得到 %d", resp.status)
	}
	if proxyHits.Load() != 0 {
		t.Fatalf("池关闭时代理不应被使用，实际 %d 次", proxyHits.Load())
	}
	if trace.finalProxy != "direct" {
		t.Fatalf("finalProxy 应为 direct，得到 %q", trace.finalProxy)
	}
}

// 对照：开启节点池出口时自定义节点参与调度（坏代理被命中后自动直连兜底）。
func TestProviderPoolSwitchOpenCodeOn(t *testing.T) {
	var proxyHits atomic.Int32
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyHits.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer proxySrv.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: true},
		proxyMode:        "custom",
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		// opencodePoolOff 零值 false = 节点池出口开启
	}
	g := newGateway(cfg)
	slot, err := slotFromRawURL(strings.TrimPrefix(proxySrv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	g.addSlot(slot, true)

	trace := newRequestTrace()
	request := upstreamRequest{
		method: http.MethodPost, path: "/v1/chat/completions", nonStream: true,
		body: []byte(`{}`), deadline: time.Now().Add(30 * time.Second),
	}
	resp, err := g.dispatchOnce(context.Background(), request, trace)
	if err != nil {
		t.Fatal(err)
	}
	if resp.status != http.StatusOK {
		t.Fatalf("坏出口后应直连兜底拿到 200，得到 %d", resp.status)
	}
	if proxyHits.Load() != 1 {
		t.Fatalf("池开启时代理应被使用 1 次，实际 %d 次", proxyHits.Load())
	}
}

func TestClineExitsDisabled(t *testing.T) {
	g := newGateway(config{})
	if exits := g.clineExits(); exits != nil {
		t.Fatalf("未启用节点池应无出口序列，得到 %d 个", len(exits))
	}
}

func TestClineExitsEnabled(t *testing.T) {
	g := newGateway(config{clinePoolEnabled: true})
	slot, err := slotFromRawURL("http://1.2.3.4:1080")
	if err != nil {
		t.Fatal(err)
	}
	g.addSlot(slot, true)
	exits := g.clineExits()
	if len(exits) != 2 {
		t.Fatalf("应含 1 个池出口 + 直连兜底，得到 %d", len(exits))
	}
	if exits[0] == nil || exits[0].Host != "1.2.3.4:1080" {
		t.Fatalf("首位应为池出口: %v", exits[0])
	}
	if exits[1] != nil {
		t.Fatalf("末位应为直连兜底: %v", exits[1])
	}
}

// Cline 聊天转发：启用节点池后，坏出口（恒 503）被轮过，直连兜底交付。
func TestClineChatExitRotation(t *testing.T) {
	var proxyHits, upstreamHits atomic.Int32
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyHits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer proxySrv.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	}))
	defer upstream.Close()

	oldBase := clineUpstreamBase
	clineUpstreamBase = upstream.URL
	defer func() { clineUpstreamBase = oldBase }()

	// 账号池指到临时目录，不碰真实数据文件。
	dir := t.TempDir()
	clinePoolMu.Lock()
	clinePoolPath = filepath.Join(dir, "accounts.json")
	clinePool = nil
	clinePoolMu.Unlock()
	clineAddAccount(&clineAccount{
		AccountID:   "acc_test",
		Email:       "test@example.com",
		AccessToken: "workos:test-token",
		ExpiresAt:   time.Now().Add(time.Hour).UnixMilli(),
		Status:      "active",
	})

	cfg := config{
		clinePoolEnabled: true,
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
	}
	g := newGateway(cfg)
	slot, err := slotFromRawURL(strings.TrimPrefix(proxySrv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	g.addSlot(slot, true)

	params := map[string]any{
		"model": "cline/test-model",
		"messages": []any{map[string]any{
			"role": "user", "content": "hi",
		}},
	}
	trace := newRequestTrace()
	trace.tag = sessionTag("ses_test123")
	resp, err := g.handleClineChat(context.Background(), httptest.NewRecorder(),
		params, "/v1/chat/completions", false, time.Now().Add(30*time.Second), trace)
	if err != nil {
		t.Fatal(err)
	}
	if resp.status != http.StatusOK {
		t.Fatalf("坏出口后应直连兜底交付 200，得到 %d body=%s", resp.status, resp.body)
	}
	if proxyHits.Load() != 1 {
		t.Fatalf("坏出口应被尝试 1 次，实际 %d", proxyHits.Load())
	}
	if upstreamHits.Load() != 1 {
		t.Fatalf("上游应命中 1 次，实际 %d", upstreamHits.Load())
	}
	if !strings.Contains(string(resp.body), "ok") {
		t.Fatalf("响应体应透传上游内容: %s", resp.body)
	}
	if trace.finalProxy != "直连" {
		t.Fatalf("finalProxy 应记录直连兜底，得到 %q", trace.finalProxy)
	}
}
