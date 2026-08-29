package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdvCollect(t *testing.T) {
	now := time.Now()
	seen := map[string]struct{}{
		"vless://keep@a:1": {}, // evict 会同时把它移出 seen，这里不会出现死链
	}
	cooldown := map[string]time.Time{
		"vless://dead@b:1": now.Add(time.Hour),  // 冷却中
		"vless://gone@c:1": now.Add(-time.Hour), // 冷却已过期：应重新纳入
	}
	manual := []string{"trojan://manual@d:1"}
	// gone@c:1 冷却已过期且源仍列出它（出现在 fresh）：应重新纳入映射
	fresh := []string{"vless://new@e:1", "vless://dead@b:1", "vless://gone@c:1"}

	set := advCollect(manual, seen, fresh, cooldown, now)
	for _, link := range []string{
		"vless://keep@a:1", "trojan://manual@d:1", "vless://new@e:1", "vless://gone@c:1",
	} {
		if _, ok := set[link]; !ok {
			t.Fatalf("应包含 %s", link)
		}
	}
	if _, ok := set["vless://dead@b:1"]; ok {
		t.Fatal("冷却中的死链不应纳入映射")
	}
}

func TestSameLinkSet(t *testing.T) {
	links := map[string]string{
		"vless://a@x:1": "127.0.0.1:21001",
		"vless://b@x:1": "127.0.0.1:21002",
	}
	items := []advancedItem{{link: "vless://a@x:1"}, {link: "vless://b@x:1"}}
	if !sameLinkSet(links, items) {
		t.Fatal("相同集合应返回 true")
	}
	shrunk := []advancedItem{{link: "vless://a@x:1"}}
	if sameLinkSet(links, shrunk) {
		t.Fatal("收缩（少一个映射）应判定为不同")
	}
}

func TestAdvEvictAddr(t *testing.T) {
	g := newGateway(config{proxyMode: "custom"})
	linkAuto := "vless://auto@server:443"
	linkManual := "trojan://manual@server:443"
	g.advBridge = &advancedBridge{Links: map[string]string{
		linkAuto:   "127.0.0.1:21001",
		linkManual: "127.0.0.1:21002",
	}}
	g.manualAdv = []string{linkManual}
	g.advSeen = map[string]struct{}{linkAuto: {}, linkManual: {}}

	// 非映射地址：无操作
	g.advEvictAddr("10.0.0.1:80")
	if len(g.advSeen) != 2 {
		t.Fatalf("非映射地址不应影响映射集: %v", g.advSeen)
	}
	// 手动链接：永不移除
	g.advEvictAddr("127.0.0.1:21002")
	if _, ok := g.advSeen[linkManual]; !ok {
		t.Fatal("手动链接不应被淘汰")
	}
	// 池来源链接：移出映射集并进入冷却
	g.advEvictAddr("127.0.0.1:21001")
	if _, ok := g.advSeen[linkAuto]; ok {
		t.Fatal("淘汰的链接应移出映射集")
	}
	if _, ok := g.advCooldown[linkAuto]; !ok {
		t.Fatal("淘汰的链接应进入冷却")
	}
}

// 波次耗尽：正式池 3 节点、竞速宽 2 路——第一波 2 个全部 503 后，
// 自动追加第二波（第 3 个节点），全部耗尽才交出可重试兜底（503）。
func TestRaceWaveExhaustion(t *testing.T) {
	var hits [3]int
	proxies := make([]*httptest.Server, 3)
	for i := range proxies {
		i := i
		proxies[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits[i]++
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer proxies[i].Close()
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: false},
		proxyMode:        "custom",
		raceEnabled:      true,
		raceWidth:        2,
		stickyEnabled:    false,
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
	}
	g := newGateway(cfg)
	for _, p := range proxies {
		addr := strings.TrimPrefix(p.URL, "http://")
		slot, err := slotFromRawURL(addr)
		if err != nil {
			t.Fatal(err)
		}
		g.addSlot(slot, true)
	}

	trace := newRequestTrace()
	request := upstreamRequest{
		method: http.MethodPost, path: "/v1/chat/completions", nonStream: true,
		body: []byte(`{}`), deadline: time.Now().Add(30 * time.Second),
	}
	resp, err := g.dispatchRace(context.Background(), request, trace)
	if err != nil {
		t.Fatal(err)
	}
	if resp.status != http.StatusServiceUnavailable {
		t.Fatalf("全池耗尽后应交出可重试兜底 503，得到 %d", resp.status)
	}
	total := hits[0] + hits[1] + hits[2]
	if total != 3 {
		t.Fatalf("三个节点都应被尝试一次，实际 %d 次", total)
	}
}

// 重试风暴回归：吸收模式 × 镜像 × 波次的乘法曾打出一个请求 1269 次
// 上游请求。按请求的出口去重后，每个出口至多尝试一次。
func TestAbsorbNoRetryStorm(t *testing.T) {
	var hits [4]int
	proxies := make([]*httptest.Server, 4)
	for i := range proxies {
		i := i
		proxies[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits[i]++
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer proxies[i].Close()
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, directFallback: false},
		proxyMode:        "custom",
		absorbStreaming:  true,
		absorbAttempts:   10,
		raceEnabled:      true,
		raceWidth:        2,
		stickyEnabled:    false,
		firstByteTimeout: 5 * time.Second,
		hardTimeout:      30 * time.Second,
		nonStreamTimeout: 30 * time.Second,
		streamResume:     false,
	}
	application := &app{gateway: newGateway(cfg)}
	for _, p := range proxies {
		addr := strings.TrimPrefix(p.URL, "http://")
		slot, err := slotFromRawURL(addr)
		if err != nil {
			t.Fatal(err)
		}
		application.gateway.addSlot(slot, true)
	}

	// 非流式请求：吸收模式此前不覆盖的形态——现在内部重试直至池耗尽。
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	trace := newRequestTrace()
	recorder := httptest.NewRecorder()
	started := time.Now()

	response, guard, err := application.handlePost(recorder, request, "/v1/chat/completions", trace.start.Add(cfg.nonStreamTimeout), trace)
	if err != nil {
		t.Fatal(err)
	}
	application.finish(recorder, request, trace, response, nil, guard)

	total := hits[0] + hits[1] + hits[2] + hits[3]
	if total != 4 {
		t.Fatalf("每个出口应恰好尝试一次（共 4 次），实际 %d 次", total)
	}
	if response.status != http.StatusServiceUnavailable {
		t.Fatalf("全池耗尽应交出可重试兜底 503，得到 %d", response.status)
	}
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Fatalf("提前收场应在池耗尽后立即返回，实际耗时 %v", elapsed)
	}
}
