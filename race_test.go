package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestDispatchRaceFastestExitWins(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: fast\n\n"))
	}))
	defer fast.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(600 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: slow\n\n"))
	}))
	defer slow.Close()

	fastURL, err := url.Parse(fast.URL)
	if err != nil {
		t.Fatal(err)
	}
	slowURL, err := url.Parse(slow.URL)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config{
		project:          projectSpec{upstream: "http://upstream.invalid/zen"},
		proxyMode:        "auto",
		proxyOrder:       []string{layerCustom},
		raceEnabled:      true,
		raceWidth:        4,
		firstByteTimeout: time.Second,
		hardTimeout:      10 * time.Second,
	}
	gw := newGateway(cfg)
	gw.custom = []slot{
		{addr: slowURL.Host, proxyURL: slowURL},
		{addr: fastURL.Host, proxyURL: fastURL},
	}

	started := time.Now()
	resp, err := gw.dispatchRace(context.Background(), upstreamRequest{
		method:   http.MethodGet,
		path:     "/zen/v1/models",
		headers:  http.Header{},
		stream:   true,
		deadline: time.Now().Add(cfg.hardTimeout),
	}, newRequestTrace())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.status != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.status)
	}
	if time.Since(started) > 400*time.Millisecond {
		t.Fatalf("slow exit won the race: %v", time.Since(started))
	}
}

// 回归测试：竞速返回后，赢家的流式响应必须仍然可读。
// 曾经的 bug：dispatchRace 返回时触发 cancel，把赢家的流当场掐断，
// 客户端表现为 Connection error。
func TestDispatchRaceWinnerStreamStaysAlive(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			_, _ = w.Write([]byte("data: first\n\n"))
			f.Flush()
		}
		<-r.Context().Done() // 保持连接直到客户端离开
	}))
	defer fast.Close()

	fastURL, err := url.Parse(fast.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{
		project:          projectSpec{upstream: "http://upstream.invalid/zen"},
		proxyMode:        "auto",
		proxyOrder:       []string{layerCustom},
		raceEnabled:      true,
		raceWidth:        4,
		firstByteTimeout: time.Second,
		hardTimeout:      10 * time.Second,
	}
	gw := newGateway(cfg)
	gw.custom = []slot{{addr: fastURL.Host, proxyURL: fastURL}}

	resp, err := gw.dispatchRace(context.Background(), upstreamRequest{
		method:   http.MethodPost,
		path:     "/zen/v1/chat/completions",
		headers:  http.Header{},
		stream:   true,
		deadline: time.Now().Add(cfg.hardTimeout),
	}, newRequestTrace())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.status != http.StatusOK || resp.live == nil {
		t.Fatalf("expected streaming 200 with live body, got status=%d live=%v", resp.status, resp.live)
	}

	// 返回之后流必须还能读出赢家已推送的数据。
	buf := make([]byte, 32)
	type readOutcome struct {
		n   int
		err error
	}
	ch := make(chan readOutcome, 1)
	go func() {
		n, readErr := resp.live.response.Body.Read(buf)
		ch <- readOutcome{n, readErr}
	}()
	select {
	case out := <-ch:
		if out.err != nil || out.n == 0 {
			t.Fatalf("winner stream was killed after return: n=%d err=%v", out.n, out.err)
		}
		if !strings.Contains(string(buf[:out.n]), "data: first") {
			t.Fatalf("unexpected stream content: %q", string(buf[:out.n]))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream unreadable 3s after return - likely cancelled by race cleanup")
	}
	resp.live.Close()
}

func TestRaceExitsManualAlwaysIncluded(t *testing.T) {
	cfg := config{raceWidth: 2}
	gw := newGateway(cfg)
	gw.custom = []slot{
		{addr: "manual-1"}, {addr: "auto-1"}, {addr: "auto-2"}, {addr: "auto-3"}, {addr: "auto-4"},
	}
	gw.markManual("manual-1")

	exits := gw.raceExits()
	found := false
	for _, s := range exits {
		if s.addr == "manual-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("手动节点应始终参加竞速")
	}
	autoCount := 0
	for _, s := range exits {
		if s.addr != "manual-1" {
			autoCount++
		}
	}
	if autoCount > 2 {
		t.Fatalf("自动节点应受宽度限制（≤%d），实际 %d", cfg.raceWidth, autoCount)
	}
}

// 空流拦截：返回 200 但不带任何数据行即关闭的出口不算赢家，
// 竞速必须落到能交付首条数据的出口上。
func TestRaceRejectsEmptyStreamWinner(t *testing.T) {
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer empty.Close()
	// 稍作延迟：确保空流出口的失败结果先于赢家到达，被主循环记账。
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: real\n\n"))
	}))
	defer good.Close()

	emptyURL, err := url.Parse(empty.URL)
	if err != nil {
		t.Fatal(err)
	}
	goodURL, err := url.Parse(good.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{
		project:          projectSpec{upstream: "http://upstream.invalid/zen"},
		proxyMode:        "auto",
		proxyOrder:       []string{layerCustom},
		raceEnabled:      true,
		raceWidth:        4,
		hedgeDelay:       200 * time.Millisecond,
		firstByteTimeout: 2 * time.Second,
		hardTimeout:      10 * time.Second,
	}
	gw := newGateway(cfg)
	gw.custom = []slot{
		{addr: emptyURL.Host, proxyURL: emptyURL},
		{addr: goodURL.Host, proxyURL: goodURL},
	}

	resp, err := gw.dispatchRace(context.Background(), upstreamRequest{
		method:   http.MethodPost,
		path:     "/zen/v1/chat/completions",
		headers:  http.Header{},
		stream:   true,
		deadline: time.Now().Add(cfg.hardTimeout),
	}, newRequestTrace())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.status != http.StatusOK || resp.live == nil {
		t.Fatalf("expected streaming 200 from the good exit, got status=%d live=%v", resp.status, resp.live)
	}
	buf := make([]byte, 128)
	n, readErr := resp.live.response.Body.Read(buf)
	if readErr != nil || !strings.Contains(string(buf[:n]), "data: real") {
		t.Fatalf("winner body should carry the good exit's data: n=%d err=%v", n, readErr)
	}
	resp.live.Close()

	// 空流出口应记一次截断（进入失败连击，单次不足坐板凳）。
	gw.exits.mu.Lock()
	stat := gw.exits.stats[emptyURL.Host]
	gw.exits.mu.Unlock()
	if stat == nil || stat.truncations != 1 {
		t.Fatalf("空流出口应记一次截断，实际 %+v", stat)
	}
}

// 对冲分批：前两批都是挂死出口时，第三批的好出口必须在加发后才能胜出。
func TestRaceHedgeEscalation(t *testing.T) {
	dead := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done() // 挂死直到客户端放弃
		}))
	}
	dead1, dead2 := dead(), dead()
	defer dead1.Close()
	defer dead2.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: ok\n\n"))
	}))
	defer good.Close()

	toSlot := func(server *httptest.Server) slot {
		u, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		return slot{addr: u.Host, proxyURL: u}
	}
	cfg := config{
		project:          projectSpec{upstream: "http://upstream.invalid/zen"},
		proxyMode:        "auto",
		proxyOrder:       []string{layerCustom},
		raceEnabled:      true,
		raceWidth:        4,
		hedgeDelay:       250 * time.Millisecond,
		firstByteTimeout: 3 * time.Second,
		hardTimeout:      10 * time.Second,
	}
	gw := newGateway(cfg)
	gw.custom = []slot{toSlot(dead1), toSlot(dead2), toSlot(good)}

	started := time.Now()
	resp, err := gw.dispatchRace(context.Background(), upstreamRequest{
		method:   http.MethodPost,
		path:     "/zen/v1/chat/completions",
		headers:  http.Header{},
		stream:   true,
		deadline: time.Now().Add(cfg.hardTimeout),
	}, newRequestTrace())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.live.Close()
	if resp.status != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.status)
	}
	// 首批两个死出口（hedgeFirstWave=2），好出口在第二批加发（hedgeBatch=3）后才能出发。
	if elapsed := time.Since(started); elapsed < cfg.hedgeDelay {
		t.Fatalf("good exit was in the first wave? elapsed=%s hedgeDelay=%s", elapsed, cfg.hedgeDelay)
	}
}

// 手动节点坐板凳：连续真实失败后暂停参与竞速，但不会被删除。
func TestRaceExitsSkipBenchedManual(t *testing.T) {
	gw := newGateway(config{raceWidth: 4})
	gw.custom = []slot{{addr: "m1"}, {addr: "m2"}}
	gw.markManual("m1")
	gw.markManual("m2")

	for range benchFailLimit {
		gw.exits.observeFail("m1")
	}
	if !gw.exits.benched("m1") {
		t.Fatal("连续失败后 m1 应坐板凳")
	}
	exits := gw.raceExits()
	if len(exits) != 1 || exits[0].addr != "m2" {
		t.Fatalf("坐板凳节点不应参与竞速，实际 exits=%v", exits)
	}

	// 探活通过立即回归。
	gw.exits.observeProbe("m1", time.Second, true)
	if gw.exits.benched("m1") {
		t.Fatal("探活通过后应解除坐板凳")
	}
}

func TestEnvIsOn(t *testing.T) {
	for _, raw := range []string{"1", "true", "ON", "yes", " y "} {
		if !envIsOn(raw) {
			t.Errorf("envIsOn(%q) 应为 true", raw)
		}
	}
	for _, raw := range []string{"0", "false", "off", "", "no"} {
		if envIsOn(raw) {
			t.Errorf("envIsOn(%q) 应为 false", raw)
		}
	}
}
