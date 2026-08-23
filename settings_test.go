package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFileString(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func TestLogRingIncrementalReads(t *testing.T) {
	ring := &logRing{}
	fmt.Fprintf(ring, "first\n")
	fmt.Fprintf(ring, "second\n")

	lines, cursor := ring.Since(0)
	if len(lines) != 2 || lines[0] != "first" || lines[1] != "second" {
		t.Fatalf("unexpected lines: %v", lines)
	}

	fmt.Fprintf(ring, "third\n")
	lines, cursor = ring.Since(cursor)
	if len(lines) != 1 || lines[0] != "third" {
		t.Fatalf("expected only third, got %v", lines)
	}

	if lines, _ = ring.Since(cursor); len(lines) != 0 {
		t.Fatalf("expected no new lines, got %v", lines)
	}
}

func TestLogRingDropsOldestBeyondCapacity(t *testing.T) {
	ring := &logRing{}
	for i := 0; i < logRingCapacity+50; i++ {
		fmt.Fprintf(ring, "line-%d\n", i)
	}
	lines, cursor := ring.Since(0)
	if len(lines) != logRingCapacity {
		t.Fatalf("expected %d retained lines, got %d", logRingCapacity, len(lines))
	}
	if cursor != logRingCapacity+50 {
		t.Fatalf("cursor = %d, want %d", cursor, logRingCapacity+50)
	}
	if lines[0] != "line-50" {
		t.Fatalf("oldest retained = %q, want line-50", lines[0])
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")

	settings := defaultSettings()
	settings.Outbound = outboundProxy
	settings.ProxyInput = "socks5://127.0.0.1:7890"
	settings.Proxies, _ = ParseProxyInput(settings.ProxyInput)
	settings.FirstByteSeconds = 45
	settings.BudgetSeconds = 200

	if err := settings.save(path); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	loaded := loadSettings(path)
	if loaded.Outbound != outboundProxy {
		t.Fatalf("outbound = %q", loaded.Outbound)
	}
	if len(loaded.Proxies) != 1 || loaded.Proxies[0] != "socks5://127.0.0.1:7890" {
		t.Fatalf("proxies = %v", loaded.Proxies)
	}
	if loaded.FirstByteSeconds != 45 || loaded.BudgetSeconds != 200 {
		t.Fatalf("timeouts = %d/%d", loaded.FirstByteSeconds, loaded.BudgetSeconds)
	}
	if len(loaded.Mirrors) != len(defaultMirrorURLs) {
		t.Fatalf("mirrors = %v", loaded.Mirrors)
	}
}

func TestLoadSettingsFallsBackWhenMissingOrBroken(t *testing.T) {
	dir := t.TempDir()
	missing := loadSettings(filepath.Join(dir, "absent.json"))
	if missing.Port != 13339 || missing.GatewayKey != defaultGatewayKey {
		t.Fatalf("unexpected defaults: %+v", missing)
	}

	broken := filepath.Join(dir, "broken.json")
	if err := writeFileString(broken, "{not json"); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	recovered := loadSettings(broken)
	if recovered.Outbound != outboundDirect || recovered.BudgetSeconds != 180 {
		t.Fatalf("unexpected recovery: %+v", recovered)
	}
}

func TestSettingsNormalizedClampsInvalidValues(t *testing.T) {
	settings := uiSettings{Port: 0, Outbound: "weird", FirstByteSeconds: -5, BudgetSeconds: 3, GatewayKey: "  "}
	normalized := settings.normalized()
	if normalized.Port != 13339 {
		t.Fatalf("port = %d", normalized.Port)
	}
	if normalized.Outbound != outboundDirect {
		t.Fatalf("outbound = %q", normalized.Outbound)
	}
	if normalized.FirstByteSeconds != 30 {
		t.Fatalf("first byte = %d", normalized.FirstByteSeconds)
	}
	if normalized.BudgetSeconds < normalized.FirstByteSeconds {
		t.Fatalf("budget %d must not be below first byte %d", normalized.BudgetSeconds, normalized.FirstByteSeconds)
	}
	if normalized.GatewayKey != defaultGatewayKey {
		t.Fatalf("key = %q", normalized.GatewayKey)
	}
}

func TestUpstreamPoolPutsPrimaryFirstAndDeduplicates(t *testing.T) {
	cfg := config{project: projectSpec{upstream: "https://opencode.ai/zen"}}
	cfg.mirrors = []string{"https://opencode.ai/zen", "https://mirror.example.com/zen"}

	pool := cfg.upstreamPool()
	if len(pool) != 2 {
		t.Fatalf("pool = %v", pool)
	}
	if pool[0] != "https://opencode.ai/zen" {
		t.Fatalf("primary upstream must be first, got %v", pool)
	}
}

func TestPickUpstreamRotatesAndSkipsCooldown(t *testing.T) {
	cfg := config{project: projectSpec{upstream: "https://primary.example/zen"}}
	cfg.mirrors = []string{"https://a.example/zen", "https://b.example/zen"}
	gw := newGateway(cfg)

	seen := make(map[string]int)
	for i := 0; i < 6; i++ {
		seen[gw.pickUpstream()]++
	}
	if len(seen) != 3 {
		t.Fatalf("expected rotation across 3 upstreams, saw %v", seen)
	}

	// 两次失败后进入冷却，不应再被选中。
	gw.noteUpstreamResult("https://a.example/zen", false)
	gw.noteUpstreamResult("https://a.example/zen", false)
	for i := 0; i < 6; i++ {
		if got := gw.pickUpstream(); got == "https://a.example/zen" {
			t.Fatalf("cooled upstream was selected")
		}
	}

	// 成功一次即清除故障计数。
	gw.noteUpstreamResult("https://b.example/zen", false)
	gw.noteUpstreamResult("https://b.example/zen", true)
	gw.mirrorMu.Lock()
	_, blocked := gw.mirrorState["https://b.example/zen"]
	gw.mirrorMu.Unlock()
	if blocked {
		t.Fatalf("successful upstream should not stay in failure state")
	}
}

// 用户清空节点源并保存后，重新加载不得回填默认值。
func TestNormalizedKeepsEmptyPoolInput(t *testing.T) {
	s := defaultSettings()
	s.PoolInput = ""
	got := s.normalized()
	if strings.TrimSpace(got.PoolInput) != "" {
		t.Fatalf("空的节点源不应被回填，实际 %q", got.PoolInput)
	}
}

// 全新安装不再预填节点池源（公共推荐源已移至 README）。
func TestDefaultSettingsPoolInputEmpty(t *testing.T) {
	s := defaultSettings()
	if strings.TrimSpace(s.PoolInput) != "" {
		t.Fatalf("新装不应预填节点池源，实际 %q", s.PoolInput)
	}
	if len(s.Mirrors) != 3 {
		t.Fatalf("应仅内置 3 条镜像，实际 %d: %v", len(s.Mirrors), s.Mirrors)
	}
}

func TestApplyEnvRespectsExistingEnvironment(t *testing.T) {
	t.Setenv("CUSTOM_PROXIES", "socks5://existing:1080")
	t.Setenv("PORT", "")
	t.Setenv("MIRROR_URLS", "")
	t.Setenv("PROXY_FIRST_BYTE_TIMEOUT", "")
	t.Setenv("HARD_TIMEOUT", "")
	t.Setenv("PROXY_ORDER", "")

	settings := defaultSettings()
	settings.Outbound = outboundProxy
	settings.Proxies = []string{"socks5://127.0.0.1:7890"}
	settings.Port = 14000
	settings.FirstByteSeconds = 25
	settings.BudgetSeconds = 150
	settings.applyEnv()

	if got := envString("CUSTOM_PROXIES", ""); got != "socks5://existing:1080" {
		t.Fatalf("existing env must win, got %q", got)
	}
	if got := envInt("PORT", 0); got != 14000 {
		t.Fatalf("port = %d", got)
	}
	if got := envInt("PROXY_FIRST_BYTE_TIMEOUT", 0); got != 25000 {
		t.Fatalf("first byte ms = %d", got)
	}
	if got := envInt("HARD_TIMEOUT", 0); got != 150000 {
		t.Fatalf("budget ms = %d", got)
	}
	if got := envString("MIRROR_URLS", ""); !strings.Contains(got, "cmliussss.net") {
		t.Fatalf("mirrors = %q", got)
	}
}
