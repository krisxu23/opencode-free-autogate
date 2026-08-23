package main

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"
)

// defaultMirrorURLs 是初始化 config.json 时预填的公共镜像列表（CM 提供的三个 CDN 反代）。
var defaultMirrorURLs = []string{
	"https://opencode.ai.cmliussss.net/zen",
	"https://opencode.fastly.cmliussss.net/zen",
	"https://opencode.gcore.cmliussss.net/zen",
}

// normalizeMirrorBase 把用户输入的镜像地址规整为“协议://主机[/路径]”，
// 允许粘贴带 /v1 后缀、结尾斜杠或 #备注 的地址。
func normalizeMirrorBase(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if i := strings.Index(raw, "#"); i >= 0 {
		raw = strings.TrimSpace(raw[:i])
	}
	if raw == "" {
		return "", fmt.Errorf("镜像地址为空")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("无法解析 %q: %w", raw, err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("镜像 %q 需要以 http:// 或 https:// 开头", raw)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("镜像 %q 缺少主机名", raw)
	}
	path := strings.TrimRight(parsed.Path, "/")
	path = strings.TrimSuffix(path, "/v1")
	return scheme + "://" + parsed.Host + path, nil
}

// parseMirrorList 解析逗号/换行分隔的镜像列表，返回规整结果与逐条错误说明。
func parseMirrorList(raw string) ([]string, []string) {
	var good []string
	var bad []string
	seen := make(map[string]struct{})
	for _, field := range splitProxyInput(raw) {
		base, err := normalizeMirrorBase(field)
		if err != nil {
			bad = append(bad, err.Error())
			continue
		}
		if _, dup := seen[base]; dup {
			continue
		}
		seen[base] = struct{}{}
		good = append(good, base)
	}
	return good, bad
}

type mirrorHealth struct {
	fails        int
	blockedUntil time.Time
}

const (
	mirrorFailThreshold = 2
	mirrorCooldown      = 3 * time.Minute
)

// upstreamPool 返回参与轮询的上游列表：主上游 opencode.ai 永远在首位，其后是镜像。
func (c config) upstreamPool() []string {
	pool := make([]string, 0, len(c.mirrors)+1)
	pool = append(pool, strings.TrimRight(c.project.upstream, "/"))
	for _, mirror := range c.mirrors {
		if mirror != "" && mirror != pool[0] {
			pool = append(pool, mirror)
		}
	}
	return pool
}

// noteUpstreamResult 记录一次上游请求成败；连续失败达到阈值后临时冷却该上游。
func (g *gateway) noteUpstreamResult(base string, ok bool) {
	if base == "" || len(g.cfg.mirrors) == 0 {
		return
	}
	g.mirrorMu.Lock()
	defer g.mirrorMu.Unlock()
	if g.mirrorState == nil {
		g.mirrorState = make(map[string]*mirrorHealth)
	}
	state, exists := g.mirrorState[base]
	if ok {
		if exists {
			delete(g.mirrorState, base)
		}
		return
	}
	if !exists {
		state = &mirrorHealth{}
		g.mirrorState[base] = state
	}
	state.fails++
	if state.fails >= mirrorFailThreshold {
		state.blockedUntil = time.Now().Add(mirrorCooldown)
		state.fails = 0
		log.Printf("[镜] %s 连续失败，暂停 %s", shortUpstream(base), mirrorCooldown)
	}
}

// pickUpstream 在主上游与镜像之间轮询，跳过冷却中的条目；只有一个上游时直接返回它。
func (g *gateway) pickUpstream() string {
	pool := g.cfg.upstreamPool()
	if len(pool) <= 1 {
		return pool[0]
	}
	now := time.Now()
	g.mirrorMu.Lock()
	defer g.mirrorMu.Unlock()
	start := int((g.mirrorCursor.Add(1) - 1) % uint64(len(pool)))
	fallback := pool[start]
	var fallbackUntil time.Time
	for i := 0; i < len(pool); i++ {
		candidate := pool[(start+i)%len(pool)]
		state, exists := g.mirrorState[candidate]
		if !exists || now.After(state.blockedUntil) {
			return candidate
		}
		if fallbackUntil.IsZero() || state.blockedUntil.Before(fallbackUntil) {
			fallback = candidate
			fallbackUntil = state.blockedUntil
		}
	}
	return fallback
}

// shortUpstream 提取上游主机名，用于日志显示。
func shortUpstream(base string) string {
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" {
		return base
	}
	return parsed.Host
}

// requestUpstream 返回该请求实际使用的上游基址，空值时回落到项目默认上游。
func requestUpstream(fallback string, request upstreamRequest) string {
	if request.upstream != "" {
		return request.upstream
	}
	return fallback
}
