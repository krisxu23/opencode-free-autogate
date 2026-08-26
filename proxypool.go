package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	poolFetchTimeout = 20 * time.Second // 单个节点源链接的拉取超时
	poolRetryBackoff = 30 * time.Minute // 探活失败节点的重试间隔
)

// parsePoolSources 把多行/逗号分隔的节点源链接整理为去重后的 URL 列表。
func parsePoolSources(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == '\r' || r == ',' })
	seen := make(map[string]struct{})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		u := normalizeSourceURL(field)
		if u == "" {
			continue
		}
		if _, dup := seen[u]; dup {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}

// normalizeSourceURL 清理链接：去片段、把 github blob 页面转成 raw 直链。
func normalizeSourceURL(raw string) string {
	u := strings.TrimSpace(raw)
	if i := strings.Index(u, "#"); i >= 0 {
		u = strings.TrimSpace(u[:i])
	}
	u = strings.TrimSuffix(u, "?plain=1")
	const blobPrefix = "https://github.com/"
	if strings.HasPrefix(u, blobPrefix) {
		rest := strings.TrimPrefix(u, blobPrefix)
		if i := strings.Index(rest, "/blob/"); i > 0 {
			rest = rest[:i] + "/" + rest[i+len("/blob/"):]
			u = "https://raw.githubusercontent.com/" + rest
		}
	}
	return u
}

// parsePoolLine 解析节点源里的一行，支持常见格式：
// socks5://user:pass@host:port#名称、socks5://host:port、host:port、host:port:user:pass。
func parsePoolLine(line string) (slot, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
		return slot{}, false
	}
	if i := strings.Index(line, "#"); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	if line == "" {
		return slot{}, false
	}
	lower := strings.ToLower(line)
	switch {
	case strings.HasPrefix(lower, "socks5://"), strings.HasPrefix(lower, "socks5h://"),
		strings.HasPrefix(lower, "http://"), strings.HasPrefix(lower, "https://"):
		s, err := slotFromRawURL(line)
		return s, err == nil
	case strings.HasPrefix(lower, "socks://"):
		converted, err := convertSharedSOCKS(line)
		if err != nil {
			return slot{}, false
		}
		s, err := slotFromRawURL(converted)
		return s, err == nil
	default:
		parts := strings.Split(line, ":")
		if len(parts) < 2 {
			return slot{}, false
		}
		candidate := "socks5://" + line
		if len(parts) >= 4 {
			hostPort := parts[0] + ":" + parts[1]
			user := strings.Join(parts[2:len(parts)-1], ":")
			pass := parts[len(parts)-1]
			candidate = "socks5://" + url.UserPassword(user, pass).String() + "@" + hostPort
		}
		s, err := slotFromRawURL(candidate)
		return s, err == nil
	}
}

type poolProbeResult struct {
	slot    slot
	latency time.Duration
	ok      bool
}

// probeSlots 并发对候选节点做真实上游探活（经代理访问 opencode.ai）。
func probeSlots(ctx context.Context, g *gateway, items []slot) []poolProbeResult {
	results := make([]poolProbeResult, len(items))
	if len(items) == 0 {
		return results
	}
	// 并发路数与 chat 深检共用同一设置（「检测并发」，默认 32 路）。
	sem := make(chan struct{}, g.probeConcurrency())
	var wg sync.WaitGroup
	for i, item := range items {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if ctx.Err() != nil {
				return
			}
			started := time.Now()
			ok := g.probe(ctx, item)
			results[i] = poolProbeResult{slot: item, latency: time.Since(started), ok: ok}
		}()
	}
	wg.Wait()
	return results
}

// startPoolWatcher 初检主循环：连轴转，无轮间间隔——测完本轮立即重拉源
// 进入下一轮。唯一护栏：本轮零新候选（源内容没更新）时歇 30 秒，
// 防止空转打爆源站点。
func (g *gateway) startPoolWatcher(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		hadWork := g.refreshPool(ctx)
		if hadWork {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}

// refreshPool 一轮完整的「拉取 → 初检 → 入过关池」流程。
// 返回本轮是否有待测候选（false = 源暂无新内容，调用方可小憩防打爆源站）。
func (g *gateway) refreshPool(ctx context.Context) bool {
	urls := g.cfg.poolURLs
	if len(urls) == 0 {
		return false
	}

	existing := make(map[string]struct{})
	for _, addr := range g.slotAddresses(true) {
		existing[addr] = struct{}{}
	}
	for _, s := range g.freshSnapshot() { // 初检已过关待复检的：本轮不重复初检
		existing[s.addr] = struct{}{}
	}

	candidates, advLinks := g.fetchPoolSources(ctx, urls)

	// 订阅/源里出现新的高级协议链接时，合并进内嵌 sing-box（重建实例）。
	var freshAdv []string
	for _, link := range advLinks {
		g.advMu.Lock()
		_, known := g.advSeen[link]
		g.advMu.Unlock()
		if !known {
			freshAdv = append(freshAdv, link)
		}
	}
	// 高级链接的本地映射端口与普通代理合并进同一条初检队列：
	// 按用户设置的「检测并发」一路测完，不再分批串行。
	advAuto := g.ensureAdvancedBridge(ctx, freshAdv)

	fresh := make([]slot, 0, len(candidates)+len(advAuto))
	for _, s := range candidates {
		if _, dup := existing[s.addr]; dup {
			continue
		}
		if g.recentlyFailed(s.addr) {
			continue
		}
		fresh = append(fresh, s)
	}
	plainNew := len(fresh)
	fresh = append(fresh, advAuto...)
	skipped := len(candidates) - plainNew
	if len(fresh) > 0 {
		log.Printf("[初检] 本轮待测候选 %d 个（普通 %d / 高级映射 %d），%d 路并发逐个检测；失败冷却跳过 %d",
			len(fresh), plainNew, len(advAuto), g.probeConcurrency(), skipped)
	}

	passed, failed := 0, 0
	for _, result := range probeSlots(ctx, g, fresh) {
		if result.slot.addr == "" {
			continue
		}
		if !result.ok {
			g.noteFailed(result.slot.addr)
			failed++
			continue
		}
		passed++
		isAdv := strings.HasPrefix(result.slot.addr, "127.0.0.1:")
		// 初检只发内部过关池资格：不参与竞速、界面不显示，
		// 等流式复检通过后才转正为正式出口。
		g.addFresh(result.slot)
		tag := "[池+]"
		if isAdv {
			tag = "[高级+]"
		}
		log.Printf("%s %s (%dms)", tag, result.slot.addr, result.latency.Milliseconds())
		// 流式复检：初检一通过立即入队深检，不等整轮结束；
		// 队列打满则丢弃，该节点随后的小时级整体复检仍会覆盖。
		select {
		case g.deepQueue <- result.slot:
		default:
		}
	}
	if len(fresh) > 0 {
		log.Printf("[初检] 本轮完毕：通过 %d / 失败 %d（通过者已进入复检流水线）", passed, failed)
	}

	// 剪除失效节点的职责已移交复检：深检连不通即淘汰/移出（见 settleDeep），
	// 初检循环不再每轮对存量正式节点做 GET 复剪。

	log.Printf("[池] 本轮汇总：源 %d | 候选 %d（普通 %d / 高级映射 %d）| 初检通过 %d / 失败 %d | 待复检 %d | 正式池 %d",
		len(urls), len(candidates)+len(advAuto), plainNew, len(advAuto), passed, failed, g.freshCount(), g.customCount())
	return len(fresh) > 0
}

// fetchPoolSources 逐个拉取节点源，自动识别 JSON（amux 风格）、纯文本列表与
// base64 订阅（机场订阅）。返回普通代理槽位与高级协议链接两个列表。
// 直连失败时自动改经最多 2 个健康出口重试：源站点（GitHub raw、境外 API 等）
// 在本地直连不通时，出口池就是现成的通道——源获取与上游请求同一哲学。
func (g *gateway) fetchPoolSources(ctx context.Context, urls []string) ([]slot, []string) {
	client := &http.Client{Transport: controlTransport(poolFetchTimeout)}
	defer client.CloseIdleConnections()

	seen := make(map[string]struct{})
	var out []slot
	var advanced []string
	for _, raw := range urls {
		if ctx.Err() != nil {
			break
		}
		source := normalizeSourceURL(raw)
		if source == "" {
			continue
		}
		body, status, err := fetchViaClient(ctx, client, source)
		if err != nil {
			log.Printf("[池] 拉取失败 %s: %v", shortSource(source), err)
			body, status, err = g.fetchSourceViaExits(ctx, source)
		}
		if err != nil {
			continue
		}
		if status < 200 || status >= 300 {
			log.Printf("[池] 拉取失败 %s: status=%d", shortSource(source), status)
			continue
		}
		count := appendPoolBody(&out, &advanced, seen, body)
		log.Printf("[池] %s -> %d 条", shortSource(source), count)
	}
	return out, advanced
}

// fetchViaClient 用给定 client 拉取一个源，返回 body/status/error。
func fetchViaClient(ctx context.Context, client *http.Client, source string) ([]byte, int, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, poolFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, source, nil)
	if err != nil {
		return nil, 0, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	body, readErr := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	status := res.StatusCode
	res.Body.Close()
	if readErr != nil {
		return nil, status, readErr
	}
	return body, status, nil
}

// fetchSourceViaExits 直连失败后的兜底：等距抽样最多 2 个现有出口，逐个经出口
// 拉取源，任一成功即返回。全部失败时返回最后一次错误；池为空直接报无可用出口。
func (g *gateway) fetchSourceViaExits(ctx context.Context, source string) ([]byte, int, error) {
	pool := sampleEvenly(g.customSnapshot(), 2)
	lastErr := errors.New("无可用出口")
	for _, s := range pool {
		if ctx.Err() != nil || s.proxyURL == nil {
			continue
		}
		client := &http.Client{Transport: requestTransport(s.proxyURL, g.cfg.transportDialTimeout, g.cfg.tlsInsecure), Timeout: poolFetchTimeout}
		body, status, err := fetchViaClient(ctx, client, source)
		client.CloseIdleConnections()
		addr := s.addr
		if len(addr) > 42 {
			addr = addr[:39] + "..."
		}
		if err == nil && status >= 200 && status < 300 {
			log.Printf("[池] 经出口 %s 拉取成功", addr)
			return body, status, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("status=%d", status)
		}
	}
	return nil, 0, lastErr
}

// appendPoolBody 解析单个源的响应体，返回解析出的条数。
// 支持三种形态：JSON 数组、纯文本列表、base64 订阅（机场分享的整包 base64，
// 解码后是 vless/vmess/ss/hysteria2/trojan 等分享链接列表）。
func appendPoolBody(out *[]slot, advanced *[]string, seen map[string]struct{}, body []byte) int {
	trimmed := strings.TrimSpace(string(body))
	count := parsePoolText(out, advanced, seen, trimmed)
	if count == 0 {
		// 尝试按 base64 订阅解码（容忍换行/空白）。
		compact := strings.Join(strings.Fields(trimmed), "")
		if decoded, err := decodeBase64Any(compact); err == nil && strings.Contains(decoded, "://") {
			count = parsePoolText(out, advanced, seen, decoded)
		}
	}
	return count
}

// parsePoolText 逐行解析明文内容；高级协议链接单独收集，不当作 host:port。
func parsePoolText(out *[]slot, advanced *[]string, seen map[string]struct{}, text string) int {
	count := 0
	appendSlot := func(s slot, ok bool) {
		if !ok || s.addr == "" {
			return
		}
		if _, dup := seen[s.addr]; dup {
			return
		}
		seen[s.addr] = struct{}{}
		*out = append(*out, s)
		count++
	}

	if strings.HasPrefix(strings.TrimSpace(text), "[") {
		var items []proxyItem
		if err := json.Unmarshal([]byte(text), &items); err == nil {
			for _, item := range items {
				s, slotErr := slotFromProxy(item.Address, item.Protocol)
				appendSlot(s, slotErr == nil)
			}
			return count
		}
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		// 高级协议链接（vless/vmess/trojan/ss/hysteria2/hy2/tuic）：
		// 校验可解析后原链接收集，#名称保留用于展示。
		if schemeEnd := strings.Index(line, "://"); schemeEnd > 0 {
			if _, isAdv := isAdvancedScheme(line[:schemeEnd]); isAdv {
				if _, err := parseAdvancedNode(line); err == nil {
					count += len(appendAdvanced(advanced, seen, line))
				}
				continue
			}
		}
		s, ok := parsePoolLine(line)
		appendSlot(s, ok)
	}
	return count
}

// appendAdvanced 去重后收集高级链接，返回收集到的切片便于计数。
func appendAdvanced(advanced *[]string, seen map[string]struct{}, links ...string) []string {
	if advanced == nil {
		return nil
	}
	var added []string
	for _, link := range links {
		link = strings.TrimSpace(link)
		if link == "" {
			continue
		}
		key := "adv:" + link
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		added = append(added, link)
	}
	*advanced = append(*advanced, added...)
	return added
}

// recentlyFailed 报告节点是否在最近一轮失败冷却期内。
func (g *gateway) recentlyFailed(addr string) bool {
	g.poolFailedMu.Lock()
	defer g.poolFailedMu.Unlock()
	ts, ok := g.poolFailed[addr]
	if !ok {
		return false
	}
	if time.Since(ts) >= poolRetryBackoff {
		delete(g.poolFailed, addr)
		return false
	}
	return true
}

func (g *gateway) noteFailed(addr string) {
	g.poolFailedMu.Lock()
	defer g.poolFailedMu.Unlock()
	// 顺手清理过期项，避免 map 无限增长。
	now := time.Now()
	for key, ts := range g.poolFailed {
		if now.Sub(ts) >= poolRetryBackoff {
			delete(g.poolFailed, key)
		}
	}
	g.poolFailed[addr] = now
}

func (g *gateway) dropCustom(address string) bool {
	g.mu.Lock()
	removed := false
	for i, current := range g.custom {
		if current.addr == address {
			g.custom = append(g.custom[:i], g.custom[i+1:]...)
			removed = true
			break
		}
	}
	g.mu.Unlock()
	return removed
}

// noteCustomResult 记录节点在真实请求中的表现；连续 3 次失败自动移出池子，
// 避免死节点反复浪费重试预算。成功即清零。
// 手动节点不参与自动淘汰——失效时由请求轮换机制自然跳过。
func (g *gateway) noteCustomResult(address string, ok bool) {
	g.customFailsMu.Lock()
	defer g.customFailsMu.Unlock()
	if ok {
		delete(g.customFails, address)
		return
	}
	if g.isManual(address) {
		return
	}
	g.customFails[address]++
	if g.customFails[address] < 3 {
		return
	}
	delete(g.customFails, address)
	if g.dropCustom(address) {
		log.Printf("[池x] %s 连续失败，移出节点池", address)
	}
}

func (g *gateway) customSnapshot() []slot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]slot(nil), g.custom...)
}

// sampleEvenly 等距抽样，避免单轮探活过多候选。
func sampleEvenly[T any](items []T, limit int) []T {
	if len(items) <= limit {
		return items
	}
	out := make([]T, 0, limit)
	step := float64(len(items)) / float64(limit)
	for i := 0; i < limit; i++ {
		out = append(out, items[int(float64(i)*step)])
	}
	return out
}

func shortSource(source string) string {
	if len(source) <= 60 {
		return source
	}
	return source[:57] + "..."
}
