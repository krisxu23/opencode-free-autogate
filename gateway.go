package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	utls "github.com/metacubex/utls"
)

var (
	errAttemptTimeout  = errors.New("代理首字节超时")
	errRequestTimeout  = errors.New("请求总超时")
	errNoProxy         = errors.New("没有可用代理")
	errAllExitsFailed  = errors.New("本轮出口全部未交付（多为上游侧故障）")
	errStreamTruncated = errors.New("上游流在首个数据块前中断")
	errUpstreamGarbage = errors.New("流式请求返回了非流式内容")
)

const maxUpstreamBody = 64 << 20

type proxyItem struct {
	Address      string `json:"address"`
	Protocol     string `json:"protocol"`
	Latency      int    `json:"latency"`
	QualityGrade string `json:"quality_grade"`
	Status       string `json:"status"`
}

type slot struct {
	addr     string
	proxyURL *url.URL
}

type requestTrace struct {
	start          time.Time
	attempts       int
	proxies        map[string]struct{}
	finalProxy     string
	finalStatus    int
	upstream       string
	winnerUpstream string         // 竞速赢家实际使用的上游基址（完整 URL），供镜像记账
	tag            string         // 日志关联标签（会话短标识）：并发请求/竞速尝试靠它归属
	model          string         // 请求模型（收尾行展示）
	firstByte      time.Duration  // 响应头到达耗时；未知为 0（收尾行展示）
	usage          *usageSnapshot // 每请求 token 用量（收尾行展示）
}

// usageSnapshot 是单请求 token 用量快照，供收尾日志展示；日聚合仍走 usageStats。
type usageSnapshot struct {
	model      string
	prompt     int64
	completion int64
	cached     int64
}

// tagString 返回带前导空格的日志标签；无标签时返回空串，调用方直接
// 内嵌进格式串（"[S级]%s ..."）不产生双空格。
func (t *requestTrace) tagString() string {
	if t == nil || t.tag == "" {
		return ""
	}
	return " " + t.tag
}

// noteFirstByte 记录响应头到达耗时（每个请求只记第一个成功交付的响应）。
// 只在调度协程串行调用，竞速输家不会触碰。
func (t *requestTrace) noteFirstByte(resp *gatewayResponse) {
	if t == nil || resp == nil || resp.live == nil || t.firstByte != 0 {
		return
	}
	if wait := resp.live.headerAt.Sub(t.start); wait > 0 {
		t.firstByte = wait
	}
}

// sessionTag 取会话标识的短形态作日志标签：同会话同标签（多轮对话可
// 串联），派生不出时为空。
func sessionTag(session string) string {
	if session == "" {
		return ""
	}
	if i := strings.IndexByte(session, '_'); i >= 0 && len(session) > i+7 {
		return session[:i+7] // 前缀 + 6 位哈希，如 ses_ab12cd
	}
	if len(session) > 12 {
		return session[:12]
	}
	return session
}

func newRequestTrace() *requestTrace {
	return &requestTrace{start: time.Now(), proxies: make(map[string]struct{})}
}

// stickyEntry 是会话粘性记录：同会话后续请求优先复用上次胜出的出口，
// 上游的提示缓存按会话+前缀命中（同类项目实测粘性 99.8% vs 竞速 0%）。
type stickyEntry struct {
	addr string
	seen time.Time
}

const stickyTTL = 30 * time.Minute

func (g *gateway) stickyRemember(session, addr string) {
	if session == "" || addr == "" || addr == "直连" {
		return
	}
	g.stickyMu.Lock()
	defer g.stickyMu.Unlock()
	now := time.Now()
	// 超限时立即清理；未超限也每 256 次插入强制清一轮——只靠超限触发的话，
	// 持续涌入的新会话会让过期项永远等不到 len 回落，map 随请求量增长。
	g.stickyWrites++
	if len(g.sticky) > 1024 || g.stickyWrites%256 == 0 {
		for key, entry := range g.sticky {
			if now.Sub(entry.seen) > stickyTTL {
				delete(g.sticky, key)
			}
		}
	}
	g.sticky[session] = stickyEntry{addr: addr, seen: now}
}

func (g *gateway) stickyLookup(session string) (string, bool) {
	if session == "" {
		return "", false
	}
	g.stickyMu.Lock()
	defer g.stickyMu.Unlock()
	entry, ok := g.sticky[session]
	if !ok || time.Since(entry.seen) > stickyTTL {
		return "", false
	}
	return entry.addr, true
}

func (t *requestTrace) addAttempt(proxyName string) {
	t.attempts++
	t.proxies[proxyName] = struct{}{}
}

type gateway struct {
	cfg config

	mu         sync.RWMutex
	candidates []proxyItem
	slots      []slot
	custom     []slot

	fillMu        sync.Mutex
	refreshMu     sync.Mutex
	refillRunning atomic.Bool
	rr            atomic.Uint64
	rootContext   context.Context
	modelMu       sync.Mutex
	modelCache    *cachedModels

	mirrorMu     sync.Mutex
	mirrorState  map[string]*mirrorHealth
	mirrorCursor atomic.Uint64

	poolFailedMu sync.Mutex
	poolFailed   map[string]time.Time

	customFailsMu sync.Mutex
	customFails   map[string]int

	manualMu    sync.RWMutex
	manualAddrs map[string]struct{}

	advBridge *advancedBridge // 内嵌 sing-box：高级协议 → 本地 socks 端口
	advMu     sync.Mutex      // 保护下面三个字段与桥接的创建/重建
	advSeen   map[string]struct{}
	manualAdv []string // 手动高级链接（桥接重建时必须保留）

	exits *exitTracker // 出口近期表现记账：评分排序 + 坐板凳

	outage outageBreaker // 全局故障熔断：跨出口/镜像的聚集失败判定（见 outage.go）

	usage *usageStats // 每日用量统计（usage_stats.json，见 usage.go）

	deepQueue chan slot // 流式复检队列：初检一通过立即入队深检（见 deepprobe.go）

	freshMu     sync.Mutex
	freshPassed map[string]slot // 初检过关待复检的内部池：不参与竞速、界面不显示，复检通过才转正

	stickyMu     sync.Mutex
	sticky       map[string]stickyEntry // 会话 → 上次胜出出口（prompt 缓存友好）
	stickyWrites int                    // 插入计数：周期性强制清理过期项（见 stickyRemember）
	deepRunning  atomic.Bool            // 深检轮重叠保护

	iprep *ipReputer // IP 信誉体检（只体检正式池节点，结果供排序/展示）

	lastStatus     atomic.Int32 // 最近一次请求的最终状态码，供界面健康色
	lastTruncation atomic.Int64 // 最近一次流截断时刻（UnixNano），供界面健康色
}

func newGateway(cfg config) *gateway {
	g := &gateway{
		cfg:         cfg,
		poolFailed:  make(map[string]time.Time),
		customFails: make(map[string]int),
		manualAddrs: make(map[string]struct{}),
		advSeen:     make(map[string]struct{}),
		exits:       newExitTracker(),
		usage:       newUsageStats(filepath.Dir(configPath())),
		deepQueue:   make(chan slot, 2048),
		freshPassed: make(map[string]slot),
		sticky:      make(map[string]stickyEntry),
	}
	usageObserver = func(model string, prompt, completion, cached int64) {
		g.usage.Observe(model, prompt, completion, cached)
	}
	g.iprep = newIPReputer(func(addr string, res *ipRepResult) {
		g.exits.observeRep(addr, res)
	})
	g.iprep.refresh = func() []string {
		return append(g.slotAddresses(true), g.slotAddresses(false)...)
	}
	return g
}

// markManual 登记手动节点：这类节点永不参与自动探活剔除。
func (g *gateway) markManual(addr string) {
	g.manualMu.Lock()
	g.manualAddrs[addr] = struct{}{}
	g.manualMu.Unlock()
}

func (g *gateway) isManual(addr string) bool {
	g.manualMu.RLock()
	defer g.manualMu.RUnlock()
	_, ok := g.manualAddrs[addr]
	return ok
}

func (g *gateway) removeManual(addr string) {
	g.manualMu.Lock()
	delete(g.manualAddrs, addr)
	g.manualMu.Unlock()
}

func (g *gateway) start(ctx context.Context) {
	g.rootContext = ctx
	go func() {
		if g.cfg.usesPublicPool() {
			if err := g.loadCandidates(ctx); err != nil {
				log.Printf("[选] load failed: %v", err)
			}
			if err := g.fillSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[槽] initial fill failed: %v", err)
			}
		}
		if err := g.initCustomSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[兜底] initial fill failed: %v", err)
		}
		log.Printf("[门] 预热完成")
	}()

	if len(g.cfg.poolURLs) > 0 {
		log.Printf("[池] 在线节点池已启用：%d 个源", len(g.cfg.poolURLs))
		go g.startPoolWatcher(ctx)
	}

	if g.cfg.usesPublicPool() {
		go func() {
			ticker := time.NewTicker(g.cfg.refreshInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.refresh(ctx)
				}
			}
		}()
	}

	// 定期回收共享连接池里闲置的条目（代理下线后旧连接自然空闲超时）。
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sharedTransports.sweep(2 * time.Minute)
			}
		}
	}()

	// 每小时 chat 深检：穿透额度门，抓"网络通但配额枯竭"的假健康节点。
	go g.startDeepProber(ctx)
	// Windows 休眠恢复检测：清死连接 + 补槽。
	startWakeDetector(g, ctx)
	// UA 版本跟随官方发版，防固定版本号成为识别特征。
	startUASync(ctx)
	// IP 信誉体检：worker + 启动补查正式池（手动节点 + 复检转正节点）。
	g.iprep.start(ctx)
	go func() {
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return
		}
		for _, addr := range g.slotAddresses(true) {
			g.iprep.Request(addr)
		}
		for _, addr := range g.slotAddresses(false) {
			g.iprep.Request(addr)
		}
	}()
}

func (g *gateway) refresh(ctx context.Context) {
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	if err := g.loadCandidates(ctx); err != nil {
		log.Printf("[刷新] candidate load failed: %v", err)
		return
	}
	if err := g.fillSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[刷新] slot fill failed: %v", err)
	}
}

func (g *gateway) loadCandidates(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.cfg.proxyAPI, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Transport: controlTransport(5 * time.Second)}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("proxy API returned %d", res.StatusCode)
	}

	var all []proxyItem
	if err := json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(&all); err != nil {
		return err
	}
	filtered := make([]proxyItem, 0, len(all))
	for _, item := range all {
		if item.QualityGrade == "S" && item.Status == "active" {
			filtered = append(filtered, item)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].Latency < filtered[j].Latency })

	g.mu.Lock()
	g.candidates = filtered
	g.mu.Unlock()
	log.Printf("[选] %d S-grade candidates", len(filtered))
	return nil
}

func controlTransport(timeout time.Duration) *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          8,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
}

func (g *gateway) fillSlots(ctx context.Context) error {
	if !g.fillMu.TryLock() {
		return nil
	}
	defer g.fillMu.Unlock()

	needed := g.cfg.slotCount - g.slotCount()
	if needed <= 0 {
		return nil
	}

	batch := g.takeCandidates(needed + 3)
	if len(batch) == 0 {
		if err := g.loadCandidates(ctx); err != nil {
			return err
		}
		batch = g.takeCandidates(needed + 3)
	}
	if len(batch) == 0 {
		return errNoProxy
	}

	type probeResult struct {
		slot    slot
		latency time.Duration
		ok      bool
	}
	results := make(chan probeResult, len(batch))
	var wg sync.WaitGroup
	for _, item := range batch {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := slotFromProxy(item.Address, item.Protocol)
			if err != nil {
				results <- probeResult{}
				return
			}
			started := time.Now()
			ok := g.probe(ctx, s)
			results <- probeResult{slot: s, latency: time.Since(started), ok: ok}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	added := 0
	for result := range results {
		if !result.ok || g.slotCount() >= g.cfg.slotCount {
			continue
		}
		if g.addSlot(result.slot, false) {
			added++
			log.Printf("[探+] %s (%dms)", result.slot.addr, result.latency.Milliseconds())
			g.iprep.Request(result.slot.addr)
		}
	}
	log.Printf("[槽] %d/%d ready (added %d)", g.slotCount(), g.cfg.slotCount, added)
	return nil
}

// Close 释放内嵌 sing-box 等后台资源（进程退出时系统也会回收）。
func (g *gateway) Close() {
	if g.usage != nil {
		g.usage.Close()
	}
	g.advMu.Lock()
	bridge := g.advBridge
	g.advMu.Unlock()
	bridge.Close()
}

func (g *gateway) initCustomSlots(ctx context.Context) error {
	// 高级协议链接（vless/vmess/trojan/ss/hysteria2/tuic）先交给内嵌
	// sing-box 转成本地 socks5 端点，再与普通节点一起走原有流程。
	// 手动高级节点与节点池高级节点统一由 ensureAdvancedBridge 管理。
	manualLinks := collectAdvancedLinks(g.cfg.customProxies)
	g.advMu.Lock()
	g.manualAdv = manualLinks
	g.advMu.Unlock()
	_ = g.ensureAdvancedBridge(ctx, manualLinks) // 手动节点桥内直接入池，映射端口返回值忽略
	parsed := g.parseCustomProxies(g.cfg.customProxies)
	if len(parsed) == 0 {
		return nil
	}

	type probeResult struct {
		slot    slot
		latency time.Duration
		ok      bool
	}
	results := make(chan probeResult, len(parsed))
	var wg sync.WaitGroup
	for _, candidate := range parsed {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			ok := g.probe(ctx, candidate)
			results <- probeResult{slot: candidate, latency: time.Since(started), ok: ok}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	ready := 0
	for result := range results {
		// 手动节点无论探活结果一律入池、永不自动移除；
		// 失效时的表现就是请求失败并轮换到下一个节点。
		g.markManual(result.slot.addr)
		if g.addSlot(result.slot, true) {
			if result.ok {
				ready++
				log.Printf("[手动+] %s (%dms)", result.slot.addr, result.latency.Milliseconds())
			} else {
				log.Printf("[手动] %s 探活未通过，仍按配置保留", result.slot.addr)
			}
		} else if result.ok {
			ready++
		}
	}
	log.Printf("[兜底] %d/%d custom proxies ready", ready, len(parsed))
	return nil
}

// collectAdvancedLinks 从配置文本里提取高级协议链接（手动节点来源）。
func collectAdvancedLinks(raw string) []string {
	parts := strings.Split(raw, ",")
	var links []string
	seen := make(map[string]struct{})
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		schemeEnd := strings.Index(part, "://")
		if schemeEnd <= 0 {
			continue
		}
		if _, ok := isAdvancedScheme(part[:schemeEnd]); !ok {
			continue
		}
		if _, dup := seen[part]; dup {
			continue
		}
		if _, err := parseAdvancedNode(part); err != nil {
			log.Printf("[高级] 忽略 %s 链接: %v", strings.ToUpper(strings.SplitN(part, ":", 2)[0]), err)
			continue
		}
		seen[part] = struct{}{}
		links = append(links, part)
	}
	return links
}

func (g *gateway) parseCustomProxies(raw string) []slot {
	parts := strings.Split(raw, ",")
	result := make([]slot, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if schemeEnd := strings.Index(part, "://"); schemeEnd > 0 {
			if _, adv := isAdvancedScheme(part[:schemeEnd]); adv {
				g.advMu.Lock()
				bridge := g.advBridge
				g.advMu.Unlock()
				if bridge == nil {
					log.Printf("[配置] 高级节点暂不可用: %s", redactProxy(part))
					continue
				}
				local, ok := bridge.Links[part]
				if !ok {
					log.Printf("[配置] 高级节点暂不可用: %s", redactProxy(part))
					continue
				}
				s, err := slotFromRawURL("socks5://" + local)
				if err != nil {
					log.Printf("[配置] 本地桥接地址无效: %v", err)
					continue
				}
				result = append(result, s)
				continue
			}
		}
		s, err := slotFromRawURL(part)
		if err != nil {
			log.Printf("[配置] 忽略无效代理 %q: %v", redactProxy(part), err)
			continue
		}
		result = append(result, s)
	}
	return result
}

func slotFromProxy(address, protocol string) (slot, error) {
	scheme := "http"
	if strings.HasPrefix(strings.ToLower(protocol), "socks5") {
		scheme = "socks5h"
	}
	return slotFromRawURL(scheme + "://" + address)
}

func slotFromRawURL(raw string) (slot, error) {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return slot{}, err
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	case "socks5", "socks5h":
		u.Scheme = "socks5h"
	default:
		return slot{}, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return slot{}, errors.New("missing proxy host")
	}
	return slot{addr: u.Host, proxyURL: u}, nil
}

func redactProxy(raw string) string {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	u.User = nil
	return u.String()
}

// quotaExhausted 判断 429 响应体是否为免费额度耗尽（FreeUsageLimitError）。
// 网关全程携带官方 CLI UA，能通过上游的 UA 门禁，因此收到该类型即说明
// 出口 IP 的共享免费配额已枯竭——小时级内不会恢复，应坐长板凳。
// 用子串匹配而非结构化解析：对上游错误体的形状变化保持鲁棒。
func quotaExhausted(body []byte) bool {
	return bytes.Contains(body, []byte("FreeUsageLimitError"))
}

// parseRetryAfter 解析 Retry-After 头：接受秒数与 RFC-7231 HTTP 日期两种
// 形式，负值/不可解析返回 0。
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// resetHintRe 从限流响应体提取上游声明的恢复时间，如
// "resets in 13 hours" / "try again in 20 minutes"。
var resetHintRe = regexp.MustCompile(`(?i)(?:resets?|retry|try again)[^.\n]{0,40}?(\d+)\s*(second|sec|minute|min|hour|hr|day)`)

var resetHintUnits = map[string]time.Duration{
	"second": time.Second, "sec": time.Second,
	"minute": time.Minute, "min": time.Minute,
	"hour": time.Hour, "hr": time.Hour,
	"day": 24 * time.Hour,
}

func parseResetHint(body []byte) time.Duration {
	m := resetHintRe.FindSubmatch(body)
	if m == nil {
		return 0
	}
	n, err := strconv.Atoi(string(m[1]))
	if err != nil || n <= 0 {
		return 0
	}
	unit, ok := resetHintUnits[strings.ToLower(string(m[2]))]
	if !ok {
		return 0
	}
	return time.Duration(n) * unit
}

const (
	minQuotaBench  = 30 * time.Second // 短于该值的 Retry-After 没有板凳意义
	maxQuotaBench  = 24 * time.Hour   // 板凳上限：跨天重置的额度最多等一天
	creditBenchDur = 24 * time.Hour   // 计费类错误（402）：探活无法证伪充值，直接一天
)

// classifyUpstreamFailure 把上游错误响应翻译成出口板凳策略：
// 返回 (时长, 来源)。authoritative 表示时间来自上游声明——深检不提前戳；
// heuristic 是我们的推断——允许深检提前回归。
func classifyUpstreamFailure(status int, body []byte, retryAfter string) (time.Duration, benchSource) {
	if status == http.StatusPaymentRequired {
		return creditBenchDur, benchAuthoritative
	}
	if status != http.StatusTooManyRequests {
		return 0, benchNone
	}
	if d := parseRetryAfter(retryAfter); d > 0 {
		return clampBench(d), benchAuthoritative
	}
	if d := parseResetHint(body); d > 0 {
		return clampBench(d), benchAuthoritative
	}
	if quotaExhausted(body) {
		// 上游只说超了没说何时恢复：2 小时是推断值，允许深检提前推翻。
		return quotaBenchDuration, benchHeuristic
	}
	return 0, benchNone
}

func clampBench(d time.Duration) time.Duration {
	if d < minQuotaBench {
		d = minQuotaBench
	}
	if d > maxQuotaBench {
		d = maxQuotaBench
	}
	return d
}

// probeBody 构造深检请求体：max_tokens=1 的非流式对话，穿透上游真正的
// 额度门。模型名固定 big-pickle（长期在售，PROXY_PROBE_MODEL 可覆盖）。
func (g *gateway) probeBody() []byte {
	body, _ := json.Marshal(map[string]any{
		"model":      g.probeModelID(),
		"max_tokens": 1,
		"stream":     false,
		"messages":   []map[string]string{{"role": "user", "content": "ping"}},
	})
	return body
}

// probeModelID 返回深检固定使用的模型：big-pickle 是 opencode 长期在售的
// 免费模型，其他名字随时会下线。可用 PROXY_PROBE_MODEL 环境变量覆盖。
func (g *gateway) probeModelID() string {
	if g.cfg.probeModel != "" {
		return g.cfg.probeModel
	}
	return "big-pickle"
}

// probe 是常态探活：GET /v1/models，零模型配额，验证"网络路径通不通"。
// 额度门由每小时一轮的 chat 深检负责（startDeepProber）——两层各司其职，
// 避免每轮探活都烧真实对话配额。
func (g *gateway) probe(ctx context.Context, candidate slot) bool {
	deadline := time.Now().Add(g.cfg.probeTimeout)
	request := upstreamRequest{
		method:   http.MethodGet,
		path:     g.cfg.project.probePath,
		headers:  g.cfg.project.probeHeaders.Clone(),
		deadline: deadline,
	}
	started := time.Now()
	live, err := g.openUpstream(ctx, request, candidate.proxyURL, g.cfg.probeTimeout)
	if err != nil {
		g.exits.observeProbe(candidate.addr, 0, false)
		return false
	}
	status := live.response.StatusCode
	var body []byte
	var retryAfter string
	if status >= 400 {
		body, _ = live.readAll(deadline)
		retryAfter = live.response.Header.Get("Retry-After")
	}
	live.Close()
	if d, src := classifyUpstreamFailure(status, body, retryAfter); src != benchNone {
		// GET 探活也可能撞上额度门（共享 IP 的限流不分端点）：同样记账。
		g.exits.observeQuotaBurn(candidate.addr, d, src)
		return false
	}
	ok := status >= 200 && status < 400
	// 探活同时喂养出口评分：探活即预热连接，也为对冲排序提供延迟样本。
	g.exits.observeProbe(candidate.addr, time.Since(started), ok)
	return ok
}

// deepProbeOne 是 chat 深检：max_tokens=1 的真实对话，穿透上游额度门。
// 专抓"GET 探活通过但额度已枯竭"的假健康节点。
// 返回 ok=深检是否通过；hardFail=true 表示节点本身连不通/不可达
// （传输失败或持续 5xx），可安全淘汰；额度限流与模型名被拒属软失败，
// 节点可能没坏，只走既有板凳/忽略逻辑，不淘汰。
func (g *gateway) deepProbeOne(ctx context.Context, candidate slot) (ok bool, hardFail bool) {
	deadline := time.Now().Add(g.cfg.probeChatTimeout)
	headers := g.cfg.project.probeHeaders.Clone()
	headers.Set("Content-Type", "application/json")
	request := upstreamRequest{
		method:   http.MethodPost,
		path:     "/v1/chat/completions",
		headers:  headers,
		body:     g.probeBody(),
		deadline: deadline,
	}
	started := time.Now()
	live, err := g.openUpstream(ctx, request, candidate.proxyURL, g.cfg.probeChatTimeout)
	if err != nil {
		// 传输失败走普通失败阶梯，不算额度问题。
		g.exits.observeProbe(candidate.addr, 0, false)
		return false, true
	}
	status := live.response.StatusCode
	var body []byte
	var retryAfter string
	if status >= 400 {
		body, _ = live.readAll(deadline)
		retryAfter = live.response.Header.Get("Retry-After")
	}
	live.Close()
	if status == http.StatusNotFound || status == http.StatusBadRequest {
		// 模型名被上游拒绝是网关侧配置问题（列表里有 ≠ chat 门放行），
		// 不是出口健康问题：只报告不记账，避免一个坏模型名全员误伤。
		log.Printf("[深检] %s 拒绝模型 %s（HTTP %d），不计出口失败", candidate.addr, g.probeModelID(), status)
		return false, false
	}
	if d, src := classifyUpstreamFailure(status, body, retryAfter); src != benchNone {
		g.exits.observeQuotaBurn(candidate.addr, d, src)
		return false, false
	}
	ok = status >= 200 && status < 300
	g.exits.observeProbe(candidate.addr, time.Since(started), ok)
	return ok, !ok
}

// addFresh 初检通过：进内部过关池等待复检。此池不参与竞速、界面不显示。
func (g *gateway) addFresh(s slot) {
	g.freshMu.Lock()
	defer g.freshMu.Unlock()
	if g.freshPassed == nil {
		g.freshPassed = make(map[string]slot)
	}
	g.freshPassed[s.addr] = s
}

func (g *gateway) hasFresh(addr string) bool {
	g.freshMu.Lock()
	defer g.freshMu.Unlock()
	_, ok := g.freshPassed[addr]
	return ok
}

func (g *gateway) removeFresh(addr string) bool {
	g.freshMu.Lock()
	defer g.freshMu.Unlock()
	if _, ok := g.freshPassed[addr]; !ok {
		return false
	}
	delete(g.freshPassed, addr)
	return true
}

func (g *gateway) freshSnapshot() []slot {
	g.freshMu.Lock()
	defer g.freshMu.Unlock()
	out := make([]slot, 0, len(g.freshPassed))
	for _, s := range g.freshPassed {
		out = append(out, s)
	}
	return out
}

func (g *gateway) freshCount() int {
	g.freshMu.Lock()
	defer g.freshMu.Unlock()
	return len(g.freshPassed)
}

// settleDeep 依据复检结果安置节点：
//   - 通过：内部过关池 → 晋升正式池（此刻起参与竞速）；
//   - 硬失败（连不通）：候选直接淘汰，正式节点移出正式池；
//   - 软失败（额度限流/模型被拒）：不动池位，交给既有板凳/忽略逻辑。
//
// 手动节点永不自动移除。
func (g *gateway) settleDeep(s slot, ok, hardFail bool) {
	if ok {
		if g.removeFresh(s.addr) {
			if g.addSlot(s, true) {
				log.Printf("[转正] %s（复检通过，进入正式池）", s.addr)
				// 连接预热：转正时立即建立 TCP+TLS 连接并缓存到 transportPool，
				// 确保第一个用户请求命中热连接，省掉冷启动的握手延迟。
				go g.prewarmExit(s)
				// IP 信誉体检：正式池节点转正即排队体检（7 天缓存，fail-open）。
				g.iprep.Request(s.addr)
			}
		}
		return
	}
	if !hardFail {
		return // 软失败：板凳已记账，保留池位
	}
	if g.removeFresh(s.addr) {
		log.Printf("[淘汰] %s（复检连不通）", s.addr)
		return
	}
	if !g.isManual(s.addr) && g.dropCustom(s.addr) {
		log.Printf("[池-] %s（复检连不通）", s.addr)
	}
}

// prewarmExit 为指定出口预热连接：从 transportPool 获取（或创建）Transport，
// 这会触发底层 TCP+TLS 揜手并将连接缓存。后续真实请求直接复用热连接，
// 省掉首次请求的握手延迟（通常 100-300ms）。预热失败不影响功能，只是
// 回退到按需建连。
func (g *gateway) prewarmExit(s slot) {
	proxyURL := s.proxyURL
	transport := sharedTransports.get(proxyURL)
	// 发一个轻量 HEAD 请求触发实际建连（TLS 握手），完成后立即关闭响应体。
	// 目标用 /healthz 或根路径均可——关键是触发 transport 建立底层连接。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, g.cfg.project.upstream+"/v1/models", nil)
	if err != nil {
		return
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return // 预热失败静默忽略，后续请求按需建连
	}
	resp.Body.Close()
}

func (g *gateway) takeCandidates(limit int) []proxyItem {
	g.mu.Lock()
	defer g.mu.Unlock()
	used := make(map[string]struct{}, len(g.slots))
	for _, current := range g.slots {
		used[current.addr] = struct{}{}
	}

	result := make([]proxyItem, 0, limit)
	for len(g.candidates) > 0 && len(result) < limit {
		item := g.candidates[0]
		g.candidates = g.candidates[1:]
		if _, exists := used[item.Address]; exists {
			continue
		}
		used[item.Address] = struct{}{}
		result = append(result, item)
	}
	return result
}

func (g *gateway) addSlot(candidate slot, custom bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	target := &g.slots
	if custom {
		target = &g.custom
	}
	for _, existing := range *target {
		if existing.addr == candidate.addr {
			return false
		}
	}
	*target = append(*target, candidate)
	return true
}

func (g *gateway) slotCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.slots)
}

func (g *gateway) slotAddresses(custom bool) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	source := g.slots
	if custom {
		source = g.custom
	}
	result := make([]string, 0, len(source))
	for _, current := range source {
		result = append(result, current.addr)
	}
	return result
}

// nextSlot 选取下一个代理槽位。session 非空时使用 rendezvous 哈希，
// 让同一会话在槽位存活期间固定同一出口（匿名通道按出口 IP 限流），
// 槽位增删只影响映射到该槽位的会话；session 为空时保持轮询行为。
func (g *gateway) nextSlot(custom bool, tried map[string]struct{}, session string, attempt int) (slot, bool) {
	g.mu.RLock()
	source := g.slots
	if custom {
		source = g.custom
	}
	snapshot := append([]slot(nil), source...)
	g.mu.RUnlock()
	if len(snapshot) == 0 {
		return slot{}, false
	}

	if session != "" {
		sort.SliceStable(snapshot, func(i, j int) bool {
			return rendezvousScore(session, snapshot[i].addr) > rendezvousScore(session, snapshot[j].addr)
		})
		if custom {
			return snapshot[attempt%len(snapshot)], true
		}
		for _, candidate := range snapshot {
			if _, exists := tried[candidate.addr]; !exists {
				return candidate, true
			}
		}
		return slot{}, false
	}

	start := int(g.rr.Add(1)-1) % len(snapshot)
	for offset := 0; offset < len(snapshot); offset++ {
		candidate := snapshot[(start+offset)%len(snapshot)]
		if custom {
			return candidate, true
		}
		if _, exists := tried[candidate.addr]; !exists {
			return candidate, true
		}
	}
	return slot{}, false
}

func rendezvousScore(session, addr string) uint64 {
	sum := sha256.Sum256([]byte(session + "\x00" + addr))
	return binary.BigEndian.Uint64(sum[:8])
}

func (g *gateway) dropSlot(address string) {
	g.mu.Lock()
	removed := false
	for i, current := range g.slots {
		if current.addr == address {
			g.slots = append(g.slots[:i], g.slots[i+1:]...)
			removed = true
			break
		}
	}
	remaining := len(g.slots)
	g.mu.Unlock()
	if !removed {
		return
	}
	log.Printf("[弃] %s -> %d/%d", address, remaining, g.cfg.slotCount)
	g.scheduleFill()
}

func (g *gateway) scheduleFill() {
	if g.rootContext == nil || !g.refillRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer g.refillRunning.Store(false)
		ctx, cancel := context.WithTimeout(g.rootContext, g.cfg.probeChatTimeout+5*time.Second)
		defer cancel()
		if err := g.fillSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[槽] refill failed: %v", err)
		}
	}()
}

type upstreamRequest struct {
	method    string
	path      string
	headers   http.Header
	body      []byte
	stream    bool
	nonStream bool
	session   string
	deadline  time.Time
	upstream  string // 本次请求使用的上游基址；空值表示项目默认上游
}

type gatewayResponse struct {
	status int
	header http.Header
	body   []byte
	live   *liveResponse
	origin *upstreamRequest // 产生本响应的原始请求：中流续写需要用它构造补尾请求
}

type liveResponse struct {
	response *http.Response
	cancel   context.CancelFunc
	once     sync.Once
	headerAt time.Time // 响应头到达时刻，用于胜出日志拆解排队/首数据耗时
}

func (r *liveResponse) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.response != nil && r.response.Body != nil {
			_ = r.response.Body.Close()
		}
		r.cancel()
	})
}

func (r *liveResponse) readAll(deadline time.Time) ([]byte, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		r.Close()
		return nil, errRequestTimeout
	}
	timedOut := make(chan struct{})
	timer := time.AfterFunc(remaining, func() {
		close(timedOut)
		r.cancel()
	})
	body, err := io.ReadAll(io.LimitReader(r.response.Body, maxUpstreamBody+1))
	stopped := timer.Stop()
	r.Close()
	if !stopped {
		<-timedOut
		return nil, errRequestTimeout
	}
	if err != nil {
		return nil, err
	}
	if len(body) > maxUpstreamBody {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", maxUpstreamBody)
	}
	return body, nil
}

func (g *gateway) openUpstream(ctx context.Context, request upstreamRequest, proxyURL *url.URL, maxWait time.Duration) (*liveResponse, error) {
	base := request.upstream
	if base == "" {
		base = g.cfg.project.upstream
	}
	target := strings.TrimRight(base, "/") + request.path
	wait := g.openTimeouts(request.deadline, maxWait)
	return openHTTP(ctx, request.method, target, request.headers, request.body, proxyURL, wait)
}

// openTimeouts 返回本次尝试的总体预算：首字节受限的请求取 min(剩余预算, 首字节上限)，
// 非流式请求取完整剩余预算。拨号/响应头的精确截止都由这个预算计时器兜底。
func (g *gateway) openTimeouts(deadline time.Time, firstByteLimit time.Duration) time.Duration {
	if firstByteLimit > 0 {
		return boundedWait(deadline, firstByteLimit)
	}
	return boundedWait(deadline, 0)
}

func openHTTP(ctx context.Context, method, target string, headers http.Header, body []byte, proxyURL *url.URL, wait time.Duration) (*liveResponse, error) {
	if wait <= 0 {
		return nil, errRequestTimeout
	}

	attemptContext, cancel := context.WithCancel(ctx)
	timedOut := make(chan struct{})
	timer := time.AfterFunc(wait, func() {
		close(timedOut)
		cancel()
	})

	// Transport 由共享连接池按代理复用；成功路径的连接归还与闲置回收由
	// transportpool 管理。失败路径仍会 CloseIdleConnections 终止被放弃的拨号。
	transport := sharedTransports.get(proxyURL)
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(attemptContext, method, target, bytes.NewReader(body))
	if err != nil {
		timer.Stop()
		cancel()
		return nil, err
	}
	req.Header = headers.Clone()
	req.Header.Del("Host")
	req.Header.Del("Content-Length")

	res, err := client.Do(req)
	stopped := timer.Stop()
	if err != nil {
		cancel()
		// Go 1.24.x 把拨号（含代理 CONNECT 握手）与请求 ctx 解耦：请求被
		// 取消后进行中的拨号仍会继续。CloseIdleConnections 会终止这些已被
		// 放弃的拨号（不影响使用中的连接），否则卡死的代理会泄漏连接到
		// CONNECT 超时（最长 1 分钟）。
		transport.CloseIdleConnections()
		select {
		case <-timedOut:
			return nil, errAttemptTimeout
		default:
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if !stopped {
		<-timedOut
		_ = res.Body.Close()
		cancel()
		transport.CloseIdleConnections()
		return nil, errAttemptTimeout
	}
	return &liveResponse{response: res, cancel: cancel, headerAt: time.Now()}, nil
}

func requestTransport(proxyURL *url.URL, dialTimeout time.Duration, tlsInsecure bool) *http.Transport {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:      true,
		DisableCompression:     true,
		MaxIdleConns:           128,
		MaxIdleConnsPerHost:    16,
		IdleConnTimeout:        120 * time.Second,
		TLSHandshakeTimeout:    dialTimeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			// INSECURE_TLS=1 时放行非标证书（自签镜像/代理环境），默认严格校验。
			InsecureSkipVerify: tlsInsecure,
		},
	}
	// 直连时使用 uTLS 模拟 Chrome 指纹，让网关的 TLS 握手外观与 Chrome 一致。
	// 经代理时不替换：代理负责上游 TLS，我们只与代理协商。
	if proxyURL == nil {
		transport.DialTLSContext = utlsDialContext(dialTimeout, tlsInsecure)
	} else {
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	return transport
}

// utlsDialContext 返回一个自定义的 TLS 拨号函数，使用 uTLS 模拟 Chrome 的 TLS 指纹。
// 这使得网关的 TLS ClientHello 看起来像 Chrome 浏览器，而不是 Go 标准库。
func utlsDialContext(dialTimeout time.Duration, tlsInsecure bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		// 1. 建立 TCP 连接
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		// 2. 从 addr 中提取 hostname（用于 SNI）
		hostname, _, err := net.SplitHostPort(addr)
		if err != nil {
			conn.Close()
			return nil, err
		}

		// 3. 用 uTLS 包装 TCP 连接，模拟 Chrome 的 TLS 指纹
		utlsConfig := &utls.Config{
			ServerName:         hostname,
			InsecureSkipVerify: tlsInsecure,
			MinVersion:         utls.VersionTLS12,
		}
		uConn := utls.UClient(conn, utlsConfig, utls.HelloChrome_Auto)

		// 4. 执行 TLS 握手
		if err := uConn.Handshake(); err != nil {
			uConn.Close()
			return nil, err
		}

		return uConn, nil
	}
}

// rotateToTail 把 items[i] 原地挪到队尾并保持其余元素的相对顺序。
func rotateToTail[T any](items []T, i int) []T {
	item := items[i]
	items = append(items, item)
	return append(items[:i], items[i+1:]...)
}

func boundedWait(deadline time.Time, maximum time.Duration) time.Duration {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}
	if maximum <= 0 || remaining < maximum {
		return remaining
	}
	return maximum
}

func (g *gateway) perform(ctx context.Context, request upstreamRequest, proxyURL *url.URL) (*gatewayResponse, error) {
	firstByteLimit := g.cfg.firstByteTimeout
	if request.nonStream {
		firstByteLimit = 0
	}
	live, err := g.openUpstream(ctx, request, proxyURL, firstByteLimit)
	if err != nil {
		if errors.Is(err, errAttemptTimeout) && time.Until(request.deadline) <= 0 {
			return nil, errRequestTimeout
		}
		return nil, err
	}
	status := live.response.StatusCode
	header := cloneEndToEndHeaders(live.response.Header)
	if request.stream && status < 400 {
		// 非 SSE 形态拦截（9router 经验）：流式请求却拿到 HTML 错误页或
		// 被上游忽略 stream 的 JSON 时，在 Content-Type 层就拦下换出口，
		// 不让垃圾进入转发管道（更早、比 SSE 行级清洗更稳）。Content-Type
		// 缺失时放行，交给 validateStreamHead 按首段数据判定。
		if ct := live.response.Header.Get("Content-Type"); ct != "" && !isStreamContentType(ct) {
			body, _ := live.readAll(request.deadline)
			garbageBase := request.upstream
			if garbageBase == "" {
				garbageBase = g.cfg.project.upstream
			}
			log.Printf("[形态] %s 流式请求返回非流式内容 Content-Type=%q（%d 字节），换出口重试",
				shortUpstream(garbageBase), ct, len(body))
			return nil, errUpstreamGarbage
		}
		// 胜利者验证门：等到首个真实 SSE 数据行才把响应交给调用方。
		// 200 + 空流（上游提前掐断的高发形态）在这里被拦下换下一出口，
		// 客户端永远不会收到一条没有任何内容的流。
		prefix, err := validateStreamHead(ctx, live, request.deadline)
		if err != nil {
			live.Close()
			return nil, err
		}
		live.response.Body = &prefixedBody{prefix: prefix, src: live.response.Body}
		return &gatewayResponse{status: status, header: header, live: live, origin: &request}, nil
	}
	body, err := live.readAll(request.deadline)
	if err != nil {
		return nil, err
	}
	return &gatewayResponse{status: status, header: header, body: body}, nil
}

// streamHeadPrefixLimit 是验证门最多预读的字节数：读满仍无数据行则按
// 非 SSE 形态放行，避免对非常规响应体误判。
const streamHeadPrefixLimit = 16 << 10

// prefixedBody 把验证阶段预读的首段数据接回流的开头，下游读取逻辑无感知。
type prefixedBody struct {
	prefix []byte
	src    io.ReadCloser
}

func (b *prefixedBody) Read(p []byte) (int, error) {
	if len(b.prefix) > 0 {
		n := copy(p, b.prefix)
		b.prefix = b.prefix[n:]
		return n, nil
	}
	return b.src.Read(p)
}

func (b *prefixedBody) Close() error { return b.src.Close() }

// validateStreamHead 持续读取直到出现第一个真实数据行（"data:"）。
// 在此之前 EOF/读错误视为空流（errStreamTruncated）；请求取消按 ctx 错误
// 返回（竞速输家被取消时不应记为截断）；请求截止按超时处理。
func validateStreamHead(ctx context.Context, live *liveResponse, deadline time.Time) ([]byte, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, errRequestTimeout
	}
	timedOut := make(chan struct{})
	timer := time.AfterFunc(remaining, func() {
		close(timedOut)
		live.cancel()
	})
	defer timer.Stop()

	buf := make([]byte, 0, 4<<10)
	chunk := make([]byte, 2<<10)
	for {
		n, err := live.response.Body.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			if bytes.Contains(buf, []byte("data:")) {
				if timer.Stop() {
					return buf, nil
				}
				<-timedOut
				return nil, errRequestTimeout
			}
			if len(buf) >= streamHeadPrefixLimit {
				return buf, nil
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errStreamTruncated
		}
	}
}

// dispatch 在主上游与镜像之间轮询：任一上游给出非重试状态即返回，
// 失败的上游会被记录并在连续失败后短暂冷却。
func (g *gateway) dispatch(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	pool := g.cfg.upstreamPool()
	attempts := len(pool)
	if attempts > 3 {
		attempts = 3
	}
	if request.upstream != "" {
		attempts = 1
	}

	var lastResponse *gatewayResponse
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		current := request
		if current.upstream == "" {
			current.upstream = g.pickUpstream()
		}
		if len(pool) > 1 {
			log.Printf("[镜]%s 上游: %s", trace.tagString(), shortUpstream(current.upstream))
		}
		trace.upstream = shortUpstream(current.upstream)
		response, err := g.dispatchOnce(ctx, current, trace)
		if err != nil {
			if isTerminalContextError(ctx, err) {
				// 请求取消/预算到期不是镜像的错：先于记账判断，防止误冷却。
				return nil, err
			}
			if g.punishExit("", shortUpstream(current.upstream)) {
				g.noteUpstreamResult(current.upstream, false)
			}
			lastErr = err
			continue
		}
		if !retryableStatus(response.status) {
			// 竞速模式下赢家可能走的是另一个镜像：以实际赢家为准记账。
			base := trace.winnerUpstream
			if base == "" {
				base = current.upstream
			}
			g.noteUpstreamResult(base, true)
			return response, nil
		}
		g.noteUpstreamResult(current.upstream, false)
		lastResponse = response
	}
	if lastResponse != nil {
		return lastResponse, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errNoProxy
}

func (g *gateway) dispatchOnce(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	if g.cfg.forceRelay {
		if g.cfg.zenKey == "" {
			return jsonGatewayResponse(http.StatusBadGateway, "FORCE_RELAY 但未配置 ZENPROXY_KEY"), nil
		}
		return g.dispatchZen(ctx, request, trace)
	}

	// 供应商出口开关（providers.go）：OpenCode 未启用节点池出口时跳过
	// 全部代理层直接直连；镜像轮换仍生效——那是供应商内部的上游分散，
	// 与出口 IP 无关。
	if !g.cfg.poolEnabledFor(providerOpenCode) {
		if g.cfg.project.directFallback {
			return g.dispatchDirect(ctx, request, trace)
		}
		return nil, errNoProxy
	}

	var last *gatewayResponse
	// 并行竞速模式：手动节点 + 轮选在线池节点 + 直连同时出发，最快返回者胜出。
	// 仅直连模式（PROXY_ORDER=direct）不参与竞速。
	if g.cfg.raceEnabled && g.customCount() > 0 {
		skip := false
		for _, layer := range g.cfg.orderedLayers() {
			if layer == layerDirect {
				skip = true
				break
			}
		}
		if !skip {
			return g.dispatchRace(ctx, request, trace)
		}
	}
	for _, layer := range g.cfg.orderedLayers() {
		var response *gatewayResponse
		var err error
		switch layer {
		case layerPublic:
			response, err = g.dispatchPublicLayer(ctx, request, trace)
		case layerZen:
			if g.cfg.zenKey == "" {
				continue
			}
			response, err = g.dispatchZen(ctx, request, trace)
		case layerCustom:
			if g.customCount() == 0 {
				continue
			}
			response, err = g.dispatchCustomLayer(ctx, request, trace)
		default:
			continue
		}
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			if !errors.Is(err, errNoProxy) && !errors.Is(err, errAllExitsFailed) {
				log.Printf("[层错]%s %s: %v", trace.tagString(), layer, err)
			}
			continue
		}
		if !retryableStatus(response.status) {
			return response, nil
		}
		last = response
	}

	if g.cfg.project.directFallback {
		return g.dispatchDirect(ctx, request, trace)
	}
	if last != nil {
		return last, nil
	}
	return nil, errNoProxy
}

func (g *gateway) dispatchPublicLayer(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	if g.slotCount() == 0 {
		fillContext, cancel := context.WithDeadline(ctx, request.deadline)
		_ = g.fillSlots(fillContext)
		cancel()
	}

	var last *gatewayResponse
	lastProxy := ""
	tried := make(map[string]struct{})
	for retry := 0; retry < g.cfg.slotRetries; retry++ {
		if err := requestBudgetError(ctx, request.deadline); err != nil {
			return nil, err
		}
		candidate, ok := g.nextSlot(false, tried, request.session, retry)
		if !ok {
			break
		}
		tried[candidate.addr] = struct{}{}
		trace.addAttempt(candidate.addr)
		log.Printf("[S级]%s %s (%d/%d)", trace.tagString(), candidate.addr, retry+1, g.cfg.slotRetries)
		response, err := g.perform(ctx, request, candidate.proxyURL)
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			log.Printf("[错]%s %s: %v", trace.tagString(), candidate.addr, err)
			g.dropSlot(candidate.addr)
			continue
		}
		if !retryableStatus(response.status) {
			trace.finalProxy = candidate.addr
			trace.noteFirstByte(response)
			return response, nil
		}
		log.Printf("[错码]%s %s 状态码 %d", trace.tagString(), candidate.addr, response.status)
		g.dropSlot(candidate.addr)
		last = response
		lastProxy = candidate.addr
	}
	if last != nil {
		trace.finalProxy = lastProxy
		return last, nil
	}
	return nil, errNoProxy
}

func (g *gateway) dispatchCustomLayer(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	maxRetries := g.cfg.customRetries
	if maxRetries == 0 {
		maxRetries = g.customCount()
	}
	var last *gatewayResponse
	lastProxy := ""
	for retry := 0; retry < maxRetries; retry++ {
		if err := requestBudgetError(ctx, request.deadline); err != nil {
			return nil, err
		}
		candidate, ok := g.nextSlot(true, nil, request.session, retry)
		if !ok {
			break
		}
		trace.addAttempt(candidate.addr)
		log.Printf("[自定义]%s %s (%d/%d)", trace.tagString(), candidate.addr, retry+1, maxRetries)
		response, err := g.perform(ctx, request, candidate.proxyURL)
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			g.noteCustomResult(candidate.addr, false)
			log.Printf("[错]%s %s: %v", trace.tagString(), candidate.addr, err)
			continue
		}
		if !retryableStatus(response.status) {
			g.noteCustomResult(candidate.addr, true)
			trace.finalProxy = candidate.addr
			trace.noteFirstByte(response)
			return response, nil
		}
		// 注意：429 等状态码说明节点能连通上游（只是暂时限流），不算节点故障。
		log.Printf("[错码]%s %s 状态码 %d", trace.tagString(), candidate.addr, response.status)
		last = response
		lastProxy = candidate.addr
	}
	if last != nil {
		trace.finalProxy = lastProxy
		return last, nil
	}
	return nil, errNoProxy
}

// raceExits 组装对冲竞速的出口序列：手动节点全部保留、自动节点轮选补足
// 宽度，剔除坐板凳节点（全部坐板凳时原样返回，保证仍有出口可用），再按
// 近期表现排序（截断少 > 首字节快 > 未知样本居后）。排序即出发顺序。
func (g *gateway) raceExits() []slot {
	all := g.customSnapshot()
	var manual, auto []slot
	for _, s := range all {
		if g.isManual(s.addr) {
			manual = append(manual, s)
		} else {
			auto = append(auto, s)
		}
	}
	width := g.cfg.raceWidth
	if width < 2 {
		width = 2
	}
	exits := make([]slot, 0, len(manual)+width)
	exits = append(exits, manual...)
	if len(auto) > 0 {
		start := int(g.rr.Add(1)-1) % len(auto)
		for i := 0; i < len(auto) && len(exits) < width+len(manual); i++ {
			exits = append(exits, auto[(start+i)%len(auto)])
		}
	}
	exits = g.exits.filterBenched(exits)
	return g.exits.rank(exits, g.cfg.preferredRegionSet())
}

// 对冲批次大小：首批少发保住指纹与配额，迟迟无人交付再加发。
// 注意：首批实际数量由 raceWidth 决定（用户设几路就全发），这里仅作为
// hedge 阶段性加发的步长；首批全发后 hedge 不再触发（已无剩余出口）。
const (
	hedgeBatch = 3 // 每一加发批次的出口数（仅在首批未覆盖全部出口时生效）
)

// stickyTruncSkip 出口发生流截断后，粘性暂时不再钉它的时长。客户端收到
// "Stream ended without finish_reason" 会自动重试；若粘性仍钉回刚被上游
// 掐断的同一条链路，重试大概率二次截断（生产日志实测连坑两次）。
const stickyTruncSkip = 5 * time.Minute

// dispatchRace 对冲竞速：出口按近期表现排序后分批出发——首批数量少，
// hedgeDelay 内无人交付首个真实数据块就加发下一批，首个数据块即赢家
// （perform 的验证门已保证赢家交过首数据块）。赢家出现后立即取消在途
// 输家并停止加发；每路出口独立 context，取消输家不影响赢家的流。
func (g *gateway) dispatchRace(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	exits := g.raceExits()

	// 镜像分散：出口轮流指到主上游与各镜像，一笔请求同时探多个镜像的
	// 队列，避免整个竞速被单一镜像的慢速拖死。起点跟随 dispatch 已轮换
	// 到的镜像，保持整体轮换公平。
	mirrors := g.cfg.upstreamPool()
	rotate := 0
	if request.upstream != "" {
		for i, base := range mirrors {
			if base == request.upstream {
				rotate = i
				break
			}
		}
	}
	ordinal := 0
	nextUpstream := func() string {
		base := mirrors[(rotate+ordinal)%len(mirrors)]
		ordinal++
		return base
	}

	type raceResult struct {
		addr          string
		isDirect      bool
		upstream      string
		resp          *gatewayResponse
		err           error
		elapsed       time.Duration
		headerElapsed time.Duration
	}
	results := make(chan raceResult, len(exits)+2)

	var inFlightMu sync.Mutex
	inFlight := make(map[int]context.CancelFunc)
	nextID := 0
	launch := func(addr string, direct bool, proxyURL *url.URL) {
		id := nextID
		nextID++
		attempt := request
		attempt.upstream = nextUpstream()
		if !direct {
			g.exits.enterExit(addr)
		}
		attemptCtx, cancel := context.WithCancel(ctx)
		inFlightMu.Lock()
		inFlight[id] = cancel
		inFlightMu.Unlock()
		go func() {
			started := time.Now()
			resp, err := g.perform(attemptCtx, attempt, proxyURL)
			elapsed := time.Since(started)
			var headerElapsed time.Duration
			if resp != nil && resp.live != nil && !resp.live.headerAt.IsZero() {
				if wait := resp.live.headerAt.Sub(started); wait > 0 {
					headerElapsed = wait
				}
			}
			inFlightMu.Lock()
			delete(inFlight, id)
			inFlightMu.Unlock()
			if !direct {
				g.exits.exitExit(addr)
			}
			results <- raceResult{addr: addr, isDirect: direct, upstream: attempt.upstream, resp: resp, err: err, elapsed: elapsed, headerElapsed: headerElapsed}
			// 释放本路 context；仍在传输的流式响应除外（那由 live.Close 终结，
			// 提前取消会掐断赢家/兜底的流）。
			if resp == nil || resp.live == nil {
				cancel()
			}
		}()
	}
	cancelInFlight := func() {
		inFlightMu.Lock()
		for _, cancel := range inFlight {
			cancel()
		}
		inFlightMu.Unlock()
	}

	launchedExits := 0
	directLaunched := false
	// launchWave 出发一批出口。观察窗内的出口（板凳刚到期）一次只放一路：
	// 若它已有一路在途则跳过排到队尾，让首批火力铺到不同出口上。
	launchWave := func(count int) {
		skipped := 0
		for count > 0 && launchedExits+skipped < len(exits) {
			candidate := exits[launchedExits]
			if g.exits.softLimited(candidate.addr) && g.exits.inFlightCount(candidate.addr) > 0 && skipped < len(exits)-launchedExits {
				exits = rotateToTail(exits, launchedExits)
				skipped++
				continue
			}
			trace.addAttempt(candidate.addr)
			launch(candidate.addr, false, candidate.proxyURL)
			launchedExits++
			count--
		}
	}
	launchedCount := func() int {
		if directLaunched {
			return launchedExits + 1
		}
		return launchedExits
	}

	var hedgeTimer *time.Timer
	var hedgeCh <-chan time.Time
	stopHedge := func() {
		if hedgeTimer != nil {
			hedgeTimer.Stop()
			hedgeTimer = nil
		}
		hedgeCh = nil
	}
	armHedge := func() {
		if launchedExits < len(exits) {
			hedgeTimer = time.NewTimer(g.cfg.hedgeDelay)
			hedgeCh = hedgeTimer.C
			return
		}
		stopHedge()
	}

	const directAddr = "直连"
	// 会话粘性：同会话优先复用上次胜出的出口——上游提示缓存按会话前缀
	// 命中（同类项目实测粘性 99.8% vs 竞速 0%），换出口等于缓存全冷。
	// 粘性出口排到队首且首批只发它一路：快速交付就是零浪费的一击，
	// 失败则照常按对冲阶梯升级，不影响可用性。
	// 首批全发：用户设了 raceWidth 就全部并发出发，不再分批等待。
	// 粘性模式例外：只发粘性出口一路，快速命中缓存；失败再按对冲阶梯升级。
	firstWave := len(exits)
	if g.cfg.stickyEnabled {
		if addr, ok := g.stickyLookup(request.session); ok && !g.exits.recentlyTruncated(addr, stickyTruncSkip) {
			for i := range exits {
				if exits[i].addr == addr {
					pinned := exits[i]
					exits = append(exits[:i], exits[i+1:]...)
					exits = append([]slot{pinned}, exits...)
					firstWave = 1
					log.Printf("[粘性]%s %s 钉住 %s（首批单发，吃上游提示缓存）", trace.tagString(), request.session, addr)
					break
				}
			}
		}
	}
	launchWave(firstWave)
	if g.cfg.project.directFallback {
		trace.addAttempt(directAddr)
		launch(directAddr, true, nil)
		directLaunched = true
	}
	armHedge()
	log.Printf("[竞速]%s 对冲竞速：%d 个出口待发，已发 %d 路（含直连）", trace.tagString(), len(exits), launchedCount())

	var last *gatewayResponse
	var lastAddr string
	var lastUpstream string
	received := 0
	for received < launchedCount() || hedgeCh != nil {
		select {
		case res := <-results:
			received++
			if res.err != nil {
				if ctx.Err() != nil {
					cancelInFlight()
					stopHedge()
					return nil, res.err
				}
				if !res.isDirect && g.punishExit(res.addr, shortUpstream(res.upstream)) {
					g.noteCustomResult(res.addr, false)
					if errors.Is(res.err, errStreamTruncated) {
						g.exits.observeTruncation(res.addr)
						// 空流是上游侧行为，镜像一并记账。
						g.noteUpstreamResult(res.upstream, false)
						log.Printf("[流截断]%s %s 返回空流，换下一出口（镜像 %s）", trace.tagString(), res.addr, shortUpstream(res.upstream))
					} else {
						g.exits.observeFail(res.addr)
					}
				}
				continue
			}
			if retryableStatus(res.resp.status) {
				// 额度受限（429/402）：按上游声明或推断给出口坐板凳。
				// 上游声明的时间是权威值，深检不会提前戳；推断值允许提前回归。
				if !res.isDirect {
					if d, src := classifyUpstreamFailure(res.resp.status, res.resp.body, res.resp.header.Get("Retry-After")); src != benchNone {
						g.exits.observeQuotaBurn(res.addr, d, src)
						log.Printf("[限流]%s %s 额度受限，暂停 %s（%s）", trace.tagString(), res.addr, d.Truncate(time.Second), benchSourceName(src))
					}
				}
				// 限流/5xx 不算赢家；留第一个作为全军覆没时的兜底，重复的直接关闭。
				if last == nil {
					last = res.resp
					lastAddr = res.addr
					lastUpstream = res.upstream
				} else {
					res.resp.live.Close()
				}
				continue
			}
			// 赢家出现（已通过首数据块验证）：取消在途输家，停止加发；
			// 迟到的响应由收割协程关闭，防止泄漏。
			cancelInFlight()
			stopHedge()
			if last != nil {
				last.live.Close()
				last = nil
			}
			if !res.isDirect {
				g.noteCustomResult(res.addr, true)
				g.stickyRemember(request.session, res.addr)
				g.exits.observeWin(res.addr, res.elapsed)
			}
			trace.finalProxy = res.addr
			trace.upstream = shortUpstream(res.upstream)
			trace.winnerUpstream = res.upstream
			trace.noteFirstByte(res.resp)
			if res.resp.live != nil && res.headerElapsed > 0 {
				log.Printf("[竞速] 胜出: %s | 镜像:%s | 总耗时 %s（到响应头 %s + 到首数据 %s）",
					res.addr, trace.upstream,
					res.elapsed.Round(time.Millisecond),
					res.headerElapsed.Round(time.Millisecond),
					(res.elapsed - res.headerElapsed).Round(time.Millisecond))
			} else {
				log.Printf("[竞速] 胜出: %s (%s)", res.addr, res.elapsed.Round(time.Millisecond))
			}
			go func() {
				deadline := time.After(40 * time.Second) // 覆盖最长的首字节超时窗口
				for {
					select {
					case extra := <-results:
						if extra.resp != nil {
							extra.resp.live.Close()
						}
					case <-deadline:
						return
					}
				}
			}()
			return res.resp, nil
		case <-hedgeCh:
			launchWave(hedgeBatch)
			armHedge()
		case <-ctx.Done():
			cancelInFlight()
			stopHedge()
			return nil, ctx.Err()
		}
	}
	if last != nil {
		log.Printf("[竞速]%s 本轮 %d 个出口均未交付可用响应，交出可重试兜底: %s（正式池 %d 个仍在册）", trace.tagString(),
			launchedCount(), lastAddr, g.customCount())
		trace.finalProxy = lastAddr
		trace.upstream = shortUpstream(lastUpstream)
		trace.winnerUpstream = lastUpstream
		return last, nil
	}
	// 区分「池子空了」和「池子有节点但这一轮全部未交付」：后者绝大多数是
	// 上游/链路侧问题，不该向用户报「没有可用代理」。
	if g.customCount() > 0 {
		log.Printf("[竞速]%s 本轮 %d 路全部失败，但正式池仍有 %d 个节点在册——按上游侧故障处理，稍后重试", trace.tagString(),
			launchedCount(), g.customCount())
		return nil, errAllExitsFailed
	}
	return nil, errNoProxy
}

// punishExit 判定一次真实失败是否应记到具体出口/镜像头上。
// 全局熔断期间只留证据不惩罚——上游级故障不该由健康出口连坐买单。
func (g *gateway) punishExit(exit, mirror string) bool {
	g.outage.record(exit, mirror)
	return !g.outage.Tripped()
}

// absorbBackoff 吸收模式重试间隔：平时固定短间隔快速换道；全局熔断期间
// 指数退避，停止对故障中的上游风暴式轰炸。
func (g *gateway) absorbBackoff(attempt int) time.Duration {
	if !g.outage.Tripped() {
		return absorbRetryDelay
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	delay := absorbRetryDelay << uint(shift)
	if delay <= 0 || delay > 10*time.Second {
		return 10 * time.Second
	}
	return delay
}

// noteStreamTruncation 记一次流中途夭折：出口记截断降权，所属镜像按失败记账。
func (g *gateway) noteStreamTruncation(addr, upstream string) {
	if !g.punishExit(addr, shortUpstream(upstream)) {
		return // 全局熔断期间只留证据不降权
	}
	g.lastTruncation.Store(time.Now().UnixNano())
	if trackableExit(addr) {
		g.exits.observeTruncation(addr)
	}
	if upstream != "" {
		g.noteUpstreamResult(upstream, false)
	}
}

func (g *gateway) dispatchZen(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	retries := g.cfg.zenRetries
	if retries < 1 {
		retries = 1
	}
	var last *gatewayResponse
	for retry := 0; retry < retries; retry++ {
		if err := requestBudgetError(ctx, request.deadline); err != nil {
			return nil, err
		}
		trace.addAttempt("ZenProxy")
		log.Printf("[ZenProxy]%s (%d/%d)", trace.tagString(), retry+1, retries)
		response, err := g.performRelay(ctx, request)
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			log.Printf("[ZenProxy]%s 错误: %v", trace.tagString(), err)
			continue
		}
		if !retryableStatus(response.status) {
			trace.finalProxy = "ZenProxy"
			trace.noteFirstByte(response)
			return response, nil
		}
		log.Printf("[ZenProxy]%s 状态码 %d，重试", trace.tagString(), response.status)
		last = response
	}
	if last != nil {
		trace.finalProxy = "ZenProxy"
		return last, nil
	}
	return nil, errNoProxy
}

func (g *gateway) performRelay(ctx context.Context, request upstreamRequest) (*gatewayResponse, error) {
	relay, err := url.Parse(g.cfg.zenRelay)
	if err != nil {
		return nil, err
	}
	upstream := request.upstream
	if upstream == "" {
		upstream = g.cfg.project.upstream
	}
	target := strings.TrimRight(upstream, "/") + request.path
	query := relay.Query()
	query.Set("api_key", g.cfg.zenKey)
	query.Set("url", target)
	query.Set("method", request.method)
	relay.RawQuery = query.Encode()

	headers := request.headers.Clone()
	headers.Del("Host")
	headers.Del("Content-Length")
	headers.Del("Authorization")
	firstByteLimit := g.cfg.firstByteTimeout
	if request.nonStream {
		firstByteLimit = 0
	}
	wait := g.openTimeouts(request.deadline, firstByteLimit)
	live, err := openHTTP(ctx, http.MethodPost, relay.String(), headers, request.body, nil, wait)
	if err != nil {
		if errors.Is(err, errAttemptTimeout) && time.Until(request.deadline) <= 0 {
			return nil, errRequestTimeout
		}
		return nil, err
	}
	status := live.response.StatusCode
	header := cloneEndToEndHeaders(live.response.Header)
	if request.stream && status < 400 {
		// 与代理层同一道非 SSE 形态拦截：中继返回 HTML/JSON 时不进管道。
		if ct := live.response.Header.Get("Content-Type"); ct != "" && !isStreamContentType(ct) {
			body, _ := live.readAll(request.deadline)
			log.Printf("[形态] 中继流式请求返回非流式内容 Content-Type=%q（%d 字节），换道重试", ct, len(body))
			return nil, errUpstreamGarbage
		}
		// 与代理层同一道胜利者验证门：等到首个真实 SSE 数据行才交付，
		// 中继返回 200 空流时同样拦下重试，客户端不会收到无内容响应。
		prefix, err := validateStreamHead(ctx, live, request.deadline)
		if err != nil {
			live.Close()
			return nil, err
		}
		live.response.Body = &prefixedBody{prefix: prefix, src: live.response.Body}
		return &gatewayResponse{status: status, header: header, live: live, origin: &request}, nil
	}
	body, err := live.readAll(request.deadline)
	if err != nil {
		return nil, err
	}
	return &gatewayResponse{status: status, header: header, body: body}, nil
}

func (g *gateway) dispatchDirect(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	trace.addAttempt("direct")
	log.Printf("[直连]%s directly connecting to upstream", trace.tagString())
	response, err := g.perform(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	trace.finalProxy = "direct"
	trace.noteFirstByte(response)
	return response, nil
}

func (g *gateway) customCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.custom)
}

func requestBudgetError(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if time.Until(deadline) <= 0 {
		return errRequestTimeout
	}
	return nil
}

func isTerminalContextError(ctx context.Context, err error) bool {
	return errors.Is(err, errRequestTimeout) || ctx.Err() != nil
}

func retryableStatus(status int) bool {
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests {
		return true
	}
	return status >= 500 && status <= 599
}

// isStreamContentType 判定响应是否为 SSE 流。空 Content-Type 不在此判定
// （交由 validateStreamHead 按首段数据把关），其余非 event-stream 形态
// （HTML 错误页、被忽略 stream 的 JSON）视为垃圾拦截。
func isStreamContentType(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "text/event-stream")
}

func jsonGatewayResponse(status int, message string) *gatewayResponse {
	body, _ := json.Marshal(map[string]string{"error": message})
	return &gatewayResponse{
		status: status,
		header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
		body:   body,
	}
}

func cloneEndToEndHeaders(source http.Header) http.Header {
	result := make(http.Header, len(source))
	for key, values := range source {
		if hopByHopHeader(key) {
			continue
		}
		result[key] = append([]string(nil), values...)
	}
	return result
}

func hopByHopHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	default:
		return false
	}
}
