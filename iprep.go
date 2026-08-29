package main

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// IP 信誉体检（单权威源：iprisk.top）。
//
// iprisk.top 聚合 16 个独立数据源（Scamalytics/IPQualityScore/AbuseIPDB/
// Spamhaus/Tor 出口列表等）给出 0-100 纯净度评分，页面服务端渲染、无反爬、
// 无需任何 key。没有官方 API，这里抓取查询页并解析"纯净度评分为 N/100"——
// 解析器很小，失败按源退避处理（fail-open），不会拖垮网关。
//
// 设计约定：
//   - 只体检正式池节点（转正时查一次 + 启动补查 + 每日重查），压力极低；
//   - 结果缓存 7 天（页面数据变化很慢），失败不缓存、退避后自动重试；
//   - 信誉是排序先验不是判决：只影响出场顺序，绝不单独剔除节点。

const (
	ipRepTTL          = 7 * 24 * time.Hour // 体检结果缓存期
	ipRepInterval     = 2 * time.Second    // 串行 worker 相邻请求间隔（对第三方礼貌）
	ipRepResolveTTL   = time.Hour          // 域名出口 → IP 的解析缓存
	ipRepSourceOff    = 30 * time.Minute   // 连续失败后的源退避
	ipRepSourceFails  = 3                  // 触发退避的连续失败次数
	ipRepTimeout      = 20 * time.Second   // 单次抓取超时
	ipRepRefreshEvery = 24 * time.Hour     // 正式池重查周期
	ipRepMaxCache     = 4096               // 结果缓存上限
	repUnknown        = -1                 // Score 取值：未体检/体检失败
)

// ipRepResult 是一次体检的结果。
type ipRepResult struct {
	IP      string
	Region  string // 国家码（US/JP/...），地区偏好的依据
	ASN     string // "AS15169 Google" 短形态
	Score   int    // 0-100 纯净度（越高越干净）；repUnknown = 未体检
	Grade   string // A/B/C/D
	Checked time.Time
}

// gradeOf 信誉分级（对齐 iprisk.top 的分档语义）：
// A ≥85 高纯净；B ≥70 可用；C ≥55 已有平台标记；D <55 污染/黑名单。
func gradeOf(score int) string {
	switch {
	case score >= 85:
		return "A"
	case score >= 70:
		return "B"
	case score >= 55:
		return "C"
	default:
		return "D"
	}
}

type ipReputer struct {
	mu      sync.Mutex
	cache   map[string]*ipRepResult // key: 出口 addr
	hostIP  map[string]hostIPEntry  // 域名出口 → IP 解析缓存
	pending map[string]bool
	queue   chan string
	client  *http.Client
	sink    func(addr string, res *ipRepResult) // 结果落 exitTracker
	refresh func() []string                     // 正式池清单（每日重查用）

	fails    int
	offUntil time.Time

	// originHost 把本地映射地址（sing-box 的 127.0.0.1:本地端口）反查成
	// 原始服务器地址——否则信誉体检会去查本机回环，毫无意义。
	originHost func(addr string) string

	ipriskBase string

	// 测试注入点
	lookupDNS func(name string) ([]net.IP, error)
	resolver  func(host string) ([]net.IP, error)
	fetch     func(url string) (int, string, error) // 返回 status, body, err
}

type hostIPEntry struct {
	ip   string
	seen time.Time
}

func newIPReputer(sink func(addr string, res *ipRepResult)) *ipReputer {
	r := &ipReputer{
		cache:      make(map[string]*ipRepResult),
		hostIP:     make(map[string]hostIPEntry),
		pending:    make(map[string]bool),
		queue:      make(chan string, 4096),
		client:     &http.Client{Timeout: ipRepTimeout},
		sink:       sink,
		ipriskBase: "https://iprisk.top",
		lookupDNS:  net.LookupIP,
		resolver:   net.LookupIP,
	}
	r.fetch = r.httpFetch
	return r
}

// httpFetch 默认抓取实现。
func (r *ipReputer) httpFetch(url string) (int, string, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	buf := make([]byte, 256<<10)
	n, _ := io.ReadFull(resp.Body, buf)
	if n < 0 {
		n = 0
	}
	return resp.StatusCode, string(buf[:n]), nil
}

// start 启动串行体检 worker 与每日重查。
func (r *ipReputer) start(ctx context.Context) {
	go func() {
		refresh := time.NewTicker(ipRepRefreshEvery)
		defer refresh.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case addr := <-r.queue:
				r.process(addr)
				time.Sleep(ipRepInterval)
			case <-refresh.C:
				if r.refresh != nil {
					for _, addr := range r.refresh() {
						r.Request(addr)
					}
				}
			}
		}
	}()
}

// Request 申请体检一个出口（转正/启动补查/每日重查时调用）。
// 已缓存新鲜结果或排队中的跳过。
func (r *ipReputer) Request(addr string) {
	if addr == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if res, ok := r.cache[addr]; ok && time.Since(res.Checked) < ipRepTTL {
		return
	}
	if r.pending[addr] {
		return
	}
	if len(r.queue) >= cap(r.queue) {
		return
	}
	r.pending[addr] = true
	select {
	case r.queue <- addr:
	default:
		delete(r.pending, addr)
	}
}

// process 执行单个体检并落记账。
func (r *ipReputer) process(addr string) {
	defer func() {
		r.mu.Lock()
		delete(r.pending, addr)
		r.mu.Unlock()
	}()
	r.mu.Lock()
	if res, ok := r.cache[addr]; ok && time.Since(res.Checked) < ipRepTTL {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	res := r.check(addr)
	if res == nil {
		return // 失败：不缓存，退避后由每日重查兜底
	}
	r.mu.Lock()
	r.cache[addr] = res
	if len(r.cache) > ipRepMaxCache {
		r.cache = make(map[string]*ipRepResult)
	}
	r.mu.Unlock()
	if r.sink != nil {
		r.sink(addr, res)
	}
}

// resolveHost 把出口地址解析成 IP（域名出口先查 A 记录，缓存 1 小时）。
func (r *ipReputer) resolveHost(host string) string {
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	r.mu.Lock()
	if entry, ok := r.hostIP[host]; ok && time.Since(entry.seen) < ipRepResolveTTL {
		r.mu.Unlock()
		return entry.ip
	}
	r.mu.Unlock()
	ips, err := r.resolver(host)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || len(ips) == 0 {
		r.hostIP[host] = hostIPEntry{seen: time.Now()}
		return ""
	}
	ip := ips[0].String()
	r.hostIP[host] = hostIPEntry{ip: ip, seen: time.Now()}
	return ip
}

// available 报告源是否处于退避期。
func (r *ipReputer) available() bool {
	return time.Now().After(r.offUntil)
}

// isPrivateOrLoopback 报告 IP 是否为回环/内网/链路本地等不可公网体检的地址。
func isPrivateOrLoopback(ip net.IP) bool {
	return ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()
}

// check 抓取 iprisk.top 查询页并解析评分/归属。
func (r *ipReputer) check(addr string) *ipRepResult {
	if !r.available() {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		host = addr
	}
	ip := r.resolveHost(host)
	if ip == "" {
		return nil
	}
	// 回环/内网地址（sing-box 本地映射、内网代理）查信誉毫无意义：
	// 反查原始服务器地址；映射不出来就跳过体检（不计故障）。
	if pip := net.ParseIP(ip); isPrivateOrLoopback(pip) {
		origin := ""
		if r.originHost != nil {
			origin = r.originHost(addr)
		}
		if origin == "" || origin == host {
			return nil
		}
		if h, _, err2 := net.SplitHostPort(origin); err2 == nil {
			origin = h // 防御：反查结果若带端口则剥离
		}
		ip = r.resolveHost(origin)
		if ip == "" || isPrivateOrLoopback(net.ParseIP(ip)) {
			return nil
		}
	}
	status, body, err := r.fetch(r.ipriskBase + "/ip/" + ip)
	if err != nil || status != http.StatusOK {
		// 404 = 该 IP 不在 iprisk 库：个体属性而非站点故障，不计退避。
		if err == nil && status == http.StatusNotFound {
			return nil
		}
		r.noteFailure()
		return nil
	}
	res := parseIPRiskPage(ip, body)
	if res == nil {
		r.noteFailure()
		return nil
	}
	r.fails = 0
	return res
}

func (r *ipReputer) noteFailure() {
	r.fails++
	if r.fails >= ipRepSourceFails {
		r.offUntil = time.Now().Add(ipRepSourceOff)
		r.fails = 0
		log.Printf("[信誉] iprisk.top 连续 %d 次抓取/解析失败，暂停 %v（fail-open，不影响节点使用）",
			ipRepSourceFails, ipRepSourceOff)
	}
}

var (
	ipRiskScoreRe = regexp.MustCompile(`纯净度评分为\s*(\d{1,3})\s*/\s*100`)
	ipRiskASRe    = regexp.MustCompile(`AS(\d+)`)
	ipRiskCCRe    = regexp.MustCompile(`[（(]AS\d+[），,]{1,2}\s*([A-Z]{2})`)
)

// parseIPRiskPage 从查询页 HTML 解析评分/国家/ASN。
// 页面锚点："纯净度评分为 32/100"、"归属 Google LLC（AS15169），US Mountain View"。
func parseIPRiskPage(ip, html string) *ipRepResult {
	m := ipRiskScoreRe.FindStringSubmatch(html)
	if m == nil {
		return nil
	}
	score, err := strconv.Atoi(m[1])
	if err != nil || score < 0 || score > 100 {
		return nil
	}
	res := &ipRepResult{IP: ip, Score: score, Grade: gradeOf(score), Checked: time.Now()}
	seg := segmentAfter(html, "归属")
	if seg == "" {
		return res
	}
	if am := ipRiskASRe.FindStringSubmatch(seg); am != nil {
		res.ASN = "AS" + am[1]
	}
	if cm := ipRiskCCRe.FindStringSubmatch(seg); cm != nil {
		res.Region = cm[1]
	}
	if org := orgName(seg); org != "" && res.ASN != "" {
		res.ASN += " " + org
	}
	return res
}

// segmentAfter 返回锚点之后的可见文本片段（截断到下一个标签）。
func segmentAfter(html, anchor string) string {
	idx := strings.Index(html, anchor)
	if idx < 0 {
		return ""
	}
	rest := html[idx+len(anchor):]
	if end := strings.IndexAny(rest, "<\r\n"); end >= 0 {
		rest = rest[:end]
	}
	return rest
}

// orgName 从归属片段提取组织名：锚点形态 "归属 Google LLC（AS15169），US"。
func orgName(seg string) string {
	head := seg
	if i := strings.IndexAny(head, "（("); i >= 0 {
		head = head[:i]
	}
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(head), "归属"))
}

func firstWord(s string) string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}
