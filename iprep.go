package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// IP 信誉体检（双层：Blackbox 打底 + iprisk.top 增强）。
//
// 实测结论（2026-08-29）：iprisk.top 的 /ip/{IP} 页面只对"站点已有缓存
// 结果"的 IP 静态渲染评分；新 IP 返回的是查询表单外壳（前端 JS 异步调
// /api/check*，而内部 API 有人机验证墙，脚本无法调用）。因此：
//
//	第一层 Blackbox（blackbox.ipinfo.app/api/v3beta/{IP}，免 key）：
//	    每个节点都能测，hosting/proxy/vpn/tor/spamhaus/suspicious 信号
//	    映射为 0-100 评分——打分主力；
//	第二层 iprisk.top（16 源聚合，仅当站点已有缓存结果时可解析）：
//	    有评分则与第一层混合，无缓存结果（外壳页）静默跳过、不计故障。
//
// 设计约定：
//   - 只体检正式池节点（转正时查一次 + 启动补查 + 每日重查），压力极低；
//   - 结果按解析后的 IP 缓存 7 天——同 IP 多端口（118.145.128.100 有十个
//     端口）只查一次，各端口槽位共享同一份标签；
//   - 信誉是排序先验不是判决：只影响出场顺序，绝不单独剔除节点。

const (
	ipRepTTL          = 7 * 24 * time.Hour      // 体检结果缓存期
	ipRepWorkers      = 3                       // 并发体检 worker 数（ip-api 专用限速兜底）
	ipRepNodeGap      = 400 * time.Millisecond  // 单 worker 相邻节点间隔
	geoMinInterval    = 1400 * time.Millisecond // ip-api 45 次/分限速：geo 查询全局限速
	ipRepResolveTTL   = time.Hour               // 域名出口 → IP 的解析缓存
	ipRepSourceOff    = 30 * time.Minute        // 连续失败后的源退避
	ipRepSourceFails  = 5                       // 触发退避的连续失败次数（扫池高峰抖动多，别太敏感）
	ipRepTimeout      = 12 * time.Second        // JSON 源单次查询超时
	ipRepPageTimeout  = 8 * time.Second         // iprisk 页面单次抓取超时（增强层不许久拖）
	ipRepRefreshEvery = 24 * time.Hour          // 正式池重查周期
	ipRepMaxCache     = 4096                    // 结果缓存上限
	repUnknown        = -1                      // Score 取值：未体检/体检失败
)

// ipRepResult 是一次体检的结果。
type ipRepResult struct {
	IP      string
	Region  string // 国家码（US/JP/...），地区偏好的依据
	ASN     string // "AS15169 Google" 短形态
	Score   int    // 0-100 纯净度（越高越干净）；repUnknown = 未体检
	Grade   string // A/B/C/D
	Source  string // 评分来源：blackbox / blackbox+iprisk / iprisk
	Hosting bool   // 机房/托管判定（blackbox 或 ipapi.is）
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

	fails    int // iprisk 抓取/解析连续失败（仅站点级故障计入）
	offUntil time.Time

	bbState    srcState // Blackbox 打分源
	ipapiState srcState // ipapi.is 判定源（独立第二意见）
	geoState   srcState // ip-api 地理源

	// originHost 把本地映射地址（sing-box 的 127.0.0.1:本地端口）反查成
	// 原始服务器地址——否则信誉体检会去查本机回环，毫无意义。
	originHost func(addr string) string

	// exitURLs 提供正式池出口（供直连抓取失败时借道重试）。
	exitURLs func() []*url.URL

	// pageFetch 页面抓取入口（可注入；默认 pageHTTPFetch）。
	pageFetch func(pageURL string) (int, string, error)

	ipriskBase string
	bbBase     string
	geoBase    string
	ipapiBase  string
	geoMu      sync.Mutex
	geoNext    time.Time
	pageClient *http.Client

	// 测试注入点
	lookupDNS func(name string) ([]net.IP, error)
	resolver  func(host string) ([]net.IP, error)
	fetch     func(url string) (int, string, error) // 返回 status, body, err
}

// isPrivateOrLoopback 报告 IP 是否为回环/内网/链路本地等不可公网体检的地址。
func isPrivateOrLoopback(ip net.IP) bool {
	return ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()
}

// srcState 是单数据源的失败退避状态：连续失败达到阈值后停用一段时间。
// 多 worker 并发访问，内部自带锁。
type srcState struct {
	mu       sync.Mutex
	fails    int
	offUntil time.Time
}

func (s *srcState) note(ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok {
		s.fails = 0
		return
	}
	s.fails++
	if s.fails >= ipRepSourceFails {
		s.offUntil = time.Now().Add(ipRepSourceOff)
		s.fails = 0
	}
}

func (s *srcState) available() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().After(s.offUntil)
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
		pageClient: &http.Client{Timeout: ipRepPageTimeout},
		sink:       sink,
		ipriskBase: "https://iprisk.top",
		bbBase:     "https://blackbox.ipinfo.app/api/v3beta",
		geoBase:    "http://ip-api.com/json",
		ipapiBase:  "https://api.ipapi.is",
		lookupDNS:  net.LookupIP,
		resolver:   net.LookupIP,
	}
	r.fetch = r.httpFetch
	r.pageFetch = r.pageHTTPFetch
	return r
}

// fetchPage 抓取页面：先直连；失败时借道正式池出口重试（最多 2 个）。
// 初检扫池高峰期上行被 128 路探测打满，直连到检测站容易超时——
// 而正式池出口刚通过探活，链路质量有保障。
func (r *ipReputer) fetchPage(pageURL string) (int, string, error) {
	status, body, err := r.pageFetch(pageURL)
	if err == nil && status == http.StatusOK {
		return status, body, nil
	}
	firstErr, firstStatus := err, status
	if r.exitURLs == nil {
		return firstStatus, body, firstErr
	}
	exits := r.exitURLs()
	if len(exits) > 1 {
		exits = exits[:1] // 增强层只借道一次：扫池高峰的超时不许拖住队列
	}
	for _, pu := range exits {
		if pu == nil {
			continue
		}
		client := &http.Client{Timeout: ipRepPageTimeout, Transport: sharedTransports.get(pu)}
		req, err := http.NewRequest(http.MethodGet, pageURL, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		buf := make([]byte, 256<<10)
		n, _ := io.ReadFull(resp.Body, buf)
		if n < 0 {
			n = 0
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			log.Printf("[信誉] 直连失败，借道出口 %s 抓取成功", pu.Host)
			return resp.StatusCode, string(buf[:n]), nil
		}
	}
	return firstStatus, body, firstErr
}

// pageHTTPFetch 页面抓取默认实现（独立短超时客户端）。
func (r *ipReputer) pageHTTPFetch(pageURL string) (int, string, error) {
	req, err := http.NewRequest(http.MethodGet, pageURL, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
	resp, err := r.pageClient.Do(req)
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

// start 启动并发体检 worker 与每日重查。
func (r *ipReputer) start(ctx context.Context) {
	refresh := time.NewTicker(ipRepRefreshEvery)
	defer refresh.Stop()
	worker := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case addr := <-r.queue:
				r.process(addr)
				time.Sleep(ipRepNodeGap)
			}
		}
	}
	for w := 0; w < ipRepWorkers; w++ {
		go worker()
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
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

// process 执行单个体检并落记账。缓存按解析后的 IP 键存——同 IP 多端口
// （118.145.128.100:44xxx × 10）只体检一次，其余端口直接复用结果。
func (r *ipReputer) process(addr string) {
	defer func() {
		r.mu.Lock()
		delete(r.pending, addr)
		r.mu.Unlock()
	}()
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		host = addr
	}
	ip := r.resolveHost(host)
	if ip == "" {
		return
	}
	// 回环/内网地址（sing-box 本地映射、内网代理）查信誉毫无意义：
	// 反查原始服务器地址；映射不出来就跳过（不打任何源的查询）。
	if pip := net.ParseIP(ip); isPrivateOrLoopback(pip) {
		origin := ""
		if r.originHost != nil {
			origin = r.originHost(addr)
		}
		if origin == "" || origin == host {
			return
		}
		if h, _, e2 := net.SplitHostPort(origin); e2 == nil {
			origin = h
		}
		ip = r.resolveHost(origin)
		if ip == "" || isPrivateOrLoopback(net.ParseIP(ip)) {
			return
		}
	}
	r.mu.Lock()
	if res, ok := r.cache[ip]; ok && time.Since(res.Checked) < ipRepTTL {
		r.mu.Unlock()
		if r.sink != nil {
			r.sink(addr, res) // 同 IP 的其他端口槽位直接复用标签
		}
		return
	}
	r.mu.Unlock()
	res := r.check(addr, ip)
	if res == nil {
		return // 失败：不缓存，退避后由每日重查兜底
	}
	r.mu.Lock()
	r.cache[ip] = res
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

// available 报告 iprisk 增强源是否处于退避期。
func (r *ipReputer) available() bool {
	return time.Now().After(r.offUntil)
}

// check 双层体检：Blackbox 打底（必得分数），iprisk.top 有缓存结果则混合。
// ip 前置传入（process 已完成解析与私有地址反查）。
func (r *ipReputer) check(addr, ip string) *ipRepResult {
	res := &ipRepResult{IP: ip, Score: repUnknown, Checked: time.Now()}

	// 第一层：Blackbox v3beta（免 key）——hosting/proxy/vpn/tor/spamhaus/
	// suspicious 信号映射扣分。打分主力，几乎总能给出结果。
	if r.bbState.available() {
		if d, err := r.queryBlackbox(ip); err != nil {
			r.noteBBFailure()
		} else {
			r.bbState.note(true)
			res.Score = blackboxScore(d)
			res.Grade = gradeOf(res.Score)
			res.Source = "blackbox"
			if d.ASN.Number != 0 {
				res.ASN = fmt.Sprintf("AS%d %s", d.ASN.Number, firstWord(d.ASN.Name))
			}
		}
	}

	// 第二意见：ipapi.is（免 key，1000 次/天）——与 Blackbox 独立的
	// 滥用/代理/VPN 判定。两源一致的节点评分会显著下沉。
	if r.ipapiState.available() {
		if d, err := r.queryIPAPIIS(ip); err != nil {
			r.ipapiState.note(false)
		} else {
			r.ipapiState.note(true)
			if res.Region == "" && d.CC != "" {
				res.Region = d.CC
			}
			if res.ASN == "" && d.ASNNum != 0 {
				res.ASN = fmt.Sprintf("AS%d %s", d.ASNNum, firstWord(d.ASNOrg))
			}
			if !res.Hosting && d.IsDatacenter {
				res.Hosting = true
			}
			if res.Score == repUnknown {
				res.Score = 100
			}
			penalty := 0
			if d.IsAbuser {
				penalty += 35
			}
			if d.IsTor {
				penalty += 20
			}
			if d.IsProxy {
				penalty += 10
			}
			if d.IsVPN {
				penalty += 10
			}
			if penalty > 45 {
				penalty = 45
			}
			res.Score -= penalty
			if res.Score < 0 {
				res.Score = 0
			}
			res.Grade = gradeOf(res.Score)
		}
	}

	// 地理（信息性，不扣分）：地区偏好的数据源。
	if r.geoState.available() && res.Region == "" {
		if cc, err := r.queryGeoCountry(ip); err != nil {
			r.geoState.note(false)
		} else {
			r.geoState.note(true)
			res.Region = cc
		}
	}

	// 第二层：iprisk.top（16 源聚合）——仅当站点已有缓存结果时可解析。
	// 新 IP 返回查询表单外壳（前端 JS 才能驱动内部 API，且有人机验证墙），
	// 静默跳过不计故障；站点级故障（网络/非 200/意外页面）才计退避。
	if r.available() {
		status, body, err := r.fetchPage(r.ipriskBase + "/ip/" + ip)
		switch {
		case err != nil:
			log.Printf("[信誉] iprisk 抓取失败 ip=%s err=%v", ip, err)
			r.noteFailure()
		case status == http.StatusNotFound:
			// 该 IP 不在 iprisk 库：个体属性，不计退避
		case status != http.StatusOK:
			log.Printf("[信誉] iprisk 抓取失败 ip=%s status=%d body=%q", ip, status, truncateForLog(body, 120))
			r.noteFailure()
		case isIPRiskShell(body):
			// 新 IP 的查询表单外壳：站点还没有该 IP 的缓存结果，
			// 内部 API 有人机验证墙无法脚本调用——静默跳过不计故障，
			// Blackbox 层的分数已经足够排序。
		default:
			parsed := parseIPRiskPage(ip, body)
			if parsed == nil {
				log.Printf("[信誉] iprisk 页面解析失败 ip=%s len=%d body=%q", ip, len(body), truncateForLog(body, 120))
				r.noteFailure()
			} else {
				r.fails = 0
				if res.Score == repUnknown {
					res.Score = parsed.Score
					res.Source = "iprisk"
				} else {
					res.Score = (res.Score + parsed.Score) / 2
					res.Source = "blackbox+iprisk"
				}
				res.Grade = gradeOf(res.Score)
				if res.Region == "" {
					res.Region = parsed.Region
				}
				if res.ASN == "" {
					res.ASN = parsed.ASN
				}
			}
		}
	}

	if res.Score == repUnknown {
		return nil // 两层都不可用：不产出结果，交由每日重查兜底
	}
	return res
}

// blackboxScore 把 v3beta 信号映射成 0-100 纯净度扣分制评分。
// 滥用类信号重罚（proxy/spamhaus/suspicious），机房类轻罚（池内节点
// 几乎全是机房，轻重才是相对排序的有效信息）。
func blackboxScore(d *blackboxResp) int {
	penalty := 0
	if d.Signals.Hosting {
		penalty += 10
	}
	if d.Signals.Proxy {
		penalty += 25
	}
	if d.Signals.Tor {
		penalty += 40
	}
	if d.Signals.Spamhaus {
		penalty += 25
	}
	if d.Signals.SfsListed {
		penalty += 15
	}
	if d.Suspicious {
		penalty += 10
	}
	if d.Classification == "vpn" {
		penalty += 15
	}
	if d.Categories.VPN >= 0.5 {
		penalty += 10
	}
	if d.Categories.Bogon > 0 || d.Signals.Bogon {
		penalty += 40
	}
	if penalty > 90 {
		penalty = 90
	}
	return 100 - penalty
}

func (r *ipReputer) noteBBFailure() { r.bbState.note(false) }

func (r *ipReputer) noteFailure() {
	r.fails++
	if r.fails >= ipRepSourceFails {
		r.offUntil = time.Now().Add(ipRepSourceOff)
		r.fails = 0
		log.Printf("[信誉] iprisk.top 连续 %d 次抓取/解析失败，暂停 %v（fail-open，不影响节点使用）",
			ipRepSourceFails, ipRepSourceOff)
	}
}

// isIPRiskShell 识别"查询表单外壳页"：新 IP 的 /ip/ 路径返回它，
// 标题无 IP、无评分锚点。这不是站点故障，是"该 IP 尚无缓存结果"。
func isIPRiskShell(body string) bool {
	return strings.Contains(body, "IP纯净度检测与IP风险查询") &&
		!strings.Contains(body, "纯净度评分为")
}

var (
	ipRiskScoreRe = regexp.MustCompile(`纯净度评分[为：|\s]*(\d{1,3})\s*/\s*100`)
	ipRiskTitleRe = regexp.MustCompile(`<title>[^<]*?(\d{1,3})/100[^<]*?</title>`)
	ipRiskAnyRe   = regexp.MustCompile(`(\d{1,3})\s*/\s*100`)
	ipRiskASRe    = regexp.MustCompile(`AS(\d+)`)
	ipRiskCCRe    = regexp.MustCompile(`[（(]AS\d+[），,]{1,2}\s*([A-Z]{2})`)
)

type blackboxResp struct {
	IP             string  `json:"ip"`
	Classification string  `json:"classification"`
	Confidence     float64 `json:"confidence"`
	ASN            struct {
		Name   string `json:"name"`
		Number int    `json:"number"`
	} `json:"asn"`
	Categories struct {
		Hosting     float64 `json:"hosting"`
		Residential float64 `json:"residential"`
		Mobile      float64 `json:"mobile"`
		VPN         float64 `json:"vpn"`
		Tor         float64 `json:"tor"`
		Bogon       float64 `json:"bogon"`
	} `json:"categories"`
	Signals struct {
		Bogon     bool `json:"bogon"`
		Cloud     bool `json:"cloud"`
		Hosting   bool `json:"hosting"`
		Proxy     bool `json:"proxy"`
		Spamhaus  bool `json:"spamhaus"`
		Tor       bool `json:"tor"`
		SfsListed bool `json:"sfs_listed"`
	} `json:"signals"`
	Suspicious bool `json:"suspicious"`
}

// queryBlackbox 免 key 纯净度信号查询（blackbox.ipinfo.app v3beta，
// 无注册、实测对脏代理 IP 判定与 iprisk 互相印证）。
func (r *ipReputer) queryBlackbox(ip string) (*blackboxResp, error) {
	status, body, err := r.fetch(r.bbBase + "/" + ip)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("blackbox status %d", status)
	}
	var data blackboxResp
	if err := json.Unmarshal([]byte(body), &data); err != nil {
		return nil, err
	}
	return &data, nil
}

type ipapiISResp struct {
	IsBogon      bool   `json:"is_bogon"`
	IsDatacenter bool   `json:"is_datacenter"`
	IsTor        bool   `json:"is_tor"`
	IsProxy      bool   `json:"is_proxy"`
	IsVPN        bool   `json:"is_vpn"`
	IsAbuser     bool   `json:"is_abuser"`
	CompanyName  string `json:"company_name"`
	ASNNum       int    `json:"asn_num"`
	ASNOrg       string `json:"asn_org"`
	CC           string `json:"cc"`
}

// queryIPAPIIS 免 key 判定查询（ipapi.is，1000 次/天）：五个独立判定
// 标志 + 国家/ASN，与 Blackbox 互为独立第二意见。
func (r *ipReputer) queryIPAPIIS(ip string) (*ipapiISResp, error) {
	status, body, err := r.fetch(r.ipapiBase + "/?q=" + url.QueryEscape(ip))
	if err != nil {
		return nil, err
	}
	if status == http.StatusTooManyRequests || status == http.StatusForbidden {
		// 匿名额度每客户端 IP 每 UTC 日 100 次（免费 key 1000 次/天）：
		// 触发即按日配额退避，而不是按连续失败次数。
		r.ipapiState.offUntil = time.Now().Add(24 * time.Hour)
		log.Printf("[信誉] ipapi.is 匿名额度耗尽或被拒（status=%d），暂停 24h", status)
		return nil, fmt.Errorf("ipapi.is quota: %d", status)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("ipapi.is status %d", status)
	}
	var data ipapiISResp
	if err := json.Unmarshal([]byte(body), &data); err != nil {
		return nil, err
	}
	return &data, nil
}

type geoResp struct {
	Status      string `json:"status"`
	Message     string `json:"message"`
	CountryCode string `json:"countryCode"`
}

// queryGeoCountry 免 key 地理查询（ip-api.com，45 次/分）——仅供地区偏好。
// 要 countryCode（CN/US/...）：地区分组匹配的是国家码而非国家名。
// paceGeo ip-api 全局限速：多 worker 并发时 geo 查询串行化到 45 次/分以内。
func (r *ipReputer) paceGeo() {
	r.geoMu.Lock()
	wait := time.Until(r.geoNext)
	if wait <= 0 {
		r.geoNext = time.Now().Add(geoMinInterval)
		r.geoMu.Unlock()
		return
	}
	r.geoNext = time.Now().Add(geoMinInterval)
	r.geoMu.Unlock()
	time.Sleep(wait)
}

func (r *ipReputer) queryGeoCountry(ip string) (string, error) {
	r.paceGeo()
	status, body, err := r.fetch(r.geoBase + "/" + url.QueryEscape(ip) + "?fields=status,countryCode")
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("ip-api status %d", status)
	}
	var data geoResp
	if err := json.Unmarshal([]byte(body), &data); err != nil {
		return "", err
	}
	if data.Status != "success" {
		return "", fmt.Errorf("ip-api: %s", data.Message)
	}
	return data.CountryCode, nil
}

// parseIPRiskScore 从结果页提取 "纯净度评分为 N/100" 的 N。
// 三层兜底：中文锚点 → 标题 N/100 → 任意 N/100（限定 0-100）。
func parseIPRiskScore(html string) (int, error) {
	if m := ipRiskScoreRe.FindStringSubmatch(html); m != nil {
		return parseScore100(m[1])
	}
	if m := ipRiskTitleRe.FindStringSubmatch(html); m != nil {
		return parseScore100(m[1])
	}
	for _, m := range ipRiskAnyRe.FindAllStringSubmatch(html, 8) {
		if v, err := strconv.Atoi(m[1]); err == nil && v >= 0 && v <= 100 {
			return v, nil
		}
	}
	return 0, fmt.Errorf("无评分锚点")
}

func parseScore100(raw string) (int, error) {
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 || v > 100 {
		return 0, fmt.Errorf("评分越界 %q", raw)
	}
	return v, nil
}

// parseIPRiskPage 从查询页 HTML 解析评分/国家/ASN。
// 页面锚点："纯净度评分为 32/100"、"归属 Google LLC（AS15169），US Mountain View"。
func parseIPRiskPage(ip, html string) *ipRepResult {
	score, err := parseIPRiskScore(html)
	if err != nil {
		return nil
	}
	res := &ipRepResult{IP: ip, Score: score, Grade: gradeOf(score), Checked: time.Now()}
	text := stripTags(html)
	idx := strings.Index(text, "归属")
	if idx < 0 {
		return res
	}
	seg := text[idx:minInt(idx+240, len(text))]
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

// orgName 从归属片段提取组织名：锚点形态 "归属 Google LLC（AS15169），US"。
func orgName(seg string) string {
	head := seg
	if i := strings.IndexAny(head, "（("); i >= 0 {
		head = head[:i]
	}
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(head), "归属"))
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// stripTags 把 HTML 标签替换为空格，得到近似可见文本。
var htmlTagRe = regexp.MustCompile(`<[^>]*>`)

func stripTags(html string) string {
	return htmlTagRe.ReplaceAllString(html, " ")
}

func firstWord(s string) string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}
