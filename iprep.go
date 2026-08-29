package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// IP 信誉体检（精简版）：只对正式节点池的节点做一次三源交叉检查，
// 结果缓存 7 天，作为排序先验。行为数据（探活/竞速/截断记账）仍是
// 最终权威——信誉分只影响出场顺序，绝不单独剔除节点（fail-open）。
//
// 数据源（全部可选，缺 key 自动降级为剩余源/中性分）：
//   - AbuseIPDB（免费 1000 次/天）：滥用举报置信度、举报数、国家、usageType
//   - IPinfo（免费 5 万次/月）：国家/城市、ASN/运营商（地区偏好的数据源）
//   - Spamhaus DQS（免费 key）：SBL/CSS/XBL 黑名单（PBL 是策略表，不算黑）
//
// 配额账：正式池 ≤ ~150 节点，转正时查一次 + 启动补查 + 7 天缓存，
// AbuseIPDB 1000 次/天绰绰有余；串行 worker + 间隔节流，不给第三方压力。
//
// 说明：检查的是出口端点 IP 的信誉（大多数代理单跳，端点即出口 IP）；
// 多级链式代理的末端 IP 不在检测范围内。

const (
	ipRepTTL          = 7 * 24 * time.Hour // 信誉缓存有效期：滥用史/黑名单变化很慢
	ipRepInterval     = 400 * time.Millisecond
	ipRepResolveTTL   = time.Hour        // 域名出口 → IP 的解析缓存
	ipRepSourceOff    = 10 * time.Minute // 单源连续失败后的退避
	ipRepTimeout      = 10 * time.Second // 单次 HTTP 查询超时
	repUnknown        = -1               // Score 取值：无任何源可用/未体检
)

// ipRepResult 是一次三源体检的聚合结果。
type ipRepResult struct {
	IP      string
	Region  string // 国家码（US/JP/...），未知为空
	ASN     string // "AS9009 M247" 短形态
	Score   int    // 0-100；repUnknown = 未知
	Grade   string // A/B/C/D
	SpamHit string // 命中的 Spamhaus 名单（空 = 未命中或未启用）
	Reports int    // AbuseIPDB 累计举报数
	Checked time.Time
}

// gradeOf 信誉分级（对应用户约定的 A/B/C/D）：
// A ≥80 信誉良好；B ≥55 少量痕迹；C ≥35 多源风险；D <35 严重滥用。
func gradeOf(score int) string {
	switch {
	case score >= 80:
		return "A"
	case score >= 55:
		return "B"
	case score >= 35:
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

	ipinfoToken string
	abuseKey    string
	dqsKey      string

	// 源退避：连续失败暂停该源，fail-open
	abuseFails     int
	ipinfoFails    int
	abuseOffUntil  time.Time
	ipinfoOffUntil time.Time

	// 测试注入点
	abuseBase  string
	ipinfoBase string
	lookupDNS  func(name string) ([]net.IP, error)
	resolver   func(host string) ([]net.IP, error)
}

type hostIPEntry struct {
	ip    string
	seen  time.Time
}

func newIPReputer(cfg config, sink func(addr string, res *ipRepResult)) *ipReputer {
	return &ipReputer{
		cache:       make(map[string]*ipRepResult),
		hostIP:      make(map[string]hostIPEntry),
		pending:     make(map[string]bool),
		queue:       make(chan string, 4096),
		client:      &http.Client{Timeout: ipRepTimeout},
		sink:        sink,
		ipinfoToken: cfg.ipinfoToken,
		abuseKey:    cfg.abuseIPDBKey,
		dqsKey:      cfg.spamhausDQSKey,
		abuseBase:   "https://api.abuseipdb.com/api/v2",
		ipinfoBase:  "https://ipinfo.io",
		lookupDNS:   net.LookupIP,
		resolver:    net.LookupIP,
	}
}

// start 启动串行体检 worker。
func (r *ipReputer) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case addr := <-r.queue:
				r.process(addr)
				time.Sleep(ipRepInterval)
			}
		}
	}()
}

// Request 申请体检一个出口（转正/启动补查时调用）。已缓存新鲜结果或
// 排队中的跳过；无任何已配置数据源时直接忽略。
func (r *ipReputer) Request(addr string) {
	if addr == "" {
		return
	}
	if r.abuseKey == "" && r.ipinfoToken == "" && r.dqsKey == "" {
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

// process 执行单个出口的体检并落记账。
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
		return
	}
	r.mu.Lock()
	r.cache[addr] = res
	if len(r.cache) > 4096 {
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

// check 串行调用已配置的数据源并聚合。所有源失败 → Score = repUnknown。
func (r *ipReputer) check(addr string) *ipRepResult {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		host = addr
	}
	ip := r.resolveHost(host)
	if ip == "" {
		return nil
	}
	res := &ipRepResult{IP: ip, Score: repUnknown, Checked: time.Now()}

	// AbuseIPDB：滥用置信度是主扣分项，顺带给国家。
	if r.abuseAvailable() {
		if data, err := r.queryAbuse(ip); err != nil {
			r.noteAbuseFailure()
		} else {
			r.noteAbuseSuccess()
			res.Reports = data.TotalReports
			if res.Region == "" {
				res.Region = data.CountryCode
			}
			penalty := data.AbuseConfidenceScore * 6 / 10
			if data.TotalReports > 20 && data.AbuseConfidenceScore > 0 {
				penalty += 10
			}
			if penalty > 70 {
				penalty = 70
			}
			if res.Score == repUnknown {
				res.Score = 100
			}
			res.Score -= penalty
		}
	}

	// IPinfo：地区偏好的数据源，顺带 ASN/运营商展示。信息性，不扣分。
	if r.ipinfoAvailable() {
		if data, err := r.queryIPinfo(ip); err != nil {
			r.noteIPinfoFailure()
		} else {
			r.noteIPinfoSuccess()
			if data.Country != "" {
				res.Region = data.Country
			}
			res.ASN = shortASN(data)
		}
	}

	// Spamhaus DQS：SBL/CSS 扣 40、XBL 扣 30；PBL 是策略表不算黑；
	// 127.255.255.x 是查询错误码，忽略。
	if r.dqsKey != "" {
		rev := strings.Join(reverseSegments(ip), ".")
		name := rev + "." + r.dqsKey + ".zen.dq.spamhaus.net"
		ips, err := r.lookupDNS(name)
		if err == nil {
			if hit, penalty := spamhausVerdict(ips); hit != "" {
				res.SpamHit = hit
				if res.Score == repUnknown {
					res.Score = 100
				}
				res.Score -= penalty
			}
		} else if !isDNSNotFound(err) {
			// NXDOMAIN = 未命中（干净）；其他 DNS 错误静默忽略，fail-open。
			_ = err
		}
	}

	if res.Score < 0 {
		res.Score = 0
	}
	if res.Score != repUnknown {
		res.Grade = gradeOf(res.Score)
	}
	return res
}

func (r *ipReputer) abuseAvailable() bool {
	return r.abuseKey != "" && time.Now().After(r.abuseOffUntil)
}

func (r *ipReputer) ipinfoAvailable() bool {
	return r.ipinfoToken != "" && time.Now().After(r.ipinfoOffUntil)
}

func (r *ipReputer) noteAbuseFailure() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.abuseFails++
	if r.abuseFails >= 3 {
		r.abuseOffUntil = time.Now().Add(ipRepSourceOff)
		r.abuseFails = 0
	}
}

func (r *ipReputer) noteAbuseSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.abuseFails = 0
}

func (r *ipReputer) noteIPinfoFailure() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ipinfoFails++
	if r.ipinfoFails >= 3 {
		r.ipinfoOffUntil = time.Now().Add(ipRepSourceOff)
		r.ipinfoFails = 0
	}
}

func (r *ipReputer) noteIPinfoSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ipinfoFails = 0
}

type abuseData struct {
	AbuseConfidenceScore int    `json:"abuseConfidenceScore"`
	TotalReports         int    `json:"totalReports"`
	CountryCode          string `json:"countryCode"`
	UsageType            string `json:"usageType"`
	IsTor                bool   `json:"isTor"`
}

type abuseResp struct {
	Data abuseData `json:"data"`
}

func (r *ipReputer) queryAbuse(ip string) (*abuseData, error) {
	req, err := http.NewRequest(http.MethodGet, r.abuseBase+"/check?maxAgeInDays=90&ipAddress="+ip, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Key", r.abuseKey)
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("abuseipdb status %d", resp.StatusCode)
	}
	var payload abuseResp
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return &payload.Data, nil
}

type ipinfoResp struct {
	Country string `json:"country"`
	City    string `json:"city"`
	Org     string `json:"org"`
	ASN     *struct {
		ASN  string `json:"asn"`
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"asn"`
}

func (r *ipReputer) queryIPinfo(ip string) (*ipinfoResp, error) {
	url := r.ipinfoBase + "/" + ip + "/json"
	if r.ipinfoToken != "" {
		url += "?token=" + r.ipinfoToken
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ipinfo status %d", resp.StatusCode)
	}
	var data ipinfoResp
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

// shortASN 从 ipinfo 的 org/asn 字段提取 "AS9009 M247" 短形态。
func shortASN(data *ipinfoResp) string {
	if data.ASN != nil && data.ASN.ASN != "" {
		name := data.ASN.Name
		if name == "" {
			name = data.ASN.Type
		}
		if name != "" {
			return data.ASN.ASN + " " + name
		}
		return data.ASN.ASN
	}
	fields := strings.Fields(data.Org)
	if len(fields) >= 2 {
		return fields[0] + " " + fields[1]
	}
	return data.Org
}

// reverseSegments 把 "1.2.3.4" 反转成 ["4","3","2","1"]（DNSBL 查询格式）。
func reverseSegments(ip string) []string {
	parts := strings.Split(ip, ".")
	if len(parts) != 4 {
		return nil
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return parts
}

// spamhausVerdict 解析 zen A 记录返回码：SBL(2)/CSS(3)/XBL(4) 算命中，
// PBL(10/11) 是策略表不算黑，127.255.255.x 是查询错误码。
func spamhausVerdict(ips []net.IP) (hit string, penalty int) {
	for _, ip := range ips {
		v4 := ip.To4()
		if v4 == nil || v4[0] != 127 {
			continue
		}
		if v4[1] == 255 {
			continue // 查询错误码段
		}
		switch v4[3] {
		case 2, 3:
			return "SBL/CSS", 40
		case 4:
			return "XBL", 30
		case 10, 11:
			// PBL：该段按策略不应直接发邮件，不代表 IP 恶意
		}
	}
	return "", 0
}

func isDNSNotFound(err error) bool {
	if dnsErr, ok := err.(*net.DNSError); ok {
		return dnsErr.IsNotFound
	}
	return strings.Contains(fmt.Sprint(err), "no such host")
}
