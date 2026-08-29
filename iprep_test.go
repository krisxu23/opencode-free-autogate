package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestIPReputer(t *testing.T, abuseBody string, ipinfoBody string) (*ipReputer, *int, *int) {
	t.Helper()
	var abuseHits, ipinfoHits int
	abuse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		abuseHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(abuseBody))
	}))
	t.Cleanup(abuse.Close)
	ipinfo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ipinfoHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(ipinfoBody))
	}))
	t.Cleanup(ipinfo.Close)

	r := newIPReputer(config{
		abuseIPDBKey:    "test-key",
		ipinfoToken:     "test-token",
		spamhausDQSKey:  "dqskey",
	}, nil)
	r.abuseBase = abuse.URL
	r.ipinfoBase = ipinfo.URL
	r.lookupDNS = func(name string) ([]net.IP, error) {
		// zen 查询默认 NXDOMAIN（未命中）：测试里用地址段约定注入
		if strings.Contains(name, "xbl-inject") {
			return []net.IP{net.IPv4(127, 0, 0, 4)}, nil
		}
		return nil, &net.DNSError{Err: "no such host", IsNotFound: true}
	}
	return r, &abuseHits, &ipinfoHits
}

func TestRepScoring(t *testing.T) {
	r, abuseHits, ipinfoHits := newTestIPReputer(t,
		`{"data":{"abuseConfidenceScore":25,"totalReports":30,"countryCode":"JP","usageType":"Data Center/Web Hosting"}}`,
		`{"country":"JP","org":"AS9009 M247 Europe","asn":{"asn":"AS9009","name":"M247 Europe","type":"hosting"}}`)

	r.process("1.2.3.4:1080")
	res := r.cache["1.2.3.4:1080"]
	if res == nil {
		t.Fatal("process 后应写入缓存并产出结果")
	}
	if res.Region != "JP" {
		t.Fatalf("地区应来自 AbuseIPDB/IPinfo: %q", res.Region)
	}
	if res.ASN != "AS9009 M247 Europe" {
		t.Fatalf("ASN 短形态错误: %q", res.ASN)
	}
	if res.Reports != 30 {
		t.Fatalf("举报数错误: %d", res.Reports)
	}
	// 置信 25×0.6 = 15，举报>20 再 +10 → 扣 25 → 75 = B
	if res.Score != 75 || res.Grade != "B" {
		t.Fatalf("评分应 75/B，得到 %d/%s", res.Score, res.Grade)
	}
	if res.SpamHit != "" {
		t.Fatalf("未注入黑名单不应命中: %q", res.SpamHit)
	}
	if *abuseHits != 1 || *ipinfoHits != 1 {
		t.Fatalf("两个源都应各查一次: %d/%d", *abuseHits, *ipinfoHits)
	}

	// 缓存生效：同一出口第二次 check 不再打 HTTP。
	before := *abuseHits
	r2 := r.cache["1.2.3.4:1080"] // 缓存按出口 addr 键
	if r2 == nil {
		t.Fatal("结果应按 IP 缓存")
	}
	_ = r2
	r.process("1.2.3.4:1080")
	if *abuseHits != before {
		t.Fatal("缓存期内不应重复查询")
	}

	// XBL 注入：再扣 30 → 45 = C，且记录命中。
	r.lookupDNS = func(name string) ([]net.IP, error) {
		return []net.IP{net.IPv4(127, 0, 0, 4)}, nil
	}
	res2 := r.check("5.6.7.8:1080")
	if res2.SpamHit != "XBL" {
		t.Fatalf("应命中 XBL: %q", res2.SpamHit)
	}
	if res2.Score != 45 || res2.Grade != "C" {
		t.Fatalf("XBL 后应 45/C，得到 %d/%s", res2.Score, res2.Grade)
	}
}

func TestSpamhausVerdict(t *testing.T) {
	if hit, penalty, _ := spamhausVerdict([]net.IP{net.IPv4(127, 0, 0, 2)}); hit != "SBL/CSS" || penalty != 40 {
		t.Fatalf("SBL 应扣 40: %q %d", hit, penalty)
	}
	if hit, penalty, _ := spamhausVerdict([]net.IP{net.IPv4(127, 0, 0, 10)}); hit != "" || penalty != 0 {
		t.Fatalf("PBL 是策略表不算黑: %q %d", hit, penalty)
	}
	if _, _, blocked := spamhausVerdict([]net.IP{net.IPv4(127, 255, 255, 254)}); !blocked {
		t.Fatal("公共 resolver 封锁特征码应报 blocked")
	}
	if hit, _, _ := spamhausVerdict(nil); hit != "" {
		t.Fatal("空结果应干净")
	}
}

func TestRepRankOrdering(t *testing.T) {
	tr := newExitTracker()
	// A 信誉 + 偏好地区；D 信誉 + 非偏好；未体检 + 非偏好。
	tr.observeRep("a.node:1", &ipRepResult{Score: 90, Grade: "A", Region: "JP"})
	tr.observeRep("d.node:1", &ipRepResult{Score: 20, Grade: "D", Region: "US"})
	exits := []slot{
		{addr: "d.node:1"},
		{addr: "unknown.node:1"},
		{addr: "a.node:1"},
	}
	ordered := tr.rank(exits, map[string]struct{}{"JP": {}})
	if ordered[0].addr != "a.node:1" {
		t.Fatalf("偏好地区 + A 信誉应最先，得到 %s", ordered[0].addr)
	}
	if ordered[len(ordered)-1].addr != "d.node:1" {
		t.Fatalf("D 信誉应最后，得到 %s", ordered[len(ordered)-1].addr)
	}
	// 未体检 = 中性档：无偏好时排在 A 后、D 前。
	ordered = tr.rank(exits, nil)
	if ordered[0].addr != "a.node:1" || ordered[len(ordered)-1].addr != "d.node:1" {
		t.Fatalf("无偏好时信誉仍应生效: %v", ordered)
	}
}

func TestRepTagFormat(t *testing.T) {
	tr := newExitTracker()
	if tag := tr.repTag("none:1"); tag != "" {
		t.Fatalf("未体检应无标签: %q", tag)
	}
	tr.observeRep("x.node:1", &ipRepResult{Score: 92, Grade: "A", Region: "SG", ASN: "AS9009 M247"})
	tag := tr.repTag("x.node:1")
	if !strings.Contains(tag, "SG") || !strings.Contains(tag, "A(92)") || !strings.Contains(tag, "AS9009") {
		t.Fatalf("标签格式错误: %q", tag)
	}
}

// 源退避：连续 3 次失败后该源暂停，fail-open 不拖垮体检。
func TestRepSourceBackoff(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	r := newIPReputer(config{abuseIPDBKey: "k"}, nil)
	r.abuseBase = srv.URL
	for i := 0; i < 3; i++ {
		if _, err := r.queryAbuse("1.2.3.4"); err != nil {
			r.noteAbuseFailure()
		}
	}
	if !time.Now().Before(r.abuseOffUntil) {
		t.Fatal("连续 3 次失败后应进入退避")
	}
	if r.abuseAvailable() {
		t.Fatal("退避期内源应不可用")
	}
	_ = fmt.Sprint(hits) // hits 仅用于确认服务器被调用过
	if hits < 3 {
		t.Fatal("退避前应有真实请求")
	}
}

// 零配置：完全不填 key 也能体检——地理走 ip-api（免 key），Spamhaus 免 key
// 直查（本测试注入 NXDOMAIN = 干净）。单打分源 → 封顶 B(74)。
func TestKeylessGeo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","country":"DE","as":"AS24940 Hetzner Online GmbH","hosting":true,"proxy":false}`))
	}))
	defer srv.Close()

	r := newIPReputer(config{}, nil)
	r.ipAPIBase = srv.URL
	r.lookupDNS = func(name string) ([]net.IP, error) {
		return nil, &net.DNSError{Err: "no such host", IsNotFound: true}
	}
	r.process("5.6.7.8:1080")
	res := r.cache["5.6.7.8:1080"]
	if res == nil {
		t.Fatal("零配置也应产出结果")
	}
	if res.Region != "DE" || res.ASN != "AS24940 Hetzner" || !res.Hosting {
		t.Fatalf("免 key 地理源应填充: %+v", res)
	}
	if res.Score != 74 || res.Grade != "B" {
		t.Fatalf("单打分源应封顶 74/B，得到 %d/%s", res.Score, res.Grade)
	}
}

// 零配置 + resolver 被公共 DNS 封锁：Spamhaus 自动停用 24h，分数退化为未知。
func TestSpamhausBlockedKeyless(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","country":"US","as":"AS64500 Example"}`))
	}))
	defer srv.Close()

	r := newIPReputer(config{}, nil)
	r.ipAPIBase = srv.URL
	r.lookupDNS = func(name string) ([]net.IP, error) {
		return []net.IP{net.IPv4(127, 255, 255, 254)}, nil
	}
	r.process("1.2.3.4:1080")
	if !time.Now().Before(r.spamhausOffUntil) {
		t.Fatal("被拦截后应停用 Spamhaus 24h")
	}
	res := r.cache["1.2.3.4:1080"]
	if res == nil || res.Score != repUnknown {
		t.Fatalf("无任何打分源应保持未知: %+v", res)
	}
}
