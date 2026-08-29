package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const sampleIPRiskHTML = `<title>8.8.8.8 IP风险查询：32/100 · VPN 出口 | IPRisk</title>` +
	`<div>纯净度评分为 32/100。归属 Google LLC（AS15169），US Mountain View，类型 datacenter。</div>`

func TestParseIPRiskPage(t *testing.T) {
	res := parseIPRiskPage("8.8.8.8", sampleIPRiskHTML)
	if res == nil {
		t.Fatal("应解析出结果")
	}
	if res.Score != 32 || res.Grade != "D" {
		t.Fatalf("评分应 32/D，得到 %d/%s", res.Score, res.Grade)
	}
	if res.Region != "US" {
		t.Fatalf("地区应 US，得到 %q", res.Region)
	}
	if res.ASN != "AS15169 Google LLC" {
		t.Fatalf("ASN 短形态应 AS15169 Google LLC，得到 %q", res.ASN)
	}
}

func TestParseIPRiskPageNoScore(t *testing.T) {
	if parseIPRiskPage("1.2.3.4", "<html>维护中</html>") != nil {
		t.Fatal("无评分锚点应返回 nil（fail-open 退避）")
	}
}

// 体检 + 缓存：同一出口第二次 process 不再发起抓取。
func TestIPRiskCheckAndCache(t *testing.T) {
	r := newIPReputer(nil)
	fetches := 0
	r.fetch = func(url string) (int, string, error) {
		fetches++
		if !strings.Contains(url, "iprisk.top/ip/1.2.3.4") {
			return 0, "", fmt.Errorf("unexpected url %s", url)
		}
		return 200, sampleIPRiskHTML, nil
	}
	r.process("1.2.3.4:1080")
	if r.cache["1.2.3.4:1080"] == nil {
		t.Fatal("结果应写入缓存")
	}
	r.process("1.2.3.4:1080")
	if fetches != 1 {
		t.Fatalf("缓存期内不应重复抓取，实际 %d 次", fetches)
	}
}

// 连续失败退避：3 次失败后源暂停（fail-open）。
func TestIPRiskBackoff(t *testing.T) {
	r := newIPReputer(nil)
	r.fetch = func(url string) (int, string, error) {
		return 503, "", errors.New("boom")
	}
	for i := 0; i < 5; i++ {
		r.process("1.2.3.4:1080")
	}
	if r.available() {
		t.Fatal("连续 5 次失败后应进入退避")
	}
	if r.cache["1.2.3.4:1080"] != nil {
		t.Fatal("失败结果不应写入缓存")
	}
}

func TestRepRankOrdering(t *testing.T) {
	tr := newExitTracker()
	// JP 节点 A 信誉（命中偏好）；US 节点未选中且非其他；BR 节点命中"其他"。
	tr.observeRep("jp.node:1", &ipRepResult{Score: 90, Grade: "A", Region: "JP"})
	tr.observeRep("us.node:1", &ipRepResult{Score: 70, Grade: "B", Region: "US"})
	tr.observeRep("br.node:1", &ipRepResult{Score: 70, Grade: "B", Region: "BR"})
	exits := []slot{{addr: "us.node:1"}, {addr: "jp.node:1"}, {addr: "br.node:1"}}

	preferred := map[string]struct{}{"JP": {}, regionOther: {}}
	ordered := tr.rank(exits, preferred)
	if ordered[0].addr != "jp.node:1" {
		t.Fatalf("偏好 JP + A 信誉应最先，得到 %s", ordered[0].addr)
	}
	if ordered[1].addr != "br.node:1" {
		t.Fatalf("BR 命中「其他」应次之，得到 %s", ordered[1].addr)
	}
	if ordered[2].addr != "us.node:1" {
		t.Fatalf("未选中的 US 应最后，得到 %s", ordered[2].addr)
	}

	// 不设偏好：地区不参与，信誉排序生效（A 先于 B）。
	ordered = tr.rank(exits, nil)
	if ordered[0].addr != "jp.node:1" || ordered[1].addr != "us.node:1" || ordered[2].addr != "br.node:1" {
		t.Fatalf("无偏好时应按信誉排序: %v", ordered)
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

// sing-box 本地映射节点（127.0.0.1:端口）不得拿去查本机回环的信誉：
// 无反查结果时直接跳过体检（不计失败、不写缓存）。
func TestPrivateSkipNoFetch(t *testing.T) {
	r := newIPReputer(nil)
	fetches := 0
	r.fetch = func(url string) (int, string, error) {
		fetches++
		return 200, sampleIPRiskHTML, nil
	}
	r.process("127.0.0.1:21004")
	if fetches != 0 {
		t.Fatalf("回环地址不应发起抓取，实际 %d 次", fetches)
	}
	if r.cache["127.0.0.1:21004"] != nil {
		t.Fatal("回环地址不应写缓存")
	}
	if !r.available() {
		t.Fatal("跳过体检不应计入源退避")
	}
}

// 反查成功：本地映射节点应按原始服务器地址体检，结果挂在映射地址名下。
func TestOriginHostMapping(t *testing.T) {
	r := newIPReputer(nil)
	fetches := 0
	r.fetch = func(url string) (int, string, error) {
		fetches++
		if !strings.Contains(url, "iprisk.top/ip/203.0.113.10") {
			return 0, "", fmt.Errorf("应查真实出口 IP，得到 %s", url)
		}
		return 200, sampleIPRiskHTML, nil
	}
	r.originHost = func(addr string) string {
		if addr == "127.0.0.1:21004" {
			return "203.0.113.10"
		}
		return ""
	}
	r.process("127.0.0.1:21004")
	res := r.cache["127.0.0.1:21004"]
	if res == nil {
		t.Fatal("反查成功应写缓存")
	}
	if res.IP != "203.0.113.10" {
		t.Fatalf("应按原始服务器 IP 体检: %q", res.IP)
	}
	if fetches != 1 {
		t.Fatalf("应恰好抓取 1 次，实际 %d", fetches)
	}
}

// 未体检节点的显示与排序：不得显示成 "(0)"，也不得当 D 档垫底。
func TestRepUnknownDefaults(t *testing.T) {
	tr := newExitTracker()
	tr.observeRep("d.node:1", &ipRepResult{Score: 20, Grade: "D", Region: "US"})
	if tag := tr.repTag("none:1"); tag != "" {
		t.Fatalf("未体检节点应无信誉标签，得到 %q", tag)
	}
	exits := []slot{{addr: "none:1"}, {addr: "d.node:1"}}
	ordered := tr.rank(exits, nil)
	if ordered[0].addr != "none:1" {
		t.Fatalf("未体检节点应排在中性档（D 之前），得到 %s", ordered[0].addr)
	}
}

// 404 = 该 IP 不在 iprisk 库：个体属性，不计入源退避。
func TestNotFoundNoBackoff(t *testing.T) {
	r := newIPReputer(nil)
	r.fetch = func(url string) (int, string, error) {
		return 404, "<html>not found</html>", nil
	}
	for i := 0; i < 6; i++ {
		r.process("192.0.2.1:1080")
	}
	if !r.available() {
		t.Fatal("404 不应触发源退避")
	}
}

// 直连抓取失败时，借道正式池出口重试成功。
func TestFetchPageViaExitFallback(t *testing.T) {
	var exitHits int
	// 假代理：http 目标走绝对形式请求，直接回 200 + 页面
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exitHits++
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(sampleIPRiskHTML))
	}))
	defer proxy.Close()

	r := newIPReputer(nil)
	r.ipriskBase = "http://iprisk.test"
	r.exitURLs = func() []*url.URL {
		pu, _ := url.Parse(proxy.URL)
		return []*url.URL{pu}
	}
	r.fetch = func(url string) (int, string, error) {
		return 503, "upstream busy", nil // 直连必败
	}
	status, body, err := r.fetchPage("http://iprisk.test/ip/1.2.3.4")
	if err != nil || status != 200 {
		t.Fatalf("借道应成功: status=%d err=%v", status, err)
	}
	if !strings.Contains(body, "纯净度评分为 32/100") {
		t.Fatalf("借道抓到的应是页面: %q", body[:60])
	}
	if exitHits != 1 {
		t.Fatalf("出口应被使用 1 次，实际 %d", exitHits)
	}
}

// 无出口可用时透传直连失败。
func TestFetchPageNoExits(t *testing.T) {
	r := newIPReputer(nil)
	r.exitURLs = func() []*url.URL { return nil }
	r.fetch = func(url string) (int, string, error) {
		return 503, "busy", nil
	}
	status, _, err := r.fetchPage("http://x/ip/1.2.3.4")
	if err != nil || status != 503 {
		t.Fatalf("无出口应透传直连结果: %d %v", status, err)
	}
}
