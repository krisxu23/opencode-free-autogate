package main

import (
	"errors"
	"fmt"
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
	for i := 0; i < 3; i++ {
		r.process("1.2.3.4:1080")
	}
	if r.available() {
		t.Fatal("连续 3 次失败后应进入退避")
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
