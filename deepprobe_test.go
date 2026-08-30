package main

import (
	"net/url"
	"testing"
	"time"
)

// TestDeepProbeIPCacheShare 验证：同 IP 的 ok 结果在 TTL 内共享。
func TestDeepProbeIPCacheShare(t *testing.T) {
	c := newDeepProbeIPCache()
	if _, share := c.lookup("1.2.3.4"); share {
		t.Fatal("未探测过的 IP 不应共享")
	}
	c.store("1.2.3.4", true, false)
	if ok, share := c.lookup("1.2.3.4"); !share || !ok {
		t.Fatalf("ok 结果应共享: ok=%v share=%v", ok, share)
	}
	// 软失败（额度受限）也共享：同 IP 其他端口同样额度耗尽。
	c2 := newDeepProbeIPCache()
	c2.store("5.6.7.8", false, false)
	if ok, share := c2.lookup("5.6.7.8"); !share || ok {
		t.Fatalf("软失败应共享: ok=%v share=%v", ok, share)
	}
	// 硬失败（连不通）不共享：只淘汰代表端口，同 IP 其他端口可能存活。
	c3 := newDeepProbeIPCache()
	c3.store("9.9.9.9", false, true)
	if _, share := c3.lookup("9.9.9.9"); share {
		t.Fatal("硬失败不应共享")
	}
}

// TestDeepProbeIPCacheTTL 验证：TTL 过期后重新探测。
func TestDeepProbeIPCacheTTL(t *testing.T) {
	c := newDeepProbeIPCache()
	now := time.Now()
	c.now = func() time.Time { return now }
	c.store("1.2.3.4", true, false)
	if _, share := c.lookup("1.2.3.4"); !share {
		t.Fatal("TTL 内应共享")
	}
	now = now.Add(deepProbeDedupTTL + time.Minute)
	if _, share := c.lookup("1.2.3.4"); share {
		t.Fatal("TTL 过期后不应共享")
	}
}

// TestSlotExitIP 验证出口 IP 解析：
// 普通代理取 URL 主机名；高级映射（127.0.0.1 本地端口）反查原始服务器。
func TestSlotExitIP(t *testing.T) {
	g := newGateway(config{})
	// 普通代理：ip:port
	u, _ := url.Parse("socks5://1.2.3.4:1080")
	if got := g.slotExitIP(slot{proxyURL: u}); got != "1.2.3.4" {
		t.Fatalf("普通代理出口 IP=%q，want 1.2.3.4", got)
	}
	// 域名代理：取主机名
	u2, _ := url.Parse("http://proxy.example.com:8080")
	if got := g.slotExitIP(slot{proxyURL: u2}); got != "proxy.example.com" {
		t.Fatalf("域名代理出口=%q", got)
	}
	// 直连：空
	if got := g.slotExitIP(slot{proxyURL: nil}); got != "" {
		t.Fatalf("直连出口 IP=%q，want 空", got)
	}
	// 高级映射未建桥时不反查（advOriginHost 返回空）：回退到 ""。
	u3, _ := url.Parse("socks5://127.0.0.1:21001")
	if got := g.slotExitIP(slot{proxyURL: u3, addr: "127.0.0.1:21001"}); got != "" {
		t.Fatalf("未建桥的 127 映射出口 IP=%q，want 空", got)
	}
}
