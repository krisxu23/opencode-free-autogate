package main

import (
	"encoding/base64"
	"strings"
	"testing"
)

func stdB64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func TestParseVLESSLink(t *testing.T) {
	link := "vless://b831381d-6324-4d53-ad4f-8cda48b30811@example.com:443?encryption=none&security=reality&sni=www.microsoft.com&fp=chrome&pbk=aGVsbG8&sid=6ba85179&type=tcp&flow=xtls-rprx-vision#香港节点"
	node, err := parseAdvancedNode(link)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if node.kind != "vless" || node.server != "example.com" || node.port != 443 {
		t.Fatalf("基础字段不符: %+v", node)
	}
	if node.uuid != "b831381d-6324-4d53-ad4f-8cda48b30811" || node.flow != "xtls-rprx-vision" {
		t.Fatalf("uuid/flow 不符: %+v", node)
	}
	if node.realityPBK != "aGVsbG8" || !node.tls {
		t.Fatalf("reality/tls 不符: %+v", node)
	}
}

func TestParseVMessBase64Link(t *testing.T) {
	// v2rayN 风格 base64 JSON
	raw := `{"v":"2","ps":"测试","add":"a.com","port":"8443","id":"b831381d-6324-4d53-ad4f-8cda48b30811","aid":"0","net":"ws","host":"cdn.a.com","path":"/ws","tls":"tls"}`
	link := "vmess://" + base64Std(raw)
	node, err := parseAdvancedNode(link)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if node.server != "a.com" || node.port != 8443 || node.transport != "ws" {
		t.Fatalf("字段不符: %+v", node)
	}
	if node.wsPath != "/ws" || node.wsHost != "cdn.a.com" || !node.tls {
		t.Fatalf("ws/tls 不符: %+v", node)
	}
}

func TestParseHysteria2Link(t *testing.T) {
	link := "hy2://pass123@1.2.3.4:36712?sni=h.example.com&insecure=1&obfs=salamander&obfs-password=ob123#JP"
	node, err := parseAdvancedNode(link)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if node.kind != "hysteria2" || node.password != "pass123" || node.port != 36712 {
		t.Fatalf("字段不符: %+v", node)
	}
	if node.obfsPassword != "ob123" || !node.allowInsecure {
		t.Fatalf("obfs/insecure 不符: %+v", node)
	}
}

func TestParseSSLinkUserinfo(t *testing.T) {
	// method:pass = aes-128-gcm:test 的 base64
	cred := base64Std("aes-128-gcm:test")
	link := "ss://" + cred + "@5.6.7.8:8388#ss-node"
	node, err := parseAdvancedNode(link)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if node.method != "aes-128-gcm" || node.password != "test" || node.port != 8388 {
		t.Fatalf("字段不符: %+v", node)
	}
}

func TestParseTrojanLink(t *testing.T) {
	link := "trojan://pass@9.9.9.9:443?sni=t.example.com&type=ws&path=%2Ftr#trojan"
	node, err := parseAdvancedNode(link)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if node.kind != "trojan" || node.password != "pass" || !node.tls || node.transport != "ws" {
		t.Fatalf("字段不符: %+v", node)
	}
}

func TestNormalizeProxyLineAcceptsAdvanced(t *testing.T) {
	link := "vless://b831381d-6324-4d53-ad4f-8cda48b30811@example.com:443?security=tls&sni=a.com"
	got, err := normalizeProxyLine(link)
	if err != nil {
		t.Fatalf("高级链接应被接受: %v", err)
	}
	if !strings.HasPrefix(got, "vless://") {
		t.Fatalf("原样保留链接，实际 %q", got)
	}
	if _, err := normalizeProxyLine("hysteria://1.2.3.4:443"); err == nil {
		t.Fatal("hysteria(v1) 应仍然不支持")
	}
}

func base64Std(s string) string {
	return stdB64Encode(s)
}
