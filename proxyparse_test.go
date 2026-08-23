package main

import (
	"strings"
	"testing"
)

func TestParseProxyInputSupportedSchemes(t *testing.T) {
	input := strings.Join([]string{
		"socks5://127.0.0.1:7890",
		"http://user:pass@10.0.0.1:8080",
		"https://1.2.3.4:8443",
		"socks5h://127.0.0.1:1080",
	}, "\n")

	good, bad := ParseProxyInput(input)
	if len(bad) != 0 {
		t.Fatalf("unexpected errors: %v", bad)
	}
	if len(good) != 4 {
		t.Fatalf("expected 4 proxies, got %d: %v", len(good), good)
	}
}

func TestParseProxyInputSharedSOCKSLink(t *testing.T) {
	// base64("phah8Ienguli:ahS6aeFad5du")
	line := "socks://cGhhaDhJZW5ndWxpOmFoUzZhZUZhZDVkdQ@207.148.124.56:1080#%F0%9F%87%B8%F0%9F%87%AC%20Singapore"

	good, bad := ParseProxyInput(line)
	if len(bad) != 0 {
		t.Fatalf("unexpected errors: %v", bad)
	}
	want := "socks5://phah8Ienguli:ahS6aeFad5du@207.148.124.56:1080"
	if len(good) != 1 || good[0] != want {
		t.Fatalf("got %v, want %q", good, want)
	}
}

func TestParseProxyInputRejectsUnsupportedAndInvalid(t *testing.T) {
	input := strings.Join([]string{
		"hysteria://key@1.2.3.4:443", // Hysteria v1 仍不支持
		"socks5://127.0.0.1",
		"not-a-url",
		"vless://example.com:443", // 缺少 UUID
	}, "\n")

	good, bad := ParseProxyInput(input)
	if len(good) != 0 {
		t.Fatalf("expected no valid proxies, got %v", good)
	}
	if len(bad) != 4 {
		t.Fatalf("expected 4 errors, got %d: %v", len(bad), bad)
	}
	if !strings.Contains(bad[1].Error(), "端口") {
		t.Fatalf("expected port hint, got %q", bad[1].Error())
	}
}

// 高级协议链接（经内嵌 sing-box 支持）应被原样接受。
func TestParseProxyInputAcceptsAdvancedLinks(t *testing.T) {
	input := strings.Join([]string{
		"vless://b831381d-6324-4d53-ad4f-8cda48b30811@example.com:443?type=ws&security=tls",
		"hysteria2://key@1.2.3.4:443",
	}, "\n")
	good, bad := ParseProxyInput(input)
	if len(bad) != 0 {
		t.Fatalf("unexpected errors: %v", bad)
	}
	if len(good) != 2 {
		t.Fatalf("expected 2 advanced links, got %v", good)
	}
}

func TestParseProxyInputDeduplicatesAndAcceptsCommas(t *testing.T) {
	good, bad := ParseProxyInput("socks5://127.0.0.1:7890, socks5://127.0.0.1:7890\nsocks5://127.0.0.1:7891")
	if len(bad) != 0 {
		t.Fatalf("unexpected errors: %v", bad)
	}
	if len(good) != 2 {
		t.Fatalf("expected 2 unique proxies, got %v", good)
	}
}

func TestParseMirrorListNormalizesSuffixes(t *testing.T) {
	good, bad := parseMirrorList(strings.Join([]string{
		"https://opencode.ai.cmliussss.net/zen/v1",
		"https://opencode.fastly.cmliussss.net/zen/",
		"https://opencode.gcore.cmliussss.net/zen  # 备注",
		"ftp://bad.example.com",
	}, "\n"))

	if len(bad) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(bad), bad)
	}
	want := []string{
		"https://opencode.ai.cmliussss.net/zen",
		"https://opencode.fastly.cmliussss.net/zen",
		"https://opencode.gcore.cmliussss.net/zen",
	}
	if len(good) != len(want) {
		t.Fatalf("got %v, want %v", good, want)
	}
	for i := range want {
		if good[i] != want[i] {
			t.Fatalf("mirror %d = %q, want %q", i, good[i], want[i])
		}
	}
}
