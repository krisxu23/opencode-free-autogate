package main

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestParsePoolLine(t *testing.T) {
	cases := []struct {
		name string
		line string
		want string // 期望的 slot.addr；空串表示应解析失败
	}{
		{"纯IP端口", "1.2.3.4:1080", "1.2.3.4:1080"},
		{"带scheme", "socks5://1.2.3.4:1080", "1.2.3.4:1080"},
		{"带凭据", "socks5://user:pass@5.6.7.8:9999", "5.6.7.8:9999"},
		{"带名称片段", "socks5://user:pass@5.6.7.8:9999#US-node", "5.6.7.8:9999"},
		{"host:port:user:pass", "9.9.9.9:1080:admin:123456", "9.9.9.9:1080"},
		{"注释行", "# comment", ""},
		{"空行", "   ", ""},
		{"垃圾文本", "hello world", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, ok := parsePoolLine(tc.line)
			if tc.want == "" {
				if ok {
					t.Fatalf("期望解析失败，实际得到 %q", s.addr)
				}
				return
			}
			if !ok {
				t.Fatalf("期望解析成功，实际失败")
			}
			if s.addr != tc.want {
				t.Fatalf("addr = %q, 期望 %q", s.addr, tc.want)
			}
			if s.proxyURL == nil || !strings.HasPrefix(s.proxyURL.Scheme, "socks5") {
				t.Fatalf("scheme 应为 socks5h，实际 %v", s.proxyURL)
			}
		})
	}
}

func TestNormalizeSourceURL(t *testing.T) {
	cases := map[string]string{
		"https://github.com/roosterkid/openproxylist/blob/main/SOCKS5.txt":            "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5.txt",
		"https://github.com/a/b/blob/main/x.txt?plain=1":                              "https://raw.githubusercontent.com/a/b/main/x.txt",
		"https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt":    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
		"  https://bestcf.pages.dev/s5gy/all.txt#fragment ":                           "https://bestcf.pages.dev/s5gy/all.txt",
	}
	for input, want := range cases {
		if got := normalizeSourceURL(input); got != want {
			t.Errorf("normalizeSourceURL(%q) = %q, 期望 %q", input, got, want)
		}
	}
}

func TestParsePoolSources(t *testing.T) {
	raw := "https://a.example/list.txt\nhttps://b.example/list.txt, https://a.example/list.txt#x"
	got := parsePoolSources(raw)
	if len(got) != 2 {
		t.Fatalf("期望去重后 2 个源，实际 %d: %v", len(got), got)
	}
	if got[0] != "https://a.example/list.txt" || got[1] != "https://b.example/list.txt" {
		t.Fatalf("源顺序或内容不符: %v", got)
	}
}

func TestAppendPoolBodyJSON(t *testing.T) {
	body := []byte(`[{"address":"1.2.3.4:8080","protocol":"socks5","latency":100,"quality_grade":"S","status":"active"}]`)
	var out []slot
	var advanced []string
	seen := make(map[string]struct{})
	n := appendPoolBody(&out, &advanced, seen, body)
	if n != 1 || len(out) != 1 || out[0].addr != "1.2.3.4:8080" {
		t.Fatalf("JSON 解析结果不符: n=%d out=%v", n, out)
	}
}

// base64 订阅（机场分享格式）：整包解码后应得到高级链接与普通节点。
func TestAppendPoolBodyBase64Subscription(t *testing.T) {
	text := strings.Join([]string{
		"vless://b831381d-6324-4d53-ad4f-8cda48b30811@example.com:443?security=tls&sni=a.com#HK",
		"hysteria2://pass@1.2.3.4:36712?sni=b.com",
		"socks5://5.6.7.8:1080",
		"9.9.9.9:1080", // host:port 也应兼容
	}, "\n")
	body := []byte(base64.StdEncoding.EncodeToString([]byte(text)))
	var out []slot
	var advanced []string
	seen := make(map[string]struct{})
	n := appendPoolBody(&out, &advanced, seen, body)
	if n != 4 {
		t.Fatalf("订阅应解析出 4 条，实际 %d: out=%v adv=%v", n, out, advanced)
	}
	if len(out) != 2 || len(advanced) != 2 {
		t.Fatalf("普通/高级分类不符: out=%d adv=%d", len(out), len(advanced))
	}
	if !strings.HasPrefix(advanced[0], "vless://") || !strings.HasPrefix(advanced[1], "hysteria2://") {
		t.Fatalf("高级链接内容不符: %v", advanced)
	}
}

func TestSampleSlots(t *testing.T) {
	items := make([]slot, 1000)
	for i := range items {
		items[i] = slot{addr: string(rune('a' + i%26))}
	}
	got := sampleSlots(items, 400)
	if len(got) != 400 {
		t.Fatalf("期望抽样 400 条，实际 %d", len(got))
	}
	if len(sampleSlots(items, 2000)) != 1000 {
		t.Fatal("limit 大于长度时应返回原列表")
	}
}
