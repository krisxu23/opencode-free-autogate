package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRedactLine(t *testing.T) {
	cases := []struct{ name, in, wantSub, notSub string }{
		{"sk- Key", "[配置] Key: sk-abcd1234efgh5678ijkl", "sk-<已脱敏>", "abcd1234efgh5678"},
		{"Bearer 头", "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig", "Bearer <已脱敏>", "eyJhbGciOiJIUzI1NiJ9"},
		{"api_key 查询参数", "https://zenproxy.top/api/relay?api_key=secret123&url=x", "api_key=<已脱敏>", "secret123"},
	}
	for _, c := range cases {
		if got := redactLine(c.in); !strings.Contains(got, c.wantSub) || strings.Contains(got, c.notSub) {
			t.Errorf("%s: redactLine(%q) = %q", c.name, c.in, got)
		}
	}
	if got := redactLine("[S级] 1.2.3.4 (1/5)"); got != "[S级] 1.2.3.4 (1/5)" {
		t.Errorf("普通行应原样直通，得到 %q", got)
	}
}

func TestMaskSecret(t *testing.T) {
	key := "sk-" + strings.Repeat("a", 27)
	masked := maskSecret(key)
	if strings.Contains(masked, strings.Repeat("a", 27)) {
		t.Fatalf("掩码不应包含完整 Key: %q", masked)
	}
	if !strings.HasPrefix(masked, "sk-aaaaa") || !strings.Contains(masked, "len=30") {
		t.Fatalf("掩码应保留前缀与长度: %q", masked)
	}
	if maskSecret("") != "" {
		t.Fatal("空值应返回空串")
	}
}

func TestLogKitDedup(t *testing.T) {
	var sink strings.Builder
	k := newLogKit(&sink)
	k.dupWindow = time.Second
	k.summaryIn = 10 * time.Second // 不让定时器抢在断言前汇报

	k.Write([]byte("2026/08/29 10:00:00.000000 [错] 1.2.3.4: boom\n"))
	k.Write([]byte("2026/08/29 10:00:00.100000 [错] 1.2.3.4: boom\n"))
	if n := strings.Count(sink.String(), "boom"); n != 1 {
		t.Fatalf("重复行应被抑制，得到 %d 条", n)
	}
	k.Write([]byte("2026/08/29 10:00:00.200000 [错] 5.6.7.8: other\n"))
	out := sink.String()
	if !strings.Contains(out, "×2 已合并") || !strings.Contains(out, "boom") {
		t.Fatalf("新日志到来时应汇报合并统计: %q", out)
	}
	if strings.Count(out, "other") != 1 {
		t.Fatalf("不同日志应正常放行: %q", out)
	}
}

func TestLogKitRateLimit(t *testing.T) {
	var sink strings.Builder
	k := newLogKit(&sink)
	k.floodPerSec = 3
	for i := 0; i < 10; i++ {
		k.Write([]byte("2026/08/29 10:00:00.000000 line " + strconv.Itoa(i) + "\n"))
	}
	if passed := strings.Count(sink.String(), "line "); passed != 3 {
		t.Fatalf("限速后应只放行 %d 行，实际 %d", 3, passed)
	}
	// 人为把时钟拨到下一秒，触发丢弃统计汇报。
	k.mu.Lock()
	k.sec = 0
	k.mu.Unlock()
	k.Write([]byte("2026/08/29 10:00:02.000000 later\n"))
	if !strings.Contains(sink.String(), "限流丢弃 7 行") {
		t.Fatalf("跨秒后应汇报丢弃统计: %q", sink.String())
	}
}

func TestLogKitRotation(t *testing.T) {
	dir := t.TempDir()
	var sink strings.Builder
	k := newLogKit(&sink)
	k.maxFileSize = 200
	k.maxBackups = 2
	defer func() {
		if k.file != nil {
			k.file.Close() // Windows 上打开中的文件无法被 TempDir 清理
		}
	}()
	if path := k.openFile(dir); path == "" {
		t.Fatal("日志文件打开失败")
	}
	for i := 0; i < 30; i++ {
		k.Write([]byte("2026/08/29 10:00:00.000000 [日志] 轮转测试行 i=" + strconv.Itoa(i) + " padding padding\n"))
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "logs", "gateway-*.log"))
	if len(matches) > 2 {
		t.Fatalf("归档应 ≤%d 份，实际 %d", 2, len(matches))
	}
	if _, err := os.Stat(filepath.Join(dir, "logs", "gateway.log")); err != nil {
		t.Fatalf("当前日志文件应存在: %v", err)
	}
}
