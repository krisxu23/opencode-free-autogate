package main

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func captureEchoLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)
	fn()
	return buf.String()
}

func TestEchoHeadersMasksSensitiveValues(t *testing.T) {
	header := map[string][]string{
		"Authorization": {"Bearer sk-live-abcdef1234567890"},
		"X-Api-Key":     {"supersecret"},
		"Content-Type":  {"text/event-stream; charset=utf-8"},
		"Set-Cookie":    {"session=abc123; Path=/"},
	}
	output := captureEchoLog(t, func() {
		logEchoHeaders("/openai/v1/chat/completions", 200, header)
	})
	if strings.Contains(output, "sk-live") || strings.Contains(output, "supersecret") || strings.Contains(output, "abc123") {
		t.Fatalf("敏感值必须脱敏：%s", output)
	}
	if !strings.Contains(output, "<已脱敏 len=") {
		t.Fatalf("脱敏占位缺失：%s", output)
	}
	if !strings.Contains(output, "Content-Type[0]: text/event-stream; charset=utf-8") {
		t.Fatalf("普通头应可见：%s", output)
	}
}

func TestEchoHeadersClipsLongValues(t *testing.T) {
	longUA := strings.Repeat("u", echoValueClip+50)
	header := map[string][]string{"User-Agent": {longUA}}
	output := captureEchoLog(t, func() {
		logEchoHeaders("/p", 200, header)
	})
	if strings.Contains(output, longUA) {
		t.Fatal("超长值应截断")
	}
	if !strings.Contains(output, strings.Repeat("u", echoValueClip)+"…") {
		t.Fatal("截断应保留前缀并加省略号")
	}
}

func TestEchoHeadersDeterministicOrder(t *testing.T) {
	header := map[string][]string{
		"Z-Beta":  {"2"},
		"A-Alpha": {"1"},
		"M-Mid":   {"3"},
	}
	output := captureEchoLog(t, func() {
		logEchoHeaders("/p", 200, header)
	})
	iA := strings.Index(output, "A-Alpha")
	iM := strings.Index(output, "M-Mid")
	iZ := strings.Index(output, "Z-Beta")
	if !(iA < iM && iM < iZ) {
		t.Fatalf("键序应为字典序：%s", output)
	}
}
