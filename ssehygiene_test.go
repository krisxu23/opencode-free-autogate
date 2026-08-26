package main

import (
	"bytes"
	"strings"
	"testing"
)

func feedAll(s *sseSanitizer, chunks ...string) string {
	var out []byte
	for _, c := range chunks {
		out = append(out, s.feed([]byte(c))...)
	}
	return string(out)
}

func TestSSEHygieneDropsInvalidDataLines(t *testing.T) {
	s := newSSESanitizer()
	out := feedAll(s,
		": keep-alive\n",
		"data: {\"choices\":[],\"object\":\"chat.completion.chunk\"}\n",
		"data: <html>bad gateway</html>\n",
		"data: [DONE]\n",
	)
	if strings.Contains(out, "<html>") {
		t.Fatalf("HTML 垃圾行应被丢弃：%s", out)
	}
	if !strings.Contains(out, "data: [DONE]\n") || !strings.Contains(out, ": keep-alive\n") {
		t.Fatalf("合法行与注释应原样保留：%s", out)
	}
	if s.dropped != 1 {
		t.Fatalf("应记录 1 次丢弃，实际 %d", s.dropped)
	}
}

func TestSSEHygienePatchesEmptyToolCallsAndFillsFields(t *testing.T) {
	s := newSSESanitizer()
	out := feedAll(s, "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[]}}]}\n")
	if strings.Contains(out, "tool_calls") {
		t.Fatalf("空 tool_calls 应被删除：%s", out)
	}
	for _, want := range []string{`"object":"chat.completion.chunk"`, `"created":`, `"delta":{"role":"assistant"}`} {
		if !strings.Contains(out, want) {
			t.Fatalf("输出缺 %s：%s", want, out)
		}
	}
}

func TestSSEHygieneLeavesResponsesShapeUntouched(t *testing.T) {
	line := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n"
	s := newSSESanitizer()
	out := feedAll(s, line)
	if out != line {
		t.Fatalf("无 choices 的 Responses 事件不应被修补：\n%s\n%s", line, out)
	}
}

func TestSSEHygieneKeepsValidLineByteExact(t *testing.T) {
	line := "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"},\"finish_reason\":null}]}\n"
	s := newSSESanitizer()
	out := feedAll(s, line)
	if !strings.Contains(out, `"content":"你好"`) {
		t.Fatalf("多字节内容不应损坏：%s", out)
	}
	if !strings.Contains(out, `"finish_reason":null`) {
		t.Fatalf("合法行内容不应被改写：%s", out)
	}
}

func TestSSEHygieneAssemblesSplitLines(t *testing.T) {
	s := newSSESanitizer()
	out := feedAll(s, "data: {\"a\":", "1}\ndata: [DO", "NE]\n")
	got := strings.Count(out, "\n")
	if got != 2 || !strings.Contains(out, "{\"a\":1}") || !strings.Contains(out, "[DONE]") {
		t.Fatalf("跨块半行应拼齐处理：%q", out)
	}
}

func TestSSEHygieneOversizedPassthrough(t *testing.T) {
	big := strings.Repeat("x", sseMaxLineBytes+100)
	s := newSSESanitizer()
	out := feedAll(s, big+"\n", "data: {\"ok\":true}\n")
	if !strings.Contains(out, big[:32]) || !strings.Contains(out, "\"ok\":true") {
		t.Fatal("超长行应整体透传且后续正常处理")
	}
	if s.dropped != 0 {
		t.Fatalf("超长行不应计为丢弃：%d", s.dropped)
	}
}

func TestSSEHygieneValveSkipsPatchOnHugePayload(t *testing.T) {
	huge := `{"choices":[{"delta":{"content":"` + strings.Repeat("y", ssePatchValveBytes+10) + `","tool_calls":[]}}]}`
	s := newSSESanitizer()
	out := feedAll(s, "data: "+huge+"\n")
	if !strings.Contains(out, `"tool_calls":[]`) {
		t.Fatal("超阀值行应跳过结构修补原样透传")
	}
}

func TestSSEHygieneCRLFHandled(t *testing.T) {
	s := newSSESanitizer()
	out := feedAll(s, "data: {\"choices\":[],\"object\":\"chat.completion.chunk\"}\r\ndata: [DONE]\r\n")
	if strings.Contains(out, "\r") {
		t.Fatalf("CRLF 行尾应规整为 LF：%q", out)
	}
}

func TestSanitizeSSEBytesComplete(t *testing.T) {
	in := "data: junk-line-not-json\n\ndata: {\"choices\":[{\"delta\":{\"tool_calls\":[]}}]}\n\ndata: [DONE]\n"
	out := string(sanitizeSSEBytes([]byte(in)))
	if strings.Contains(out, "junk") || strings.Contains(out, "tool_calls") {
		t.Fatalf("缓冲路径清洗失效：%s", out)
	}
	if !strings.Contains(out, "[DONE]") {
		t.Fatalf("[DONE] 必须保留：%s", out)
	}
}

func TestSSEHygieneFlushEmitsPendingTail(t *testing.T) {
	s := newSSESanitizer()
	feedAll(s, "data: {\"partial\":")
	tail := s.flush()
	if tail == nil || !bytes.HasPrefix(tail, []byte("data: ")) {
		t.Fatalf("残留半行应在流结束时原样交还：%q", tail)
	}
}
