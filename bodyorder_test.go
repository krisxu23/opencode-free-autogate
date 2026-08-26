package main

import (
	"strings"
	"testing"
)

func indexOf(t *testing.T, s, sub string) int {
	t.Helper()
	i := strings.Index(s, sub)
	if i < 0 {
		t.Fatalf("输出缺少 %s：%s", sub, s)
	}
	return i
}

func TestMarshalOrderedBodyChatOrder(t *testing.T) {
	payload := map[string]any{
		"stream":      true,
		"messages":    []any{map[string]any{"role": "user", "content": "hi"}},
		"temperature": 0.7,
		"model":       "m",
		"tools":       []any{},
		"foo":         "bar",
	}
	out, ok := marshalOrderedBody(payload, pickBodyOrder("/openai/v1/chat/completions"))
	if !ok {
		t.Fatal("正常 payload 不应失败")
	}
	s := string(out)
	seq := []string{`"model":`, `"temperature":`, `"messages":`, `"tools":`, `"stream":`, `"foo":`}
	prev := -1
	for _, key := range seq {
		i := indexOf(t, s, key)
		if i < prev {
			t.Fatalf("键序错误：%s 出现在前一键之前\n%s", key, s)
		}
		prev = i
	}
}

func TestMarshalOrderedBodyResponsesOrder(t *testing.T) {
	payload := map[string]any{
		"instructions":     "sys",
		"input":            "hi",
		"model":            "m",
		"prompt_cache_key": "k",
		"store":            true,
	}
	out, ok := marshalOrderedBody(payload, pickBodyOrder("/v1/responses"))
	if !ok {
		t.Fatal("正常 payload 不应失败")
	}
	s := string(out)
	seq := []string{`"model":`, `"input":`, `"instructions":`, `"store":`, `"prompt_cache_key":`}
	prev := -1
	for _, key := range seq {
		i := indexOf(t, s, key)
		if i < prev {
			t.Fatalf("Responses 键序错误：%s\n%s", key, s)
		}
		prev = i
	}
}

func TestMarshalOrderedBodyTrailerLast(t *testing.T) {
	payload := map[string]any{
		"model":                  "glm-x",
		"messages":               []any{},
		"prompt_cache_retention": "24h",
		"cache_control":          map[string]any{"type": "ephemeral"},
	}
	out, ok := marshalOrderedBody(payload, chatBodyFieldOrder)
	if !ok {
		t.Fatal("正常 payload 不应失败")
	}
	s := string(out)
	iModel := indexOf(t, s, `"model":`)
	iRetention := indexOf(t, s, `"prompt_cache_retention":`)
	iCache := indexOf(t, s, `"cache_control":`)
	if !(iModel < iRetention && iRetention < iCache) {
		t.Fatalf("注入字段必须垫底：%s", s)
	}
}

func TestMarshalOrderedBodyUnknownKeysSortedTail(t *testing.T) {
	payload := map[string]any{"zeta": 1, "alpha": 2, "model": "m"}
	out, ok := marshalOrderedBody(payload, chatBodyFieldOrder)
	if !ok {
		t.Fatal("正常 payload 不应失败")
	}
	s := string(out)
	if indexOf(t, s, `"alpha":`) > indexOf(t, s, `"zeta":`) {
		t.Fatalf("未知键应按字母序追加：%s", s)
	}
}

func TestMarshalOrderedBodyUnserializableValueFails(t *testing.T) {
	payload := map[string]any{"model": "m", "bad": make(chan int)}
	if _, ok := marshalOrderedBody(payload, chatBodyFieldOrder); ok {
		t.Fatal("含不可序列化值时应整体放弃（ok=false）")
	}
}

func TestPickBodyOrder(t *testing.T) {
	if len(pickBodyOrder("/openai/v1/chat/completions")) == 0 {
		t.Fatal("chat 路径应有字段序表")
	}
	if len(pickBodyOrder("/codex/v1/responses")) == 0 {
		t.Fatal("responses 路径应有字段序表")
	}
	if pickBodyOrder("/v1/messages") != nil {
		t.Fatal("Anthropic 形态暂不整形，应为 nil")
	}
}
