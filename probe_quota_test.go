package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestQuotaExhaustedClassification(t *testing.T) {
	quoted := []byte(`{"type":"error","error":{"type":"FreeUsageLimitError","message":"Rate limit exceeded. Please try again later."}}`)
	if !quotaExhausted(quoted) {
		t.Fatal("FreeUsageLimitError 应判定为额度枯竭")
	}
	other := []byte(`{"type":"error","error":{"type":"OverloadedError","message":"busy"}}`)
	if quotaExhausted(other) {
		t.Fatal("普通限流不应判定为额度枯竭")
	}
	if quotaExhausted(nil) || quotaExhausted([]byte("")) {
		t.Fatal("空响应体不应判定为额度枯竭")
	}
}

func TestObserveQuotaBurnLongBench(t *testing.T) {
	tracker := newExitTracker()
	addr := "10.0.0.1:1080"
	tracker.observeQuotaBurn(addr, 0, benchNone)
	if !tracker.benched(addr) {
		t.Fatal("额度枯竭后应立即坐板凳")
	}
	kept := tracker.filterBenched([]slot{{addr: addr}, {addr: "10.0.0.2:1080"}})
	if len(kept) != 1 || kept[0].addr != "10.0.0.2:1080" {
		t.Fatalf("长板凳出口应被过滤，实际 %v", kept)
	}
	// 胜出即提前回归：上游提前放行时不应被旧账拖住。
	tracker.observeWin(addr, 100*time.Millisecond)
	if tracker.benched(addr) {
		t.Fatal("胜出应解除额度板凳")
	}
}

func TestProbeBodyShape(t *testing.T) {
	gw := newGateway(config{})
	var parsed struct {
		Model     string               `json:"model"`
		MaxTokens int                  `json:"max_tokens"`
		Stream    bool                 `json:"stream"`
		Messages  []map[string]string `json:"messages"`
	}
	if err := json.Unmarshal(gw.probeBody(), &parsed); err != nil {
		t.Fatalf("探活请求体应为合法 JSON: %v", err)
	}
	// 深检模型固定 big-pickle：长期在售；列表里的其他名字随时下线，
	// 且"列表有"不代表 chat 门放行，不参与选择。
	if parsed.Model != "big-pickle" {
		t.Fatalf("深检应固定用 big-pickle，实际 %q", parsed.Model)
	}
	if parsed.MaxTokens != 1 || parsed.Stream {
		t.Fatalf("迷你请求应为 max_tokens=1 且非流式，实际 %+v", parsed)
	}
	if len(parsed.Messages) != 1 || parsed.Messages[0]["role"] != "user" {
		t.Fatalf("缺少用户消息: %+v", parsed.Messages)
	}
}

func TestProbeBodyFallbackWithoutCache(t *testing.T) {
	gw := newGateway(config{})
	var parsed struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(gw.probeBody(), &parsed); err != nil {
		t.Fatalf("探活请求体应为合法 JSON: %v", err)
	}
	if parsed.Model != "big-pickle" {
		t.Fatalf("深检模型固定 big-pickle，实际 %q", parsed.Model)
	}
}
