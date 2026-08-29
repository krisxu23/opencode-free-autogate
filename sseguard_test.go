package main

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 快速请求（竞速在宽限期内出赢家）不得被 Finish 拖住：这是对旧实现
// 「AfterFunc 定时 + Finish 等 stopped 导致首字节硬顶 5 秒」回归的回归测试。
func TestSseGuardFinishReturnsImmediatelyBeforeCommit(t *testing.T) {
	rec := httptest.NewRecorder()
	g := newSseGuard(rec, "/v1/chat/completions")
	time.Sleep(50 * time.Millisecond) // 模拟竞速 50ms 决出赢家
	start := time.Now()
	g.Finish()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("未提交时 Finish 应立即返回，实际阻塞 %v", elapsed)
	}
	if g.Committed() {
		t.Fatal("宽限期内 Finish 不应提交响应头")
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("不应有任何客户端写入，得到 %q", rec.Body.String())
	}
}

// 提交后心跳按协议变形发出，Finish 后不再有任何写入。
func TestSseGuardCommittedHeartbeatStopsOnFinish(t *testing.T) {
	oldDelay, oldBeat := sseCommitDelay, sseBeatInterval
	sseCommitDelay = 80 * time.Millisecond
	sseBeatInterval = 40 * time.Millisecond
	defer func() { sseCommitDelay, sseBeatInterval = oldDelay, oldBeat }()

	rec := httptest.NewRecorder()
	g := newSseGuard(rec, "/codex/v1/responses")
	time.Sleep(300 * time.Millisecond)
	if !g.Committed() {
		t.Fatal("宽限期后应提交响应头")
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("提交的 Content-Type=%q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "response.in_progress") {
		t.Fatalf("responses 路径应注入 response.in_progress 事件，得到 %q", body)
	}
	if strings.Count(body, "\n\n") < 2 {
		t.Fatalf("应有至少两次心跳，得到 %q", body)
	}
	before := rec.Body.Len()
	g.Finish()
	time.Sleep(120 * time.Millisecond)
	if rec.Body.Len() != before {
		t.Fatalf("Finish 后仍有写入：%d → %d", before, rec.Body.Len())
	}
}

// 连续两次 Finish（handlePost 与 finish 各调一次）不得 panic。
func TestSseGuardFinishIdempotent(t *testing.T) {
	rec := httptest.NewRecorder()
	g := newSseGuard(rec, "/v1/chat/completions")
	g.Finish()
	g.Finish()
}

// 心跳载荷按客户端协议变形；chat chunk 不得携带 finish_reason（null 值
// 会被终止标记扫描误判为流完整）。
func TestHeartbeatPayloadByPath(t *testing.T) {
	if p := heartbeatPayload("/codex/v1/responses"); !strings.Contains(p, `"type":"response.in_progress"`) {
		t.Fatalf("responses 心跳载荷错误: %q", p)
	}
	if p := heartbeatPayload("/v1/messages"); !strings.Contains(p, `"type":"ping"`) || !strings.Contains(p, "event: ping") {
		t.Fatalf("anthropic 心跳载荷错误: %q", p)
	}
	p := heartbeatPayload("/v1/chat/completions")
	if !strings.Contains(p, "chat.completion.chunk") {
		t.Fatalf("chat 心跳载荷错误: %q", p)
	}
	if strings.Contains(p, "finish_reason") {
		t.Fatalf("chat 心跳不得携带 finish_reason: %q", p)
	}
	for _, payload := range []string{p, heartbeatPayload("/codex/v1/responses"), heartbeatPayload("/v1/messages")} {
		for _, marker := range streamTerminals {
			if strings.Contains(payload, string(marker)) {
				t.Fatalf("心跳载荷命中终止标记 %q: %q", marker, payload)
			}
		}
	}
}
