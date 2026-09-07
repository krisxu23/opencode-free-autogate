package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// newFakeLive 用内存字符串伪装一条上游流式响应，供吸收循环测试读取。
func newFakeLive(sse string) *liveResponse {
	return &liveResponse{
		response: &http.Response{Body: io.NopCloser(strings.NewReader(sse)), Header: http.Header{}},
		cancel:   func() {},
		headerAt: time.Now(),
	}
}

func TestStreamCompleteMarkers(t *testing.T) {
	full := "data: {\"choices\":[{\"finish_reason\":null}]}\n\ndata: [DONE]\n\n"
	partial := "data: {\"choices\":[{\"delta\":{\"content\":\"he"
	if !streamComplete([]byte(full)) {
		t.Error("含 [DONE] 应判定完整")
	}
	if streamComplete([]byte(partial)) {
		t.Error("半截流不应判定完整")
	}
	if !streamComplete([]byte("event: message_stop")) {
		t.Error("anthropic message_stop 应判定完整")
	}
}

func TestDispatchAbsorbRetriesUntilComplete(t *testing.T) {
	hits := 0
	dispatch := func(ctx context.Context, req upstreamRequest, tr *requestTrace) (*gatewayResponse, error) {
		hits++
		switch hits {
		case 1, 2:
			return &gatewayResponse{status: 200, header: http.Header{}, live: newFakeLive("data: {\"delta\":\"par")}, nil
		default:
			return &gatewayResponse{status: 200, header: http.Header{}, live: newFakeLive("data: {\"ok\":1}\n\ndata: [DONE]\n\n")}, nil
		}
	}
	g := newGateway(config{absorbAttempts: 5})
	trace := newRequestTrace()
	resp, err := g.dispatchAbsorbWith(context.Background(), upstreamRequest{stream: true}, trace, dispatch)
	if err != nil || hits != 3 {
		t.Fatalf("err=%v hits=%d（期望第 3 次成功）", err, hits)
	}
	if resp.live != nil || !strings.Contains(string(resp.body), "[DONE]") || resp.status != http.StatusOK {
		t.Fatalf("应返回缓冲的完整体，得到 live=%v body=%q status=%d", resp.live != nil, resp.body, resp.status)
	}
}

func TestDispatchAbsorbExhaustedReturnsTruncated(t *testing.T) {
	hits := 0
	dispatch := func(ctx context.Context, req upstreamRequest, tr *requestTrace) (*gatewayResponse, error) {
		hits++
		return &gatewayResponse{status: 200, header: http.Header{}, live: newFakeLive("data: {\"delta\":\"cut")}, nil
	}
	g := newGateway(config{absorbAttempts: 2})
	resp, err := g.dispatchAbsorbWith(context.Background(), upstreamRequest{stream: true}, newRequestTrace(), dispatch)
	if hits != 2 || err != errStreamTruncated || resp != nil {
		t.Fatalf("用尽应返回截断错误：hits=%d err=%v resp=%v", hits, err, resp)
	}
}

func TestDispatchAbsorb400ImmediateReturn(t *testing.T) {
	hits := 0
	dispatch := func(ctx context.Context, req upstreamRequest, tr *requestTrace) (*gatewayResponse, error) {
		hits++
		return &gatewayResponse{status: http.StatusBadRequest, header: http.Header{}, body: []byte(`{"error":"bad request"}`)}, nil
	}
	g := newGateway(config{absorbAttempts: 5})
	resp, err := g.dispatchAbsorbWith(context.Background(), upstreamRequest{stream: true}, newRequestTrace(), dispatch)
	if hits != 1 || err != nil || resp.status != http.StatusBadRequest {
		t.Fatalf("400 不应重试：hits=%d err=%v status=%d", hits, err, resp.status)
	}
}

func TestDispatchAbsorbRetryableThenSuccess(t *testing.T) {
	hits := 0
	dispatch := func(ctx context.Context, req upstreamRequest, tr *requestTrace) (*gatewayResponse, error) {
		hits++
		if hits == 1 {
			return &gatewayResponse{status: http.StatusBadGateway, header: http.Header{}, body: []byte(`{"error":"upstream"}`)}, nil
		}
		return &gatewayResponse{status: 200, header: http.Header{}, live: newFakeLive("data: ok\n\ndata: [DONE]\n\n")}, nil
	}
	g := newGateway(config{absorbAttempts: 3})
	resp, err := g.dispatchAbsorbWith(context.Background(), upstreamRequest{stream: true}, newRequestTrace(), dispatch)
	if err != nil || hits != 2 || !strings.Contains(string(resp.body), "[DONE]") {
		t.Fatalf("5xx 后应换道重试成功：hits=%d err=%v body=%q", hits, err, resp.body)
	}
}

// 供应商请求必须绕过吸收循环：m365/（有账号时）与已启用通用供应商
// 前缀请求直通独立通道，绝不发往 opencode.ai 上游（节点池/竞速/吸收
// 是 opencode.ai 专属管道）。
func TestDispatchAbsorbBypassesSupplierRequests(t *testing.T) {
	g := newGateway(config{
		absorbAttempts: 3,
		suppliers: []Supplier{{
			ID: "local-m365", Name: "Local", BaseURL: "http://127.0.0.1:9/v1",
			Models: []string{"gpt-5.6-sol"}, Enabled: true,
		}},
	})
	for _, tc := range []struct {
		name, model string
		bypass      bool
	}{
		{"通用供应商前缀", "local-m365/gpt-5.6-sol", true},
		{"未知前缀", "other/model", false},
		{"无前缀 opencode 模型", "big-pickle", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := upstreamRequest{stream: true, body: []byte(`{"model":"` + tc.model + `","messages":[{"role":"user","content":"hi"}]}`)}
			if got := g.isSupplierRouted(req); got != tc.bypass {
				t.Fatalf("isSupplierRouted(%q) = %v, 期望 %v", tc.model, got, tc.bypass)
			}
		})
	}
}

// 吸收缓存只存 2xx：错误响应（401 等）不得缓存回放，否则一次失败
// 会在 TTL 内被放大成所有重试都秒回同一错误。
func TestAbsorbCacheSkipsNon2xx(t *testing.T) {
	g := newGateway(config{absorbCacheTTL: time.Minute})
	g.absorbCache = newAbsorbCache(time.Minute)
	req := upstreamRequest{body: []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)}
	g.storeAbsorbResult(req, &gatewayResponse{status: http.StatusUnauthorized, header: http.Header{}, body: []byte(`{"error":"401"}`)})
	if cached := g.lookupAbsorbResult(req); cached != nil {
		t.Fatalf("401 不应被缓存，却命中了: status=%d", cached.status)
	}
	g.storeAbsorbResult(req, &gatewayResponse{status: http.StatusOK, header: http.Header{}, body: []byte(`{"ok":1}`)})
	if cached := g.lookupAbsorbResult(req); cached == nil || cached.status != http.StatusOK {
		t.Fatalf("200 应被缓存命中，得到 %v", cached)
	}
}
