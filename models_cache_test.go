package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// 免费模型列表基本不变：成功拉取一次后，后续调用不得再请求上游。
func TestModelMapsFetchesOncePerRun(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"mimo-v2.5-free"},{"id":"deepseek-v4-flash-free"}]}`))
	}))
	defer upstream.Close()

	gw := newGateway(config{project: projectSpec{upstream: upstream.URL, modelPath: "/zen/v1/models", modelMode: modelOpenCode}})

	for i := 0; i < 5; i++ {
		rename, _ := gw.modelMaps(context.Background())
		if len(rename) == 0 {
			t.Fatalf("第 %d 次调用应拿到模型映射", i+1)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("上游应只被请求 1 次，实际 %d 次", got)
	}
}

// 启动时拉取失败：不应把失败结果当成功缓存，下次调用仍会重试。
func TestModelMapsRetriesUntilFirstSuccess(t *testing.T) {
	var hits atomic.Int32
	fail := atomic.Bool{}
	fail.Store(true)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) > 0 && !fail.Load() {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"mimo-v2.5-free"}]}`))
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer upstream.Close()

	gw := newGateway(config{project: projectSpec{upstream: upstream.URL, modelPath: "/zen/v1/models", modelMode: modelOpenCode}})

	// 第一次：失败，得到兜底（specialModels 可能为空，但不应 panic）。
	if _, redirect := gw.modelMaps(context.Background()); redirect == nil {
		t.Fatal("失败路径也应返回可用映射")
	}
	if gw.modelCache != nil && gw.modelCache.committed {
		t.Fatal("失败结果不应标记为已提交")
	}

	// 上游恢复；负缓存窗口由 loadedAt 回拨控制，这里直接清空时间戳模拟到期。
	gw.modelMu.Lock()
	gw.modelCache.loadedAt = gw.modelCache.loadedAt.Add(-modelCacheTTL)
	gw.modelMu.Unlock()
	fail.Store(false)

	rename, _ := gw.modelMaps(context.Background())
	if len(rename) == 0 || hits.Load() < 2 {
		t.Fatalf("恢复后应重试并成功: hits=%d rename=%v", hits.Load(), rename)
	}

	// 成功后再调用不再打上游。
	before := hits.Load()
	_, _ = gw.modelMaps(context.Background())
	if hits.Load() != before {
		t.Fatal("成功后不应再次请求上游")
	}
}
