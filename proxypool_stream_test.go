package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestForEachProbeResultStreams 验证流式语义：单个候选探测完成的瞬间结果即送达
// 回调，绝不等待整批结束（屏障）。这是「初检过关立刻进内部候选池并触发复检」
// 的基石——第一波里通过的那个节点，此刻就必须已经排队深检。
func TestForEachProbeResultStreams(t *testing.T) {
	items := []slot{{addr: "slow"}, {addr: "fast1"}, {addr: "fast2"}}
	release := make(chan struct{})
	slowStarted := make(chan struct{})
	var mu sync.Mutex
	delivered := 0

	probe := func(ctx context.Context, s slot) bool {
		if s.addr == "slow" {
			close(slowStarted)
			<-release // slow 卡住不放行，两个 fast 先完成
		}
		return true
	}
	done := make(chan struct{})
	go func() {
		forEachProbeResult(context.Background(), 4, items, probe, func(poolProbeResult) {
			mu.Lock()
			delivered++
			mu.Unlock()
		})
		close(done)
	}()

	<-slowStarted
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := delivered
		mu.Unlock()
		if n >= 2 {
			break // fast 的结果已在 slow 完成前送达 ⇒ 流式成立
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	n := delivered
	mu.Unlock()
	if n < 2 {
		t.Fatalf("屏障式行为：slow 未完成时仅送达 %d 个结果，期望 ≥2（流式）", n)
	}
	close(release)
	<-done
	mu.Lock()
	defer mu.Unlock()
	if delivered != 3 {
		t.Fatalf("共应送达 3 个结果，实际 %d", delivered)
	}
}

// TestForEachProbeResultRespectsLimit 验证并发上限不被突破且全员被探测。
func TestForEachProbeResultRespectsLimit(t *testing.T) {
	const limit = 3
	var inflight, maxInflight atomic.Int64
	items := make([]slot, 0, 12)
	for i := 0; i < 12; i++ {
		items = append(items, slot{addr: string(rune('a' + i))})
	}
	var doneCount atomic.Int64
	probe := func(ctx context.Context, s slot) bool {
		now := inflight.Add(1)
		for {
			max := maxInflight.Load()
			if now <= max || maxInflight.CompareAndSwap(max, now) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inflight.Add(-1)
		doneCount.Add(1)
		return true
	}
	called := make(chan struct{})
	go func() {
		forEachProbeResult(context.Background(), limit, items, probe, func(poolProbeResult) {})
		close(called)
	}()
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("forEachProbeResult 未在限时内完成")
	}
	if got := maxInflight.Load(); got > limit {
		t.Fatalf("并发峰值 %d 超过上限 %d", got, limit)
	}
	if doneCount.Load() != int64(len(items)) {
		t.Fatalf("应探测全部 %d 个候选，实际完成 %d", len(items), doneCount.Load())
	}
}
