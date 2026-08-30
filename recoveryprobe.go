package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"time"
)

// 熔断 OPEN 态主动恢复探测（借鉴 cc-relay health/checker）：既有熔断恢复
// 是"反应式"的——半开探针由请求触发（TryProbe），没有请求进来时只能干等
// outageHoldQuiet 到期。这里补一个后台轻量探活：熔断生效期间周期性对上游
// 直连发 GET /v1/models（零配额，与常态探活同款），探测成功即 NoteSuccess
// 提前解除熔断，把恢复窗口从"等到期"提前到"一恢复就发现"。
//
// 纪律：
//   - 探活间隔带 ±30% 抖动，避免多个出口同时恢复时形成 thundering herd；
//   - 只在熔断保持期内工作，熔断解除后安静退出；
//   - 探测走直连 transport（与模型/出口健康无关，只验证上游可达）；
//   - 探活失败不记账（那是熔断已经在处理的状态，无需重复证据）。

const (
	outageProbeInterval   = 15 * time.Second
	outageProbeJitter     = 0.3
	outageProbeAttemptMax = 3 // 每轮周期最多连续探几次，防死循环占 CPU
)

// startOutageRecoveryProbe 启动熔断恢复探针：熔断期间按间隔直连上游探活，
// 成功即提前解除熔断。ctx 取消时退出。
func (g *gateway) startOutageRecoveryProbe(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(outageProbeInterval + jitterDuration(outageProbeInterval, outageProbeJitter)):
		}
		if !g.outage.Tripped() {
			continue // 未熔断：安静休息
		}
		if g.recoveryProbeOnce(ctx) {
			g.outage.NoteSuccess()
		}
	}
}

// recoveryProbeOnce 对上游直连做一次轻量探活：GET /v1/models（零配额）。
// 返回是否成功（2xx/3xx 视为上游恢复）。
func (g *gateway) recoveryProbeOnce(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	deadline := time.Now().Add(g.cfg.probeTimeout)
	request := upstreamRequest{
		method:   http.MethodGet,
		path:     g.cfg.project.probePath,
		headers:  g.cfg.project.probeHeaders.Clone(),
		deadline: deadline,
	}
	// 直连（nil proxyURL）：探测只验证上游可达，不绕出口。
	live, err := g.openUpstream(ctx, request, nil, g.cfg.probeTimeout)
	if err != nil {
		return false
	}
	status := live.response.StatusCode
	live.Close()
	if status >= 200 && status < 400 {
		log.Printf("[全局熔断] 恢复探针成功（直连 %d），提前解除熔断", status)
		return true
	}
	return false
}

// jitterDuration 返回 d 的 ±jitter 随机抖动时长（供探针/周期任务错峰）。
func jitterDuration(d time.Duration, jitter float64) time.Duration {
	delta := int64(float64(d) * jitter)
	off := rand.Int63n(2*delta+1) - delta
	return time.Duration(off)
}
