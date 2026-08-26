package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// startDeepProber 对"GET 探活已通过"的全部在场出口做 chat 深检（max_tokens=1
// 的真实对话）：GET 探活只证明网络通，深检穿透上游额度门，专抓"网络通但
// 共享 IP 额度已枯竭"的假健康节点。
//
// 节奏（对应用户的两轮检测模型）：
//
//	第一轮 = 常态 GET 探活（零配额），节点池全量，产出延迟样本；
//	第二轮 = chat 深检（每 IP 一次迷你对话），覆盖第一轮的全部幸存者，
//	数量随池子规模动态决定——活下来多少就检多少，不设固定上限。
//
// 启动后槽位一就绪立刻跑首轮深检（用户第一句话之前假健康节点已被筛掉），
// 之后按间隔 ±20% 抖动进入常规节奏。其余纪律：
//   - 并发受深检并发封顶（默认 32 路，可配置），既并行又不至于同一瞬间
//     几十个 IP 一起戳上游；
//   - 打乱顺序，避免按固定次序形成可预测模式；
//   - 重叠保护：上一轮没跑完就跳过本轮；
//   - 坐"上游声明"板凳的出口不检（时间来自上游，戳了也白戳），
//     坐"推断"板凳的出口优先检——成功即提前回归。
func (g *gateway) startDeepProber(ctx context.Context) {
	interval := g.cfg.deepProbeInterval
	if interval < 10*time.Minute {
		interval = 10 * time.Minute
	}
	if !g.waitForCandidates(ctx) {
		return
	}
	g.runDeepProbePass(ctx)
	for {
		next := interval + time.Duration(rand.Int63n(int64(interval)*2/5)) - interval/5 // ±20%
		if next < time.Minute {
			next = time.Minute
		}
		select {
		case <-time.After(next):
		case <-ctx.Done():
			return
		}
		g.runDeepProbePass(ctx)
	}
}

// waitForCandidates 等初始槽位就绪（sing-box 桥接＋首次拉取通常几十秒），
// 最长等 10 分钟。期间每 5 秒看一眼在场快照。
func (g *gateway) waitForCandidates(ctx context.Context) bool {
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		for _, candidate := range g.customSnapshot() {
			if trackableExit(candidate.addr) {
				return true
			}
		}
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return false
		}
	}
	return false
}

// deepConcurrency 是深检的并发封顶：幸存者按 N 路并行分波消化，
// 全轮耗时 ≈ ⌈候选数/并发路数⌉ × 单次对话耗时，而不是串行累加。
// 可经界面「深检并发」或环境变量 PROXY_DEEP_CONCURRENCY 调整（默认 32，
// 上限 128——再高容易同一瞬间集中触发上游风控）。
func (g *gateway) deepConcurrency() int {
	n := g.cfg.deepConcurrency
	if n <= 0 {
		n = 32
	}
	if n > 128 {
		n = 128
	}
	return n
}

func (g *gateway) runDeepProbePass(ctx context.Context) {
	if !g.deepRunning.CompareAndSwap(false, true) {
		log.Printf("[深检] 上一轮尚未结束，跳过本轮")
		return
	}
	defer g.deepRunning.Store(false)

	candidates := g.customSnapshot()
	pool := make([]slot, 0, len(candidates))
	for _, candidate := range candidates {
		if !trackableExit(candidate.addr) {
			continue
		}
		// 上游声明的板凳不戳；推断板凳与健康出口都检（前者是提前回归的机会）。
		if g.exits.authoritativelyBenched(candidate.addr) {
			continue
		}
		pool = append(pool, candidate)
	}
	if len(pool) == 0 {
		return
	}
	rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	model := g.probeModelID()
	log.Printf("[深检] 本轮开始：%d 个候选（全部 GET 幸存者），模型 %s", len(pool), model)

	var mu sync.Mutex
	alive, dead := 0, 0
	work := make(chan slot)
	var wg sync.WaitGroup
	for worker := 0; worker < g.deepConcurrency(); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for candidate := range work {
				select {
				case <-ctx.Done():
					return
				default:
				}
				ok := g.deepProbeOne(ctx, candidate)
				mu.Lock()
				if ok {
					alive++
				} else {
					dead++
				}
				mu.Unlock()
			}
		}()
	}
	for _, candidate := range pool {
		select {
		case work <- candidate:
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return
		}
	}
	close(work)
	wg.Wait()
	log.Printf("[深检] 本轮 %d 个出口：通过 %d / 受限或失败 %d", alive+dead, alive, dead)
}
