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
// 启动后复检走两条并行通道：
//  1. 流式：初检一有健康节点入池立即进 deepQueue 深检——不等整轮初检结束，
//     边测边筛，用户第一句话之前假健康节点已被筛掉；
//  2. 周期：按间隔 ±20% 抖动对全池做整体深检，兜住流式阶段之后才枯竭的额度。
//
// 其余纪律：
//   - 流式 worker 数 = 「检测并发」的 1/8（1-16 个）；周期轮并发同上限；
//   - 周期轮打乱顺序，避免按固定次序形成可预测模式；
//   - 重叠保护：上一轮没跑完就跳过本轮；流式与周期轮可能对同一节点偶发
//     重复深检，代价是多花一次迷你对话，可接受；
//   - 坐"上游声明"板凳的出口不检（时间来自上游，戳了也白戳），
//     坐"推断"板凳的出口优先检——成功即提前回归。
func (g *gateway) startDeepProber(ctx context.Context) {
	workers := g.probeConcurrency() / 8
	if workers < 1 {
		workers = 1
	}
	if workers > 16 {
		workers = 16
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case s := <-g.deepQueue:
					g.deepProbeOne(ctx, s)
				}
			}
		}()
	}

	interval := g.cfg.deepProbeInterval
	if interval < 10*time.Minute {
		interval = 10 * time.Minute
	}
	for {
		next := interval + time.Duration(rand.Int63n(int64(interval)*2/5)) - interval/5 // ±20%
		if next < time.Minute {
			next = time.Minute
		}
		select {
		case <-time.After(next):
		case <-ctx.Done():
			wg.Wait()
			return
		}
		g.runDeepProbePass(ctx)
	}
}

// probeConcurrency 是检测并发的统一封顶：GET 初检与 chat 深检/复检共用
// 同一设置，候选按 N 路并行分波消化，全轮耗时 ≈ ⌈候选数/并发路数⌉ ×
// 单次耗时，而不是串行累加。可经界面「检测并发」或环境变量
// PROXY_PROBE_CONCURRENCY 调整（默认 32，上限 128——再高容易同一瞬间
// 集中触发上游风控）。
func (g *gateway) probeConcurrency() int {
	n := g.cfg.probeConcurrency
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
	for worker := 0; worker < g.probeConcurrency(); worker++ {
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
