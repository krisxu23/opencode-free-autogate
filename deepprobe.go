package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"strings"
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
					ok, hard := g.deepProbeDedup(ctx, s)
					g.settleDeep(s, ok, hard)
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

	// 周期复检覆盖两池：正式池（再验证）＋ 内部过关池（漏网流式的兜底）。
	candidates := g.customSnapshot()
	candidates = append(candidates, g.freshSnapshot()...)
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
				ok, hard := g.deepProbeDedup(ctx, candidate)
				g.settleDeep(candidate, ok, hard)
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

// slotExitIP 返回节点出口的真实 IP（高级映射节点经 sing-box 反查原始服务器）：
// 深检按出口 IP 聚合去重——同一出口 IP 的多个端口节点只打一次 chat 深检，
// 结果共享给同 IP 的全部节点（按 IP 计额度的上游，同 IP 每端口各打一次 =
// 同一 IP 被重复烧额度，曾实测同 IP 40+ 端口被深检轰炸打爆额度）。
// 反查失败的本地映射节点返回空（不去重）：127.0.0.1 是 sing-box 本地入口，
// 不能作为出口 IP 去重键——否则全部高级节点会被误当成同一个出口。
func (g *gateway) slotExitIP(s slot) string {
	if s.proxyURL == nil {
		return "" // 直连无出口 IP 概念
	}
	host := s.proxyURL.Hostname()
	if host == "" {
		return ""
	}
	// 高级映射节点：本地地址 127.0.0.1:port，反查原始服务器。
	if strings.HasPrefix(host, "127.") || host == "localhost" || host == "::1" {
		if origin := g.advOriginHost(s.addr); origin != "" {
			if h, _, err := net.SplitHostPort(origin); err == nil {
				return h
			}
			return origin
		}
		return "" // 反查失败：无法确定出口 IP，不去重
	}
	return host
}

// deepProbeIPCache 深检按出口 IP 的去重缓存：同一 IP 一次深检，TTL 内
// 结果共享。key = 出口 IP。
type deepProbeIPCache struct {
	mu      sync.Mutex
	entries map[string]deepProbeIPEntry
	now     func() time.Time
}

type deepProbeIPEntry struct {
	ok   bool
	hard bool // 硬失败（连不通）：只淘汰代表节点，不共享给同 IP 其他节点
	at   time.Time
}

// deepProbeDedupTTL 是去重结果的共享窗口：与深检间隔同量级，窗口内
// 同 IP 不再重复烧配额；窗口外允许重新检测（额度可能已恢复）。
const deepProbeDedupTTL = 30 * time.Minute

func newDeepProbeIPCache() *deepProbeIPCache {
	return &deepProbeIPCache{entries: make(map[string]deepProbeIPEntry), now: time.Now}
}

// lookup 返回同 IP 的可共享结果（ok=true 或软失败）。硬失败不共享。
func (c *deepProbeIPCache) lookup(ip string) (ok bool, shareable bool) {
	if ip == "" {
		return false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, exists := c.entries[ip]
	if !exists || c.now().Sub(e.at) > deepProbeDedupTTL {
		return false, false
	}
	if e.hard {
		return false, false // 硬失败仅代表该端口，不共享
	}
	return e.ok, true
}

// store 记录一次深检结果。
func (c *deepProbeIPCache) store(ip string, ok, hard bool) {
	if ip == "" {
		return
	}
	c.mu.Lock()
	c.entries[ip] = deepProbeIPEntry{ok: ok, hard: hard, at: c.now()}
	c.mu.Unlock()
}

// deepProbeDedup 按出口 IP 聚合去重的深检入口：同 IP 已有可共享结果则
// 直接复用（不烧上游配额）；否则打一次并记录。返回 (ok, hardFail)。
func (g *gateway) deepProbeDedup(ctx context.Context, s slot) (bool, bool) {
	ip := g.slotExitIP(s)
	if ip != "" {
		if ok, share := g.deepDedup.lookup(ip); share {
			return ok, false
		}
	}
	ok, hard := g.deepProbeOne(ctx, s)
	if ip != "" {
		g.deepDedup.store(ip, ok, hard)
	}
	return ok, hard
}
