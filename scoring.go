package main

import (
	"sort"
	"sync"
	"time"
)

// exitTracker 记录每个出口的近期表现（首字节延迟、失败连击、空流截断），
// 供对冲竞速的出发排序与坐板凳决策使用。手动节点也参与记账：连续失败只会
// 被暂停参与竞速（坐板凳），永远不会被删除；探活通过或竞速胜出即恢复在场。
type exitTracker struct {
	mu    sync.Mutex
	stats map[string]*exitStat
}

type exitStat struct {
	latency     time.Duration // 首字节 EWMA（探活与真实胜出共同贡献）
	seen        bool          // 是否已有延迟样本
	failStreak  int           // 连续真实失败（传输错误/空流）；限流 429 不计入
	benchLevel  int           // 坐板凳时长阶梯索引
	benchUntil  time.Time     // 坐板凳截止时间；零值表示在场
	benchSource benchSource   // 板凳依据：自己推断 vs 上游亲口说
	softUntil   time.Time     // 板凳到期后的观察窗：期间同出口仅允许一路在途
	inFlight    int           // 在途租约数（对冲并发下防同一出口被集体压爆）
	truncations int           // 累计空流/中途断流次数
}

// benchSource 标记板凳的依据来源，决定能否被探活提前释放：
// heuristic 是我们自己的推断（可被深检/胜出提前推翻），
// authoritative 是上游亲口给出的时间（Retry-After/响应体），只信事实不信猜测。
type benchSource uint8

const (
	benchNone         benchSource = iota
	benchHeuristic                // 推断值：允许探活提前回归
	benchAuthoritative            // 上游声明值：不主动探，等真实胜出或到期
)

func benchSourceName(s benchSource) string {
	switch s {
	case benchAuthoritative:
		return "上游声明"
	case benchHeuristic:
		return "推断"
	default:
		return "无"
	}
}

// 坐板凳时长阶梯：连续失败次数越多，暂停越久，死节点占用逐步趋近于零。
var benchDurations = [...]time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute}

// quotaBenchDuration 是额度枯竭型限流（FreeUsageLimitError）的板凳时长。
// 上游免费额度按出口 IP 共享计数、约 24 小时滚动重置：收到该错误的 IP 在
// 小时级时间内都不会恢复，短板凳放回来只是浪费竞速位。
const quotaBenchDuration = 2 * time.Hour

const (
	benchFailLimit = 3   // 连续真实失败达到该次数触发坐板凳
	latencyAlpha   = 0.5 // EWMA 平滑系数
	statsHardCap   = 8192
)

func newExitTracker() *exitTracker {
	return &exitTracker{stats: make(map[string]*exitStat)}
}

func (t *exitTracker) statLocked(addr string) *exitStat {
	s := t.stats[addr]
	if s == nil {
		s = &exitStat{}
		t.stats[addr] = s
	}
	return s
}

// observeWin 记录一次成功（探活通过或竞速胜出）：更新延迟 EWMA、清零失败
// 连击并立即解除坐板凳——成功本身就是最好的健康证明，事实可以推翻一切推断。
func (t *exitTracker) observeWin(addr string, latency time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.statLocked(addr)
	if !s.seen {
		s.latency = latency
		s.seen = true
	} else {
		s.latency += time.Duration(latencyAlpha * float64(latency-s.latency))
	}
	s.failStreak = 0
	s.benchUntil = time.Time{}
	s.benchSource = benchNone
	s.benchLevel = 0
}

// observeSuccess 记录一次流完整结束（见到终止标记）：只清失败连击，不采样延迟。
func (t *exitTracker) observeSuccess(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.statLocked(addr).failStreak = 0
}

// observeFail 记录一次真实失败（传输错误/探活不通过）；限流 429 说明节点
// 连通只是被限，不进此函数。连续 benchFailLimit 次触发坐板凳。
func (t *exitTracker) observeFail(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failLocked(t.statLocked(addr))
}

// observeTruncation 记一次空流/中途断流：截断是最差信号，计入截断计数
// 并按失败连击处理。
func (t *exitTracker) observeTruncation(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.statLocked(addr)
	s.truncations++
	t.failLocked(s)
}

func (t *exitTracker) failLocked(s *exitStat) {
	s.failStreak++
	if s.failStreak >= benchFailLimit {
		if s.benchLevel < len(benchDurations) {
			s.benchLevel++
		}
		s.benchUntil = time.Now().Add(benchDurations[min(s.benchLevel, len(benchDurations))-1])
		s.benchSource = benchHeuristic
		s.failStreak = 0
	}
}

// observeQuotaBurn 记一次额度受限（429 FreeUsageLimitError / 402 等）：
// duration<=0 时回退到默认推断时长。authoritative 来源（上游声明的时间）
// 不会被深检提前释放——上游说了 24 小时就是 24 小时，别去戳。
func (t *exitTracker) observeQuotaBurn(addr string, duration time.Duration, source benchSource) {
	if duration <= 0 {
		duration = quotaBenchDuration
		source = benchHeuristic
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.statLocked(addr)
	if until := time.Now().Add(duration); until.After(s.benchUntil) {
		s.benchUntil = until
		s.benchSource = source
	}
	s.failStreak = 0
}

// enterExit / exitExit 维护出口在途租约：对冲竞速同批并发时，同一出口的
// 多路请求会同时读到"可用"并集体出发；观察窗内的出口一次只放一路在途，
// 防止刚恢复的额度被瞬间压爆。
func (t *exitTracker) enterExit(addr string) {
	t.mu.Lock()
	t.statLocked(addr).inFlight++
	t.mu.Unlock()
}

func (t *exitTracker) exitExit(addr string) {
	t.mu.Lock()
	if s := t.stats[addr]; s != nil && s.inFlight > 0 {
		s.inFlight--
	}
	t.mu.Unlock()
}

// softLimited 报告出口是否处于板凳到期后的观察窗内。
func (t *exitTracker) softLimited(addr string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats[addr]
	return s != nil && time.Now().Before(s.softUntil)
}

func (t *exitTracker) inFlightCount(addr string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats[addr]
	if s == nil {
		return 0
	}
	return s.inFlight
}

// observeProbe 汇报一次探活结果。
func (t *exitTracker) observeProbe(addr string, latency time.Duration, ok bool) {
	if ok {
		t.observeWin(addr, latency)
		return
	}
	t.observeFail(addr)
}

func (t *exitTracker) benched(addr string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats[addr]
	return s != nil && time.Now().Before(s.benchUntil)
}

// authoritativelyBenched 报告出口是否坐着"上游声明"的板凳：时间来自
// Retry-After 或响应体声明，探活无法证伪，深检不碰。
func (t *exitTracker) authoritativelyBenched(addr string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats[addr]
	return s != nil && s.benchSource == benchAuthoritative && time.Now().Before(s.benchUntil)
}

// probationWindow 是板凳到期后的观察窗时长：期间同出口仅允许一路在途，
// 让第一个真实请求充当探针，避免刚恢复的出口被对冲并发瞬间压爆。
const probationWindow = time.Minute

// filterBenched 剔除坐板凳的出口；全部坐板凳时原样返回——宁可带伤上场
// 也不能让竞速一个出口都没有。板凳刚到期的出口进入观察窗（软限制）。
func (t *exitTracker) filterBenched(exits []slot) []slot {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	kept := make([]slot, 0, len(exits))
	for _, candidate := range exits {
		s := t.stats[candidate.addr]
		if s != nil && !s.benchUntil.IsZero() {
			if now.Before(s.benchUntil) {
				continue // 仍在板凳上
			}
			// 板凳刚到期：进入观察窗，清空板凳记账。
			if s.softUntil.IsZero() {
				s.softUntil = now.Add(probationWindow)
			}
			s.benchUntil = time.Time{}
			s.benchSource = benchNone
		}
		kept = append(kept, candidate)
	}
	if len(kept) == 0 {
		return exits
	}
	return kept
}

// rank 把出口按近期表现排序，排序结果即对冲竞速的出发顺序：
// 截断少者优先 > 首字节快者优先 > 未知样本者居后（稳定排序保留原顺序）。
func (t *exitTracker) rank(exits []slot) []slot {
	t.mu.Lock()
	defer t.mu.Unlock()
	// 防止节点池长期轮换导致记账无限增长：超限时只保留本次在场的出口。
	if len(t.stats) > statsHardCap {
		fresh := make(map[string]*exitStat, len(exits))
		for _, candidate := range exits {
			if s := t.stats[candidate.addr]; s != nil {
				fresh[candidate.addr] = s
			}
		}
		t.stats = fresh
	}
	type item struct {
		exit   slot
		trunc  int
		lat    time.Duration
		known  bool
	}
	items := make([]item, 0, len(exits))
	for _, candidate := range exits {
		s := t.stats[candidate.addr]
		if s == nil {
			items = append(items, item{exit: candidate})
			continue
		}
		items = append(items, item{exit: candidate, trunc: s.truncations, lat: s.latency, known: s.seen})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].trunc != items[j].trunc {
			return items[i].trunc < items[j].trunc
		}
		if items[i].known != items[j].known {
			return items[i].known
		}
		return items[i].known && items[i].lat < items[j].lat
	})
	out := make([]slot, 0, len(items))
	for _, it := range items {
		out = append(out, it.exit)
	}
	return out
}

// trackableExit 判断标识是否为可记账的代理出口（排除直连/本地等伪出口名）。
func trackableExit(addr string) bool {
	switch addr {
	case "", "direct", "直连", "ZenProxy", "local", "unknown":
		return false
	}
	return true
}
