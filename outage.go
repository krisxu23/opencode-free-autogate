package main

import (
	"log"
	"sync"
	"time"
)

// 全局故障熔断器：把「上游/链路级故障」与「个别出口故障」区分开。
//
// 背景（2026-08-26 事故复盘）：上游源站抖动 + 直连线路被 RST 的窗口内，
// 吸收模式以固定 300ms 间隔连发多轮竞速，每次网络错误都被记到具体出口/
// 镜像头上——健康出口 3 连败即被踢出节点池，4 个镜像轮流进入冷却，最终
// 自我制造"没有可用代理"的全灭。而同一窗口内订阅拉取经同一批出口成功，
// 证明出口本身是好的：坏的是归因，不是出口。
//
// 规则：滑动窗口内若失败事件足够多、且横跨多个出口与多个镜像，判定为
// 全局性故障：
//   - 冻结一切出口/镜像惩罚记账（证据仍记录，惩罚暂停）；
//   - 吸收模式重试切换为指数退避，停止风暴式轰炸；
//   - 静默 60 秒后自动解除，无需人工干预。

// outageNow 可注入时钟：测试固定时间线，生产用 time.Now。
var outageNow = time.Now

type outageEvent struct {
	at     time.Time
	exit   string // 出口地址；纯镜像级事件为空
	mirror string // 镜像主机名
}

type outageBreaker struct {
	mu           sync.Mutex
	events       []outageEvent
	trippedUntil time.Time
	probeAt      time.Time // 本轮熔断允许半开探针的最早时刻
	probed       bool      // 本轮熔断是否已放行过探针
}

const (
	outageWindow      = 45 * time.Second
	outageHoldQuiet   = 60 * time.Second // 判定后静默多久自动解除
	outageProbeDelay  = 30 * time.Second // 静默多久后允许单发半开探针（借鉴 cc-switch HalfOpen 态）
	outageTripEvents  = 6                // 窗口内失败事件数阈值
	outageTripExits   = 4                // 且横跨的不同出口数
	outageTripMirrors = 2                // 且横跨的不同镜像数
)

// record 记录一次失败证据并重估熔断状态。窗口外旧证据随手清理。
func (b *outageBreaker) record(exit, mirror string) {
	b.recordAt(outageNow(), exit, mirror)
}

func (b *outageBreaker) recordAt(now time.Time, exit, mirror string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, outageEvent{at: now, exit: exit, mirror: mirror})
	kept := b.events[:0]
	for _, e := range b.events {
		if now.Sub(e.at) < outageWindow {
			kept = append(kept, e)
		}
	}
	b.events = kept
	if !b.qualifiesLocked() {
		return
	}
	if now.After(b.trippedUntil) {
		log.Printf("[全局熔断] %d 秒内 %d 个出口 / %d 个镜像同时失败，疑似上游或链路级故障：冻结惩罚并转入指数退避",
			int(outageWindow.Seconds()), countDistinctExits(b.events), countDistinctMirrors(b.events))
		b.probeAt = now.Add(outageProbeDelay)
		b.probed = false
	}
	b.trippedUntil = now.Add(outageHoldQuiet)
}

// Tripped 报告当前是否处于全局熔断保持期。风暴持续期间每个新证据都会顺延。
func (b *outageBreaker) Tripped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return outageNow().Before(b.trippedUntil)
}

// TryProbe 熔断保持期间申请放行一次半开探针：每轮熔断仅限一次，
// 且需已静默 outageProbeDelay。返回 false 表示继续按指数退避等待；
// 返回 true 表示本轮请求可立即发出、不受退避约束（借鉴 cc-switch
// circuit_breaker 的 HalfOpen 态，把最坏 60 秒的恢复等待压缩一半）。
func (b *outageBreaker) TryProbe() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := outageNow()
	if !now.Before(b.trippedUntil) {
		return true // 已自然解除，无需探针语义
	}
	if b.probed || now.Before(b.probeAt) {
		return false
	}
	b.probed = true
	log.Printf("[全局熔断] 已静默 %d 秒，放行半开探针请求验证上游恢复", int(outageProbeDelay.Seconds()))
	return true
}

// NoteSuccess 记录一次上游完整成功：若本轮半开探针已放行，则提前解除熔断。
func (b *outageBreaker) NoteSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := outageNow()
	if now.Before(b.trippedUntil) && b.probed {
		b.trippedUntil = now
		b.probed = false
		log.Printf("[全局熔断] 半开探针成功，提前解除全局熔断")
	}
}

// countDistinctField 统计事件列表中某字段的非空去重计数。
func countDistinctField(events []outageEvent, extract func(outageEvent) string) int {
	set := make(map[string]struct{})
	for _, e := range events {
		if v := extract(e); v != "" {
			set[v] = struct{}{}
		}
	}
	return len(set)
}

func countDistinctExits(events []outageEvent) int {
	return countDistinctField(events, func(e outageEvent) string { return e.exit })
}

func countDistinctMirrors(events []outageEvent) int {
	return countDistinctField(events, func(e outageEvent) string { return e.mirror })
}

// qualifiesLocked 判定窗口内证据是否满足全局故障特征：
// 事件数达标 且 横跨至少两个镜像（单镜像多出口属于该镜像自己的问题，
// 应走既有冷却逻辑而非全局熔断）且 横跨至少四个不同出口。
func (b *outageBreaker) qualifiesLocked() bool {
	if len(b.events) < outageTripEvents {
		return false
	}
	return countDistinctExits(b.events) >= outageTripExits &&
		countDistinctMirrors(b.events) >= outageTripMirrors
}
