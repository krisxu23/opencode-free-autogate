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

type outageEvent struct {
	at     time.Time
	exit   string // 出口地址；纯镜像级事件为空
	mirror string // 镜像主机名
}

type outageBreaker struct {
	mu           sync.Mutex
	events       []outageEvent
	trippedUntil time.Time
}

const (
	outageWindow      = 45 * time.Second
	outageHoldQuiet   = 60 * time.Second // 判定后静默多久自动解除
	outageTripEvents  = 6                // 窗口内失败事件数阈值
	outageTripExits   = 4                // 且横跨的不同出口数
	outageTripMirrors = 2                // 且横跨的不同镜像数
)

// record 记录一次失败证据并重估熔断状态。窗口外旧证据随手清理。
func (b *outageBreaker) record(exit, mirror string) {
	b.recordAt(time.Now(), exit, mirror)
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
	}
	b.trippedUntil = now.Add(outageHoldQuiet)
}

// Tripped 报告当前是否处于全局熔断保持期。风暴持续期间每个新证据都会顺延。
func (b *outageBreaker) Tripped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.trippedUntil)
}

func countDistinctExits(events []outageEvent) int {
	set := make(map[string]struct{})
	for _, e := range events {
		if e.exit != "" {
			set[e.exit] = struct{}{}
		}
	}
	return len(set)
}

func countDistinctMirrors(events []outageEvent) int {
	set := make(map[string]struct{})
	for _, e := range events {
		if e.mirror != "" {
			set[e.mirror] = struct{}{}
		}
	}
	return len(set)
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
