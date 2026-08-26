package main

import (
	"testing"
	"time"
)

func newTestGateway() *gateway {
	return &gateway{exits: newExitTracker()}
}

func TestOutageBreakerTripsOnCrossExitMirrorStorm(t *testing.T) {
	var b outageBreaker
	now := time.Now()
	exits := []string{"a", "b", "c", "d"}
	mirrors := []string{"m1", "m2"}
	for i := 0; i < outageTripEvents; i++ {
		b.recordAt(now.Add(-time.Duration(outageTripEvents-i)*time.Second), exits[i%len(exits)], mirrors[i%len(mirrors)])
	}
	if !b.Tripped() {
		t.Fatal("6 次失败横跨 4 出口 2 镜像应触发全局熔断")
	}
}

func TestOutageBreakerNoTripSingleMirror(t *testing.T) {
	var b outageBreaker
	now := time.Now()
	for i := 0; i < outageTripEvents+4; i++ {
		b.recordAt(now.Add(-time.Duration(i%30)*time.Second), exitsFor(i), "m1")
	}
	if b.Tripped() {
		t.Fatal("单镜像多出口失败属于该镜像自身问题，不应全局熔断")
	}
}

func exitsFor(i int) string {
	suffix := string(rune('a' + i%8))
	return "exit-" + suffix + string(rune('a'+i/8))
}

func TestOutageBreakerQuietWindowRecovers(t *testing.T) {
	var b outageBreaker
	now := time.Now()
	for i := 0; i < outageTripEvents; i++ {
		b.recordAt(now, "e", "")
	}
	// 强制把保持期推入过去，模拟静默窗口走完。
	b.mu.Lock()
	b.trippedUntil = now.Add(-time.Second)
	b.mu.Unlock()
	if b.Tripped() {
		t.Fatal("静默超时应自动解除熔断")
	}
}

func TestPunishGateSuppressesUnderOutage(t *testing.T) {
	g := newTestGateway()
	if !g.punishExit("a", "m1") || !g.punishExit("b", "m1") {
		t.Fatal("未达阈值前应正常惩罚")
	}
	now := time.Now()
	pairs := [][2]string{{"c", "m1"}, {"d", "m2"}, {"e", "m2"}, {"f", "m2"}, {"g", "m2"}}
	for _, p := range pairs {
		g.outage.recordAt(now, p[0], p[1])
	}
	if !g.outage.Tripped() {
		t.Fatal("证据充足应已触发熔断")
	}
	if g.punishExit("h", "m2") {
		t.Fatal("熔断期间 punishExit 应返回 false（只留证据不惩罚）")
	}
}

func TestAbsorbBackoffSchedule(t *testing.T) {
	g := newTestGateway()
	if got := g.absorbBackoff(1); got != absorbRetryDelay {
		t.Fatalf("未熔断时应为固定短间隔，得到 %v", got)
	}
	g.outage.trippedUntil = time.Now().Add(time.Minute)
	want := []time.Duration{300 * time.Millisecond, 600 * time.Millisecond, 1200 * time.Millisecond, 2400 * time.Millisecond}
	for i, w := range want {
		if got := g.absorbBackoff(i + 1); got != w {
			t.Fatalf("attempt=%d 期望 %v 得到 %v", i+1, w, got)
		}
	}
	if got := g.absorbBackoff(50); got != 9600*time.Millisecond {
		t.Fatalf("档位封顶后应停在 9.6 秒，得到 %v", got)
	}
}
