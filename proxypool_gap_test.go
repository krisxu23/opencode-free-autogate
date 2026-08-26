package main

import (
	"testing"
	"time"
)

// TestProbeRoundGapClamp 验证初检轮间隔的夹紧规则：
// 默认 60 秒；下限 15 秒（再快就是打源站）；上限 30 分钟（再慢失去"连轴"意义）。
func TestProbeRoundGapClamp(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"未配置走默认60秒", 0, 60 * time.Second},
		{"负值走默认60秒", -3 * time.Second, 60 * time.Second},
		{"过快抬到15秒下限", 5 * time.Second, 15 * time.Second},
		{"正常值原样通过", 90 * time.Second, 90 * time.Second},
		{"过慢压到30分钟上限", 2 * time.Hour, 30 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &gateway{cfg: config{probeRoundGap: tc.in}}
			if got := g.probeRoundGap(); got != tc.want {
				t.Fatalf("probeRoundGap(%v) = %v，期望 %v", tc.in, got, tc.want)
			}
		})
	}
}
