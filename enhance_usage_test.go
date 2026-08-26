package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ===== P3：全局熔断半开探针 =====

// tripBreakerAt 在 t0 时刻制造一次达标风暴（6 事件 / 4 出口 / 2 镜像）。
func tripBreakerAt(b *outageBreaker, t0 time.Time) {
	exits := []string{"e1", "e2", "e3", "e4", "e5", "e6"}
	for i, e := range exits {
		mirror := "m1"
		if i%2 == 1 {
			mirror = "m2"
		}
		b.recordAt(t0, e, mirror)
	}
}

func TestOutageProbeFlow(t *testing.T) {
	restore := outageNow
	defer func() { outageNow = restore }()
	base := time.Date(2026, 8, 26, 12, 0, 0, 0, time.Local)
	current := base
	outageNow = func() time.Time { return current }

	var b outageBreaker
	tripBreakerAt(&b, base)
	if !b.Tripped() {
		t.Fatal("风暴应触发熔断")
	}
	// 静默 10 秒：未到探针窗口，不放行
	current = base.Add(10 * time.Second)
	if b.TryProbe() {
		t.Fatal("静默未满 30 秒不应放行探针")
	}
	// 静默 31 秒：放行唯一探针
	current = base.Add(31 * time.Second)
	if !b.TryProbe() {
		t.Fatal("静默满 30 秒应放行半开探针")
	}
	// 探针只放行一次
	if b.TryProbe() {
		t.Fatal("本轮熔断的探针已消耗，不应二次放行")
	}
	if !b.Tripped() {
		t.Fatal("探针失败未上报前应保持熔断")
	}
	// 探针成功 → 提前解除
	current = base.Add(35 * time.Second)
	b.NoteSuccess()
	if b.Tripped() {
		t.Fatal("半开探针成功后应提前解除熔断")
	}
}

func TestOutageNoEarlyClearWithoutProbe(t *testing.T) {
	restore := outageNow
	defer func() { outageNow = restore }()
	base := time.Date(2026, 8, 26, 13, 0, 0, 0, time.Local)
	current := base
	outageNow = func() time.Time { return current }

	var b outageBreaker
	tripBreakerAt(&b, base)
	current = base.Add(40 * time.Second)
	b.NoteSuccess()
	if !b.Tripped() {
		t.Fatal("未放行探针时成功不应解除熔断")
	}
	// 满 60 秒静默自然解除
	current = base.Add(61 * time.Second)
	if b.Tripped() {
		t.Fatal("静默满 60 秒应自然解除")
	}
}

// ===== P1：空 arguments 兜底 =====

func TestHygieneEmptyToolArguments(t *testing.T) {
	body := `{"model":"deepseek-v4-flash","messages":[
		{"role":"assistant","content":"","tool_calls":[
			{"id":"c1","type":"function","function":{"name":"get_weather","arguments":""}},
			{"id":"c2","type":"function","function":{"name":"get_time","arguments":"   "}}
		]},
		{"role":"user","content":"hi"}
	]}`
	out := enhanceRequestBody([]byte(body), false)
	var payload struct {
		Messages []struct {
			ToolCalls []struct {
				Function struct {
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("输出应为合法 JSON: %v", err)
	}
	got := payload.Messages[0].ToolCalls
	if got[0].Function.Arguments != "{}" || got[1].Function.Arguments != "{}" {
		t.Fatalf("空 arguments 应补成 {}：%q / %q", got[0].Function.Arguments, got[1].Function.Arguments)
	}
}

// ===== P4：cache_control 断点预算 =====

func TestHygieneCacheControlBudget(t *testing.T) {
	// 客户端自带 4 个断点（上限）：不应再叠加注入
	body := `{"model":"deepseek-v4-flash","system":[{"type":"text","text":"a","cache_control":{"type":"ephemeral"}}],
		"messages":[
			{"role":"user","content":[{"type":"text","text":"b","cache_control":{"type":"ephemeral"}}]},
			{"role":"assistant","content":[{"type":"text","text":"c","cache_control":{"type":"ephemeral"}}]},
			{"role":"user","content":[{"type":"text","text":"d","cache_control":{"type":"ephemeral"}}]}
		]}`
	out := enhanceRequestBody([]byte(body), true)
	if got := countCacheControl(mustPayload(t, out)); got != maxCacheControlBreakpoints {
		t.Fatalf("已有 %d 个断点时应保持不变，实际 %d", maxCacheControlBreakpoints, got)
	}
	// 只有 1 个断点：预算充足，顶层注入补位
	small := `{"model":"deepseek-v4-flash","system":[{"type":"text","text":"a","cache_control":{"type":"ephemeral"}}],"messages":[]}`
	out2 := enhanceRequestBody([]byte(small), true)
	if got := countCacheControl(mustPayload(t, out2)); got != 2 {
		t.Fatalf("预算充足时应补位注入到 %d 个，实际 %d", 2, got)
	}
}

func mustPayload(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("非法 JSON: %v", err)
	}
	return p
}

// ===== P2：用量统计 =====

func TestExtractLastUsageSSELastWins(t *testing.T) {
	data := []byte("data: {\"model\":\"m-a\",\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n" +
		"data: {\"model\":\"m-b\",\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":8,\"prompt_tokens_details\":{\"cached_tokens\":7}}}\n\n" +
		"data: [DONE]\n\n")
	model, u, ok := ExtractLastUsage(data)
	if !ok {
		t.Fatal("应能提取 usage")
	}
	if model != "m-b" || u.PromptTokens != 20 || u.CompletionTokens != 8 || u.CachedTokens != 7 {
		t.Fatalf("最后一个非空 usage 应胜出：%+v (%s)", u, model)
	}
}

func TestExtractLastUsagePlainJSON(t *testing.T) {
	data := []byte(`{"model":"m-c","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4}}`)
	model, u, ok := ExtractLastUsage(data)
	if !ok || model != "m-c" || u.PromptTokens != 3 || u.CompletionTokens != 4 {
		t.Fatalf("普通 JSON 应可解析：%+v ok=%v (%s)", u, ok, model)
	}
	if _, _, ok := ExtractLastUsage([]byte("data: {\"choices\":[]}\n\n")); ok {
		t.Fatal("无 usage 块不应报告成功")
	}
}

func TestUsageStatsDayRollAndPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), usageFileName)
	base := time.Date(2026, 8, 26, 23, 59, 0, 0, time.Local)
	current := base
	u := newUsageStatsAt(path, func() time.Time { return current })
	u.Observe("deepseek-v4-flash", 100, 50, 20)
	u.Observe("deepseek-v4-flash", 10, 5, 0)
	rows := u.Snapshot()
	if len(rows) != 1 || rows[0].Requests != 2 || rows[0].PromptTokens != 110 {
		t.Fatalf("同日聚合错误：%+v", rows)
	}
	// 跨天滚动：旧数据清零
	current = base.Add(2 * time.Hour)
	u.Observe("x-preview-f", 1, 2, 0)
	rows = u.Snapshot()
	if len(rows) != 1 || rows[0].Model != "x-preview-f" {
		t.Fatalf("跨天应清零重计：%+v", rows)
	}
	// 落盘 + 重载（同日假时钟）
	dirtySave(u)
	reloaded := newUsageStatsAt(path, func() time.Time { return current })
	rows = reloaded.Snapshot()
	if len(rows) != 1 || rows[0].PromptTokens != 1 {
		t.Fatalf("重载应恢复当日数据：%+v", rows)
	}
	os.Remove(path)
}

func dirtySave(u *usageStats) {
	u.mu.Lock()
	u.saveLocked()
	u.mu.Unlock()
}
