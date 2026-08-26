package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// 每日用量统计（借鉴 cc-switch usage 模块的最小子集）：
// 从上游响应中提取 usage 块，按「自然日 × 模型」聚合请求数与 token 数，
// 落盘到 exe 同目录 usage_stats.json，跨天自动滚动清零。
//
// 设计取舍：只记单日明细（历史汇总不做），文件损坏即重建——统计是
// 观赏性功能，绝不因它影响转发主路径。

type modelUsage struct {
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	CachedTokens     int64 `json:"cached_tokens"`
}

func (m *modelUsage) add(other modelUsage) {
	m.Requests += other.Requests
	m.PromptTokens += other.PromptTokens
	m.CompletionTokens += other.CompletionTokens
	m.CachedTokens += other.CachedTokens
}

type usageRow struct {
	Model string `json:"model"`
	modelUsage
}

type usageStats struct {
	mu     sync.Mutex
	path   string
	day    string
	models map[string]modelUsage
	dirty  bool

	now  func() time.Time // 可注入时钟，测试用
	stop chan struct{}
}

const usageFileName = "usage_stats.json"

func newUsageStats(dir string) *usageStats {
	u := &usageStats{
		path: filepath.Join(dir, usageFileName),
		day:  time.Now().Format("2006-01-02"),
		now:  time.Now,
		stop: make(chan struct{}),
	}
	u.load()
	go u.saveLoop()
	return u
}

// newUsageStatsAt 测试用构造：指定落盘路径与时钟（now 为 nil 用真实时钟）。
func newUsageStatsAt(path string, now func() time.Time) *usageStats {
	if now == nil {
		now = time.Now
	}
	u := &usageStats{
		path: path,
		day:  now().Format("2006-01-02"),
		now:  now,
		stop: make(chan struct{}),
	}
	u.load()
	return u
}

func (u *usageStats) load() {
	data, err := os.ReadFile(u.path)
	if err != nil {
		return
	}
	var snap struct {
		Day    string                `json:"day"`
		Models map[string]modelUsage `json:"models"`
	}
	if json.Unmarshal(data, &snap) != nil || snap.Day == "" {
		return
	}
	today := u.now().Format("2006-01-02")
	if snap.Day != today {
		return // 历史日期：跨天滚动，直接丢弃
	}
	u.day = snap.Day
	u.models = snap.Models
}

func (u *usageStats) saveLocked() {
	if !u.dirty {
		return
	}
	snap := struct {
		Day    string                `json:"day"`
		Models map[string]modelUsage `json:"models"`
	}{Day: u.day, Models: u.models}
	data, err := json.MarshalIndent(snap, "", " ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(u.path), 0o755); err == nil {
		if err := os.WriteFile(u.path, data, 0o644); err == nil {
			u.dirty = false
		}
	}
}

// saveLoop 周期性把脏数据落盘；进程退出时最多丢一个周期的增量。
func (u *usageStats) saveLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-u.stop:
			u.mu.Lock()
			u.saveLocked()
			u.mu.Unlock()
			return
		case <-ticker.C:
			u.mu.Lock()
			u.saveLocked()
			u.mu.Unlock()
		}
	}
}

func (u *usageStats) Close() {
	select {
	case <-u.stop:
	default:
		close(u.stop)
	}
}

// Observe 累加一次请求的用量；跨天自动滚动清零。
func (u *usageStats) Observe(model string, prompt, completion, cached int64) {
	if model == "" {
		model = "未知模型"
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	today := u.now().Format("2006-01-02")
	if today != u.day {
		u.day = today
		u.models = make(map[string]modelUsage)
	}
	if u.models == nil {
		u.models = make(map[string]modelUsage)
	}
	entry := u.models[model]
	entry.Requests++
	entry.PromptTokens += prompt
	entry.CompletionTokens += completion
	entry.CachedTokens += cached
	u.models[model] = entry
	u.dirty = true
}

// Snapshot 返回按请求数降序的当日用量行。
func (u *usageStats) Snapshot() []usageRow {
	u.mu.Lock()
	defer u.mu.Unlock()
	rows := make([]usageRow, 0, len(u.models))
	for model, m := range u.models {
		rows = append(rows, usageRow{Model: model, modelUsage: m})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Requests > rows[j].Requests })
	return rows
}

// ===== usage 提取 =====

// sseUsageChunk OpenAI 流式 chunk 中我们关心的字段。
type sseUsageChunk struct {
	Model string `json:"model"`
	Usage *struct {
		PromptTokens        int64 `json:"prompt_tokens"`
		CompletionTokens    int64 `json:"completion_tokens"`
		PromptTokensDetails *struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// ExtractLastUsage 从完整响应体提取最后一次出现的 usage 统计。
// 同时兼容两种形态：SSE 缓冲（吸收模式产物，逐 data: 行解析，
// 取最后一个非空 usage）与普通 JSON（非流式响应整体解析）。
func ExtractLastUsage(data []byte) (model string, usage modelUsage, ok bool) {
	trimmed := strings.TrimLeft(string(data), " \t\r\n")
	isSSE := strings.HasPrefix(trimmed, "data:") || strings.Contains(trimmed, "\ndata:")
	if !isSSE && strings.HasPrefix(trimmed, "{") {
		var chunk sseUsageChunk
		if json.Unmarshal(data, &chunk) == nil && chunk.Usage != nil {
			return fillUsage(chunk)
		}
		return "", modelUsage{}, false
	}
	found := false
	for _, line := range strings.Split(trimmed, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var chunk sseUsageChunk
		if json.Unmarshal([]byte(payload), &chunk) != nil || chunk.Usage == nil {
			continue
		}
		model, usage, ok = fillUsage(chunk)
		found = true
	}
	return model, usage, found
}

func fillUsage(chunk sseUsageChunk) (string, modelUsage, bool) {
	usage := modelUsage{
		PromptTokens:     chunk.Usage.PromptTokens,
		CompletionTokens: chunk.Usage.CompletionTokens,
	}
	if chunk.Usage.PromptTokensDetails != nil {
		usage.CachedTokens = chunk.Usage.PromptTokensDetails.CachedTokens
	}
	return chunk.Model, usage, true
}
