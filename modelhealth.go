package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

// 模型级健康探测与自动剔除（借鉴 zen-proxy 的 syncModels）：
// 出口 IP 信誉只评价"网络路径通不通/出口干不干净"，不评价"模型还活不活"。
// 免费模型时常变动——新模型上线、旧模型下线、某模型被全局限流——静态列表
// 很快过时。这里每小时（启动时立即跑一次）对在册模型发 max_tokens=1 的迷你
// chat 请求，按结果分类：
//
//	working     200 OK 无 error
//	rateLimited 429（模型被限流，可能恢复）
//	flaky       其他异常，失败 < 2 次（观察期）
//	dead        404 / model_not_found / 连续失败 ≥ 2 次
//
// dead 模型从对外模型列表与模型 fallback 链里剔除，避免客户端/网关反复
// 撞一个已下线的模型。

type modelHealthState struct {
	status    string // working / rateLimited / flaky / dead
	fails     int    // 连续失败次数
	lastProbe time.Time
}

const (
	modelHealthInterval  = time.Hour
	modelHealthMinTTL    = 15 * time.Minute // 两次探测之间的最小间隔（避免频繁烧配额）
	modelHealthDeadFails = 2                // 连续失败达到该值判定 dead
)

type modelHealthTracker struct {
	mu     sync.Mutex
	states map[string]*modelHealthState
	now    func() time.Time
}

func newModelHealthTracker() *modelHealthTracker {
	return &modelHealthTracker{states: make(map[string]*modelHealthState), now: time.Now}
}

// status 返回模型当前健康分类（未知模型按 working 对待，不误杀）。
func (h *modelHealthTracker) status(model string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.states[model]; ok && s.status == "dead" {
		return "dead"
	}
	return "working"
}

// isAlive 报告模型是否可对外提供（dead 才剔除，rateLimited 保留观察）。
func (h *modelHealthTracker) isAlive(model string) bool {
	return h.status(model) != "dead"
}

// needsProbe 报告该模型是否已到探测时机（距上次探测超过最小间隔）。
func (h *modelHealthTracker) needsProbe(model string, minTTL time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.states[model]; ok {
		return h.now().Sub(s.lastProbe) >= minTTL
	}
	return true // 从未探测过
}

// record 记录一次探测结果。now 可注入（测试）。
func (h *modelHealthTracker) record(model, status string, ok bool, now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, exists := h.states[model]
	if !exists {
		s = &modelHealthState{}
		h.states[model] = s
	}
	s.lastProbe = now
	if ok {
		s.status = "working"
		s.fails = 0
		return
	}
	s.fails++
	switch {
	case strings.HasPrefix(status, "rateLimited"):
		s.status = "rateLimited"
		s.fails = 0 // 限流不计入连续失败（会恢复）
	case s.fails >= modelHealthDeadFails || status == "dead":
		s.status = "dead"
	default:
		s.status = "flaky"
	}
}

// filterAlive 过滤出仍健康的模型（保留原顺序）。
func (h *modelHealthTracker) filterAlive(models []string) []string {
	if len(models) == 0 {
		return models
	}
	out := make([]string, 0, len(models))
	for _, m := range models {
		if h.isAlive(m) {
			out = append(out, m)
		}
	}
	return out
}

// startModelProber 周期性对在册模型做健康探测。启动立即跑一次，
// 之后按 interval ±20% 抖动。并发路数复用检测并发设置。
func (g *gateway) startModelProber(ctx context.Context) {
	if g.modelHealth == nil {
		g.modelHealth = newModelHealthTracker()
	}
	// 启动立即跑一次：网关起来时模型列表可能已过时。
	g.runModelProbePass(ctx)

	interval := g.cfg.deepProbeInterval
	if interval < 10*time.Minute {
		interval = 10 * time.Minute
	}
	for {
		next := interval + time.Duration(rand.Int63n(int64(interval)*2/5)) - interval/5
		if next < time.Minute {
			next = time.Minute
		}
		select {
		case <-time.After(next):
		case <-ctx.Done():
			return
		}
		g.runModelProbePass(ctx)
	}
}

// runModelProbePass 对在册模型逐个发 max_tokens=1 的迷你 chat 请求。
// 探测走直连 controlTransport（模型健康与出口无关），复用模型映射的
// 上游基址。模型列表含 fallback 候选。
func (g *gateway) runModelProbePass(ctx context.Context) {
	if g.modelHealth == nil {
		return
	}
	models := g.modelUpstreamIDs(ctx)
	if len(models) == 0 {
		return
	}
	// 探测节奏纪律：距上次探测不足 TTL 的模型跳过（防频繁烧配额）。
	var pending []string
	for _, m := range models {
		if g.modelHealth.needsProbe(m, modelHealthMinTTL) {
			pending = append(pending, m)
		}
	}
	if len(pending) == 0 {
		return
	}
	rand.Shuffle(len(pending), func(i, j int) { pending[i], pending[j] = pending[j], pending[i] })
	log.Printf("[模型体检] 开始探测 %d 个模型（直连 max_tokens=1）", len(pending))

	work := make(chan string)
	var wg sync.WaitGroup
	for worker := 0; worker < g.probeConcurrency(); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for model := range work {
				select {
				case <-ctx.Done():
					return
				default:
				}
				status, ok := g.probeModelHealth(ctx, model)
				g.modelHealth.record(model, status, ok, time.Now())
				log.Printf("[模型体检] %s -> %s", model, g.modelHealth.status(model))
			}
		}()
	}
	for _, m := range pending {
		select {
		case work <- m:
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return
		}
	}
	close(work)
	wg.Wait()
}

// probeModelHealth 对单个模型发迷你 chat 请求并分类结果。
// 返回 (status, ok)；status 用于分类（rateLimited/dead/...），ok 为是否健康。
func (g *gateway) probeModelHealth(ctx context.Context, model string) (string, bool) {
	deadline := time.Now().Add(g.cfg.probeChatTimeout)
	headers := g.cfg.project.probeHeaders.Clone()
	headers.Set("Content-Type", "application/json")
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": 1,
		"stream":     false,
		"messages":   []map[string]string{{"role": "user", "content": "ping"}},
	})
	request := upstreamRequest{
		method:   http.MethodPost,
		path:     "/v1/chat/completions",
		headers:  headers,
		body:     body,
		deadline: deadline,
	}
	// 模型健康与出口无关：走直连（nil = 无代理）。
	live, err := g.openUpstream(ctx, request, nil, g.cfg.probeChatTimeout)
	if err != nil {
		return "flaky", false
	}
	status := live.response.StatusCode
	var respBody []byte
	if status >= 400 {
		respBody, _ = live.readAll(deadline)
	}
	live.Close()

	switch {
	case status == http.StatusNotFound:
		return "dead", false
	case status == http.StatusBadRequest && strings.Contains(strings.ToLower(string(respBody)), "model_not_found"):
		return "dead", false
	case status == http.StatusTooManyRequests:
		return "rateLimited", false
	case status >= 200 && status < 300:
		return "working", true
	default:
		return "flaky", false
	}
}
