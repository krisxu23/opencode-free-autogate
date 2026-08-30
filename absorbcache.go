package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// 吸收产物响应缓存（借鉴 ferro ai-gateway responsecache 的思路裁剪）：
// 吸收模式（dispatchAbsorb）天然产出一份"已验证完整"的响应体——这正是
// 该缓存的东西。相同请求（模型 + 规范化 messages + 采样参数）命中时
// 直接回放完整响应，省一次上游调用与免费额度（agent 工具反复调同 prompt
// 的场景命中率高）。
//
// 纪律：
//   - 只缓存吸收模式的完整产物（缓冲响应，非流式 live）；透传流不缓存
//     （流式增量无法安全复放）；
//   - 键 = SHA-256(模型 + 规范化 messages + 采样参数)，不含时间戳/request-id；
//   - TTL 默认 5 分钟（PROXY_ABSORB_CACHE_TTL_MS 可覆盖）；0 关闭；
//   - 缓存命中仍走完整收尾逻辑（日志/用量采集），客户端无感知；
//   - 内存有界：容量上限（默认 256 条），超出淘汰最旧。

const (
	absorbCacheDefaultTTL = 5 * time.Minute
	absorbCacheMaxEntries = 256
)

type absorbCacheEntry struct {
	body     []byte
	header   httpHeaderSnapshot
	status   int
	storedAt time.Time
	lastUsed time.Time
}

type absorbCache struct {
	mu      sync.Mutex
	entries map[string]*absorbCacheEntry
	ttl     time.Duration
	now     func() time.Time
}

type httpHeaderSnapshot struct {
	keys   []string
	values map[string][]string
}

func snapshotHeader(h http.Header) httpHeaderSnapshot {
	keys := make([]string, 0, len(h))
	values := make(map[string][]string, len(h))
	for k, v := range h {
		keys = append(keys, k)
		values[k] = append([]string(nil), v...)
	}
	sort.Strings(keys)
	return httpHeaderSnapshot{keys: keys, values: values}
}

func (s httpHeaderSnapshot) restore() http.Header {
	h := make(http.Header, len(s.keys))
	for _, k := range s.keys {
		h[k] = append([]string(nil), s.values[k]...)
	}
	return h
}

func newAbsorbCache(ttl time.Duration) *absorbCache {
	if ttl <= 0 {
		ttl = absorbCacheDefaultTTL
	}
	return &absorbCache{entries: make(map[string]*absorbCacheEntry), ttl: ttl, now: time.Now}
}

// cacheTTL 返回配置的缓存 TTL（0 = 关闭缓存）。
func (c config) cacheTTL() time.Duration {
	return c.absorbCacheTTL
}

// cacheKeyForRequest 构造缓存键：模型 + 规范化 messages + 采样参数。
// 规范化 = 按 messages 内容稳定序列化（键序无关）。不含时间戳/request-id。
func cacheKeyForRequest(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	canonical := make(map[string]any, 6)
	for _, key := range []string{"model", "temperature", "top_p", "max_tokens", "max_completion_tokens", "stop", "seed", "stream"} {
		if v, ok := payload[key]; ok {
			canonical[key] = v
		}
	}
	if msgs, ok := payload["messages"].([]any); ok {
		canonical["messages"] = canonicalMessages(msgs)
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// canonicalMessages 对 messages 做稳定序列化：递归规范化 map（键排序），
// 使相同内容的不同键序/空白产生同一键。
func canonicalMessages(messages []any) []any {
	out := make([]any, 0, len(messages))
	for _, m := range messages {
		out = append(out, canonicalValue(m))
	}
	return out
}

func canonicalValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ordered := make(map[string]any, len(t))
		for _, k := range keys {
			ordered[k] = canonicalValue(t[k])
		}
		return ordered
	case []any:
		out := make([]any, 0, len(t))
		for _, item := range t {
			out = append(out, canonicalValue(item))
		}
		return out
	default:
		return v
	}
}

// lookup 命中返回完整响应体快照；未命中返回 nil。
func (c *absorbCache) lookup(key string) (*absorbCacheEntry, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if c.now().Sub(entry.storedAt) > c.ttl {
		delete(c.entries, key)
		return nil, false
	}
	entry.lastUsed = c.now()
	return entry, true
}

// store 缓存一条完整响应体；容量超限时淘汰最旧未使用项。
func (c *absorbCache) store(key string, status int, header http.Header, body []byte) {
	if c == nil || key == "" || len(body) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = &absorbCacheEntry{
		body:     append([]byte(nil), body...),
		header:   snapshotHeader(header),
		status:   status,
		storedAt: c.now(),
		lastUsed: c.now(),
	}
	if len(c.entries) > absorbCacheMaxEntries {
		// 淘汰最旧 lastUsed 的条目（简单线性扫描，256 上限足够小）。
		var oldestKey string
		var oldest time.Time
		for k, e := range c.entries {
			if oldestKey == "" || e.lastUsed.Before(oldest) {
				oldestKey = k
				oldest = e.lastUsed
			}
		}
		if oldestKey != "" {
			delete(c.entries, oldestKey)
		}
	}
}

// storeAbsorbResult 吸收成功后缓存（调用方在验证完整后调用）。
func (g *gateway) storeAbsorbResult(request upstreamRequest, resp *gatewayResponse) {
	if g.absorbCache == nil || resp == nil || resp.live != nil {
		return
	}
	key := cacheKeyForRequest(parseJSONObject(request.body))
	if key == "" {
		return
	}
	g.absorbCache.store(key, resp.status, resp.header, resp.body)
}

// lookupAbsorbResult 吸收前查缓存：命中返回缓冲响应（含日志），未命中 nil。
func (g *gateway) lookupAbsorbResult(request upstreamRequest) *gatewayResponse {
	if g.absorbCache == nil {
		return nil
	}
	key := cacheKeyForRequest(parseJSONObject(request.body))
	if key == "" {
		return nil
	}
	entry, ok := g.absorbCache.lookup(key)
	if !ok {
		return nil
	}
	log.Printf("[缓存] 命中吸收产物（%d 字节，TTL 内），直接回放", len(entry.body))
	return &gatewayResponse{
		status: entry.status,
		header: entry.header.restore(),
		body:   append([]byte(nil), entry.body...),
	}
}
