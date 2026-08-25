package main

import (
	"net/http"
	"net/url"
	"sync"
	"time"
)

// transportPool 按出口代理缓存 http.Transport，让同一出口的连续请求复用
// 连接，省掉每次请求完整的 TCP+TLS 握手。键只含代理地址（直连为空串）：
// 这样节点池探活与真实请求命中同一条目——探活即预热，竞速起跑时全是热连接。
// 每请求的精确截止由 openHTTP 自己的计时器兜底，因此 Transport 层不再
// 携带按请求变化的超时参数。条目闲置超过阈值后由后台清扫回收；已被取出
// 的 transport 即使被回收也仍可安全使用，只是不再接受新连接。
type transportPool struct {
	mu          sync.Mutex
	entries     map[string]*transportEntry
	dialTimeout time.Duration // 拨号/TLS 握手超时，启动时由 loadConfig 注入
	tlsInsecure bool          // INSECURE_TLS=1 时放行非标证书
}

type transportEntry struct {
	transport *http.Transport
	lastUsed  time.Time
}

var sharedTransports = &transportPool{entries: make(map[string]*transportEntry)}

// configure 在启动阶段一次性注入拨号参数；此后 get 构造的 transport 均携带。
func (p *transportPool) configure(dialTimeout time.Duration, tlsInsecure bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dialTimeout = dialTimeout
	p.tlsInsecure = tlsInsecure
}

func (p *transportPool) get(proxyURL *url.URL) *http.Transport {
	key := ""
	if proxyURL != nil {
		key = proxyURL.String()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[key]; ok {
		entry.lastUsed = time.Now()
		return entry.transport
	}
	transport := requestTransport(proxyURL, p.dialTimeout, p.tlsInsecure)
	p.entries[key] = &transportEntry{transport: transport, lastUsed: time.Now()}
	return transport
}

// sweep 关闭并移除超过 idle 未使用的条目，防止节点池轮换导致缓存无限增长。
func (p *transportPool) sweep(idle time.Duration) {
	p.mu.Lock()
	now := time.Now()
	for key, entry := range p.entries {
		if now.Sub(entry.lastUsed) >= idle {
			entry.transport.CloseIdleConnections()
			delete(p.entries, key)
		}
	}
	p.mu.Unlock()
}

// resetAll 关闭全部条目的闲置连接并清空缓存（系统休眠恢复后调用）：
// 睡眠期间所有 TCP 连接都已死亡，留在池里只会让第一批请求撞陈旧套接字。
// 已被取出的 transport 仍可安全使用（CloseIdleConnections 只清闲置连接），
// 下一次 get 会按需重建。
func (p *transportPool) resetAll() {
	p.mu.Lock()
	for _, entry := range p.entries {
		entry.transport.CloseIdleConnections()
	}
	p.entries = make(map[string]*transportEntry)
	p.mu.Unlock()
}
