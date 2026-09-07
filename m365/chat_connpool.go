package m365

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type pooledConn struct {
	conn      *websocket.Conn
	created   time.Time
	handshook bool
	taken     atomic.Bool
	writeMu   sync.Mutex
	frames    chan []byte
	errs      chan error
}

const (
	maxPoolPerKey = 2
	poolConnTTL   = 300 * time.Second
)

type ConnPool struct {
	mu     sync.Mutex
	conns  map[string][]*pooledConn // key = oid|tid
	dialer *websocket.Dialer
	header http.Header
	stop   chan struct{}
}

func NewConnPool(dialer *websocket.Dialer, header http.Header) *ConnPool {
	p := &ConnPool{
		conns:  make(map[string][]*pooledConn),
		dialer: dialer,
		header: header,
		stop:   make(chan struct{}),
	}
	go p.gcLoop()
	return p
}

func (p *ConnPool) key(oid, tid string) string { return oid + "|" + tid }

// startPark keeps a parked connection alive by answering SignalR pings while
// it waits in the pool. The pump is the connection's PERMANENT single reader:
// gorilla poisons a conn after any read error (including deadline expiry), so
// ownership is never handed off. Once taken, frames are forwarded to Chat via
// channels instead.
func (p *ConnPool) startPark(key string, pc *pooledConn) {
	pc.frames = make(chan []byte, 64)
	pc.errs = make(chan error, 1)
	go func() {
		for {
			_, msg, err := pc.conn.ReadMessage()
			if err != nil {
				if pc.taken.Load() {
					select {
					case pc.errs <- err:
					default:
					}
					close(pc.frames)
				} else {
					p.evict(key, pc)
				}
				return
			}
			if strings.HasPrefix(string(msg), `{"type":6}`) && !pc.taken.Load() {
				pc.writeMu.Lock()
				_ = pc.conn.WriteMessage(websocket.TextMessage, []byte(`{"type":6}`+rs))
				pc.writeMu.Unlock()
				continue
			}
			if pc.taken.Load() {
				select {
				case pc.frames <- msg:
				case <-time.After(30 * time.Second):
					return
				}
			}
		}
	}()
}

func (p *ConnPool) evict(key string, target *pooledConn) {
	p.mu.Lock()
	conns := p.conns[key]
	for i, pc := range conns {
		if pc == target {
			p.conns[key] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	p.mu.Unlock()
	target.conn.Close()
}

func (p *ConnPool) Take(ctx context.Context, oid, tid string, wsURL string) (*websocket.Conn, *sync.Mutex, <-chan []byte, <-chan error, bool, error) {
	_ = wsURL
	p.mu.Lock()
	key := p.key(oid, tid)
	conns := p.conns[key]
	var picked *pooledConn
	var stale []*pooledConn
	kept := conns[:0]
	for _, pc := range conns {
		if picked == nil && pc.handshook && time.Since(pc.created) < poolConnTTL {
			picked = pc
			continue
		}
		if time.Since(pc.created) >= poolConnTTL {
			stale = append(stale, pc)
			continue
		}
		kept = append(kept, pc)
	}
	if len(kept) == 0 {
		delete(p.conns, key)
	} else {
		p.conns[key] = kept
	}
	p.mu.Unlock()

	for _, pc := range stale {
		pc.taken.Store(true)
		pc.conn.Close()
	}

	if picked != nil {
		picked.taken.Store(true)
		log.Printf("[connpool] hit oid=%s age_ms=%d", oid, time.Since(picked.created).Milliseconds())
		return picked.conn, &picked.writeMu, picked.frames, picked.errs, true, nil
	}

	conn, resp, err := p.dialer.DialContext(ctx, wsURL, p.header.Clone())
	if err != nil {
		if resp != nil {
			log.Printf("[connpool] dial failed oid=%s status=%d", oid, resp.StatusCode)
		}
		return nil, nil, nil, nil, false, err
	}
	return conn, nil, nil, nil, false, nil
}

func (p *ConnPool) Warm(ctx context.Context, acc Account, wsURL string) {
	if wsURL == "" {
		return
	}
	key := p.key(acc.OID, acc.TID)

	p.mu.Lock()
	if len(p.conns[key]) >= maxPoolPerKey {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	conn, resp, err := p.dialer.DialContext(ctx, wsURL, p.header.Clone())
	if err != nil {
		if resp != nil {
			log.Printf("[connpool] warm dial failed oid=%s status=%d err=%v", acc.OID, resp.StatusCode, err)
		} else {
			log.Printf("[connpool] warm dial failed oid=%s err=%v", acc.OID, err)
		}
		return
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"protocol":"json","version":1}`+"\x1e")); err != nil {
		log.Printf("[connpool] warm handshake send failed: %v", err)
		conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, _, err = conn.ReadMessage()
	if err != nil {
		log.Printf("[connpool] warm handshake recv failed: %v", err)
		conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	pc := &pooledConn{conn: conn, created: time.Now(), handshook: true}
	p.mu.Lock()
	if len(p.conns[key]) >= maxPoolPerKey {
		p.mu.Unlock()
		conn.Close()
		return
	}
	p.conns[key] = append(p.conns[key], pc)
	p.mu.Unlock()
	p.startPark(key, pc)

	log.Printf("[connpool] warmed connection oid=%s tid=%s", acc.OID, acc.TID)
}

func (p *ConnPool) WarmWithProbe(ctx context.Context, acc Account, wsURL string) {
	p.Warm(ctx, acc, wsURL)
}

func (p *ConnPool) Return(oid, tid string, conn *websocket.Conn) {
	if conn != nil {
		conn.Close()
	}
}

func (p *ConnPool) Discard(oid, tid string, conn *websocket.Conn) {
	if conn != nil {
		conn.Close()
	}
}

func (p *ConnPool) GC() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, conns := range p.conns {
		kept := conns[:0]
		for _, pc := range conns {
			if now.Sub(pc.created) > poolConnTTL {
				pc.taken.Store(true)
				pc.conn.Close()
			} else {
				kept = append(kept, pc)
			}
		}
		if len(kept) == 0 {
			delete(p.conns, k)
		} else {
			p.conns[k] = kept
		}
	}
}

func (p *ConnPool) Close() {
	close(p.stop)
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, conns := range p.conns {
		for _, pc := range conns {
			pc.taken.Store(true)
			pc.conn.Close()
		}
		delete(p.conns, k)
	}
}

func (p *ConnPool) Stats() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	details := make([]map[string]any, 0)
	for k, conns := range p.conns {
		for _, pc := range conns {
			total++
			details = append(details, map[string]any{"key": k, "age_ms": time.Since(pc.created).Milliseconds(), "handshook": pc.handshook})
		}
	}
	return map[string]any{"mode": "connpool", "pooled_connections": total, "details": details}
}

func (p *ConnPool) gcLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.GC()
		}
	}
}
