package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
)

// holdbackBuffer 是 FCC stream_recovery.RecoveryHoldbackBuffer 的移植。
// 透传流在"提交"给客户端之前，先暂存开头一小段到缓冲里：
//   - 缓冲期内上游截断（未见终止标记）→ 静默丢弃缓冲，换出口重发；
//   - 超过缓冲窗口（时间/字节上限）→ 缓冲作为流前缀一次性交付，标记已提交，
//     剩余流继续由 streamResponse 透传（终止标记判定从前缀开始，不重不漏）；
//   - 缓冲期内已见终止标记 → 整段交付，无需等待窗口。
//
// 这给透传路径（非吸收模式）提供了"提交前截断静默重试"的能力——客户端在
// 缓冲窗口内不会看到任何字节，因此截断无感知。与吸收模式（dispatchAbsorb
// 整段 readAll 验证后一次性转发）的区别：holdback 只缓冲开头一小段，提交后
// 照常流式透传，首字节延迟只增加一个窗口时间。

type holdbackConfig struct {
	enabled  bool
	window   time.Duration
	maxBytes int
	retries  int
}

type holdbackBuffer struct {
	window    time.Duration
	maxBytes  int
	buf       []byte
	startedAt time.Time
	committed bool
}

func newHoldbackBuffer(window time.Duration, maxBytes int) *holdbackBuffer {
	return &holdbackBuffer{window: window, maxBytes: maxBytes}
}

// push 接收一段已清洗完毕的待转发字节，返回 (toFlush, complete)。
//   - complete=true：缓冲内已含终止标记，流完整，可整段交付；
//   - toFlush 非空：窗口到期，这些字节需要提交给客户端；之后 committed；
//   - 否则：仍在缓冲期，继续读。
func (h *holdbackBuffer) push(chunk []byte) (toFlush []byte, complete bool) {
	h.buf = append(h.buf, chunk...)
	if h.startedAt.IsZero() {
		h.startedAt = time.Now()
	}
	if len(h.buf) > 0 && streamComplete(h.buf) {
		h.committed = true
		return h.buf, true
	}
	if len(h.buf) >= h.maxBytes || time.Since(h.startedAt) >= h.window {
		h.committed = true
		return h.buf, false
	}
	return nil, false
}

// discard 丢弃缓冲，用于静默重发（仅未提交时调用）。
func (h *holdbackBuffer) discard() {
	h.buf = nil
	h.startedAt = time.Time{}
}

// holdbackRetrier 管理一次 holdback 周期：缓冲 → 截断时静默换出口重发。
type holdbackRetrier struct {
	gateway *gateway
	origin  *upstreamRequest
	trace   *requestTrace
	cfg     holdbackConfig
}

// writeWithHoldback 是 streamResponse 的 holdback 包装入口。
// 返回 true 表示缓冲期内已完整交付（含终止标记）；false 表示未在缓冲期内
// 交付（已提交转交 streamResponse，或重试耗尽转交既有截断收尾）。
func (h *holdbackRetrier) writeWithHoldback(w http.ResponseWriter, ctx context.Context, live *liveResponse, opts streamOptions) bool {
	if h == nil || !h.cfg.enabled || h.cfg.window <= 0 || h.origin == nil {
		streamResponse(w, ctx, live, opts)
		return false
	}
	flusher, _ := w.(http.Flusher)
	buffer := make([]byte, 32<<10)
	attempts := h.cfg.retries + 1 // 首次 + 静默重发次数
	for attempt := 0; attempt < attempts; attempt++ {
		hb := newHoldbackBuffer(h.cfg.window, h.cfg.maxBytes)
		for {
			n, err := live.response.Body.Read(buffer)
			if n > 0 {
				out := buffer[:n]
				if opts.hyg != nil {
					out = opts.hyg.feed(out)
				}
				toFlush, complete := hb.push(out)
				if complete {
					// 缓冲内已含终止标记：整段交付
					if _, werr := w.Write(toFlush); werr != nil {
						return false
					}
					if flusher != nil {
						flusher.Flush()
					}
					if opts.observe != nil {
						opts.observe(false)
					}
					return true
				}
				if len(toFlush) > 0 {
					// 窗口到期，提交：缓冲作为流前缀交给 streamResponse，
					// 由它统一做终止标记扫描/用量采集/续写素材收集。
					live.response.Body = &prefixedBody{prefix: toFlush, src: live.response.Body}
					streamResponse(w, ctx, live, opts)
					return false
				}
			}
			if err != nil {
				if ctx.Err() != nil {
					return false
				}
				if len(hb.buf) > 0 && streamComplete(hb.buf) {
					// 上游结束但缓冲内已有终止标记：正常收尾交付。
					flush := hb.buf
					hb.buf = nil
					if _, werr := w.Write(flush); werr != nil {
						return false
					}
					if flusher != nil {
						flusher.Flush()
					}
					if opts.observe != nil {
						opts.observe(false)
					}
					return true
				}
				// 提交前截断：未向客户端写任何字节。
				if !errors.Is(err, io.EOF) {
					log.Printf("[holdback]%s 读上游错误: %v", h.trace.tagString(), err)
				}
				if attempt < attempts-1 {
					log.Printf("[holdback]%s 提交前截断，第 %d/%d 次静默换道重发",
						h.trace.tagString(), attempt+1, attempts-1)
					live.Close()
					resp, derr := h.gateway.dispatch(ctx, *h.origin, h.trace)
					if derr != nil || resp == nil || resp.live == nil {
						// 重发失败：把已缓冲的字节（可能为空）交给 streamResponse
						// 走既有截断收尾（续写/静默关闭），客户端拿到明确语义。
						log.Printf("[holdback]%s 重发失败: %v", h.trace.tagString(), derr)
						live.response.Body = &prefixedBody{prefix: hb.buf, src: live.response.Body}
						streamResponse(w, ctx, live, opts)
						return false
					}
					live = resp.live
					break // 跳出内层读循环，进入下一次尝试
				}
				// 重试耗尽：把缓冲交给 streamResponse 走既有截断收尾
				// （续写/静默关闭），与中流截断语义一致。
				live.response.Body = &prefixedBody{prefix: hb.buf, src: live.response.Body}
				streamResponse(w, ctx, live, opts)
				return false
			}
		}
	}
	return false
}
