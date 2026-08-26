package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"time"
)

// 吸收模式：客户端只看到 sseGuard 的保活心跳（既有设施，SSE 注释行对
// 客户端不可见），网关在内部反复请求上游，直到拿到带终止标记的完整回复
// 才一次性转发。截断/空流/限流/5xx 立即换道重来；DSH 侧全程显示“生成中”，
// 不感知任何上游抖动。

const (
	absorbTotalBudget = 10 * time.Minute       // 全部尝试共享的总预算
	absorbRetryDelay  = 300 * time.Millisecond // 相邻尝试之间的基础退避
)

// streamComplete 判断一段 SSE 原始字节是否带有正常收尾标记。
// 复用 streamTerminals（[DONE]/非空 finish_reason/message_stop/response.*），
// 与透传路径的截断判定完全同一套标准。
func streamComplete(data []byte) bool {
	for _, marker := range streamTerminals {
		if bytes.Contains(data, marker) {
			return true
		}
	}
	return false
}

// absorbDispatcher 抽象单次调度，测试可注入假上游行为。
type absorbDispatcher func(context.Context, upstreamRequest, *requestTrace) (*gatewayResponse, error)

// dispatchAbsorb 是吸收模式入口：handlePost 对开启该功能的流式请求改走这里。
func (g *gateway) dispatchAbsorb(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	return g.dispatchAbsorbWith(ctx, request, trace, g.dispatch)
}

// dispatchAbsorbWith 反复调用 dispatch 直到拿到完整流或耗尽尝试/预算：
//   - 调度失败 / 可重试状态码：记账后短暂退避换道重来（首失败留作兜底）；
//   - 非重试 4xx（key/参数错）：重试无意义，立即原样交还上层；
//   - 流式赢家先 readAll 收全量并验证终止标记，不完整按截断记账后重试；
//   - 全部用尽：优先交出留存的兜底响应，否则返回 errStreamTruncated，
//     由 finish/writeCommittedStream 按既有语义收尾（干净关闭或 SSE 错误事件）。
func (g *gateway) dispatchAbsorbWith(ctx context.Context, request upstreamRequest, trace *requestTrace, dispatch absorbDispatcher) (*gatewayResponse, error) {
	maxAttempts := g.cfg.absorbAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	overall := time.Now().Add(absorbTotalBudget)
	var fallback *gatewayResponse
	stormLogged := false
	pauseAbsorb := func(attempt int) {
		if g.outage.Tripped() {
			if !stormLogged {
				stormLogged = true
				log.Printf("[吸收] 全局熔断生效，切换指数退避")
			}
			if g.outage.TryProbe() {
				return // 半开探针：跳过本次退避，立即放行一次尝试验证恢复
			}
			time.Sleep(g.absorbBackoff(attempt))
			return
		}
		time.Sleep(g.absorbBackoff(attempt))
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Until(overall) < 2*time.Second {
			log.Printf("[吸收] 总预算将尽，停止于第 %d/%d 次尝试", attempt-1, maxAttempts)
			break
		}
		attemptCtx, cancel := context.WithDeadline(ctx, overall)
		resp, err := dispatch(attemptCtx, request, trace)
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			log.Printf("[吸收] 第 %d/%d 次未取得响应: %v", attempt, maxAttempts, err)
			pauseAbsorb(attempt)
			continue
		}
		if resp.live == nil {
			// 缓冲响应：非 2xx 或非流式形态。cancel 此刻安全（无存活流）。
			cancel()
			if retryableStatus(resp.status) {
				if fallback == nil {
					fallback = resp // 与竞速同约定：全灭时交出的最后兜底
				}
				log.Printf("[吸收] 第 %d/%d 次可重试错误 status=%d，换道", attempt, maxAttempts, resp.status)
				pauseAbsorb(attempt)
				continue
			}
			return resp, nil
		}
		// 流式赢家：readAll 收全量（内部会关流并触发取消），再验完整性。
		data, rerr := resp.live.readAll(overall)
		cancel()
		if rerr != nil {
			g.noteStreamTruncation(trace.finalProxy, trace.upstream)
			log.Printf("[吸收] 第 %d/%d 次读取失败（出口:%s）: %v", attempt, maxAttempts, trace.finalProxy, rerr)
			pauseAbsorb(attempt)
			continue
		}
		// 分块卫生先行：完整性判定与最终转发共用清洗后的字节，
		// 保证"客户端看到什么，就以什么为准判断完整"。
		if g.cfg.sseHygiene {
			data = sanitizeSSEBytes(data)
		}
		if !streamComplete(data) {
			g.noteStreamTruncation(trace.finalProxy, trace.upstream)
			log.Printf("[吸收] 第 %d/%d 次截断（出口:%s 镜像:%s），换道重试", attempt, maxAttempts, trace.finalProxy, trace.upstream)
			continue
		}
		header := resp.header.Clone()
		if header.Get("Content-Type") == "" {
			header.Set("Content-Type", "text/event-stream; charset=utf-8")
		}
		g.outage.NoteSuccess() // 完整回复=上游活着：半开探针成功则提前解除熔断
		log.Printf("[吸收] 第 %d 次尝试取得完整回复（出口:%s 镜像:%s，%d 字节）",
			attempt, trace.finalProxy, trace.upstream, len(data))
		return &gatewayResponse{status: http.StatusOK, header: header, body: data}, nil
	}
	if fallback != nil {
		log.Printf("[吸收] 尝试用尽，交出可重试兜底 status=%d", fallback.status)
		return fallback, nil
	}
	return nil, errStreamTruncated
}
