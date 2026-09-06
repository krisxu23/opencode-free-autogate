package main

import (
	"context"
	"strings"
)

// parseAutoPools 解析 PROXY_AUTO_POOLS：分号分池，逗号分成员。
// 例：auto=m365/gpt-5.6-sol,opencode/big-pickle;code=m365/gpt-5.6-luna
func parseAutoPools(raw string) map[string][]string {
	out := map[string][]string{}
	for _, pool := range strings.Split(raw, ";") {
		kv := strings.SplitN(pool, "=", 2)
		if len(kv) != 2 {
			continue
		}
		name := strings.TrimSpace(kv[0])
		var members []string
		seen := map[string]struct{}{}
		for _, m := range strings.Split(kv[1], ",") {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			if _, dup := seen[m]; dup {
				continue
			}
			seen[m] = struct{}{}
			members = append(members, m)
		}
		if name != "" && len(members) > 0 {
			out[name] = members
		}
	}
	return out
}

// lookupSupplier 按 "供应商/模型" 前缀查找启用的通用供应商。
func (g *gateway) lookupSupplier(model string) (Supplier, bool) {
	supID, _, ok := SplitSupplierPrefix(model)
	if !ok {
		return Supplier{}, false
	}
	for _, s := range g.cfg.suppliers {
		if s.Enabled && s.ID == supID {
			return s, true
		}
	}
	return Supplier{}, false
}

// resolveUnifiedChain 把 auto[/池名] 展开成成员链；其余走既有 fallback 链。
func (g *gateway) resolveUnifiedChain(request upstreamRequest) []string {
	current := requestModelName(request)
	if current == "auto" {
		if members, ok := g.cfg.autoPools["auto"]; ok {
			return append([]string(nil), members...)
		}
		return []string{current}
	}
	if name, ok := strings.CutPrefix(current, "auto/"); ok {
		if members, exists := g.cfg.autoPools[name]; exists {
			return append([]string(nil), members...)
		}
		return []string{current}
	}
	return g.resolveModelChain(request)
}

func isChainRetryableErr(err error) bool {
	return err == errAllExitsFailed || err == errNoProxy || err == errStreamTruncated
}

// dispatchUnified 是 handlePost 的统一分发入口：auto 池/前缀成员走供应商
// 路由，其余等价于原 dispatchModelChain（零回归）。
func (g *gateway) dispatchUnified(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	chain := g.resolveUnifiedChain(request)
	original := requestModelName(request)
	if len(chain) == 1 && chain[0] == original {
		if sup, ok := g.lookupSupplier(chain[0]); ok {
			return g.dispatchSupplier(ctx, request, trace, sup)
		}
		return g.dispatchModelChain(ctx, request, trace)
	}
	var lastResp *gatewayResponse
	var lastErr error
	for i, model := range chain {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		req := request
		if model != original {
			body, ok := rewriteBodyModel(request.body, model)
			if !ok {
				break
			}
			req.body = body
			trace.clearTried()
			trace.noteModelFallback(original, model)
		}
		var resp *gatewayResponse
		var err error
		if sup, ok := g.lookupSupplier(model); ok {
			resp, err = g.dispatchSupplier(ctx, req, trace, sup)
		} else {
			resp, err = g.dispatch(ctx, req, trace)
		}
		if err != nil {
			if isChainRetryableErr(err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		if retryableStatus(resp.status) && i < len(chain)-1 {
			lastResp = resp
			continue
		}
		if model != original {
			resp = rewriteResponseModel(resp, original, model)
		}
		return resp, nil
	}
	if lastResp != nil {
		return lastResp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errNoProxy
}

// dispatchSupplier 把请求发往通用供应商：覆盖上游基址与认证 Key，剥掉
// 供应商前缀用真模型名；出口竞速/熔断/重试复用既有 dispatch 管道。
func (g *gateway) dispatchSupplier(ctx context.Context, request upstreamRequest, trace *requestTrace, sup Supplier) (*gatewayResponse, error) {
	req := request
	req.upstream = strings.TrimRight(sup.BaseURL, "/")
	if req.headers == nil {
		req.headers = make(map[string][]string)
	} else {
		req.headers = req.headers.Clone()
	}
	if sup.APIKey != "" {
		req.headers.Set("Authorization", "Bearer "+sup.APIKey)
	}
	if _, real, ok := SplitSupplierPrefix(requestModelName(request)); ok {
		if body, good := rewriteBodyModel(req.body, real); good {
			req.body = body
		}
	}
	return g.dispatch(ctx, req, trace)
}
