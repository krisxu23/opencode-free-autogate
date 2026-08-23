package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const maxRequestBody = 32 << 20

// uiMode 由链接期 -ldflags "-X main.uiMode=..." 指定：gui 打开窗口，console 保持纯日志。
var uiMode = "console"

type app struct {
	gateway *gateway
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	gui := strings.EqualFold(uiMode, "gui")

	settingsPath := configPath()
	settings := loadSettings(settingsPath)
	// config.json 在 GUI 与控制台两种模式下都生效（行为一致）；
	// 已显式导出的环境变量仍然优先，不会被配置文件覆盖。
	settings.applyEnv()
	if gui {
		log.SetOutput(uiLog)
		log.Printf("[启动] 界面模式已激活，日志缓冲就绪")
	}

	cfg := loadConfig(currentProject())
	gw := newGateway(cfg)
	handler := &app{gateway: gw}

	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	gw.start(rootContext)

	server := &http.Server{
		Addr:              net.JoinHostPort(cfg.listenAddr, strconv.Itoa(cfg.port)),
		Handler:           handler,
		ReadHeaderTimeout: cfg.hardTimeout,
		ReadTimeout:       cfg.hardTimeout,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	logStartup(cfg)
	errCh := make(chan error, 1)
	go func() {
		errCh <- listenAndServeWithRetry(server)
	}()

	if gui {
		go func() {
			if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("[门] server failed: %v", err)
			}
		}()
		if err := runUI(handler, settings, settingsPath, stop); err != nil {
			log.Printf("[门] 界面启动失败，退回控制台模式: %v", err)
			select {
			case <-rootContext.Done():
			case err := <-errCh:
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Printf("[门] server failed: %v", err)
				}
			}
		}
	} else {
		select {
		case <-rootContext.Done():
		case err := <-errCh:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("[门] server failed: %v", err)
				stop()
			}
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("[门] graceful shutdown failed: %v", err)
		_ = server.Close()
	}
}

// listenAndServeWithRetry 处理「保存并重启」的端口竞争：旧进程释放端口可能比
// 新进程绑定更慢，端口被占用（EADDRINUSE）期间短暂重试，而不是直接失败退出。
func listenAndServeWithRetry(server *http.Server) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := server.ListenAndServe()
		if !errors.Is(err, syscall.EADDRINUSE) || time.Now().After(deadline) {
			return err
		}
		log.Printf("[门] 端口暂被占用（旧进程退出中），稍后重试: %v", err)
		time.Sleep(300 * time.Millisecond)
	}
}

func logStartup(cfg config) {
	log.Printf("[门] http://localhost:%d", cfg.port)
	log.Printf("[门] 项目:      %s", cfg.project.displayName)
	log.Printf("[门] 上游:      %s", cfg.project.upstream)
	if cfg.project.gatewayAuth {
		state := "未启用（任何人可访问）"
		if cfg.gatewayKey != "" {
			state = "已启用 GATEWAY_KEY"
		}
		log.Printf("[门] 认证:      %s", state)
	}
	log.Printf("[门] 模式:      %s", cfg.proxyMode)
	log.Printf("[门] 超时:      流式首字节 %s / 流式总预算 %s / 非流式最高 %s", cfg.firstByteTimeout, cfg.hardTimeout, cfg.nonStreamTimeout)
	layers := make([]string, 0, 4)
	for _, layer := range cfg.orderedLayers() {
		switch layer {
		case layerPublic:
			layers = append(layers, fmt.Sprintf("S级代理(%d槽,重试%d次)", cfg.slotCount, cfg.slotRetries))
		case layerZen:
			layers = append(layers, fmt.Sprintf("ZenProxy(%d次)", cfg.zenRetries))
		case layerCustom:
			layers = append(layers, "自定义代理")
		}
	}
	if cfg.project.directFallback {
		layers = append(layers, "直连")
	}
	log.Printf("[门] 策略:      %s", strings.Join(layers, " -> "))
}

func (a *app) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	trace := newRequestTrace()
	log.Printf("[>] %s %s", r.Method, r.URL.Path)
	if !a.authorized(r) {
		trace.finalStatus = http.StatusUnauthorized
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})
		a.logCompletion(r, trace)
		return
	}

	if r.URL.Path == "/" || r.URL.Path == "/v1" {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":      "ok",
			"upstream":    a.gateway.cfg.project.upstream,
			"mode":        a.gateway.cfg.proxyMode,
			"slots":       a.gateway.slotAddresses(false),
			"customSlots": a.gateway.slotAddresses(true),
		})
		return
	}

	path, ok := normalizePath(a.gateway.cfg.project, r.URL.Path)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}

	deadline := time.Now().Add(a.gateway.cfg.hardTimeout)
	if path == "/v1/models" && r.Method == http.MethodGet {
		response, err := a.handleModels(r.Context(), r.Header, pathWithQuery(path, r.URL.RawQuery), deadline, trace)
		a.finish(w, r, trace, response, err, nil)
		return
	}
	if _, allowed := a.gateway.cfg.project.postPaths[path]; allowed && r.Method == http.MethodPost {
		response, guard, err := a.handlePost(w, r, pathWithQuery(path, r.URL.RawQuery), deadline, trace)
		a.finish(w, r, trace, response, err, guard)
		return
	}

	writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
}

func (a *app) authorized(r *http.Request) bool {
	if !a.gateway.cfg.project.gatewayAuth || a.gateway.cfg.gatewayKey == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	parts := strings.Fields(auth)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	// 先各自哈希再常量时间比较，避免长度差异提前泄露信息。
	tokenDigest := sha256.Sum256([]byte(parts[1]))
	keyDigest := sha256.Sum256([]byte(a.gateway.cfg.gatewayKey))
	return subtle.ConstantTimeCompare(tokenDigest[:], keyDigest[:]) == 1
}

func normalizePath(project projectSpec, raw string) (string, bool) {
	if len(project.prefixes) == 0 {
		if strings.HasPrefix(raw, "/v1/") {
			return raw, true
		}
		return "", false
	}
	for _, prefix := range project.prefixes {
		base := "/" + prefix
		if strings.HasPrefix(raw, base+"/v1/") {
			return strings.TrimPrefix(raw, base), true
		}
	}
	return "", false
}

func pathWithQuery(path, rawQuery string) string {
	if rawQuery == "" {
		return path
	}
	return path + "?" + rawQuery
}

func (a *app) handleModels(ctx context.Context, sourceHeaders http.Header, path string, deadline time.Time, trace *requestTrace) (*gatewayResponse, error) {
	if a.gateway.cfg.project.modelMode != modelPassthrough {
		modelContext, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()
		trace.finalProxy = "local"
		return a.gateway.modelsResponse(modelContext), nil
	}
	request := upstreamRequest{
		method:   http.MethodGet,
		path:     path,
		headers:  a.collectHeaders(sourceHeaders),
		deadline: deadline,
	}
	return a.gateway.dispatch(ctx, request, trace)
}

func (a *app) handlePost(w http.ResponseWriter, r *http.Request, path string, deadline time.Time, trace *requestTrace) (*gatewayResponse, *sseGuard, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return jsonGatewayResponse(http.StatusRequestEntityTooLarge, "请求体过大"), nil, nil
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return jsonGatewayResponse(http.StatusGatewayTimeout, "请求超时"), nil, nil
		}
		return jsonGatewayResponse(http.StatusBadRequest, "读取请求体失败"), nil, nil
	}

	payload := parseJSONObject(body)
	ids := deriveRequestIDs(r.Header, payload)
	stream := wantsStream(r.Header, payload)
	// 本地管家拦截：客户端的配额探测类请求零上游消耗直接应答。
	if a.gateway.cfg.localMocks {
		if canned, hit := tryLocalHousekeeping(path, stream, payload); hit {
			trace.finalProxy = "local"
			return &gatewayResponse{
				status: http.StatusOK,
				header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
				body:   canned,
			}, nil, nil
		}
	}
	if !stream {
		deadline = trace.start.Add(a.gateway.cfg.nonStreamTimeout)
	}
	if stream {
		body = ensureStream(body, payload)
	}
	if a.gateway.cfg.project.modelMode != modelPassthrough {
		modelContext, cancel := context.WithDeadline(r.Context(), deadline)
		body = a.gateway.rewriteModel(modelContext, body)
		cancel()
	}
	// 请求体卫生（防 400 烧出口）+ prompt 缓存字段（仅 OpenAI 协议路径，
	// 延长上游缓存 TTL）。
	body = enhanceRequestBody(body, strings.HasSuffix(path, "/v1/chat/completions") && a.gateway.cfg.cacheFields)
	headers := a.collectHeaders(r.Header)
	applyRequestIDs(headers, ids)
	if strings.HasPrefix(path, "/v1/messages") {
		applyAnthropicAuth(headers)
	}
	if headers.Get("Accept") == "" {
		headers.Set("Accept", "application/json, text/event-stream")
	}
	request := upstreamRequest{
		method:    http.MethodPost,
		path:      path,
		headers:   headers,
		body:      body,
		stream:    stream,
		nonStream: !stream,
		session:   ids.Session,
		deadline:  deadline,
	}
	if stream {
		request.headers.Set("Accept", "text/event-stream")
	}
	// 流式请求配备 SSE 保活：竞速迟迟未决时提前提交响应头并周期发送
	// 注释行心跳，客户端（ZCode 等工具）不会因等不到响应头而误判断线。
	var guard *sseGuard
	if stream {
		guard = newSseGuard(w)
	}
	response, err := a.gateway.dispatch(r.Context(), request, trace)
	if guard != nil {
		guard.Finish()
	}
	return response, guard, err
}

const (
	// sseCommitDelay 是竞速决出赢家的宽限期：正常请求（几乎都小于该值）
	// 走原有路径，保留完整的状态码/错误重试语义；超过该值仍未决出赢家
	// 才提前提交 SSE 头转入心跳模式。
	sseCommitDelay = 5 * time.Second
	// sseBeatInterval 是心跳注释行的发送间隔。
	sseBeatInterval = 3 * time.Second
)

// sseGuard 让长时间竞速的流式请求在客户端看来始终“活着”。
// 所有写入都在互斥锁内且先检查 done，因此 Finish 返回后保证不会再有
// 任何写入，调用方可以安全接管 ResponseWriter。
type sseGuard struct {
	w         http.ResponseWriter
	mu        sync.Mutex
	done      bool
	committed bool
}

func newSseGuard(w http.ResponseWriter) *sseGuard {
	g := &sseGuard{w: w}
	time.AfterFunc(sseCommitDelay, g.run)
	return g
}

// run 由 AfterFunc 触发：提交 200 + SSE 头后按间隔发送心跳注释行，
// 直到 Finish。SSE 规范里冒号开头的注释行会被所有客户端忽略。
func (g *sseGuard) run() {
	g.mu.Lock()
	if g.done {
		g.mu.Unlock()
		return
	}
	g.committed = true
	header := g.w.Header()
	if header.Get("Content-Type") == "" {
		header.Set("Content-Type", "text/event-stream; charset=utf-8")
	}
	header.Set("Cache-Control", "no-cache")
	g.w.WriteHeader(http.StatusOK)
	g.beatLocked()
	for !g.done {
		g.mu.Unlock()
		time.Sleep(sseBeatInterval)
		g.mu.Lock()
		if g.done {
			break
		}
		g.beatLocked()
	}
	g.mu.Unlock()
}

func (g *sseGuard) beatLocked() {
	_, _ = io.WriteString(g.w, ": keep-alive\n\n")
	if flusher, ok := g.w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Finish 停止心跳；返回后不会再有任何写入。
func (g *sseGuard) Finish() {
	g.mu.Lock()
	g.done = true
	g.mu.Unlock()
}

func (g *sseGuard) Committed() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.committed
}

// writeCommittedStream 处理已提前提交 SSE 头的响应：赢家的流直接透传；
// 其余情形（错误或非流式兜底）转为 SSE 错误事件，让客户端拿到明确原因。
func writeCommittedStream(w http.ResponseWriter, r *http.Request, response *gatewayResponse, streamIdle time.Duration, observe func(truncated bool)) {
	if response.live != nil {
		streamResponse(w, r.Context(), response.live, streamIdle, observe)
		return
	}
	message := "上游请求失败"
	var payload map[string]any
	if err := json.Unmarshal(response.body, &payload); err == nil {
		switch value := payload["error"].(type) {
		case string:
			if value != "" {
				message = value
			}
		case map[string]any:
			if text, _ := value["message"].(string); text != "" {
				message = text
			}
		}
	}
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{"message": message, "code": response.status},
	})
	_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", body)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// applyRequestIDs 用网关派生的标识覆盖发往上游的 OpenCode 头。
func applyRequestIDs(headers http.Header, ids requestIDs) {
	headers.Set("X-Opencode-Session", ids.Session)
	headers.Set("X-Opencode-Request", ids.Request)
	headers.Set("X-Opencode-Project", ids.Project)
}

// applyAnthropicAuth 让 /v1/messages 与真实 OpenCode 客户端一致：
// 使用 x-api-key 而非 Authorization Bearer，并补齐默认 anthropic-version。
func applyAnthropicAuth(headers http.Header) {
	token := strings.TrimPrefix(headers.Get("Authorization"), "Bearer ")
	headers.Del("Authorization")
	if token != "" {
		headers.Set("X-Api-Key", token)
	}
	if headers.Get("Anthropic-Version") == "" {
		headers.Set("Anthropic-Version", "2023-06-01")
	}
}

func (a *app) collectHeaders(source http.Header) http.Header {
	result := make(http.Header)
	for _, key := range a.gateway.cfg.project.forwardHeaders {
		if source == nil {
			continue
		}
		for _, value := range source.Values(key) {
			result.Add(key, value)
		}
	}
	if auth := a.gateway.cfg.project.upstreamAuthorization; auth != "" {
		result.Set("Authorization", auth)
	}
	if client := a.gateway.cfg.project.defaultClientHeader; client != "" {
		result.Set("X-Opencode-Client", client)
	}
	result.Set("User-Agent", opencodeUserAgent())
	if result.Get("Content-Type") == "" {
		result.Set("Content-Type", "application/json")
	}
	return result
}

func parseJSONObject(body []byte) map[string]any {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return nil
	}
	return payload
}

func wantsStream(headers http.Header, payload map[string]any) bool {
	if strings.Contains(strings.ToLower(headers.Get("Accept")), "event-stream") {
		return true
	}
	stream, _ := payload["stream"].(bool)
	return stream
}

func ensureStream(body []byte, payload map[string]any) []byte {
	if payload == nil {
		return body
	}
	if stream, _ := payload["stream"].(bool); stream {
		return body
	}
	payload["stream"] = true
	rewritten, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return rewritten
}

func (a *app) finish(w http.ResponseWriter, r *http.Request, trace *requestTrace, response *gatewayResponse, err error, guard *sseGuard) {
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		switch {
		case errors.Is(err, errRequestTimeout), errors.Is(err, errAttemptTimeout), errors.Is(err, context.DeadlineExceeded):
			response = jsonGatewayResponse(http.StatusGatewayTimeout, "请求超时")
		case errors.Is(err, errNoProxy):
			response = jsonGatewayResponse(http.StatusBadGateway, "没有可用代理")
		case errors.Is(err, errStreamTruncated):
			response = jsonGatewayResponse(http.StatusBadGateway, "上游流中断")
		default:
			response = jsonGatewayResponse(http.StatusBadGateway, "上游请求失败")
		}
		log.Printf("[请求错] %s %s: %v", r.Method, r.URL.Path, err)
	}
	if response == nil {
		response = jsonGatewayResponse(http.StatusBadGateway, "上游请求失败")
	}
	trace.finalStatus = response.status
	if trace.finalProxy == "" && trace.attempts == 0 {
		trace.finalProxy = "local"
	}
	a.logCompletion(r, trace)
	// 流式响应结束后由 streamResponse 回报是否见到终止标记：
	// 没见到即视为中途夭折，出口降权、镜像记账，下次绕开。
	observe := func(truncated bool) {
		if truncated {
			log.Printf("[流截断] %s | 出口:%s | 上游:%s | 流未带终止标记即结束", r.URL.Path, trace.finalProxy, trace.upstream)
			a.gateway.noteStreamTruncation(trace.finalProxy, trace.upstream)
			return
		}
		if trackableExit(trace.finalProxy) {
			a.gateway.exits.observeSuccess(trace.finalProxy)
		}
	}
	if guard.Committed() {
		// 响应头已提前提交，状态码无法再改变：赢家流透传，其余转为 SSE 错误事件。
		writeCommittedStream(w, r, response, a.gateway.cfg.streamIdle, observe)
		return
	}
	writeGatewayResponse(w, r, response, a.gateway.cfg.streamIdle, observe)
}

func (a *app) logCompletion(r *http.Request, trace *requestTrace) {
	status := trace.finalStatus
	a.gateway.lastStatus.Store(int32(status))
	result := "完成"
	if status < 200 || status >= 400 {
		result = "失败"
	}
	retries := trace.attempts - 1
	if retries < 0 {
		retries = 0
	}
	proxy := trace.finalProxy
	if proxy == "" {
		proxy = "unknown"
	}
	log.Printf("[%s] %s %s | 状态:%d | 重试:%d | 代理IP:%d | 耗时:%dms | 代理:%s",
		result, r.Method, r.URL.Path, status, retries, len(trace.proxies), time.Since(trace.start).Milliseconds(), proxy)
}

func writeGatewayResponse(w http.ResponseWriter, r *http.Request, response *gatewayResponse, streamIdle time.Duration, observe func(truncated bool)) {
	for key, values := range response.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if response.live != nil {
		w.Header().Del("Content-Length")
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		}
		if w.Header().Get("Cache-Control") == "" {
			w.Header().Set("Cache-Control", "no-cache")
		}
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	w.WriteHeader(response.status)
	if response.live == nil {
		_, _ = w.Write(response.body)
		return
	}
	streamResponse(w, r.Context(), response.live, streamIdle, observe)
}

// streamTerminals 是判定流完整结束的标记：见到任一个即认为上游正常收尾。
// 涵盖三种路由：OpenAI chat（[DONE] / 非空 finish_reason）、Anthropic
// （message_stop）、Codex responses（response.completed 等）。
var streamTerminals = [][]byte{
	[]byte("[DONE]"),
	[]byte(`"finish_reason":"`), // null 写法是 "finish_reason":null，不含引号值
	[]byte("message_stop"),
	[]byte("response.completed"),
	[]byte("response.failed"),
	[]byte("response.incomplete"),
}

func streamResponse(w http.ResponseWriter, ctx context.Context, live *liveResponse, idle time.Duration, observe func(truncated bool)) {
	defer live.Close()
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	buffer := make([]byte, 32<<10)
	// 终止标记扫描：窗口保留上次尾部，防止标记被拆在两次读取里。
	terminalSeen := false
	var window []byte
	scan := func(chunk []byte) {
		if terminalSeen {
			return
		}
		window = append(window, chunk...)
		for _, marker := range streamTerminals {
			if bytes.Contains(window, marker) {
				terminalSeen = true
				break
			}
		}
		if len(window) > 64 {
			window = append(window[:0], window[len(window)-64:]...)
		}
	}
	// report 只在上游侧结束时调用；客户端主动断开（写失败/请求取消）
	// 与上游质量无关，不记账。
	report := func() {
		if observe != nil {
			observe(!terminalSeen)
		}
	}
	// injectStreamError 在流未能正常收尾时补发一个格式合法的 SSE 错误事件。
	// 没有它，出口代理中途暴毙表现为 TCP 半途重置，客户端判定传输层崩溃
	// （network_error）直接放弃任务、不走重试；补上可解析的错误事件后，
	// 同样的中断会被当作生成错误进入客户端的自动重试。同时覆盖两种路由
	// 的解析习惯：OpenAI 兼容读 data.error，Anthropic 读 event: error，
	// 各自忽略不认识的那段。
	injectStreamError := func() {
		if flusher == nil {
			return
		}
		var buf bytes.Buffer
		buf.WriteString("data: {\"error\":{\"message\":\"upstream stream truncated mid-generation\",\"type\":\"upstream_error\",\"code\":\"stream_truncated\"}}\n\n")
		buf.WriteString("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"upstream stream truncated\"}}\n\n")
		if _, err := w.Write(buf.Bytes()); err == nil && flusher != nil {
			flusher.Flush()
		}
	}
	// 复用单个计时器实现空闲超时：到点取消上游请求，阻塞中的 Read 随之返回。
	timer := time.AfterFunc(idle, live.cancel)
	defer timer.Stop()
	for {
		timer.Reset(idle)
		n, err := live.response.Body.Read(buffer)
		stopped := timer.Stop()
		if n > 0 {
			scan(buffer[:n])
			if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if !stopped {
			log.Printf("[流] %s 内无数据，已关闭连接", idle)
			injectStreamError()
			report()
			return
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				log.Printf("[流] upstream stream ended: %v", err)
				// 上游异常断开（节点暴毙/连接重置）：补发错误事件，让客户端
				// 把这次中断当作可重试的生成错误而不是传输崩溃。
				injectStreamError()
			}
			if ctx.Err() == nil {
				report()
			}
			return
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
