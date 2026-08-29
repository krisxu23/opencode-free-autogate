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
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const maxRequestBody = 32 << 20 // 32 MiB

// usageObserver 由 newGateway 装配：流式转发路径边写边解析 usage 块，
// 避免给 streamResponse 增加网关依赖（缓冲响应在 finish() 统一记账）。
var usageObserver func(model string, prompt, completion, cached int64)

// uiMode 由链接期 -ldflags "-X main.uiMode=..." 指定：gui 打开窗口，console 保持纯日志。
var uiMode = "console"

type app struct {
	gateway *gateway
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	gui := strings.EqualFold(uiMode, "gui")

	settingsPath := configPath()
	// 统一日志出口最先就位：界面/控制台 + logs/gateway.log 轮转文件双写，
	// 全行脱敏与防洪水兜底——后续所有日志（含首次运行的 Key 提示）都走这里。
	var logSinks []io.Writer
	if gui {
		logSinks = append(logSinks, uiLog)
	} else {
		logSinks = append(logSinks, os.Stderr)
	}
	logKit := newLogKit(logSinks...)
	log.SetOutput(logKit)
	if path := logKit.openFile(filepath.Dir(settingsPath)); path != "" {
		log.Printf("[日志] 文件日志已开启：%s（单文件 %dMB，保留 %d 份）", path, logFileMaxSize>>20, logFileBackups)
	} else {
		log.Printf("[日志] 文件日志开启失败，仅输出到界面/控制台")
	}

	settings := loadSettings(settingsPath)
	// 首次运行无 config.json 时自动保存默认配置（含随机生成的 Key），
	// 确保后续启动复用同一个 Key。Key 只展示掩码：日志会被用户整段贴出求助，
	// 完整 Key 请到 config.json 或界面查看。
	if _, err := os.Stat(settingsPath); os.IsNotExist(err) {
		if saveErr := settings.save(settingsPath); saveErr != nil {
			log.Printf("[配置] 自动保存默认配置失败: %v", saveErr)
		} else {
			log.Printf("[配置] 首次运行，已生成 config.json（Key: %s；完整 Key 见 config.json 或界面）", maskSecret(settings.GatewayKey))
		}
	}
	// config.json 在 GUI 与控制台两种模式下都生效（行为一致）；
	// 已显式导出的环境变量仍然优先，不会被配置文件覆盖。
	settings.applyEnv()
	if gui {
		log.Printf("[启动] 界面模式已激活，日志缓冲就绪")
	}

	cfg := loadConfig(currentProject())
	// 非回环监听会把网关暴露给局域网/外网：没有认证凭据就拒绝启动，宁可失败不可裸奔。
	if !isLoopbackListen(cfg.listenAddr) && cfg.gatewayKey == "" {
		log.Fatalf("[门] LISTEN_ADDR=%s 会把网关暴露到局域网/外网：请先设置 GATEWAY_KEY（客户端需携带 Bearer 认证），或保持默认 127.0.0.1 仅本机监听", cfg.listenAddr)
	}
	gw := newGateway(cfg)
	handler := &app{gateway: gw}

	// 后台拉取 Cline 免费模型（有账号时），使 /v1/models 与 GUI 模型列表
	// 在启动后尽快带上 Cline 上游模型。
	go func() {
		time.Sleep(2 * time.Second)
		refreshClineModels()
	}()

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
			// Walk 框架 TTM_ADDTOOL 在特定 Windows 配置下首次进程启动时失败，
			// 导致窗口建不出来。自动重启自身进程（带 DSH_RESTARTED 标记防无限循环）。
			// CI 环境（无显示器）跳过：窗口建不出是预期行为，不需要重启。
			if os.Getenv("DSH_RESTARTED") == "" && os.Getenv("GITHUB_ACTIONS") == "" {
				log.Printf("[门] 窗口创建失败，自动重启: %v", err)
				cmd := exec.Command(os.Args[0], os.Args[1:]...)
				cmd.Env = append(os.Environ(), "DSH_RESTARTED=1")
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				cmd.Stdin = os.Stdin
				if startErr := cmd.Start(); startErr == nil {
					os.Exit(0)
				}
			}
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
// isAddrInUse 识别「端口已被占用」。Go 在 Windows 上 syscall.EADDRINUSE 常量
// 与真实错误码 WSAEADDRINUSE(10048) 数值不相等，errors.Is 判定永远为假——
// 这里补上数值比对与错误原话兜底，三重保险。
func isAddrInUse(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	if runtime.GOOS == "windows" {
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == 10048 { // WSAEADDRINUSE
			return true
		}
	}
	return strings.Contains(err.Error(), "Only one usage of each socket address")
}

func listenAndServeWithRetry(server *http.Server) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := server.ListenAndServe()
		if !isAddrInUse(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("监听 %s 失败：端口被其他程序（或本程序的另一个实例）占用，已宽限重试 30 秒仍不可用: %w", server.Addr, err)
		}
		log.Printf("[门] 端口暂被占用（可能旧进程退出中），稍后重试: %v", err)
		time.Sleep(500 * time.Millisecond)
	}
}

// notifyFatal 已移除：端口占用现在通过日志与 30 秒宽限重试表达，
// 不再需要额外的系统弹窗依赖。

// isLoopbackListen 判断监听地址是否仅回环可达（localhost / 127.x / ::1）。
func isLoopbackListen(addr string) bool {
	host := strings.TrimSpace(addr)
	if host == "" || host == "localhost" {
		return true
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if ip := net.ParseIP(strings.ToLower(host)); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func logStartup(cfg config) {
	log.Printf("[门] http://localhost:%d", cfg.port)
	log.Printf("[门] 项目:      %s", cfg.project.displayName)
	log.Printf("[门] 上游:      %s", cfg.project.upstream)
	if cfg.project.gatewayAuth {
		state := "未启用（任何人可访问）"
		if cfg.gatewayKey != "" {
			// Key 只出掩码：启动日志同样会被整段贴出求助。
			state = "已启用 " + maskSecret(cfg.gatewayKey)
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

	// URL path 认证兼容模式：/vscode/{key}/v1/... （兼容不能发 Header 的客户端）
	if key, ok := extractURLKey(r.URL.Path); ok {
		r.Header.Set("Authorization", "Bearer "+key)
		// 去掉 /vscode/{key} 前缀，归一化为 /v1/...
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/vscode/"+key)
	}

	if !a.authorized(r) {
		trace.finalStatus = http.StatusUnauthorized
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})
		a.logCompletion(r, trace)
		return
	}

	if r.URL.Path == "/" || r.URL.Path == "/v1" {
		// 该端点无认证：只报数量不回显地址，节点列表属于敏感拓扑信息。
		writeJSON(w, http.StatusOK, map[string]any{
			"status":      "ok",
			"upstream":    a.gateway.cfg.project.upstream,
			"mode":        a.gateway.cfg.proxyMode,
			"slots":       a.gateway.slotCount(),
			"customSlots": a.gateway.customCount(),
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

// extractURLKey 从 /vscode/{key}/... 路径中提取 API Key。
func extractURLKey(path string) (string, bool) {
	if !strings.HasPrefix(path, "/vscode/") {
		return "", false
	}
	rest := strings.TrimPrefix(path, "/vscode/")
	idx := strings.Index(rest, "/")
	if idx <= 0 {
		return "", false
	}
	return rest[:idx], true
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
	// 标准 /v1/ 路径直接通过
	if strings.HasPrefix(raw, "/v1/") {
		return raw, true
	}
	if len(project.prefixes) == 0 {
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
	// 日志关联标签与模型名：从这里往后所有过程/收尾日志可归属到具体会话。
	trace.tag = sessionTag(ids.Session)
	trace.model, _ = payload["model"].(string)
	stream := wantsStream(r.Header, payload)
	// 本地管家拦截：客户端的配额探测类请求零上游消耗直接应答。
	if canned, hit := tryLocalHousekeeping(a.gateway.cfg.localMocks, path, stream, payload); hit {
		trace.finalProxy = "local"
		return &gatewayResponse{
			status: http.StatusOK,
			header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
			body:   canned,
		}, nil, nil
	}
	if !stream {
		deadline = trace.start.Add(a.gateway.cfg.nonStreamTimeout)
	}
	// 单次解析共享：stream 补写 / 模型重定向 / 请求体卫生直接改写同一份
	// payload，只在确有改动时重新序列化一次（原先各阶段各自反序列化，最坏三遍）。
	changed := false
	if stream {
		changed = ensureStream(payload) || changed
	}
	injectCache := strings.HasSuffix(path, "/v1/chat/completions") && a.gateway.cfg.cacheFields
	if a.gateway.cfg.project.modelMode != modelPassthrough {
		modelContext, cancel := context.WithDeadline(r.Context(), deadline)
		changed = a.gateway.rewriteModelPayload(modelContext, payload) || changed
		cancel()
	}
	changed = enhanceRequestBodyPayload(payload, injectCache) || changed
	if changed {
		if out, err := json.Marshal(payload); err == nil {
			body = out
		}
	}
	// 出站指纹整形：统一按原生客户端的字段构造序重排顶层键。卫生改写过的
	// 体是 Go map 的字母序、未改写的保留客户端原序——同一网关两种指纹本身
	// 就是可识别特征。JSON 键序无语义，纯整形零风险（见 bodyorder.go）。
	if payload != nil && a.gateway.cfg.bodyFingerprint {
		if order := pickBodyOrder(path); len(order) > 0 {
			if out, ok := marshalOrderedBody(payload, order); ok {
				body = out
			}
		}
	}
	// Cline 路由：模型名带 cline/ 前缀或属于已知 Cline 模型时，走 Cline 上游。
	if payload != nil {
		if model, _ := payload["model"].(string); isClineModel(model) {
			response, err := a.gateway.handleClineChat(r.Context(), w, payload, path, stream, deadline, trace)
			return response, nil, err
		}
	}
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
	// 流式请求配备 SSE 保活：竞速/吸收迟迟未决时提前提交响应头并周期
	// 发送协议感知心跳，客户端（含 Codex 的 300 秒空闲超时）不会因等不到
	// 数据而误判断线。吸收模式同样需要：吸收可能在网关内静默重试数分钟，
	// 没有心跳 Codex 会先于网关放弃连接。旧版曾因心跳与最终响应体交错
	// 而对吸收模式跳过 guard——那在 Finish 等待心跳协程退出（beba686 起）
	// 之后已不可能发生，恢复挂载并在此注明，防止再被误删。
	var guard *sseGuard
	if stream {
		guard = newSseGuard(w, path)
	}
	var response *gatewayResponse
	if stream && a.gateway.cfg.absorbStreaming {
		// 吸收模式：保活心跳由 sseGuard 负责，这里在网关内重试直到完整。
		response, err = a.gateway.dispatchAbsorb(r.Context(), request, trace)
	} else {
		response, err = a.gateway.dispatch(r.Context(), request, trace)
	}
	if guard != nil {
		guard.Finish()
	}
	return response, guard, err
}

var (
	// sseCommitDelay 是竞速决出赢家的宽限期：正常请求（几乎都小于该值）
	// 走原有路径，保留完整的状态码/错误重试语义；超过该值仍未决出赢家
	// 才提前提交 SSE 头转入心跳模式。var 而非 const：测试缩短宽限期用。
	sseCommitDelay = 5 * time.Second
	// sseBeatInterval 是心跳的发送间隔。
	sseBeatInterval = 3 * time.Second
)

// sseGuard 让长时间竞速/吸收的流式请求在客户端看来始终“活着”。
// 所有写入都在互斥锁内且先检查 done，因此 Finish 返回后保证不会再有
// 任何写入，调用方可以安全接管 ResponseWriter。
type sseGuard struct {
	w         http.ResponseWriter
	path      string // 客户端请求路径：决定心跳按哪种协议变形
	mu        sync.Mutex
	done      bool
	committed bool
	quit      chan struct{} // Finish 信号：未提交时心跳协程立即退出
	quitOnce  sync.Once
	stopped   chan struct{} // 心跳协程退出时关闭，Finish 等待
}

func newSseGuard(w http.ResponseWriter, path string) *sseGuard {
	g := &sseGuard{w: w, path: path, quit: make(chan struct{}), stopped: make(chan struct{})}
	go g.run()
	return g
}

// run 在宽限期内未被打断时提交 200 + SSE 头，随后按间隔发送心跳，
// 直到 Finish。SSE 规范里冒号开头的注释行会被所有客户端忽略。
// 旧实现用 time.AfterFunc(sseCommitDelay) 定时触发心跳协程：竞速 5 秒内
// 出赢家时 Finish 也要等满定时器才返回，客户端首字节被硬顶到 5 秒标记。
// 改成常驻协程 + select 后，未提交即可立即退出。
func (g *sseGuard) run() {
	defer close(g.stopped)
	timer := time.NewTimer(sseCommitDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-g.quit:
		return
	}
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
	g.mu.Unlock()
	ticker := time.NewTicker(sseBeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.mu.Lock()
			if g.done {
				g.mu.Unlock()
				return
			}
			g.beatLocked()
			g.mu.Unlock()
		case <-g.quit:
			return
		}
	}
}

// heartbeatPayload 按客户端协议构造心跳载荷。
// 注释行本身对所有客户端合法，但 Codex 的 SSE 解析器（eventsource_stream）
// 不把注释当事件，其空闲超时（stream_idle_timeout_ms，默认 300 秒）不因
// 注释复位——吸收模式/长竞速的静默会被它掐断。因此注释行之外再按路径
// 注入一条真实事件：responses 发 response.in_progress（OmniRoute 生产验证
// 的 Codex 兼容载荷）、messages 发 Anthropic ping、chat 发空 delta chunk。
// 注入的 chat chunk 刻意不带 finish_reason 字段：null 值会被本网关的终止
// 标记扫描误判为流完整。
func heartbeatPayload(path string) string {
	comment := ": keep-alive\n\n"
	switch {
	case strings.HasSuffix(path, "/v1/responses"):
		return comment + `data: {"type":"response.in_progress"}` + "\n\n"
	case strings.HasSuffix(path, "/v1/messages"):
		return comment + "event: ping\ndata: {\"type\":\"ping\"}\n\n"
	default:
		chunk, err := json.Marshal(map[string]any{
			"id":      "chatcmpl-keepalive",
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   "keepalive",
			"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}}},
		})
		if err != nil {
			return comment
		}
		return comment + "data: " + string(chunk) + "\n\n"
	}
}

// midStreamKeepalive 流内保活事件：与守卫心跳同协议形态，但 chat 形态
// 刻意不带 id/model——流中途出现不同 id 会被部分客户端当作新消息；
// 不带内容——注入文本会累积进最终回答（内容污染）。
func midStreamKeepalive(path string) string {
	switch {
	case strings.HasSuffix(path, "/v1/responses"):
		return `data: {"type":"response.in_progress"}` + "\n\n"
	case strings.HasSuffix(path, "/v1/messages"):
		return "event: ping\ndata: {\"type\":\"ping\"}\n\n"
	default:
		return `data: {"choices":[{"index":0,"delta":{}}]}` + "\n\n"
	}
}

func (g *sseGuard) beatLocked() {
	_, _ = io.WriteString(g.w, heartbeatPayload(g.path))
	if flusher, ok := g.w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Finish 停止心跳并等待心跳协程退出；返回后不会再有任何写入，
// 调用方可以安全接管 ResponseWriter 写入真实 SSE 数据。
// 未提交过心跳（竞速在宽限期内出赢家）时立即返回，不拖慢首字节。
func (g *sseGuard) Finish() {
	g.mu.Lock()
	g.done = true
	g.mu.Unlock()
	g.quitOnce.Do(func() { close(g.quit) })
	<-g.stopped
}

func (g *sseGuard) Committed() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.committed
}

// streamOptions 是流式转发的全部可调参数：writeGatewayResponse 与
// writeCommittedStream 共用，避免参数列表无限膨胀。
type streamOptions struct {
	idle          time.Duration
	stallWindow   time.Duration // 慢流看门狗窗口；0 或 stallMinBytes=0 关闭看门狗
	stallMinBytes int
	observe       func(truncated bool)
	hyg           *sseSanitizer
	resumer       *streamResumer // 中流续写；nil 表示不支持续写的形态
	usage         *usageSnapshot // 每请求 token 用量快照，streamResponse 回填
	path          string         // 客户端协议路径（/v1/responses 等）：决定保活事件形态
	keepalive     time.Duration  // 流内保活间隔：上游静默多久注入一次（0=关）
}

// writeCommittedStream 处理已提前提交 SSE 头的响应：赢家的流直接透传；
// 其余情形（错误或非流式兜底）转为 SSE 错误事件，让客户端拿到明确原因。
func writeCommittedStream(w http.ResponseWriter, r *http.Request, response *gatewayResponse, opts streamOptions) {
	if response.live == nil && response.status >= 200 && response.status < 300 && len(response.body) > 0 {
		// 吸收模式的成品：已验证完整的整段 SSE 原文一次性交付。
		_, _ = w.Write(response.body)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		if opts.observe != nil {
			opts.observe(false)
		}
		return
	}
	if response.live != nil {
		streamResponse(w, r.Context(), response.live, opts)
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

// ensureStream 就地补 stream:true（客户端用 Accept 头声明流式但 body 未带时），
// 返回是否有改动。
func ensureStream(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	if stream, _ := payload["stream"].(bool); stream {
		return false
	}
	payload["stream"] = true
	return true
}

func (a *app) finish(w http.ResponseWriter, r *http.Request, trace *requestTrace, response *gatewayResponse, err error, guard *sseGuard) {
	// 确保 sseGuard 心跳已停止，防止与后续写入交叉。
	if guard != nil {
		guard.Finish()
	}
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		switch {
		case errors.Is(err, errRequestTimeout), errors.Is(err, errAttemptTimeout), errors.Is(err, context.DeadlineExceeded):
			response = jsonGatewayResponse(http.StatusGatewayTimeout, "请求超时")
		case errors.Is(err, errNoProxy):
			response = jsonGatewayResponse(http.StatusBadGateway, "没有可用代理")
		case errors.Is(err, errAllExitsFailed):
			response = jsonGatewayResponse(http.StatusBadGateway, "上游或链路暂时故障（节点池仍有在册节点），请重试")
		case errors.Is(err, errStreamTruncated):
			response = jsonGatewayResponse(http.StatusBadGateway, "上游流中断")
		default:
			response = jsonGatewayResponse(http.StatusBadGateway, "上游请求失败")
		}
		log.Printf("[请求错]%s %s %s: %v", trace.tagString(), r.Method, r.URL.Path, err)
	}
	if response == nil {
		response = jsonGatewayResponse(http.StatusBadGateway, "上游请求失败")
	}
	trace.finalStatus = response.status
	if trace.finalProxy == "" && trace.attempts == 0 {
		trace.finalProxy = "local"
	}
	// 界面健康色即时更新（原在 logCompletion 内）；收尾日志行移到
	// 响应交付之后——耗时含流式全程，流式 token 也能进收尾行。
	a.gateway.lastStatus.Store(int32(trace.finalStatus))
	// 流式响应结束后由 streamResponse 回报是否见到终止标记：
	// 没见到即视为中途夭折，出口降权、镜像记账，下次绕开。
	observe := func(truncated bool) {
		if truncated {
			log.Printf("[流截断]%s %s | 出口:%s | 上游:%s | 流未带终止标记即结束", trace.tagString(), r.URL.Path, trace.finalProxy, trace.upstream)
			a.gateway.noteStreamTruncation(trace.finalProxy, trace.upstream)
			return
		}
		if trackableExit(trace.finalProxy) {
			a.gateway.exits.observeSuccess(trace.finalProxy)
		}
	}
	var hyg *sseSanitizer
	if a.gateway.cfg.sseHygiene && response != nil && response.live != nil {
		hyg = newSSESanitizer()
	}
	var reqUsage usageSnapshot
	opts := streamOptions{
		idle:          a.gateway.cfg.streamIdle,
		stallWindow:   a.gateway.cfg.stallWindow,
		stallMinBytes: a.gateway.cfg.stallMinBytes,
		observe:       observe,
		hyg:           hyg,
		usage:         &reqUsage,
	}
	// 中流续写：仅 chat 形态且响应来自上游（带 origin）时配备；
	// eligible/resume 内部再做开关、工具调用与预算把关。
	if response != nil && response.live != nil && response.origin != nil {
		opts.resumer = newStreamResumer(a.gateway, response.origin, trace)
	}
	// 回显取证：胜者响应头脱敏落日志，判断上游是否下发 turn-scoped 状态头。
	if a.gateway.cfg.echoDebug && response != nil {
		logEchoHeaders(r.URL.Path, response.status, response.header)
	}
	// 用量统计：缓冲响应（吸收成品/非流式 JSON）在此统一记账；
	// 流式路径由 streamResponse 内的采集器负责，两边不会重复计数。
	if a.gateway.usage != nil && response != nil && response.live == nil && len(response.body) > 0 {
		if m, us, ok := ExtractLastUsage(response.body); ok {
			a.gateway.usage.Observe(m, us.PromptTokens, us.CompletionTokens, us.CachedTokens)
			trace.usage = &usageSnapshot{model: m, prompt: us.PromptTokens, completion: us.CompletionTokens, cached: us.CachedTokens}
		}
	}
	if guard != nil && guard.Committed() {
		// 响应头已提前提交，状态码无法再改变：赢家流透传，其余转为 SSE 错误事件。
		writeCommittedStream(w, r, response, opts)
		a.logCompletion(r, trace)
		return
	}
	writeGatewayResponse(w, r, response, opts)
	a.logCompletion(r, trace)
}

func (a *app) logCompletion(r *http.Request, trace *requestTrace) {
	status := trace.finalStatus
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
	line := fmt.Sprintf("[%s]%s %s %s | 状态:%d | 重试:%d | 出口:%d | 耗时:%dms | 代理:%s",
		result, trace.tagString(), r.Method, r.URL.Path, status, retries,
		len(trace.proxies), time.Since(trace.start).Milliseconds(), proxy)
	if trace.model != "" {
		line += fmt.Sprintf(" | 模型:%s", trace.model)
	}
	if trace.firstByte > 0 {
		line += fmt.Sprintf(" | 首字节:%dms", trace.firstByte.Milliseconds())
	}
	if u := trace.usage; u != nil && (u.prompt > 0 || u.completion > 0) {
		line += fmt.Sprintf(" | 入:%d 出:%d", u.prompt, u.completion)
		if u.cached > 0 {
			line += fmt.Sprintf(" 缓存:%d", u.cached)
		}
	}
	log.Print(line)
}

func writeGatewayResponse(w http.ResponseWriter, r *http.Request, response *gatewayResponse, opts streamOptions) {
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
	streamResponse(w, r.Context(), response.live, opts)
}

// streamTerminals 是判定流完整结束的标记：见到任一个即认为上游正常收尾。
// 涵盖三种路由：OpenAI chat（[DONE] / 非空 finish_reason）、Anthropic
// （message_stop）、Codex responses（response.completed 等）。
var streamTerminals = [][]byte{
	[]byte("[DONE]"),
	[]byte(`"finish_reason":"`),
	[]byte(`"finish_reason":null`),
	[]byte("message_stop"),
	[]byte("response.completed"),
	[]byte("response.failed"),
	[]byte("response.incomplete"),
}

// streamTextCollector 从待转发的 chat 形态字节里流式提取
// choices[0].delta.content 全文与 tool_calls 出现标记，供中流续写使用。
type streamTextCollector struct {
	buf      []byte
	text     strings.Builder
	sawTools bool
}

func (c *streamTextCollector) feed(chunk []byte) {
	c.buf = append(c.buf, chunk...)
	for {
		idx := bytes.IndexByte(c.buf, '\n')
		if idx < 0 {
			if len(c.buf) > 1<<20 { // 异常无换行流防御
				c.buf = nil
			}
			break
		}
		line := string(c.buf[:idx])
		c.buf = c.buf[idx+1:]
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "data:") {
			continue
		}
		payload := strings.TrimSpace(trimmed[len("data:"):])
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var probe struct {
			Choices []struct {
				Delta struct {
					Content   string          `json:"content"`
					ToolCalls json.RawMessage `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(payload), &probe) != nil {
			continue
		}
		if len(probe.Choices) == 0 {
			continue
		}
		c.text.WriteString(probe.Choices[0].Delta.Content)
		if len(probe.Choices[0].Delta.ToolCalls) > 0 && string(probe.Choices[0].Delta.ToolCalls) != "null" {
			c.sawTools = true
		}
	}
}

func streamResponse(w http.ResponseWriter, ctx context.Context, live *liveResponse, opts streamOptions) {
	defer live.Close()
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	buffer := make([]byte, 32<<10)
	// 终止标记扫描：窗口保留上次尾部，防止标记被拆在两次读取里。
	// 扫描对象是清洗后待转发的字节——客户端看到什么，就以什么为准记账。
	terminalSeen := false
	var window []byte
	// 用量采集：流式 chunk 里出现 "usage" 的 data 行按行解析，
	// 最后一个非空 usage 为准（与 ExtractLastUsage 同口径）。
	var usageScan []byte
	collectUsage := func(chunk []byte) {
		if usageObserver == nil {
			return
		}
		usageScan = append(usageScan, chunk...)
		for {
			idx := bytes.IndexByte(usageScan, '\n')
			if idx < 0 {
				if len(usageScan) > 1<<20 { // 异常无换行流防御
					usageScan = nil
				}
				break
			}
			line := string(usageScan[:idx])
			usageScan = usageScan[idx+1:]
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "data:") || !strings.Contains(line, `"usage"`) {
				continue
			}
			if m, us, ok := ExtractLastUsage([]byte(line)); ok {
				usageObserver(m, us.PromptTokens, us.CompletionTokens, us.CachedTokens)
				if opts.usage != nil {
					*opts.usage = usageSnapshot{model: m, prompt: us.PromptTokens, completion: us.CompletionTokens, cached: us.CachedTokens}
				}
			}
		}
	}
	// 续写素材采集：只在有续写器时才做逐行 JSON 解析，热路径零开销。
	var textCol *streamTextCollector
	if opts.resumer != nil {
		textCol = &streamTextCollector{}
	}
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
	writeOut := func(chunk []byte) bool {
		if len(chunk) == 0 {
			return true
		}
		scan(chunk)
		collectUsage(chunk)
		if textCol != nil {
			textCol.feed(chunk)
		}
		if _, err := w.Write(chunk); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	// 流结束时冲洗残留半行并汇报卫生统计；必须在 report() 之前，
	// 让终止标记判定看到完整的客户端可见字节。
	flushTail := func() {
		if opts.hyg == nil {
			return
		}
		if tail := opts.hyg.flush(); len(tail) > 0 {
			writeOut(tail)
		}
		if opts.hyg.dropped > 0 {
			log.Printf("[SSE卫生] 本流丢弃 %d 行无效数据", opts.hyg.dropped)
		}
	}
	// report 只在上游侧结束时调用；客户端主动断开（写失败/请求取消）
	// 与上游质量无关，不记账。
	report := func() {
		if opts.observe != nil {
			opts.observe(!terminalSeen)
		}
	}
	// 上游侧流结束（空闲超时/慢流看门狗/EOF）的统一收尾：先按未见终止
	// 标记记账（出口确实断过，该降权降权），再尝试中流续写——续写成功
	// 时客户端拿到带终止标记的完整回复，失败则维持静默干净关闭，
	// 交给客户端既有的自动重试语义。
	finish := func() {
		flushTail()
		report()
		if terminalSeen || ctx.Err() != nil || textCol == nil {
			return
		}
		sent := textCol.text.String()
		if !opts.resumer.eligible(sent) {
			return
		}
		if opts.resumer.resume(w, ctx, sent) {
			terminalSeen = true
		}
	}
	// 实测结论：流中断时补发显式 SSE 错误事件会让 agent 工具把它当作
	// 致命 API 错误直接放弃任务（PI_AI_ERROR）；而干净的流结束（无
	// finish_reason）反而触发它的自动重试。所以这里保持静默干净关闭，
	// 不注入任何错误载荷。
	// 复用单个计时器实现空闲超时：到点取消上游请求，阻塞中的 Read 随之返回。
	timer := time.AfterFunc(opts.idle, live.cancel)
	defer timer.Stop()
	// 慢流看门狗状态：只抓"还在滴但基本卡死"的流（窗口内字节多于 0 但
	// 低于阈值）。完全静默的流交给空闲超时——思考模型合法的长停顿跨窗
	// 时清账重启，不会误杀。判定需要连续两个违规窗口：单窗口慢（首句
	// 流得慢/上游短暂抖动）只警告不掐，避免误杀正常慢流。
	var windowStart time.Time
	var windowBytes int
	var lastRead time.Time
	stallViolations := 0
	stallKill := func(n int) (bool, int) {
		if opts.stallWindow <= 0 || opts.stallMinBytes <= 0 || n <= 0 {
			return false, 0
		}
		now := time.Now()
		defer func() { lastRead = now }()
		if !lastRead.IsZero() && now.Sub(lastRead) > opts.stallWindow {
			windowStart = now
			windowBytes = 0
		}
		if windowStart.IsZero() {
			windowStart = now
		}
		windowBytes += n
		if now.Sub(windowStart) < opts.stallWindow {
			return false, windowBytes
		}
		dribble := windowBytes < opts.stallMinBytes
		observed := windowBytes
		windowStart = now
		windowBytes = 0
		if !dribble {
			stallViolations = 0
			return false, observed
		}
		stallViolations++
		if stallViolations < 2 {
			log.Printf("[流] 慢流（连续第 %d 个低流量窗口，仅 %d 字节）——继续观察", stallViolations, observed)
			return false, observed
		}
		stallViolations = 0
		return true, observed
	}
	for {
		timer.Reset(opts.idle)
		n, err := live.response.Body.Read(buffer)
		stopped := timer.Stop()
		if n > 0 {
			if dribble, observed := stallKill(n); dribble {
				log.Printf("[流] 慢流看门狗：连续 %d 个低流量窗口（%s 内仅 %d 字节），判定僵尸流关闭", stallViolations+1, opts.stallWindow, observed)
				live.cancel()
				finish()
				return
			}
			out := buffer[:n]
			if opts.hyg != nil {
				out = opts.hyg.feed(out)
			}
			if !writeOut(out) {
				return
			}
		}
		if !stopped {
			log.Printf("[流] %s 内无数据，已关闭连接", opts.idle)
			live.cancel()
			finish()
			return
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				log.Printf("[流] upstream stream ended: %v", err)
			}
			if ctx.Err() == nil {
				finish()
			} else {
				flushTail()
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
