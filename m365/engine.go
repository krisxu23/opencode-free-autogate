// Package m365 把 M365 Copilot 的反代能力内置进网关：单进程运行，无需再
// 单独启动 M365-Copilot2API 进程并把 127.0.0.1:4141 当外部供应商接入。
//
// 组成：
//   - auth_*：Microsoft PKCE 授权 / 刷新令牌 / 加密落盘的账号存储
//   - chat_*：ChatHub WebSocket 客户端（从 M365-Copilot2API 移植）
//   - outbound*：出网代理支持
//   - engine.go：对外的引擎门面，屏蔽以上细节
package m365

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// ModelMapping 描述一个对外暴露的 M365 模型：客户端看到的模型名 → 上游音色。
type ModelMapping struct {
	PublicModel           string `json:"publicModel"`
	UpstreamTone          string `json:"upstreamTone"`
	DisplayName           string `json:"displayName"`
	DefaultReasoningLevel string `json:"defaultReasoningLevel"`
}

// DefaultModelMappings 与 M365-Copilot2API 的默认目录保持一致，确保两边模型名通用。
var DefaultModelMappings = []ModelMapping{
	{PublicModel: "gpt-5.6-sol", UpstreamTone: "Gpt_5_6_Reasoning", DisplayName: "GPT-5.6-Sol", DefaultReasoningLevel: "low"},
	{PublicModel: "gpt-5.6-terra", UpstreamTone: "Gpt_5_6_Reasoning", DisplayName: "GPT-5.6-Terra", DefaultReasoningLevel: "medium"},
	{PublicModel: "gpt-5.6-luna", UpstreamTone: "Gpt_5_6_Reasoning", DisplayName: "GPT-5.6-Luna", DefaultReasoningLevel: "medium"},
}

const (
	defaultScenario    = "OfficeWebIncludedCopilot"
	defaultLicenseType = "Starter"
	// pendingTTL 未完成的授权挑战存活时间，超时后状态作废防止无限堆积。
	pendingTTL = 15 * time.Minute
	// sessionTTLM365 会话（会话/对话 ID 绑定）保活时长，超时回收。
	sessionTTLM365 = 2 * time.Hour
)

var (
	// ErrNoAccount 没有任何可用 M365 账号（未授权或全部过期）。
	ErrNoAccount = errors.New("m365: 没有可用账号，请先在 M365 账号页完成授权")
	// ErrUnknownModel 模型名不在 M365 目录内。
	ErrUnknownModel = errors.New("m365: 未知模型")
)

// AuthChallenge 是一次待完成的 PKCE 授权：打开 URL 让用户登录。
type AuthChallenge struct {
	State       string `json:"state"`
	AuthURL     string `json:"authUrl"`
	RedirectURI string `json:"redirectUri"`
}

type pendingAuth struct {
	verifier  string
	redirect  string
	createdAt time.Time
}

type conversation struct {
	accountID      string
	conversationID string
	sessionID      string
	updatedAt      time.Time
}

// Engine 是 M365 内置供应商的运行时：账号存储 + ChatHub 客户端 + 会话绑定。
// 所有方法并发安全。
type Engine struct {
	mu       sync.Mutex
	store    *Store
	client   *Client
	pending  map[string]pendingAuth
	convs    map[string]*conversation
	path     string
	mappings []ModelMapping
}

// Open 打开（必要时创建）M365 引擎；path 为账号存储文件路径。
func Open(path string) (*Engine, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("m365: 账号存储路径为空")
	}
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return nil, fmt.Errorf("m365: 创建数据目录失败: %w", err)
	}
	store, err := OpenStore(path)
	if err != nil {
		return nil, err
	}
	return &Engine{
		store:    store,
		client:   NewClient(),
		pending:  map[string]pendingAuth{},
		convs:    map[string]*conversation{},
		path:     path,
		mappings: append([]ModelMapping(nil), DefaultModelMappings...),
	}, nil
}

func filepathDir(path string) string {
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		return path[:i]
	}
	return "."
}

// Path 返回账号存储文件位置，供界面展示。
func (e *Engine) Path() string { return e.path }

// MasterKeyOK 报告 M365_MASTER_KEY 是否已设置：未设置时刷新令牌明文落盘。
// 这是安全提示而非硬性阻断——旧版无密钥的库仍要能读。
func MasterKeyOK() bool { return strings.TrimSpace(os.Getenv("M365_MASTER_KEY")) != "" }

// Models 返回对外暴露的模型名（已排序）。
func (e *Engine) Models() []string {
	e.mu.Lock()
	list := make([]string, 0, len(e.mappings))
	for _, m := range e.mappings {
		list = append(list, m.PublicModel)
	}
	e.mu.Unlock()
	sort.Strings(list)
	return list
}

// Tone 把模型名解析成上游音色；未收录的模型返回空串由调用方决定行为。
func (e *Engine) Tone(model string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, m := range e.mappings {
		if m.PublicModel == model {
			return m.UpstreamTone, true
		}
	}
	return "", false
}

// Accounts 返回全部账号快照。
func (e *Engine) Accounts() []AccountToken {
	list := e.store.List()
	out := append([]AccountToken(nil), list...)
	sort.Slice(out, func(i, j int) bool { return out[i].Email < out[j].Email })
	return out
}

// DeleteAccount 删除账号。
func (e *Engine) DeleteAccount(id string) error { return e.store.Delete(id) }

// RefreshAllExpired 主动刷新全部过期账号，返回逐账号结果。
func (e *Engine) RefreshAllExpired() []TokenRefreshResult { return e.store.RefreshAllExpired() }

// StartAuth 生成一次 PKCE 授权挑战：调用方把 AuthURL 用浏览器打开，
// 用户登录后地址栏会带 code/state，交给 CompleteAuth 完成。
func (e *Engine) StartAuth() (AuthChallenge, error) {
	v, err := Verifier()
	if err != nil {
		return AuthChallenge{}, err
	}
	state, err := Verifier()
	if err != nil {
		return AuthChallenge{}, err
	}
	redirect := RedirectURI()
	ch := AuthChallenge{
		State:       state,
		AuthURL:     AuthorizationURL(AuthorizeEndpoint(), ClientID(), redirect, state, Challenge(v), Scope()),
		RedirectURI: redirect,
	}
	e.mu.Lock()
	e.prunePendingLocked()
	e.pending[state] = pendingAuth{verifier: v, redirect: redirect, createdAt: time.Now()}
	e.mu.Unlock()
	return ch, nil
}

func (e *Engine) prunePendingLocked() {
	now := time.Now()
	for k, p := range e.pending {
		if now.Sub(p.createdAt) > pendingTTL {
			delete(e.pending, k)
		}
	}
}

// CompleteAuth 用回调地址（完整 URL 或裸 code）完成授权并存盘。
// 回调页地址形如 http://localhost:4141/api/auth/callback?code=...&state=...
func (e *Engine) CompleteAuth(state, callback string) (AccountToken, error) {
	e.mu.Lock()
	p, ok := e.pending[state]
	e.mu.Unlock()
	if !ok {
		return AccountToken{}, errors.New("m365: 授权状态已失效，请重新开始授权")
	}
	code, cbState := extractCodeState(callback)
	if code == "" {
		return AccountToken{}, errors.New("m365: 回调地址里没有 code 参数")
	}
	if cbState != "" && cbState != state {
		return AccountToken{}, errors.New("m365: 回调 state 与本次授权不匹配")
	}
	tok, err := ExchangeCode(code, p.verifier, p.redirect)
	if err != nil {
		return AccountToken{}, fmt.Errorf("m365: 交换令牌失败: %w", err)
	}
	acc, err := e.store.Upsert(tok)
	if err != nil {
		return AccountToken{}, err
	}
	e.mu.Lock()
	delete(e.pending, state)
	e.mu.Unlock()
	return acc, nil
}

// ProvisionAccount 用账号密码自动授权添加账号：内部驱动微软登录页完成
// 「输账号 → 输密码 → 拿回调 code」全流程，再用 PKCE 换 token（ROPC
// grant_type=password 对公共客户端不可用，详见 auth_autologin.go）。
// MFA/条件访问/风控账号会在中途返回错误，届时改走 PKCE 弹窗授权。
// 密码只用于本次登录，不落盘不进日志。
func (e *Engine) ProvisionAccount(email, password string) (AccountToken, error) {
	email = strings.TrimSpace(email)
	if email == "" || !strings.Contains(email, "@") {
		return AccountToken{}, errors.New("m365: 邮箱格式不正确")
	}
	if strings.TrimSpace(password) == "" {
		return AccountToken{}, errors.New("m365: 密码不能为空")
	}
	// 与 StartAuth 同源的 PKCE 材料：code 换 token 时必须配对。
	verifier, err := Verifier()
	if err != nil {
		return AccountToken{}, err
	}
	state, err := Verifier()
	if err != nil {
		return AccountToken{}, err
	}
	redirect := RedirectURI()
	res, err := AutoLogin(email, password, state, verifier, redirect, Challenge(verifier))
	if err != nil {
		return AccountToken{}, fmt.Errorf("m365: 密码授权失败: %w", err)
	}
	tok, err := ExchangeCode(res.Code, verifier, redirect)
	if err != nil {
		return AccountToken{}, fmt.Errorf("m365: 密码授权成功但换令牌失败: %w", err)
	}
	return e.store.Upsert(tok)
}

// extractCodeState 同时接受完整回调 URL 与直接粘贴的 code。
func extractCodeState(raw string) (code, state string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	if !strings.Contains(raw, "://") && !strings.Contains(raw, "?") && !strings.Contains(raw, "=") {
		return raw, ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", ""
	}
	q := parsed.Query()
	if c := q.Get("code"); c != "" {
		return c, q.Get("state")
	}
	// 少数情况下回调把参数放在 fragment 里。
	if frag, err := url.ParseQuery(strings.TrimPrefix(parsed.Fragment, "?")); err == nil {
		if c := frag.Get("code"); c != "" {
			return c, frag.Get("state")
		}
	}
	return "", q.Get("state")
}

// ChatRequest 是一次 OpenAI 风格对话的归一化入参。
type ChatRequest struct {
	Model      string
	Messages   []ChatMessage
	SessionKey string // 为空则不复用会话上下文
}

// ChatMessage 归一化后的单条消息。
type ChatMessage struct {
	Role    string
	Content string
}

// ChatResult 是一次对话的结果。
type ChatResult struct {
	Text         string
	Reasoning    string
	Conversation string
	Session      string
	AccountID    string
}

// pickAccount 轮询选一个有效账号；顺带把过期但可刷新的账号刷新好。
func (e *Engine) pickAccount() (AccountToken, error) {
	n := len(e.store.List())
	if n == 0 {
		return AccountToken{}, ErrNoAccount
	}
	var lastErr error
	for i := 0; i < n; i++ {
		acc, ok := e.store.Next()
		if !ok {
			return AccountToken{}, ErrNoAccount
		}
		valid, err := e.store.EnsureValid(acc.ID)
		if err != nil {
			lastErr = err
			continue
		}
		if valid.Status == "expired" {
			lastErr = fmt.Errorf("m365: 账号 %s 已过期", valid.Email)
			continue
		}
		// OID/TID 缺失时用 access token 里的声明兜底（老库常见）。
		if valid.OID == "" || valid.TID == "" {
			if o, t := extractOIDTID(valid.AccessToken); o != "" {
				valid.OID, valid.TID = o, t
			}
		}
		if valid.OID == "" || valid.TID == "" || valid.AccessToken == "" {
			lastErr = fmt.Errorf("m365: 账号 %s 缺少 oid/tid", valid.Email)
			continue
		}
		return valid, nil
	}
	if lastErr != nil {
		return AccountToken{}, lastErr
	}
	return AccountToken{}, ErrNoAccount
}

// Chat 执行一次对话；onDelta 收到增量文本，onReasoning 收到推理增量（可为 nil）。
func (e *Engine) Chat(ctx context.Context, req ChatRequest, onDelta, onReasoning func(string) error) (ChatResult, error) {
	tone, ok := e.Tone(req.Model)
	if !ok {
		// 未收录的模型交给上游默认音色，保持与 M365 目录更新同步时的可用性。
		tone = defaultTone
	}
	acc, err := e.pickAccount()
	if err != nil {
		return ChatResult{}, err
	}
	text, previous := flattenMessages(req.Messages)

	e.mu.Lock()
	var conv *conversation
	if req.SessionKey != "" {
		if c, ok := e.convs[req.SessionKey]; ok && time.Since(c.updatedAt) < sessionTTLM365 {
			conv = c
		}
	}
	e.mu.Unlock()

	hubReq := Request{
		Text:             text,
		Tone:             tone,
		PreviousMessages: previous,
		LicenseType:      defaultLicenseType,
		Scenario:         defaultScenario,
	}
	if conv != nil {
		hubReq.ConversationID = conv.conversationID
		hubReq.SessionID = conv.sessionID
	}

	account := Account{AccessToken: acc.AccessToken, OID: acc.OID, TID: acc.TID}
	res, err := e.client.ChatWithReasoning(ctx, account, hubReq, onDelta, onReasoning)
	if err != nil {
		return ChatResult{}, err
	}
	if req.SessionKey != "" {
		e.mu.Lock()
		e.pruneConvsLocked()
		e.convs[req.SessionKey] = &conversation{
			accountID:      acc.ID,
			conversationID: res.ConversationID,
			sessionID:      res.SessionID,
			updatedAt:      time.Now(),
		}
		e.mu.Unlock()
	}
	return ChatResult{
		Text:         res.Text,
		Reasoning:    res.Reasoning,
		Conversation: res.ConversationID,
		Session:      res.SessionID,
		AccountID:    acc.ID,
	}, nil
}

func (e *Engine) pruneConvsLocked() {
	now := time.Now()
	for k, c := range e.convs {
		if now.Sub(c.updatedAt) > sessionTTLM365 {
			delete(e.convs, k)
		}
	}
}

// extractOIDTID 从 access token 的 JWT 负载里取 oid/tid 兜底：
// 老版本账号库可能没把这两个字段落盘，而 ChatHub 拨号强依赖它们。
func extractOIDTID(accessToken string) (oid, tid string) {
	parts := strings.Split(accessToken, ".")
	if len(parts) < 2 {
		return "", ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ""
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", ""
	}
	if v, ok := m["oid"].(string); ok {
		oid = v
	}
	if v, ok := m["tid"].(string); ok {
		tid = v
	}
	return oid, tid
}

// flattenMessages 把 OpenAI 消息数组压成 M365 的“最新一条提问 + 历史上下文”。
// ChatHub 协议以单轮文本为主，历史通过 PreviousMessages 携带。
func flattenMessages(messages []ChatMessage) (text string, previous []ContextMessage) {
	var system []string
	var history []ChatMessage
	for _, m := range messages {
		switch strings.ToLower(strings.TrimSpace(m.Role)) {
		case "system":
			if strings.TrimSpace(m.Content) != "" {
				system = append(system, m.Content)
			}
		default:
			history = append(history, m)
		}
	}
	if len(history) == 0 {
		return strings.Join(system, "\n"), nil
	}
	last := history[len(history)-1]
	text = last.Content
	if len(system) > 0 {
		text = strings.Join(system, "\n") + "\n\n" + text
	}
	for _, m := range history[:len(history)-1] {
		previous = append(previous, ContextMessage{
			Author:      m.Role,
			Description: m.Content,
		})
	}
	return text, previous
}
