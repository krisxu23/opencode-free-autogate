package m365

// 自动账号密码授权：以纯 HTTP 驱动微软登录页（login.microsoftonline.com 的
// TDS 表单流），替用户完成「输入账号 → 输入密码 → 保持在登录页拿回调 code」
// 的全过程，拿到 code 后走既有 PKCE ExchangeCode 换 token。
//
// 为什么不用 ROPC（grant_type=password）：默认浏览器客户端（Office web Copilot
// first-party app）是公共客户端，微软对它一律返回 AADSTS7000218（要求
// client_secret，公共客户端没有）；换 Thunderbird 等 client_id 则是
// AADSTS65001 consent_required。此路不通，只能模拟浏览器表单流。
//
// 流程（每步都从响应页的 $Config JSON 取参数，token 逐步轮换）：
//  1. GET authorize 端点（带 PKCE challenge）→ 首屏页：canary、urlPost
//  2. POST 账号名 + canary + 完整 OAuth 上下文 → 密码页：sFT(flowToken)、sCtx、canary
//  3. POST 密码 + flowToken/ctx/canary/hpgrequestid → 响应页 urlPost 带 sso_reload
//  4. 用响应页 oPostParams（轮换后的 token）重发一次 → 302 Location 含 code=
//  5. code 走 ExchangeCode（PKCE code_verifier）换 access/refresh token
//
// 密码只在内存中流转，不写日志、不落盘。MFA/条件访问账号会在第 3/4 步
// 返回中断视图（无 code 重定向），按错误返回提示用户走 PKCE 弹窗授权。

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// loginPageConfig 是微软登录页 $Config JSON 的最小字段集。
// stsErrorCode 实测会以字符串/数字两种形态出现，统一收成字符串再判空。
type loginPageConfig struct {
	Canary     string            `json:"canary"`
	SFT        string            `json:"sFT"`
	SCtx       string            `json:"sCtx"`
	URLPost    string            `json:"urlPost"`
	SessionID  string            `json:"sessionId"`
	ErrMessage string            `json:"strServiceExceptionMessage"`
	PostParams map[string]string `json:"oPostParams"`
	StsError   json.Number       `json:"stsErrorCode"`
	ErrorCode  json.Number       `json:"iErrorCode"`
}

var loginConfigRe = regexp.MustCompile(`\$Config=(\{.*?\});`)

// parseLoginConfig 从登录页 HTML 提取 $Config JSON；解析失败返回错误。
func parseLoginConfig(body []byte) (loginPageConfig, error) {
	m := loginConfigRe.FindSubmatch(body)
	if m == nil {
		return loginPageConfig{}, errors.New("m365: 登录页无 $Config（页面结构已变化或被风控拦截）")
	}
	var cfg loginPageConfig
	if err := json.Unmarshal(m[1], &cfg); err != nil {
		return loginPageConfig{}, fmt.Errorf("m365: 解析登录页配置失败: %w", err)
	}
	return cfg, nil
}

// absLoginURL 把 urlPost（可能是相对路径）补成绝对地址。
func absLoginURL(u string) string {
	if u == "" || strings.HasPrefix(u, "http") {
		return u
	}
	return "https://login.microsoftonline.com" + u
}

// autoLoginHTTPClient 独立的 http.Client：不跟随重定向（302 的 Location
// 要自己检查），超时收紧（登录是快交互，卡住不如快速报错）。
func autoLoginHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 40 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

const autoLoginUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

// AutoLoginResult 是一次自动登录的产物：授权码 + 它配套的 PKCE 材料。
type AutoLoginResult struct {
	Code     string // 回调重定向里的授权码
	State    string // 与发起 authorize 时的 state 一致
	Verifier string // PKCE code_verifier，换 token 用
	Redirect string // redirect_uri，换 token 用
}

// AutoLogin 用账号密码走微软登录表单流拿授权码。参数由 StartAuth 同源的
// PKCE 材料生成（保证 code 与 verifier/state/redirect 一一对应）。
func AutoLogin(email, password, state, verifier, redirect, challenge string) (AutoLoginResult, error) {
	client := autoLoginHTTPClient()
	jar, err := newCookieJar()
	if err != nil {
		return AutoLoginResult{}, err
	}
	client.Jar = jar

	// OAuth 参数集：每步 POST 都要完整带上（首屏 900144 缺 client_id 的教训）。
	q := url.Values{}
	q.Set("client_id", ClientID())
	q.Set("response_type", "code")
	q.Set("redirect_uri", redirect)
	q.Set("response_mode", "query")
	q.Set("scope", Scope())
	q.Set("state", state)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")

	// Step 1: GET authorize 首屏。
	authURL := AuthorizeEndpoint() + "?" + q.Encode()
	cfg0, _, err := loginGet(client, authURL)
	if err != nil {
		return AutoLoginResult{}, fmt.Errorf("m365: 打开登录页失败: %w", err)
	}

	// Step 2: POST 账号名（type=11 = 用户名提交动作）。
	form1 := oauthForm(q)
	form1.Set("login", email)
	form1.Set("loginfmt", email)
	form1.Set("type", "11")
	form1.Set("LoginOptions", "3")
	form1.Set("canary", cfg0.Canary)
	cfg1, referer, err := loginPost(client, absLoginURL(cfg0.URLPost), form1, authURL)
	if err != nil {
		return AutoLoginResult{}, fmt.Errorf("m365: 提交账号失败: %w", err)
	}
	if cfg1.ErrMessage != "" {
		return AutoLoginResult{}, fmt.Errorf("m365: %s", cfg1.ErrMessage)
	}
	if cfg1.SFT == "" {
		return AutoLoginResult{}, errors.New("m365: 登录页未返回密码输入令牌（可能触发风控），请改用 PKCE 授权")
	}

	// Step 3: POST 密码（type=12 = 密码提交动作）。
	form2 := oauthForm(q)
	form2.Set("login", email)
	form2.Set("loginfmt", email)
	form2.Set("passwd", password)
	form2.Set("type", "12")
	form2.Set("LoginOptions", "3")
	form2.Set("canary", cfg1.Canary)
	form2.Set("ctx", cfg1.SCtx)
	form2.Set("flowToken", cfg1.SFT)
	form2.Set("hpgrequestid", cfg1.SessionID)
	cfg2, referer2, err := loginPost(client, absLoginURL(cfg1.URLPost), form2, referer)
	if err != nil {
		return AutoLoginResult{}, fmt.Errorf("m365: 提交密码失败: %w", err)
	}
	if cfg2.ErrMessage != "" {
		return AutoLoginResult{}, fmt.Errorf("m365: %s（MFA/条件访问账号请用 PKCE 授权）", cfg2.ErrMessage)
	}

	// Step 4: sso_reload 协议——用响应页轮换的 oPostParams 重发（最多 3 轮）。
	cfg := cfg2
	lastReferer := referer2
	for round := 0; round < 3; round++ {
		if cfg.PostParams == nil || cfg.URLPost == "" {
			return AutoLoginResult{}, errors.New("m365: 登录流程中断（无续发参数），请改用 PKCE 授权")
		}
		form := url.Values{}
		for k, v := range cfg.PostParams {
			form.Set(k, v)
		}
		resp, cfgN, refN, err := loginPostRaw(client, absLoginURL(cfg.URLPost), form, lastReferer)
		if err != nil {
			return AutoLoginResult{}, fmt.Errorf("m365: 登录重试失败: %w", err)
		}
		if loc := resp.Header.Get("Location"); loc != "" {
			if code, st := codeFromRedirect(loc); code != "" {
				return AutoLoginResult{Code: code, State: st, Verifier: verifier, Redirect: redirect}, nil
			}
			return AutoLoginResult{}, fmt.Errorf("m365: 重定向未带授权码: %s", truncateLogin(loc, 120))
		}
		if cfgN.ErrMessage != "" {
			return AutoLoginResult{}, fmt.Errorf("m365: %s", cfgN.ErrMessage)
		}
		cfg, lastReferer = cfgN, refN
	}
	return AutoLoginResult{}, errors.New("m365: 登录循环未拿到授权码（可能触发 MFA/风控），请改用 PKCE 授权")
}

// oauthForm 从 authorize 参数构造登录 POST 必须携带的 OAuth 上下文字段。
func oauthForm(q url.Values) url.Values {
	form := url.Values{}
	for _, k := range []string{"client_id", "redirect_uri", "scope", "response_type", "code_challenge", "code_challenge_method", "state", "response_mode"} {
		form.Set(k, q.Get(k))
	}
	return form
}

// codeFromRedirect 从重定向 URL 提取 code 与 state。
func codeFromRedirect(loc string) (code, state string) {
	u, err := url.Parse(loc)
	if err != nil {
		return "", ""
	}
	qq := u.Query()
	return qq.Get("code"), qq.Get("state")
}

func truncateLogin(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// loginGet GET 一个登录页并解析 $Config。
func loginGet(client *http.Client, u string) (loginPageConfig, string, error) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return loginPageConfig{}, "", err
	}
	req.Header.Set("User-Agent", autoLoginUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	resp, err := client.Do(req)
	if err != nil {
		return loginPageConfig{}, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return loginPageConfig{}, "", err
	}
	cfg, err := parseLoginConfig(body)
	return cfg, u, err
}

// loginPost POST 表单（跟随 3xx 拿最终页解析 $Config）。
func loginPost(client *http.Client, u string, form url.Values, referer string) (loginPageConfig, string, error) {
	resp, cfg, ref, err := loginPostRaw(client, u, form, referer)
	if err != nil {
		return loginPageConfig{}, "", err
	}
	// 若直接 302 带 code（少数账号无 sso_reload 一轮直达），当场返回。
	if loc := resp.Header.Get("Location"); loc != "" {
		if code, st := codeFromRedirect(loc); code != "" {
			return cfg, ref, &authCodeFound{code: code, state: st}
		}
	}
	if cfg.ErrMessage != "" {
		return cfg, ref, errors.New(cfg.ErrMessage)
	}
	return cfg, ref, nil
}

// loginPostRaw 底层 POST：不跟随重定向，返回原始 resp + 解析后的页面配置。
func loginPostRaw(client *http.Client, u string, form url.Values, referer string) (*http.Response, loginPageConfig, string, error) {
	req, err := http.NewRequest(http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, loginPageConfig{}, "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", autoLoginUA)
	req.Header.Set("Referer", referer)
	if referer != "" {
		req.Header.Set("Origin", originOf(referer))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, loginPageConfig{}, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp, loginPageConfig{}, "", err
	}
	cfg, perr := parseLoginConfig(body)
	if perr != nil && resp.StatusCode == 200 {
		return resp, loginPageConfig{}, "", perr
	}
	return resp, cfg, u, nil
}

// authCodeFound 是 loginPost 提前发现授权码的内部信号（error 接口实现）。
type authCodeFound struct {
	code  string
	state string
}

func (a *authCodeFound) Error() string { return "auth code found: " + truncateLogin(a.code, 24) }

func originOf(u string) string {
	if p, err := url.Parse(u); err == nil && p.Host != "" {
		return p.Scheme + "://" + p.Host
	}
	return ""
}

// newCookieJar 构造 cookie jar（net/http/cookiejar 依赖由引擎统一引入）。
func newCookieJar() (http.CookieJar, error) {
	return newStdCookieJar()
}
