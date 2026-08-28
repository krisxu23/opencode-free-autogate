package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ── WorkOS / Cline OAuth constants ────────────────────────────────────────────

const (
	clineWorkosClientID       = "client_01K3A541FN8TA3EPPHTD2325AR"
	clineWorkosDeviceAuthURL  = "https://api.workos.com/user_management/authorize/device"
	clineWorkosAuthenticateURL = "https://api.workos.com/user_management/authenticate"
	clineAPIBase              = "https://api.cline.bot/api/v1"
)

// ── HTTP client (reused across all Cline API calls) ──────────────────────────

var clineHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     60 * time.Second,
	},
	Timeout: 30 * time.Second,
}

// ── Credential / Token types ─────────────────────────────────────────────────

type clineCredentials struct {
	RefreshToken string `json:"refreshToken"`
}

type workosDeviceAuthResp struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	Interval                int    `json:"interval"`
	ExpiresIn               int    `json:"expires_in"`
}

type workosAuthenticateResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	Error        string `json:"error"`
	ErrorDesc    string `json:"error_description"`
}

type clineRegisterResp struct {
	Data struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresAt    any    `json:"expiresAt"`
		UserInfo     *struct {
			Email string `json:"email"`
		} `json:"userInfo"`
	} `json:"data"`
}

type clineRefreshResp struct {
	Data struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresAt    any    `json:"expiresAt"`
	} `json:"data"`
}

// ── Single-account token cache (legacy credentials path) ─────────────────────

var (
	clineCachedToken      string
	clineCachedExpiry     int64
	clineCachedRefreshTok string
	clineCredentialsPath  string
)

func init() {
	clineCredentialsPath = clineFindCredentialsFile()
}

// clineFindCredentialsFile locates .cline-credentials.json: next to executable first, then cwd.
func clineFindCredentialsFile() string {
	if exe, err := os.Executable(); err == nil {
		p := filepath.Join(filepath.Dir(exe), ".cline-credentials.json")
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if pwd, err := os.Getwd(); err == nil {
		p := filepath.Join(pwd, ".cline-credentials.json")
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if exe, err := os.Executable(); err == nil {
		return filepath.Join(filepath.Dir(exe), ".cline-credentials.json")
	}
	pwd, _ := os.Getwd()
	return filepath.Join(pwd, ".cline-credentials.json")
}

func loadCredentials() *clineCredentials {
	data, err := os.ReadFile(clineCredentialsPath)
	if err != nil {
		return nil
	}
	var c clineCredentials
	if err := json.Unmarshal(data, &c); err != nil {
		return nil
	}
	return &c
}

func saveCredentials(rt string) {
	c := clineCredentials{RefreshToken: rt}
	data, _ := json.MarshalIndent(c, "", "  ")
	if err := os.WriteFile(clineCredentialsPath, data, 0600); err != nil {
		log.Printf("[cline] Failed to save credentials: %v", err)
		return
	}
	log.Printf("[cline] Credentials saved to %s", clineCredentialsPath)
}

// ── WorkOS Device Authorization ──────────────────────────────────────────────

func workosDeviceAuth() (*workosDeviceAuthResp, error) {
	form := url.Values{"client_id": {clineWorkosClientID}}
	resp, err := clineHTTPPostForm(clineWorkosDeviceAuthURL, form)
	if err != nil {
		return nil, fmt.Errorf("workos device auth: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("workos device auth failed: %d %s", resp.StatusCode, truncateStr(string(body), 200))
	}
	var d workosDeviceAuthResp
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, fmt.Errorf("workos device auth decode: %w", err)
	}
	return &d, nil
}

func pollWorkosToken(deviceCode string, interval, expiresIn int) (*workosAuthenticateResp, error) {
	deadline := time.Now().Add(time.Duration(expiresIn) * time.Second)
	cur := interval
	if cur < 5 {
		cur = 5
	}
	for time.Now().Before(deadline) {
		form := url.Values{
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
			"device_code": {deviceCode},
			"client_id":   {clineWorkosClientID},
		}
		resp, err := clineHTTPPostForm(clineWorkosAuthenticateURL, form)
		if err != nil {
			return nil, fmt.Errorf("workos poll: %w", err)
		}
		var a workosAuthenticateResp
		if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("workos poll decode: %w", err)
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			return &a, nil
		}
		switch a.Error {
		case "authorization_pending":
			time.Sleep(time.Duration(cur) * time.Second)
		case "slow_down":
			cur += 5
			time.Sleep(time.Duration(cur) * time.Second)
		default:
			errDesc := a.ErrorDesc
			if errDesc == "" {
				errDesc = a.Error
			}
			return nil, fmt.Errorf("workos polling error: %s", errDesc)
		}
	}
	return nil, fmt.Errorf("device authorization expired (timeout)")
}

// ── Cline API: register & refresh ────────────────────────────────────────────

func registerWithCline(workosAccess, workosRefresh string) (*clineRegisterResp, error) {
	body := map[string]string{
		"accessToken":  workosAccess,
		"refreshToken": workosRefresh,
	}
	resp, err := clineHTTPPostJSON(clineAPIBase+"/auth/register", body)
	if err != nil {
		return nil, fmt.Errorf("cline register: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cline register failed: %d %s", resp.StatusCode, truncateStr(string(b), 200))
	}
	var c clineRegisterResp
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		return nil, fmt.Errorf("cline register decode: %w", err)
	}
	return &c, nil
}

func refreshClineToken(refreshToken string) (*clineRefreshResp, error) {
	body := map[string]string{
		"refreshToken": refreshToken,
		"grantType":    "refresh_token",
		"clientType":   "extension",
	}
	resp, err := clineHTTPPostJSON(clineAPIBase+"/auth/refresh", body)
	if err != nil {
		return nil, fmt.Errorf("cline refresh: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("cline refresh failed: %d %s", resp.StatusCode, truncateStr(string(respBody), 500))
	}
	var c clineRefreshResp
	if err := json.Unmarshal(respBody, &c); err != nil {
		return nil, fmt.Errorf("cline refresh decode: %w (body: %s)", err, truncateStr(string(respBody), 500))
	}
	if c.Data.AccessToken == "" {
		return nil, fmt.Errorf("cline refresh returned empty accessToken (body: %s)", truncateStr(string(respBody), 500))
	}
	return &c, nil
}

// ── Single-account token getter (legacy) ─────────────────────────────────────

func getClineToken() (string, error) {
	if clineCachedToken != "" && time.Now().UnixMilli() < clineCachedExpiry {
		return clineCachedToken, nil
	}
	creds := loadCredentials()
	if creds != nil && creds.RefreshToken != "" {
		resp, err := refreshClineToken(creds.RefreshToken)
		if err == nil && resp.Data.AccessToken != "" {
			clineCachedToken = "workos:" + resp.Data.AccessToken
			clineCachedRefreshTok = resp.Data.RefreshToken
			if clineCachedRefreshTok == "" {
				clineCachedRefreshTok = creds.RefreshToken
			}
			clineCachedExpiry = clineParseExpiry(resp.Data.ExpiresAt) - 60000
			saveCredentials(clineCachedRefreshTok)
			return clineCachedToken, nil
		}
		log.Printf("[cline] Token refresh failed: %v", err)
	}
	return "", fmt.Errorf("no valid credentials. Run with --login flag first")
}

// ── Expiry parser ────────────────────────────────────────────────────────────

func clineParseExpiry(exp any) int64 {
	switch v := exp.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UnixMilli()
		}
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

// ── Login (single-account, CLI-driven) ───────────────────────────────────────

func doClineLogin() error {
	fmt.Println("\nStarting Cline OAuth login...")

	device, err := workosDeviceAuth()
	if err != nil {
		return err
	}
	authURL := device.VerificationURIComplete
	if authURL == "" {
		authURL = device.VerificationURI
	}
	fmt.Println("  1. Open this URL in your browser:")
	fmt.Println("     " + authURL)
	fmt.Println("  2. Enter code: " + device.UserCode)
	fmt.Println("  3. Log in with Google, GitHub, or email")
	_ = openBrowser(authURL)
	fmt.Println("  Waiting for authorization...")

	interval := device.Interval
	if interval < 5 {
		interval = 5
	}
	expiresIn := device.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 300
	}

	workosTok, err := pollWorkosToken(device.DeviceCode, interval, expiresIn)
	if err != nil {
		return err
	}
	fmt.Println("  WorkOS authorized. Registering with Cline...")

	reg, err := registerWithCline(workosTok.AccessToken, workosTok.RefreshToken)
	if err != nil {
		return err
	}
	if reg.Data.RefreshToken == "" {
		return fmt.Errorf("cline registration missing refresh token")
	}
	saveCredentials(reg.Data.RefreshToken)
	clineCachedToken = "workos:" + reg.Data.AccessToken
	clineCachedRefreshTok = reg.Data.RefreshToken
	clineCachedExpiry = clineParseExpiry(reg.Data.ExpiresAt) - 60000

	email := "unknown"
	if reg.Data.UserInfo != nil && reg.Data.UserInfo.Email != "" {
		email = reg.Data.UserInfo.Email
	}
	fmt.Printf("  Login successful! Account: %s\n", email)
	return nil
}

// ── Account Pool types ───────────────────────────────────────────────────────

type clineAccount struct {
	AccountID       string    `json:"accountId"`
	Email           string    `json:"email"`
	RefreshToken    string    `json:"refreshToken"`
	AccessToken     string    `json:"-"`
	ExpiresAt       int64     `json:"-"`
	Status          string    `json:"status"` // active, cooldown, expired
	LastUsed        time.Time `json:"lastUsed"`
	UsageCount      int64     `json:"usageCount"`
	UsageCountToday int64     `json:"usageCountToday"`
	UsageDate       string    `json:"usageDate"`        // YYYY-MM-DD, auto-reset on day rollover
	TokensTotal     int64     `json:"tokensTotal"`      // cumulative prompt+completion tokens
	TokensToday     int64     `json:"tokensToday"`
	TokensDate      string    `json:"tokensDate"`       // YYYY-MM-DD for token counters
	CreatedAt       time.Time `json:"createdAt"`
	CooldownUntil   time.Time `json:"cooldownUntil,omitempty"`
	LastReason      string    `json:"lastReason,omitempty"`
}

type clineAccountPool struct {
	Accounts   []*clineAccount `json:"accounts"`
	CurrentIdx int             `json:"currentIdx"`
	Keys       []string        `json:"keys,omitempty"`
}

// ── Pool strategy ────────────────────────────────────────────────────────────

type clinePoolStrategy int

const (
	clineStrategyRoundRobin clinePoolStrategy = iota
	clineStrategyRandom
	clineStrategyFill
)

func parseClineStrategy(s string) clinePoolStrategy {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "random":
		return clineStrategyRandom
	case "fill":
		return clineStrategyFill
	default:
		return clineStrategyRoundRobin
	}
}

// ── Pool singleton ───────────────────────────────────────────────────────────

var (
	clinePool       *clineAccountPool
	clinePoolMu     sync.Mutex
	clinePoolSaveMu sync.Mutex
	clinePoolPath   string
)

func init() {
	clinePoolPath = clineResolveDataPath(".cline-accounts.json")
}

// clineResolveDataPath locates a data file: next to exe (under data/ first), then cwd.
func clineResolveDataPath(filename string) string {
	var exeDir, pwd string
	if exe, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(exe)
	}
	if wd, err := os.Getwd(); err == nil {
		pwd = wd
	}
	// Candidates: exe/data/f, cwd/data/f, exe/f, cwd/f
	candidates := []string{}
	if exeDir != "" {
		candidates = append(candidates, filepath.Join(exeDir, "data", filename))
	}
	if pwd != "" {
		candidates = append(candidates, filepath.Join(pwd, "data", filename))
	}
	if exeDir != "" {
		candidates = append(candidates, filepath.Join(exeDir, filename))
	}
	if pwd != "" {
		candidates = append(candidates, filepath.Join(pwd, filename))
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	// Not found: default to exe/data/ (go run falls back to cwd/data/)
	if exeDir != "" {
		os.MkdirAll(filepath.Join(exeDir, "data"), 0755)
		return filepath.Join(exeDir, "data", filename)
	}
	os.MkdirAll(filepath.Join(pwd, "data"), 0755)
	return filepath.Join(pwd, "data", filename)
}

func loadClinePool() *clineAccountPool {
	clinePoolMu.Lock()
	defer clinePoolMu.Unlock()
	if clinePool != nil {
		return clinePool
	}
	data, err := os.ReadFile(clinePoolPath)
	if err != nil {
		clinePool = &clineAccountPool{Accounts: []*clineAccount{}, Keys: []string{}}
		return clinePool
	}
	var p clineAccountPool
	if err := json.Unmarshal(data, &p); err != nil {
		clinePool = &clineAccountPool{Accounts: []*clineAccount{}, Keys: []string{}}
		return clinePool
	}
	if p.Accounts == nil {
		p.Accounts = []*clineAccount{}
	}
	if p.Keys == nil {
		p.Keys = []string{}
	}
	clinePool = &p
	return clinePool
}

func saveClinePool() {
	clinePoolMu.Lock()
	defer clinePoolMu.Unlock()
	saveClinePoolLocked()
}

func saveClinePoolLocked() {
	clinePoolSaveMu.Lock()
	defer clinePoolSaveMu.Unlock()
	data, _ := json.MarshalIndent(clinePool, "", "  ")
	if err := os.WriteFile(clinePoolPath, data, 0600); err != nil {
		log.Printf("[cline] Failed to save accounts: %v", err)
	}
}

// ── Account CRUD ─────────────────────────────────────────────────────────────

func clineAddAccount(acc *clineAccount) {
	p := loadClinePool()
	clinePoolMu.Lock()
	p.Accounts = append(p.Accounts, acc)
	clinePoolMu.Unlock()
	saveClinePool()
}

func clineRemoveAccount(accountID string) bool {
	p := loadClinePool()
	clinePoolMu.Lock()
	for i, a := range p.Accounts {
		if a.AccountID == accountID {
			p.Accounts = append(p.Accounts[:i], p.Accounts[i+1:]...)
			saveClinePoolLocked()
			clinePoolMu.Unlock()
			return true
		}
	}
	clinePoolMu.Unlock()
	return false
}

func clineGetAccountByID(accountID string) *clineAccount {
	p := loadClinePool()
	clinePoolMu.Lock()
	defer clinePoolMu.Unlock()
	for _, a := range p.Accounts {
		if a.AccountID == accountID {
			return a
		}
	}
	return nil
}

func clineRefreshAccountToken(acc *clineAccount) error {
	resp, err := refreshClineToken(acc.RefreshToken)
	if err != nil {
		clinePoolMu.Lock()
		acc.Status = "expired"
		saveClinePoolLocked()
		clinePoolMu.Unlock()
		return fmt.Errorf("token refresh failed: %w", err)
	}
	clinePoolMu.Lock()
	acc.AccessToken = "workos:" + resp.Data.AccessToken
	if resp.Data.RefreshToken != "" {
		acc.RefreshToken = resp.Data.RefreshToken
	}
	acc.ExpiresAt = clineParseExpiry(resp.Data.ExpiresAt) - 60000
	acc.Status = "active"
	saveClinePoolLocked()
	clinePoolMu.Unlock()
	log.Printf("[Cline] token refreshed for %s, expires=%d, accessTokenLen=%d", acc.Email, acc.ExpiresAt, len(acc.AccessToken))
	return nil
}

// clinePickAccount selects the next available account using the given strategy.
func clinePickAccount(strategy clinePoolStrategy) *clineAccount {
	p := loadClinePool()
	clinePoolMu.Lock()

	now := time.Now()
	active := make([]*clineAccount, 0)
	for _, a := range p.Accounts {
		// Auto-expire cooldown
		if a.Status == "cooldown" && !a.CooldownUntil.IsZero() && now.After(a.CooldownUntil) {
			a.Status = "active"
			a.CooldownUntil = time.Time{}
			a.LastReason = ""
		}
		if a.Status == "active" {
			active = append(active, a)
		}
	}
	if len(active) == 0 {
		clinePoolMu.Unlock()
		return nil
	}

	var acc *clineAccount
	switch strategy {
	case clineStrategyFill:
		acc = active[0]
	case clineStrategyRandom:
		acc = active[time.Now().UnixNano()%int64(len(active))]
	default: // round_robin
		if p.CurrentIdx >= len(active) {
			p.CurrentIdx = 0
		}
		acc = active[p.CurrentIdx]
		p.CurrentIdx = (p.CurrentIdx + 1) % len(active)
	}
	saveClinePoolLocked()
	clinePoolMu.Unlock()
	return acc
}

func clineEnsureAccountToken(acc *clineAccount) (string, error) {
	if acc.AccessToken != "" && time.Now().UnixMilli() < acc.ExpiresAt {
		return acc.AccessToken, nil
	}
	if err := clineRefreshAccountToken(acc); err != nil {
		return "", err
	}
	return acc.AccessToken, nil
}

func clineListAccounts() []*clineAccount {
	p := loadClinePool()
	clinePoolMu.Lock()

	usageDate := time.Now().Format("2006-01-02")
	for _, a := range p.Accounts {
		if a.Status == "cooldown" && !a.CooldownUntil.IsZero() && time.Now().After(a.CooldownUntil) {
			a.Status = "active"
			a.CooldownUntil = time.Time{}
			a.LastReason = ""
		}
		if a.UsageDate != usageDate {
			a.UsageDate = usageDate
			a.UsageCountToday = 0
		}
		if a.TokensDate != usageDate {
			a.TokensDate = usageDate
			a.TokensToday = 0
		}
	}
	result := make([]*clineAccount, len(p.Accounts))
	for i, a := range p.Accounts {
		result[i] = &clineAccount{
			AccountID:       a.AccountID,
			Email:           a.Email,
			Status:          a.Status,
			LastUsed:        a.LastUsed,
			UsageCount:      a.UsageCount,
			UsageCountToday: a.UsageCountToday,
			UsageDate:       a.UsageDate,
			TokensTotal:     a.TokensTotal,
			TokensToday:     a.TokensToday,
			TokensDate:      a.TokensDate,
			CreatedAt:       a.CreatedAt,
			CooldownUntil:   a.CooldownUntil,
			LastReason:      a.LastReason,
		}
	}
	saveClinePoolLocked()
	clinePoolMu.Unlock()
	return result
}

// ── 429 cooldown ─────────────────────────────────────────────────────────────

const defaultClineCooldown = 18 * time.Hour // Cline free quota resets daily

func markClineAccountCooldown(acc *clineAccount, reason string, duration time.Duration) {
	if acc == nil {
		return
	}
	if duration <= 0 {
		duration = defaultClineCooldown
	}
	clinePoolMu.Lock()
	acc.Status = "cooldown"
	acc.CooldownUntil = time.Now().Add(duration)
	acc.LastReason = reason
	saveClinePoolLocked()
	clinePoolMu.Unlock()
}

// parseClineInferenceCapDuration parses "Try again in 17h 59m" from Cline 429
// error bodies. Supports "17h 59m", "17h", "59m", "30s", "1d 2h 30m" etc.
func parseClineInferenceCapDuration(body string) time.Duration {
	idx := strings.Index(body, "Try again in")
	if idx < 0 {
		return 0
	}
	rest := body[idx+len("Try again in"):]
	end := len(rest)
	if i := strings.IndexAny(rest, "\"\n\r}"); i >= 0 {
		end = i
	}
	segment := strings.TrimSpace(rest[:end])
	return parseClineHumanDuration(segment)
}

// parseClineHumanDuration parses "17h 59m" / "2h" / "59m" / "30s" / "1d 2h" durations.
func parseClineHumanDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	var total time.Duration
	num := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			num = num*10 + int(c-'0')
		case c == 'd':
			total += time.Duration(num) * 24 * time.Hour
			num = 0
		case c == 'h':
			total += time.Duration(num) * time.Hour
			num = 0
		case c == 'm' && i+1 < len(s) && s[i+1] == 's':
			total += time.Duration(num) * time.Millisecond
			num = 0
			i++
		case c == 'm':
			total += time.Duration(num) * time.Minute
			num = 0
		case c == 's':
			total += time.Duration(num) * time.Second
			num = 0
		case c == ' ':
			// separator
		default:
			num = 0
		}
	}
	return total
}

// ── Token usage tracking ─────────────────────────────────────────────────────

func clineBumpUsage(acc *clineAccount) {
	if acc == nil {
		return
	}
	clinePoolMu.Lock()
	now := time.Now()
	today := now.Format("2006-01-02")
	if acc.UsageDate != today {
		acc.UsageDate = today
		acc.UsageCountToday = 0
	}
	acc.UsageCountToday++
	acc.UsageCount++
	acc.LastUsed = now
	saveClinePoolLocked()
	clinePoolMu.Unlock()
}

func clineResetTodayUsage(acc *clineAccount) {
	if acc == nil {
		return
	}
	clinePoolMu.Lock()
	acc.UsageDate = time.Now().Format("2006-01-02")
	acc.UsageCountToday = 0
	acc.TokensDate = time.Now().Format("2006-01-02")
	acc.TokensToday = 0
	saveClinePoolLocked()
	clinePoolMu.Unlock()
}

func clineRecordAccountTokens(acc *clineAccount, tokens int64) {
	if acc == nil || tokens <= 0 {
		return
	}
	clinePoolMu.Lock()
	today := time.Now().Format("2006-01-02")
	if acc.TokensDate != today {
		acc.TokensDate = today
		acc.TokensToday = 0
	}
	acc.TokensToday += tokens
	acc.TokensTotal += tokens
	saveClinePoolLocked()
	clinePoolMu.Unlock()
}

// ── Pool status summary ──────────────────────────────────────────────────────

func clineDescribePoolStatus() string {
	p := loadClinePool()
	clinePoolMu.Lock()
	defer clinePoolMu.Unlock()

	total := len(p.Accounts)
	if total == 0 {
		return "pool is empty, use --add-account or admin API to add accounts"
	}
	active, cooldown, expired := 0, 0, 0
	var nextRecover *time.Time
	for _, a := range p.Accounts {
		switch a.Status {
		case "active":
			active++
		case "cooldown":
			cooldown++
			if !a.CooldownUntil.IsZero() {
				if nextRecover == nil || a.CooldownUntil.Before(*nextRecover) {
					t := a.CooldownUntil
					nextRecover = &t
				}
			}
		case "expired":
			expired++
		}
	}
	s := fmt.Sprintf("total=%d active=%d cooldown=%d expired=%d", total, active, cooldown, expired)
	if cooldown > 0 && nextRecover != nil {
		s += fmt.Sprintf(", earliest recover at %s", nextRecover.Format("2006-01-02 15:04:05"))
	}
	return s
}

// ── Add account via OAuth device flow (CLI-driven) ───────────────────────────

func clineAddAccountFromDeviceAuth() (*clineAccount, error) {
	fmt.Println("\n=== Add New Cline Account (OAuth) ===")

	device, err := workosDeviceAuth()
	if err != nil {
		return nil, err
	}
	authURL := device.VerificationURIComplete
	if authURL == "" {
		authURL = device.VerificationURI
	}
	fmt.Println("  1. Open this URL in your browser:")
	fmt.Println("     " + authURL)
	fmt.Println("  2. Enter code: " + device.UserCode)
	fmt.Println("  3. Log in with Google, GitHub, or email")
	_ = openBrowser(authURL)
	fmt.Println("  Waiting for authorization...")

	interval := device.Interval
	if interval < 5 {
		interval = 5
	}
	expiresIn := device.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 300
	}

	workosTok, err := pollWorkosToken(device.DeviceCode, interval, expiresIn)
	if err != nil {
		return nil, err
	}
	fmt.Println("  WorkOS authorized. Registering with Cline...")

	reg, err := registerWithCline(workosTok.AccessToken, workosTok.RefreshToken)
	if err != nil {
		return nil, err
	}
	if reg.Data.RefreshToken == "" {
		return nil, fmt.Errorf("cline registration missing refresh token")
	}
	email := "unknown"
	if reg.Data.UserInfo != nil && reg.Data.UserInfo.Email != "" {
		email = reg.Data.UserInfo.Email
	}

	acc := &clineAccount{
		AccountID:    fmt.Sprintf("acc_%d", time.Now().UnixMilli()),
		Email:        email,
		RefreshToken: reg.Data.RefreshToken,
		AccessToken:  "workos:" + reg.Data.AccessToken,
		ExpiresAt:    clineParseExpiry(reg.Data.ExpiresAt) - 60000,
		Status:       "active",
		CreatedAt:    time.Now(),
	}
	clineAddAccount(acc)
	fmt.Printf("  Account added! Email: %s\n", email)
	return acc, nil
}

// ── HTTP helpers (stdlib only, no external kit dependency) ────────────────────

func clineHTTPPostForm(rawURL string, form url.Values) (*http.Response, error) {
	req, err := http.NewRequest("POST", rawURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return clineHTTPClient.Do(req)
}

func clineHTTPPostJSON(rawURL string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", rawURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return clineHTTPClient.Do(req)
}

// ── Browser opener ───────────────────────────────────────────────────────────

func openBrowser(rawURL string) error {
	var cmd string
	var args []string
	if isWindows() {
		cmd = "rundll32"
		args = []string{"url.dll,FileProtocolHandler", rawURL}
	} else {
		for _, candidate := range []string{"xdg-open", "open", "gnome-open"} {
			if _, err := os.Stat("/usr/bin/" + candidate); err == nil {
				cmd = candidate
				break
			}
			if _, err := os.Stat("/usr/local/bin/" + candidate); err == nil {
				cmd = candidate
				break
			}
		}
	}
	if cmd == "" {
		return fmt.Errorf("no browser opener found")
	}
	return exec.Command(cmd, args...).Start()
}

func isWindows() bool {
	return strings.Contains(strings.ToLower(os.Getenv("OS")), "windows")
}

// ── Utilities ────────────────────────────────────────────────────────────────

func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// ── Cline 请求转发 ──────────────────────────────────────────────────────────

// clineUpstreamBody 将客户端请求体转换为 Cline API 格式。
func clineUpstreamBody(params map[string]any, stream bool) map[string]any {
	sessionID := fmt.Sprintf("sess_%d", time.Now().UnixMilli())

	maxTokens := 128000
	if mt, ok := params["max_tokens"].(float64); ok {
		maxTokens = int(mt)
	} else if mt, ok := params["max_completion_tokens"].(float64); ok {
		maxTokens = int(mt)
	}

	model := "deepseek/deepseek-v4-flash"
	if m, ok := params["model"].(string); ok && m != "" {
		model = strings.TrimPrefix(m, "cline/") // 发给 Cline API 时去掉前缀
	}

	body := map[string]any{
		"model":            model,
		"max_tokens":       maxTokens,
		"session_id":       sessionID,
		"reasoning_effort": "high",
	}

	if msgsRaw, ok := params["messages"]; ok {
		body["messages"] = msgsRaw
	}
	// Cline API 强制流式，非流式返回 "generateText is not implemented"
	body["stream"] = true
	if re, ok := params["reasoning_effort"].(string); ok && re != "" {
		body["reasoning_effort"] = re
	} else if re, ok := params["reasoningEffort"].(string); ok && re != "" {
		body["reasoning_effort"] = re
	}

	// 透传客户端参数
	passThrough := []string{
		"tools", "tool_choice", "parallel_tool_calls", "functions", "function_call",
		"temperature", "top_p", "top_k", "stop", "presence_penalty", "frequency_penalty",
		"response_format", "user", "n", "logit_bias", "seed", "logprobs", "top_logprobs",
		"stream_options", "metadata",
	}
	for _, key := range passThrough {
		if val, ok := params[key]; ok {
			body[key] = val
		}
	}
	return body
}

// handleClineChat 处理 Cline 上游请求：选择账号 → 构造请求 → 转发 → 流式/非流式响应。
func (g *gateway) handleClineChat(ctx context.Context, params map[string]any, path string, stream bool, deadline time.Time, trace *requestTrace) (*gatewayResponse, error) {
	strategy := parseClineStrategy(os.Getenv("CLINE_POOL_STRATEGY"))
	acc := clinePickAccount(strategy)
	if acc == nil {
		return jsonGatewayResponse(http.StatusServiceUnavailable,
			fmt.Sprintf("no active Cline accounts: %s", clineDescribePoolStatus())), nil
	}

	token, err := clineEnsureAccountToken(acc)
	if err != nil {
		return jsonGatewayResponse(http.StatusBadGateway,
			fmt.Sprintf("Cline token failed for %s: %v", acc.Email, err)), nil
	}

	body := clineUpstreamBody(params, stream)
	bodyJSON, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, clineAPIBase+"/chat/completions", bytes.NewReader(bodyJSON))
	if err != nil {
		return jsonGatewayResponse(http.StatusInternalServerError, err.Error()), nil
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("HTTP-Referer", "https://cline.bot")
	req.Header.Set("X-Title", "Cline")
	req.Header.Set("User-Agent", "Cline/3.8.50")
	req.Header.Set("X-CLIENT-TYPE", "opencode-autogate")
	req.Header.Set("X-CLIENT-VERSION", "3.8.50")
	req.Header.Set("X-PLATFORM", "win32")
	req.Header.Set("X-PLATFORM-VERSION", "10.0")
	req.Header.Set("X-CORE-VERSION", "3.8.50")
	req.Header.Set("X-IS-MULTIROOT", "false")

	log.Printf("[Cline] account=%s model=%s stream=%v", acc.Email, body["model"], stream)

	resp, err := clineHTTPClient.Do(req)
	if err != nil {
		markClineAccountCooldown(acc, "network error: "+err.Error(), 5*time.Minute)
		return jsonGatewayResponse(http.StatusBadGateway, err.Error()), nil
	}
	defer resp.Body.Close()

	// 401 → 刷新 Token 重试
	if resp.StatusCode == http.StatusUnauthorized {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		log.Printf("[Cline] 401 from API: %s", truncateStr(string(respBody), 300))
		if err := clineRefreshAccountToken(acc); err == nil {
			token = acc.AccessToken
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err = clineHTTPClient.Do(req)
			if err != nil {
				return jsonGatewayResponse(http.StatusBadGateway, err.Error()), nil
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized {
				retryBody, _ := io.ReadAll(resp.Body)
				return jsonGatewayResponse(http.StatusUnauthorized,
					fmt.Sprintf("Cline account %s token expired: %s", acc.Email, truncateStr(string(retryBody), 300))), nil
			}
		} else {
			return jsonGatewayResponse(http.StatusUnauthorized,
				fmt.Sprintf("Cline account %s refresh failed: %v", acc.Email, err)), nil
		}
	}

	// 429 → 冷却
	if resp.StatusCode == http.StatusTooManyRequests {
		bodyBytes, _ := io.ReadAll(resp.Body)
		duration := parseClineInferenceCapDuration(string(bodyBytes))
		if duration <= 0 {
			duration = 18 * time.Hour
		}
		markClineAccountCooldown(acc, "429: "+truncateStr(string(bodyBytes), 200), duration)
		return jsonGatewayResponse(http.StatusTooManyRequests,
			fmt.Sprintf("Cline rate limited, cooldown %v", duration)), nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("[Cline] API %d: %s", resp.StatusCode, truncateStr(string(bodyBytes), 500))
		return jsonGatewayResponse(resp.StatusCode,
			fmt.Sprintf("Cline API %d: %s", resp.StatusCode, truncateStr(string(bodyBytes), 500))), nil
	}

	clineBumpUsage(acc)

	// 流式响应：通过 liveResponse 传递给 streamResponse 处理
	if stream {
		ctx, cancel := context.WithCancel(context.Background())
		_ = ctx // 仅用于构造 cancel func
		return &gatewayResponse{
			status: http.StatusOK,
			header: resp.Header,
			live:   &liveResponse{response: resp, cancel: cancel},
		}, nil
	}

	// 非流式响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return jsonGatewayResponse(http.StatusBadGateway, err.Error()), nil
	}
	return &gatewayResponse{
		status: http.StatusOK,
		header: http.Header{"Content-Type": {"application/json; charset=utf-8"}},
		body:   respBody,
	}, nil
}

// ── Cline 模型列表 ──────────────────────────────────────────────────────────

// clineFreeModels 是 Cline 支持的免费模型清单。
// Cline 模型格式: provider/model（如 anthropic/claude-sonnet-4.6）
// ClinePass 模型格式: cline-pass/model（如 cline-pass/deepseek-v4-pro）
// 所有模型名带 cline/ 前缀，网关据此区分 Cline 上游与 zen 上游。
var clineFreeModels = []string{
	// Cline 官方模型
	"cline/anthropic/claude-opus-4.7",
	"cline/anthropic/claude-sonnet-4.6",
	"cline/openai/gpt-5.3-codex",
	"cline/openai/gpt-5.4",
	"cline/google/gemini-3.1-pro-preview",
	"cline/google/gemini-3.1-flash-lite-preview",
	"cline/kwaipilot/kat-coder-pro",
	// ClinePass 模型（同一 API，不同 provider 标识）
	"cline/cline-pass/deepseek-v4-pro",
	"cline/cline-pass/qwen3.7-max",
	"cline/cline-pass/mimo-v2.5",
	"cline/cline-pass/kimi-k2.7-code",
	"cline/cline-pass/glm-5.2",
}

// clineModelListText 返回一行一个模型名的文本，用于 GUI 显示和复制。
func clineModelListText() string {
	return strings.Join(clineFreeModels, "\r\n")
}

// isClineModel 判断模型名是否属于 Cline 上游（带 cline/ 前缀）。
func isClineModel(model string) bool {
	return strings.HasPrefix(strings.TrimSpace(model), "cline/")
}
