package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── Cline 免费模型：从 api.cline.bot 动态拉取 ────────────────────────────────

// 之前 clineFreeModels 是硬编码清单，模型会随上游上下线而失真。
// 现在从 Cline API 的 /models 拉取全部模型，过滤出 ":free" 后缀（真实免费），
// 统一加 "cline/" 前缀后持久化缓存到 data/.cline-models.json。

const clineModelCacheTTL = 24 * time.Hour

type clineCachedModels struct {
	FreeIDs   []string  `json:"free_ids"` // 带 cline/ 前缀
	FetchedAt time.Time `json:"fetched_at"`
}

var (
	clineModelMu    sync.Mutex
	clineModelList  []string  // 带 cline/ 前缀，线程安全读取
	clineModelParse time.Time // 上次拉取成功时刻（防止每次请求都重复拉）
	clineModelPath  string
)

func init() {
	clineModelPath = clineResolveDataPath(".cline-models.json")
	if m := loadClineModels(); m != nil {
		clineModelList = m
		clineModelParse = time.Now()
	}
}

func loadClineModels() []string {
	raw, err := os.ReadFile(clineModelPath)
	if err != nil || len(raw) > 1<<20 {
		return nil
	}
	var c clineCachedModels
	if json.Unmarshal(raw, &c) != nil || len(c.FreeIDs) == 0 {
		return nil
	}
	if time.Since(c.FetchedAt) > 7*24*time.Hour {
		return nil // 过期缓存不用
	}
	sort.Strings(c.FreeIDs)
	return c.FreeIDs
}

// clineFreeModelIDs 返回当前缓存的 Cline 免费模型（带 cline/ 前缀）。
func clineFreeModelIDs() []string {
	clineModelMu.Lock()
	defer clineModelMu.Unlock()
	return append([]string(nil), clineModelList...)
}

// ensureClineModels 保证已拉取过（TTL 内不重复拉）。首次或过期才真正请求。
// GUI 与 /v1/models 都会调用；无可用账号时跳过，不阻塞。
func ensureClineModels() {
	clineModelMu.Lock()
	if time.Since(clineModelParse) < clineModelCacheTTL {
		clineModelMu.Unlock()
		return
	}
	doRefresh := true
	clineModelMu.Unlock()

	if doRefresh {
		refreshClineModels()
	}
}

// refreshClineModels 用首个可用账号的 token 拉取 Cline 免费模型并写缓存。
// 失败保留旧清单；成功更新内存与 data/.cline-models.json。
func refreshClineModels() {
	acc := clinePickAccount(clineStrategyFill)
	if acc == nil {
		log.Printf("[Cline] 模型拉取跳过：无可用账号")
		return
	}
	token, err := clineEnsureAccountToken(acc)
	if err != nil {
		log.Printf("[Cline] 模型拉取跳过：token 失败: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clineAPIBase+"/models", nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Cline/3.8.50")
	req.Header.Set("X-CLIENT-TYPE", "opencode-autogate")
	req.Header.Set("HTTP-Referer", "https://cline.bot")
	req.Header.Set("X-Title", "Cline")

	client := &http.Client{Transport: controlTransport(8 * time.Second)}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		log.Printf("[Cline] 模型拉取失败: %v", err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		log.Printf("[Cline] 模型拉取失败: HTTP %d", res.StatusCode)
		return
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return
	}

	var root struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &root) != nil {
		return
	}
	free := make([]string, 0, 32)
	for _, m := range root.Data {
		id := strings.TrimSpace(m.ID)
		if !strings.HasSuffix(id, ":free") {
			continue
		}
		free = append(free, "cline/"+id)
	}
	if len(free) == 0 {
		log.Printf("[Cline] 模型拉取未发现 :free 模型，保留旧缓存")
		return
	}
	sort.Strings(free)
	clineModelMu.Lock()
	clineModelList = free
	clineModelParse = time.Now()
	clineModelMu.Unlock()

	cached := clineCachedModels{FreeIDs: free, FetchedAt: time.Now()}
	raw, _ := json.Marshal(cached)
	if dir := filepath.Dir(clineModelPath); dir != "" {
		_ = os.MkdirAll(dir, 0700)
	}
	writeFileAtomically(clineModelPath, raw)
	log.Printf("[Cline] 免费模型已更新（%d 个）", len(free))
}

// writeFileAtomically 原子写文件：先写临时文件再改名覆盖（Windows 上
// os.Rename 带替换语义），避免读到半截文件。
func writeFileAtomically(path string, raw []byte) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}
