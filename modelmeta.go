package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// modelCatalog 是 models.dev 校准出的免费模型清单：探活深检的模型名
// 从这里取，上游改模型名时自动跟随，不再依赖硬编码兜底。
type modelCatalog struct {
	FreeIDs   []string  `json:"free_ids"`
	FetchedAt time.Time `json:"fetched_at"`
}

// startModelCalibrator 启动时加载本地缓存并每天从 models.dev 刷新一次。
// models.dev 聚合了各家 provider 的模型元数据（含价格），OpenCode 条目里
// cost 双零或 ID 带 -free 后缀的即免费模型。
func (g *gateway) startModelCalibrator(ctx context.Context) {
	if catalog := loadModelCatalog(); catalog != nil {
		g.calibrated.Store(catalog)
	}
	// 启动阶段退避重试：出口可能尚未就绪、直连可能被污染，失败每 2 分钟
	// 再试直到首次成功——否则一次启动期失败要让模型清单空窗整整一天。
	for {
		g.refreshModelCatalog()
		if g.calibrated.Load() != nil {
			break
		}
		select {
		case <-time.After(2 * time.Minute):
		case <-ctx.Done():
			return
		}
	}
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.refreshModelCatalog()
		}
	}
}

func modelCatalogPath() string {
	return filepath.Join(filepath.Dir(configPath()), "models.dev.json")
}

func loadModelCatalog() *modelCatalog {
	raw, err := os.ReadFile(modelCatalogPath())
	if err != nil || len(raw) > 1<<20 {
		return nil
	}
	var catalog modelCatalog
	if json.Unmarshal(raw, &catalog) != nil || len(catalog.FreeIDs) == 0 {
		return nil
	}
	sort.Strings(catalog.FreeIDs)
	return &catalog
}

func (g *gateway) refreshModelCatalog() {
	const url = "https://models.dev/api.json"
	body, err := fetchURLWithExitFallback(g, url)
	if err != nil {
		log.Printf("[校准] models.dev 拉取失败（直连+出口均试），沿用现有清单: %v", err)
		return
	}
	var tree map[string]struct {
		Models map[string]struct {
			Cost *struct {
				Input  float64 `json:"input"`
				Output float64 `json:"output"`
			} `json:"cost"`
			Deprecated any `json:"deprecated"`
		} `json:"models"`
	}
	if json.Unmarshal(body, &tree) != nil {
		return
	}
	free := make([]string, 0, 16)
	for _, provider := range tree {
		for id, meta := range provider.Models {
			if meta.Deprecated != nil && fmt.Sprintf("%v", meta.Deprecated) == "true" {
				continue
			}
			if strings.Contains(strings.ToLower(id), "free") {
				free = append(free, id)
				continue
			}
			if meta.Cost != nil && meta.Cost.Input == 0 && meta.Cost.Output == 0 {
				free = append(free, id)
			}
		}
	}
	if len(free) == 0 {
		log.Printf("[校准] models.dev 未解析出免费模型，保留现有清单")
		return
	}
	sort.Strings(free)
	catalog := &modelCatalog{FreeIDs: free, FetchedAt: time.Now()}
	g.calibrated.Store(catalog)
	writeFileAtomically(modelCatalogPath(), mustJSON(catalog))
	log.Printf("[校准] 免费模型清单已更新（%d 个）", len(free))
	// 深检模型下线哨兵：清单只做参考不参与选择，但如果哪天连深检用的
	// 固定模型都从免费清单消失，提前喊话换 PROXY_PROBE_MODEL。
	if g.cfg.probeModel != "" {
		found := false
		for _, id := range catalog.FreeIDs {
			if id == g.cfg.probeModel {
				found = true
				break
			}
		}
		if !found {
			log.Printf("[校准] 注意：深检模型 %s 不在 models.dev 免费清单里——它仍可能是上游长期保留型号；仅当深检持续 404 时才需要换 PROXY_PROBE_MODEL", g.cfg.probeModel)
		}
	}
}

// probeModelID 返回深检固定使用的模型：big-pickle 是 opencode 长期在售的
// 免费模型，其他名字随时会下线——而且"出现在模型列表里"不代表 chat 门真
// 放行（deepseek-v4-flash-free 就是反例：列表有、对话全拒）。可用
// PROXY_PROBE_MODEL 环境变量覆盖。
func (g *gateway) probeModelID() string {
	if g.cfg.probeModel != "" {
		return g.cfg.probeModel
	}
	return "big-pickle"
}

func mustJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

// fetchURLWithExitFallback 先直连拉取；失败时借道在场出口重试（最多 3 个）。
// 借道出口时域名交给出口远端解析，绕过本地 DNS 污染——models.dev 在国内
// 被解析到 Facebook IP 段（31.13.x.x），直连永远超时，这是它的救命通道。
func fetchURLWithExitFallback(g *gateway, url string) ([]byte, error) {
	if body, err := httpGetBody(url, nil); err == nil {
		return body, nil
	}
	var lastErr error
	tried := 0
	for _, candidate := range g.customSnapshot() {
		if candidate.proxyURL == nil || !trackableExit(candidate.addr) {
			continue
		}
		body, err := httpGetBody(url, candidate.proxyURL)
		if err == nil {
			log.Printf("[校准] 直连失败，已借道 %s 拉取成功", candidate.addr)
			return body, nil
		}
		lastErr = err
		tried++
		if tried >= 3 {
			break
		}
	}
	return nil, lastErr
}

func httpGetBody(url string, proxyURL *url.URL) ([]byte, error) {
	transport := &http.Transport{}
	if proxyURL != nil {
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: transport}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 8<<20))
}

// writeFileAtomically 原子写文件：先写临时文件再改名覆盖（Go 的 os.Rename
// 在 Windows 上带替换语义），避免读到半截文件。
func writeFileAtomically(path string, raw []byte) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}
