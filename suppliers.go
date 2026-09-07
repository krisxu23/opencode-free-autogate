package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Supplier 是通用 OpenAI 兼容上游供应商：baseURL + API Key + 模型列表。
// opencode / cline / m365 为内置保留 ID，不可通过通用配置注册。
type Supplier struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	BaseURL string   `json:"base_url"`
	APIKey  string   `json:"api_key"`
	Models  []string `json:"models"`
	Enabled bool     `json:"enabled"`
}

// SplitSupplierPrefix 把 "供应商/模型" 拆成两段；无前缀返回 ok=false。
func SplitSupplierPrefix(model string) (supplier, real string, ok bool) {
	idx := strings.Index(model, "/")
	if idx <= 0 || idx == len(model)-1 {
		return "", model, false
	}
	sup, real := model[:idx], model[idx+1:]
	if strings.ContainsAny(sup, " \t/") || strings.ContainsAny(real, " \t") {
		return "", model, false
	}
	return sup, real, true
}

// Validate 检查供应商 ID 与地址合法性（模型列表允许为空：按需透传）。
func (s Supplier) Validate() error {
	if strings.TrimSpace(s.ID) == "" || strings.ContainsAny(s.ID, " \t/") {
		return fmt.Errorf("supplier id %q invalid (non-empty, no spaces/slashes)", s.ID)
	}
	base := strings.TrimRight(strings.TrimSpace(s.BaseURL), "/")
	if base == "" || strings.Contains(base, " ") || !strings.Contains(base, "://") {
		return fmt.Errorf("supplier %q base_url %q invalid", s.ID, s.BaseURL)
	}
	return nil
}

var reservedSupplierIDs = map[string]struct{}{
	"opencode": {},
	"cline":    {},
	"m365":     {},
}

// ParseSuppliersJSON 解析通用供应商列表；空输入返回 nil,nil。
func ParseSuppliersJSON(raw string) ([]Supplier, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out []Supplier
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, s := range out {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		if _, reserved := reservedSupplierIDs[s.ID]; reserved {
			return nil, fmt.Errorf("supplier id %q is reserved", s.ID)
		}
		if _, dup := seen[s.ID]; dup {
			return nil, fmt.Errorf("supplier id %q duplicated", s.ID)
		}
		seen[s.ID] = struct{}{}
	}
	return out, nil
}

// parseAccountLines 解析多账号密码输入：每行 "邮箱,密码"，空行与 # 注释跳过。
// 密码只在内存里停留到换 token 为止，调用方不得落盘。
func parseAccountLines(raw string) [][2]string {
	var out [][2]string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			continue
		}
		email := strings.TrimSpace(parts[0])
		password := strings.TrimSpace(parts[1])
		if email == "" || password == "" {
			continue
		}
		out = append(out, [2]string{email, password})
	}
	return out
}

// suppliersSeedText 生成供应商编辑框的初始文本：优先回填用户原始输入，
func suppliersSeedText(s uiSettings) string {
	if input := strings.TrimSpace(s.SuppliersInput); input != "" {
		return input
	}
	if len(s.Suppliers) > 0 {
		if raw, err := json.MarshalIndent(s.Suppliers, "", "  "); err == nil {
			return string(raw)
		}
	}
	return "[]"
}

// formatAutoPools 把 auto 池渲染成 pool=m1,m2;... 文本（与 PROXY_AUTO_POOLS 同口径）。
func formatAutoPools(pools map[string][]string) string {
	names := make([]string, 0, len(pools))
	for name := range pools {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+strings.Join(pools[name], ","))
	}
	return strings.Join(parts, ";")
}
