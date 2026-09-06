package main

import (
	"encoding/json"
	"fmt"
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
