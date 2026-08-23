package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultGatewayKey = "sk-local-freegate"
	configFileName    = "config.json"

	outboundProxy  = "proxy"
	outboundDirect = "direct"
)

// uiSettings 是界面可编辑的配置，持久化在 exe 同目录的 config.json。
type uiSettings struct {
	Port             int      `json:"port"`
	Outbound         string   `json:"outbound"`           // proxy | direct
	Proxies          []string `json:"proxies"`            // 已规整的代理 URL
	ProxyInput       string   `json:"proxy_input"`        // 用户原始输入，回填输入框
	Mirrors          []string `json:"mirrors"`            // 上游镜像基址
	MirrorInput      string   `json:"mirror_input"`       // 用户原始输入，回填输入框
	FirstByteSeconds int      `json:"first_byte_seconds"` // 流式首字节超时，秒
	BudgetSeconds    int      `json:"budget_seconds"`     // 流式总预算，秒
	GatewayKey       string   `json:"gateway_key"`        // 展示用的默认 Key
	PoolEnabled      bool     `json:"pool_enabled"`       // 在线节点池开关
	PoolInput        string   `json:"pool_input"`         // 节点源链接，回填输入框
	RaceEnabled      bool     `json:"race_enabled"`       // 并行竞速开关
}

// 节点池默认源已移除：新装用户节点池为空，公共推荐源见 README。

func defaultSettings() uiSettings {
	return uiSettings{
		Port:             13339,
		Outbound:         outboundDirect,
		Proxies:          nil,
		ProxyInput:       "",
		Mirrors:          append([]string(nil), defaultMirrorURLs...),
		MirrorInput:      strings.Join(defaultMirrorURLs, "\r\n"),
		FirstByteSeconds: 30,
		BudgetSeconds:    180,
		GatewayKey:       defaultGatewayKey,
		PoolEnabled:      false,
		PoolInput:        "",
		RaceEnabled:      true,
	}
}

// configPath 返回 exe 同目录下的 config.json 路径。
func configPath() string {
	exe, err := os.Executable()
	if err != nil {
		return configFileName
	}
	return filepath.Join(filepath.Dir(exe), configFileName)
}

// loadSettings 读取配置文件，缺失或损坏时回落到默认值。
func loadSettings(path string) uiSettings {
	settings := defaultSettings()
	data, err := os.ReadFile(path)
	if err != nil {
		return settings
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		return defaultSettings()
	}
	return settings.normalized()
}

// normalized 修正越界或空缺字段，并保持 Proxies/Mirrors 与输入框内容一致。
func (s uiSettings) normalized() uiSettings {
	if s.Port <= 0 || s.Port > 65535 {
		s.Port = 13339
	}
	if s.Outbound != outboundProxy && s.Outbound != outboundDirect {
		s.Outbound = outboundDirect
	}
	if s.FirstByteSeconds <= 0 {
		s.FirstByteSeconds = 30
	}
	if s.BudgetSeconds <= 0 {
		s.BudgetSeconds = 180
	}
	if s.BudgetSeconds < s.FirstByteSeconds {
		s.BudgetSeconds = s.FirstByteSeconds
	}
	if strings.TrimSpace(s.GatewayKey) == "" {
		s.GatewayKey = defaultGatewayKey
	}
	// PoolInput 允许为空：节点池源完全由用户填写（公共推荐源见 README）。
	if len(s.Proxies) == 0 && strings.TrimSpace(s.ProxyInput) != "" {
		s.Proxies, _ = ParseProxyInput(s.ProxyInput)
	}
	if s.ProxyInput == "" && len(s.Proxies) > 0 {
		s.ProxyInput = strings.Join(s.Proxies, "\r\n")
	}
	if len(s.Mirrors) == 0 && strings.TrimSpace(s.MirrorInput) != "" {
		s.Mirrors, _ = parseMirrorList(s.MirrorInput)
	}
	if s.MirrorInput == "" && len(s.Mirrors) > 0 {
		s.MirrorInput = strings.Join(s.Mirrors, "\r\n")
	}
	return s
}

// save 以缩进 JSON 写入配置文件；先写临时文件再原子替换，
// 避免写入中途断电/崩溃留下半截配置。
func (s uiSettings) save(path string) error {
	data, err := json.MarshalIndent(s.normalized(), "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// applyEnv 把配置转换为环境变量；已显式设置的环境变量优先，不被覆盖。
func (s uiSettings) applyEnv() {
	setIfEmpty("PORT", fmt.Sprintf("%d", s.Port))
	// 仅直连模式显式跳过代理层；走代理模式从自定义池（含节点池）开始。
	if s.Outbound == outboundProxy {
		setIfEmpty("PROXY_ORDER", "custom")
	} else {
		setIfEmpty("PROXY_ORDER", "direct")
	}
	if s.Outbound == outboundProxy && len(s.Proxies) > 0 {
		setIfEmpty("CUSTOM_PROXIES", strings.Join(s.Proxies, ","))
	}
	if len(s.Mirrors) > 0 {
		setIfEmpty("MIRROR_URLS", strings.Join(s.Mirrors, ","))
	}
	setIfEmpty("PROXY_FIRST_BYTE_TIMEOUT", fmt.Sprintf("%d", s.FirstByteSeconds*1000))
	setIfEmpty("HARD_TIMEOUT", fmt.Sprintf("%d", s.BudgetSeconds*1000))
	if s.PoolEnabled {
		if urls := parsePoolSources(s.PoolInput); len(urls) > 0 {
			setIfEmpty("PROXY_LIST_URLS", strings.Join(urls, ","))
		}
	}
	if s.RaceEnabled {
		setIfEmpty("PROXY_RACE", "1")
	} else {
		setIfEmpty("PROXY_RACE", "0")
	}
}

func setIfEmpty(key, value string) {
	if strings.TrimSpace(os.Getenv(key)) == "" {
		_ = os.Setenv(key, value)
	}
}
