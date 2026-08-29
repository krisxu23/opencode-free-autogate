package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	configFileName    = "config.json"

	outboundProxy  = "proxy"
	outboundDirect = "direct"
)

// configManagedEnvKeys 是由 config.json 派生的环境变量：「保存并重启」时必须
// 剔除它们，否则旧值会在新进程里覆盖新配置（曾导致节点源改了却不生效）。
// applyEnv 的写入与此处一一对应，新增配置项时两处同步维护。
var configManagedEnvKeys = []string{
	"PORT",
	"PROXY_ORDER",
	"CUSTOM_PROXIES",
	"MIRROR_URLS",
	"PROXY_FIRST_BYTE_TIMEOUT",
	"HARD_TIMEOUT",
	"PROXY_ABSORB_STREAMING",
	"PROXY_ABSORB_ATTEMPTS",
	"PROXY_DEEP_PROBE_INTERVAL",
	"PROXY_PROBE_MODEL",
	"PROXY_PROBE_CONCURRENCY",
	"PROXY_LIST_URLS",
	"PROXY_RACE",
	"PROXY_RACE_WIDTH",
	"PROXY_POOL_OPENCODE",
	"PROXY_POOL_CLINE",
	"PROXY_PREFERRED_REGIONS",
	"GATEWAY_KEY",
}

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
	AbsorbStreaming  bool     `json:"absorb_streaming"`   // 吸收模式：网关内重试直到完整
	AbsorbAttempts   int      `json:"absorb_attempts"`    // 吸收模式最大尝试次数
	DeepProbeMinutes int      `json:"deep_probe_minutes"` // chat 深检间隔，分钟
	ProbeConcurrency int      `json:"probe_concurrency"`  // 检测并发路数（GET 初检与 chat 深检共用，1-128，默认 32）
	ProbeModel       string   `json:"probe_model"`        // chat 深检模型；空 = 自动（big-pickle）
	GatewayKey       string   `json:"gateway_key"`        // 展示用的默认 Key
	PoolEnabled      bool     `json:"pool_enabled"`       // 在线节点池开关
	PoolInput        string   `json:"pool_input"`         // 节点源链接，回填输入框
	RaceEnabled      bool     `json:"race_enabled"`       // 并行竞速开关
	RaceWidth        int      `json:"race_width"`         // 竞速并发路数（同时发往几个出口，2-32，默认8）
	// 每供应商节点池出口开关（节点池本身是全局资源，见 PoolEnabled/PoolInput）：
	// OpenCode 用反向字段保持旧 config.json 兼容——缺省 false = 走池（既有行为）；
	// Cline 缺省 false = 直连（既有行为），开 = 加入多 IP 轮询出口 + 直连兜底。
	OpenCodePoolOff bool `json:"opencode_pool_off"`
	ClinePoolEnabled bool `json:"cline_pool_enabled"`
	PreferredRegions string `json:"preferred_regions"` // 地区偏好（逗号分隔国家码，空 = 不偏好）
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
		DeepProbeMinutes: 60,
		ProbeConcurrency: 32,
		GatewayKey:       generateAPIKey(),
		PoolEnabled:      false,
		PoolInput:        "",
		RaceEnabled:      true,
		RaceWidth:        8,
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
	if s.AbsorbAttempts <= 0 || s.AbsorbAttempts > 50 {
		s.AbsorbAttempts = 10
	}
	// 深检间隔下限与 deepprobe 内部护栏一致（10 分钟），上限一天。
	if s.DeepProbeMinutes <= 0 {
		s.DeepProbeMinutes = 60
	}
	if s.DeepProbeMinutes < 10 {
		s.DeepProbeMinutes = 10
	}
	if s.DeepProbeMinutes > 1440 {
		s.DeepProbeMinutes = 1440
	}
	// 检测并发：1-128 路（GET 初检与 chat 深检共用）；缺省/越界回落默认 32。
	if s.ProbeConcurrency <= 0 || s.ProbeConcurrency > 128 {
		s.ProbeConcurrency = 32
	}
	// 竞速并发：2-32 路；缺省/越界回落默认 8。
	if s.RaceWidth < 2 {
		s.RaceWidth = 2
	}
	if s.RaceWidth > 32 {
		s.RaceWidth = 32
	}
	if strings.TrimSpace(s.GatewayKey) == "" {
		s.GatewayKey = generateAPIKey()
	}
	// 深检模型只做去空白；允许填任意 ID（上游临时下架时由校准哨兵告警）。
	s.ProbeModel = strings.TrimSpace(s.ProbeModel)
	// PoolInput 允许为空：节点池源完全由用户填写（公共推荐源见 README）。
	// ProxyInput/MirrorInput 是唯一真相源：输入框内容非空时以解析结果为准，
	// 数组字段仅用于回填空缺的输入框（兼容手改 config.json 只写数组的旧习惯）。
	if input := strings.TrimSpace(s.ProxyInput); input != "" {
		s.Proxies, _ = ParseProxyInput(input)
	} else if len(s.Proxies) > 0 {
		s.ProxyInput = strings.Join(s.Proxies, "\r\n")
	}
	if input := strings.TrimSpace(s.MirrorInput); input != "" {
		s.Mirrors, _ = parseMirrorList(input)
	} else if len(s.Mirrors) > 0 {
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
	setIfEmpty("PROXY_ABSORB_STREAMING", fmt.Sprintf("%v", s.AbsorbStreaming))
	setIfEmpty("PROXY_ABSORB_ATTEMPTS", fmt.Sprintf("%d", s.AbsorbAttempts))
	setIfEmpty("PROXY_DEEP_PROBE_INTERVAL", fmt.Sprintf("%d", s.DeepProbeMinutes*60000))
	setIfEmpty("PROXY_PROBE_CONCURRENCY", fmt.Sprintf("%d", s.ProbeConcurrency))
	if s.ProbeModel != "" {
		setIfEmpty("PROXY_PROBE_MODEL", s.ProbeModel)
	} // 空 = 不设置，config.go 回落 big-pickle
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
	if s.RaceWidth >= 2 {
		setIfEmpty("PROXY_RACE_WIDTH", fmt.Sprintf("%d", s.RaceWidth))
	}
	// 每供应商节点池出口开关：OpenCode 默认开、Cline 默认关（与两个
	// 字段的零值语义一致——旧 config.json 缺字段时行为不回归）。
	if s.OpenCodePoolOff {
		setIfEmpty("PROXY_POOL_OPENCODE", "0")
	} else {
		setIfEmpty("PROXY_POOL_OPENCODE", "1")
	}
	if s.ClinePoolEnabled {
		setIfEmpty("PROXY_POOL_CLINE", "1")
	} else {
		setIfEmpty("PROXY_POOL_CLINE", "0")
	}
	if s.PreferredRegions != "" {
		setIfEmpty("PROXY_PREFERRED_REGIONS", s.PreferredRegions)
	}
	setIfEmpty("GATEWAY_KEY", s.GatewayKey)
}

func setIfEmpty(key, value string) {
	if strings.TrimSpace(os.Getenv(key)) == "" {
		_ = os.Setenv(key, value)
	}
}
