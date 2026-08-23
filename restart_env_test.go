package main

import (
	"strings"
	"testing"
)

// 回归：「保存并重启」必须剔除所有由 config.json 派生的环境变量，
// 否则旧值会覆盖新配置（曾导致节点源链接改了却不生效）。
func TestRestartEnvStripsAllManagedVars(t *testing.T) {
	t.Setenv("PORT", "19999")
	t.Setenv("PROXY_ORDER", "custom")
	t.Setenv("CUSTOM_PROXIES", "socks5://1.2.3.4:1080")
	t.Setenv("MIRROR_URLS", "https://a.example/zen")
	t.Setenv("PROXY_FIRST_BYTE_TIMEOUT", "30000")
	t.Setenv("HARD_TIMEOUT", "180000")
	t.Setenv("PROXY_LIST_URLS", "https://old.example/sub")
	t.Setenv("PROXY_RACE", "1")
	t.Setenv("PROXY_RACE_WIDTH", "8")

	env := restartEnv()
	joined := strings.Join(env, "\n")
	for _, key := range []string{
		"PORT=", "PROXY_ORDER=", "CUSTOM_PROXIES=", "MIRROR_URLS=",
		"PROXY_FIRST_BYTE_TIMEOUT=", "HARD_TIMEOUT=",
		"PROXY_LIST_URLS=", "PROXY_RACE=", "PROXY_RACE_WIDTH=",
	} {
		if strings.Contains(joined, key) {
			t.Fatalf("%s 未被剔除，旧值会在重启后覆盖新配置", key)
		}
	}
}
