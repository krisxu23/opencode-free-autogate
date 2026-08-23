package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

type requestIDs struct {
	Session string
	Request string
	Project string
}

// deriveRequestIDs 生成上游 OpenCode 协议所需的会话、请求与项目标识。
// 会话 ID 优先取客户端显式标识，否则用第一条用户消息生成稳定哈希，
// 保证同一段多轮对话在历史增长时仍映射到同一个会话。
func deriveRequestIDs(headers http.Header, body map[string]any) requestIDs {
	signal := firstString(
		headers.Get("x-opencode-session"),
		headers.Get("x-session-id"),
		headers.Get("conversation-id"),
		stringAt(body, "conversation_id"),
		stringAt(body, "metadata", "session_id"),
	)
	if signal == "" {
		signal = conversationSeed(body)
	}
	if signal == "" {
		signal = stringAt(body, "previous_response_id")
	}
	if signal == "" || signal == "{}" {
		signal = randomID("fallback", 16)
	}
	projectSignal := firstString(headers.Get("x-opencode-project"), stringAt(body, "metadata", "project_id"))
	if projectSignal == "" {
		projectSignal = "zenfreegate:default-project"
	}
	return requestIDs{
		Session: stableID("ses", signal),
		Request: randomID("req", 16),
		Project: stableID("prj", projectSignal),
	}
}

func conversationSeed(body map[string]any) string {
	if input, ok := body["input"].(string); ok && input != "" {
		return input
	}
	for _, field := range []string{"messages", "input"} {
		items, _ := body[field].([]any)
		for _, raw := range items {
			item, ok := raw.(map[string]any)
			if !ok || stringAt(item, "role") != "user" {
				continue
			}
			encoded, _ := json.Marshal(item["content"])
			if len(encoded) > 0 && string(encoded) != "null" {
				return string(encoded)
			}
		}
	}
	return ""
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + value))
	return prefix + "_" + hex.EncodeToString(sum[:12])
}

func randomID(prefix string, size int) string {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return prefix + "_" + fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buf)
}

func firstString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func stringAt(value map[string]any, path ...string) string {
	current := any(value)
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = object[key]
	}
	result, _ := current.(string)
	return result
}

// fallbackOpencodeVersion 是 UA 同步失败时的兜底版本号。
const fallbackOpencodeVersion = "1.18.18"

var uaVersion atomic.Value // string：从 npm 同步的官方 CLI 最新版本

func opencodeUserAgent() string {
	version := fallbackOpencodeVersion
	if v, ok := uaVersion.Load().(string); ok && v != "" {
		version = v
	}
	return fmt.Sprintf("opencode/%s (%s %s; %s)", version, runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// startUASync 每天从 npm registry 拉一次 opencode CLI 的最新版本号，
// 让 UA 跟着上游发版走，避免固定版本号日久成为识别特征。拉取失败静默
// 保留上次结果；启动后延迟几秒再拉，不与预热抢路。
func startUASync(ctx context.Context) {
	go func() {
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return
		}
		syncUAOnce()
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				syncUAOnce()
			}
		}
	}()
}

func syncUAOnce() {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://registry.npmjs.org/opencode-ai/latest")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var payload struct {
		Version string `json:"version"`
	}
	if json.NewDecoder(resp.Body).Decode(&payload) != nil || payload.Version == "" {
		return
	}
	if payload.Version != fallbackOpencodeVersion {
		uaVersion.Store(payload.Version)
		log.Printf("[指纹] UA 版本已同步到 opencode/%s", payload.Version)
	}
}
