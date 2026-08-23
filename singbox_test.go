package main

import (
	"context"
	"strings"
	"testing"
)

// 回归：首次构建时 advBridge 为 nil，不得触发空指针（旧版本在这里崩溃，
// 导致 GUI 双击后无窗口直接退出）。
func TestEnsureAdvancedBridgeFirstBuildNoPanic(t *testing.T) {
	gw := newGateway(config{})
	// 故意用无法连通但格式合法的链接：只验证不 panic、桥接能建立。
	links := []string{
		"vless://b831381d-6324-4d53-ad4f-8cda48b30811@127.0.0.1:1?security=tls&sni=a.com",
	}
	gw.manualAdv = links
	gw.ensureAdvancedBridge(context.Background(), links)
	defer gw.Close()

	if gw.advBridge == nil {
		t.Fatal("首次构建后 advBridge 不应为空")
	}
	if len(gw.advBridge.Links) != 1 {
		t.Fatalf("应映射 1 个节点，实际 %d", len(gw.advBridge.Links))
	}
	for local := range gw.advBridge.Mapping {
		if !strings.HasPrefix(local, "127.0.0.1:") {
			t.Fatalf("本地地址异常: %q", local)
		}
	}
}

// 回归：没有任何高级节点时不得 panic，也不应创建实例。
func TestEnsureAdvancedBridgeEmptyNoPanic(t *testing.T) {
	gw := newGateway(config{})
	gw.ensureAdvancedBridge(context.Background(), nil)
	if gw.advBridge != nil {
		t.Fatal("无节点时不应创建 sing-box 实例")
	}
	gw.Close() // 对 nil 桥接调用 Close 也必须安全
}

// 重复调用同一组链接不应重建实例（幂等）。
func TestEnsureAdvancedBridgeIdempotent(t *testing.T) {
	gw := newGateway(config{})
	links := []string{"trojan://pass@127.0.0.1:2?sni=a.com"}
	gw.manualAdv = links
	gw.ensureAdvancedBridge(context.Background(), links)
	defer gw.Close()
	first := gw.advBridge

	gw.ensureAdvancedBridge(context.Background(), links)
	if gw.advBridge != first {
		t.Fatal("同一组链接不应重建实例")
	}
}
