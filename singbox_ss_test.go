package main

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

// TestIsAddrInUse 验证端口占用识别覆盖 Windows 真实错误码：
// Go 的 syscall.EADDRINUSE 常量在 Windows 上与 WSAEADDRINUSE(10048) 不相等，
// 旧的 errors.Is 判定永远为假，导致「占用重试」形同虚设、新实例秒变僵尸。
func TestIsAddrInUse(t *testing.T) {
	windowsReal := syscall.Errno(10048) // WSAEADDRINUSE 的真实数值
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil 不算占用", nil, false},
		{"unix 语义 EADDRINUSE", syscall.EADDRINUSE, true},
		{"Windows 真实码 10048 裸 Errno", windowsReal, true},
		{"真实形态：OpError 包 SyscallError 包 10048", &os.SyscallError{Syscall: "bind", Err: windowsReal}, true},
	}
	for _, tc := range cases {
		if got := isAddrInUse(tc.err); got != tc.want {
			t.Fatalf("%s: isAddrInUse=%v，期望 %v", tc.name, got, tc.want)
		}
	}
	// 字符串兜底：某些包装层会丢失 errno，只留原话
	msg := errors.New("listen tcp 127.0.0.1:13339: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.")
	if !isAddrInUse(msg) {
		t.Fatalf("字符串形态的端口占用未被识别")
	}
	if isAddrInUse(errors.New("connection refused")) {
		t.Fatalf("普通连接错误不应误判为端口占用")
	}
}

// TestNormalizeSSMethod 验证 SS 加密方式别名归一化与白名单（空串 = 应拒收）。
func TestNormalizeSSMethod(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"chacha20-poly1305", "chacha20-ietf-poly1305"}, // v2ray 系别名写法（本次事故主角）
		{"CHACHA20-IETF-POLY1305", "chacha20-ietf-poly1305"},
		{"chacha20-ietf-poly1305", "chacha20-ietf-poly1305"},
		{"aes-256-gcm", "aes-256-gcm"},
		{"aes-128-gcm", "aes-128-gcm"},
		{"2022-blake3-aes-256-gcm", "2022-blake3-aes-256-gcm"},
		{"rc4-md5", ""},     // 传统流加密：sing-box 不支持
		{"aes-256-cfb", ""}, // 同上
		{"", ""},
	}
	for _, tc := range cases {
		if got := normalizeSSMethod(tc.in); got != tc.want {
			t.Fatalf("normalizeSSMethod(%q) = %q，期望 %q", tc.in, got, tc.want)
		}
	}
}

// TestPrepareAdvancedNodeRejectsBadSS 验证坏节点在进桥接前就被单点剔除，
// 而不是拖垮整个 sing-box 实例重建。
func TestPrepareAdvancedNodeRejectsBadSS(t *testing.T) {
	bad := "ss://YWVzLTI1Ni1jZmI6cGFzcw==@1.2.3.4:8388#legacy-cfb" // aes-256-cfb:pass
	if _, err := prepareAdvancedNode(bad); err == nil {
		t.Fatalf("传统 cfb 加密应被拒收，实际通过")
	}
	good := "ss://Y2hhY2hhMjAtcG9seTEzMDU6cGFzcw==@1.2.3.4:8388#alias-form" // chacha20-poly1305:pass
	node, err := prepareAdvancedNode(good)
	if err != nil {
		t.Fatalf("chacha20-poly1305 别名应被放行: %v", err)
	}
	if node.method != "chacha20-ietf-poly1305" {
		t.Fatalf("别名未归一化: %q", node.method)
	}
}
