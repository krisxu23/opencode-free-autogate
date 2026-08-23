//go:build !windows

package main

import "errors"

// runUI 在非 Windows 平台上没有桌面界面实现，调用方会退回控制台模式。
func runUI(handler *app, settings uiSettings, path string, shutdown func()) error {
	return errors.New("桌面界面仅支持 Windows")
}
