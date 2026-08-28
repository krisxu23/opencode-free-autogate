//go:build windows

package main

// runUI 在 Windows 上打开桌面窗口。
func runUI(handler *app, settings uiSettings, path string, shutdown func()) error {
	return runGatewayUI(handler, settings, path, shutdown)
}
