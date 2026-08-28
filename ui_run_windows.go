//go:build windows

package main

import (
	"log"
	"os"
	"os/exec"
)

// runUI 在 Windows 上打开桌面窗口。
// Walk 框架（v0.0.0-20210112085537）在特定 Windows 配置下首次进程启动
// 时可能出现 TTM_ADDTOOL 错误（tooltip 子类化失败），导致
// MainWindow.Create() 失败，窗口建不出来。
// 修复：窗口创建失败时自动重新启动自身进程（带 DSH_RESTARTED 标记防止无限循环）。
func runUI(handler *app, settings uiSettings, path string, shutdown func()) error {
	err := runGatewayUI(handler, settings, path, shutdown)
	if err == nil {
		return nil
	}
	// 如果已经是重启后的进程，不再重启，直接返回错误。
	if os.Getenv("DSH_RESTARTED") != "" {
		log.Printf("[GUI] 窗口创建失败（已重启过）: %v", err)
		return err
	}
	log.Printf("[GUI] 窗口创建失败，自动重启进程: %v", err)
	// 启动新进程，继承当前环境并加 DSH_RESTARTED 标记。
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), "DSH_RESTARTED=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if startErr := cmd.Start(); startErr != nil {
		log.Printf("[GUI] 重启失败: %v", startErr)
		return err
	}
	// 新进程已启动，立即退出当前进程以释放端口。
	// 不用 cmd.Wait()，因为那会等新进程退出（可能运行数小时）。
	os.Exit(0)
	return nil // unreachable
}
