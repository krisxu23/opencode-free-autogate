//go:build windows

package main

import (
	"fmt"
	"log"
	"os/exec"
	"strings"

	m365pkg "github.com/krisxu23/opencode-free-autogate/m365"
)

// M365 账号页的后台逻辑：PKCE 三步授权（发起→粘贴回调→确认）与账号列表管理。
// 耗时操作（授权码交换）在后台协程跑，控件读写经 window.Synchronize 回 UI 线程。

func (ui *gatewayUI) m365SetStatus(text string) {
	if ui.window == nil {
		return
	}
	ui.window.Synchronize(func() {
		if ui.m365Status != nil {
			_ = ui.m365Status.SetText(text)
		}
	})
}

func (ui *gatewayUI) m365StartAuth() {
	eng, err := ensureM365()
	if err != nil {
		ui.m365SetStatus("M365 引擎初始化失败：" + err.Error())
		return
	}
	ch, err := eng.StartAuth()
	if err != nil {
		ui.m365SetStatus("发起授权失败：" + err.Error())
		return
	}
	ui.m365PendingState = ch.State
	ui.m365SetStatus("已打开微软登录页：登录完成后复制地址栏 URL 回来粘贴。")
	_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", ch.AuthURL).Start()
	log.Printf("[M365] 授权已发起")
}

func (ui *gatewayUI) m365ConfirmAdd() {
	var callback string
	ui.window.Synchronize(func() {
		if ui.m365Callback != nil {
			callback = ui.m365Callback.Text()
		}
	})
	if strings.TrimSpace(callback) == "" {
		ui.m365SetStatus("先粘贴回调地址再确认。")
		return
	}
	eng, err := ensureM365()
	if err != nil {
		ui.m365SetStatus("M365 引擎初始化失败：" + err.Error())
		return
	}
	ui.m365SetStatus("正在交换授权码…")
	acc, err := eng.CompleteAuth(ui.m365PendingState, strings.TrimSpace(callback))
	if err != nil {
		log.Printf("[M365] 授权失败: %v", err)
		ui.m365SetStatus("授权失败：" + err.Error())
		return
	}
	ui.m365PendingState = ""
	ui.m365SetStatus("授权成功：" + acc.Email)
	log.Printf("[M365] 账号导入成功: %s", acc.Email)
	go ui.m365RefreshAccounts()
}

func (ui *gatewayUI) m365RefreshAccounts() {
	eng, err := ensureM365()
	if err != nil {
		ui.m365SetStatus("M365 引擎初始化失败：" + err.Error())
		return
	}
	accounts := eng.Accounts()
	var sb strings.Builder
	fmt.Fprintf(&sb, "共 %d 个账号（库：%s）\r\n\r\n", len(accounts), m365StorePath())
	for i, acc := range accounts {
		id := acc.ID
		if id == "" {
			id = acc.Email
		}
		fmt.Fprintf(&sb, "%d. %s\r\n   状态: %s  ID: %s\r\n\r\n", i+1, acc.Email, acc.Status, id)
	}
	if len(accounts) == 0 {
		sb.WriteString("暂无账号。按上方 3 步添加。\r\n")
	}
	if !m365pkg.MasterKeyOK() {
		sb.WriteString("警告：未设置 M365_MASTER_KEY，刷新令牌将明文落盘。\r\n")
	}
	text := sb.String()
	ui.window.Synchronize(func() {
		if ui.m365Accounts != nil {
			_ = ui.m365Accounts.SetText(text)
		}
	})
}

func (ui *gatewayUI) m365DeleteAccount() {
	var target string
	ui.window.Synchronize(func() {
		if ui.m365DelEdit != nil {
			target = strings.TrimSpace(ui.m365DelEdit.Text())
		}
	})
	if target == "" {
		ui.m365SetStatus("先填写要删除的账号 ID 或邮箱。")
		return
	}
	eng, err := ensureM365()
	if err != nil {
		ui.m365SetStatus("M365 引擎初始化失败：" + err.Error())
		return
	}
	id := ""
	for _, acc := range eng.Accounts() {
		if acc.ID == target || acc.Email == target {
			id = acc.ID
			if id == "" {
				id = acc.Email
			}
			break
		}
	}
	if id == "" {
		ui.m365SetStatus("找不到账号：" + target)
		return
	}
	if err := eng.DeleteAccount(id); err != nil {
		ui.m365SetStatus("删除失败：" + err.Error())
		return
	}
	log.Printf("[M365] 账号已删除: %s", target)
	ui.m365SetStatus("已删除：" + target)
	go ui.m365RefreshAccounts()
}
