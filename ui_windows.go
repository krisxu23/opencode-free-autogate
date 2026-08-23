//go:build windows

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/lxn/walk"
	dcl "github.com/lxn/walk/declarative"
	"github.com/lxn/win"
)

// 界面字体：walk 的控件字体不继承父容器，必须逐个显式设置；
// 中文用微软雅黑 UI 更精致，地址/日志/模型列表用等宽字体便于对齐。
var (
	uiFont       = dcl.Font{Family: "Microsoft YaHei UI", PointSize: 9}
	headlineFont = dcl.Font{Family: "Microsoft YaHei UI", PointSize: 12, Bold: true}
	monoFont     = dcl.Font{Family: "Consolas", PointSize: 9}
)

// 健康色：蓝=尚无请求，绿=正常，橙=最近被限流/上游错误/出现截断，红=最近请求失败。
var (
	colorIdle  = walk.RGB(0, 120, 215)
	colorOK    = walk.RGB(16, 124, 16)
	colorWarn  = walk.RGB(202, 80, 0)
	colorError = walk.RGB(196, 43, 28)
)

type gatewayUI struct {
	window   *walk.MainWindow
	app      *app
	settings uiSettings
	path     string

	statusLabel   *walk.Label
	headline      *walk.Label
	headlineText  string
	headlineColor walk.Color
	titleText     string
	modelsEdit    *walk.TextEdit
	logEdit       *walk.TextEdit
	proxyEdit     *walk.TextEdit
	mirrorEdit    *walk.TextEdit
	poolCheck     *walk.CheckBox
	poolEdit      *walk.TextEdit
	raceCheck     *walk.CheckBox
	poolLive      *walk.TextEdit
	apiEdit       *walk.LineEdit
	keyEdit       *walk.LineEdit
	firstByte     *walk.NumberEdit
	budget        *walk.NumberEdit
	outboundBox   *walk.ComboBox
	logCursor     int
	modelsSeen    string
	shownText     string
	poolLiveText  string
	statusText    string
	shutdownOnce  func()
}

var outboundChoices = []string{"走代理（失败自动直连兜底）", "仅直连"}

// runGatewayUI 创建主窗口并进入消息循环；返回时代表窗口已关闭。
func runGatewayUI(handler *app, settings uiSettings, path string, shutdown func()) error {
	ui := &gatewayUI{app: handler, settings: settings, path: path, shutdownOnce: shutdown}

	outboundIndex := 1
	if settings.Outbound == outboundProxy {
		outboundIndex = 0
	}
	apiBase := fmt.Sprintf("http://localhost:%d/openai/v1", settings.Port)

	// 从 exe 资源加载图标（go-winres 嵌入的 ID=1），用于标题栏和任务栏。
	var appIcon *walk.Icon
	if i, err := walk.NewIconFromResourceId(1); err == nil {
		appIcon = i
	}

	if err := (dcl.MainWindow{
		AssignTo: &ui.window,
		Title:    "opencode-free-autogate",
		Icon:     appIcon,
		Font:     uiFont,
		MinSize:  dcl.Size{Width: 760, Height: 460},
		Size:     dcl.Size{Width: 820, Height: 560},
		Layout:   dcl.VBox{Spacing: 8, Margins: dcl.Margins{Left: 10, Top: 10, Right: 10, Bottom: 10}},
		Children: []dcl.Widget{
			dcl.TabWidget{
				Font: uiFont,
				Pages: []dcl.TabPage{
					{
						Title:  "运行状态",
						Layout: dcl.VBox{Spacing: 8},
						Children: []dcl.Widget{
							dcl.GroupBox{
								Title:  "运行状态",
								Font:   uiFont,
								Layout: dcl.Grid{Columns: 3},
								Children: []dcl.Widget{
									dcl.Label{AssignTo: &ui.headline, Text: "● 启动中…", Font: headlineFont, TextColor: colorIdle, ColumnSpan: 3},

									dcl.Label{AssignTo: &ui.statusLabel, Text: "正在初始化…", Font: uiFont, ColumnSpan: 3},

									dcl.Label{Text: "API 地址:"},
									dcl.LineEdit{AssignTo: &ui.apiEdit, Text: apiBase, ReadOnly: true, Font: monoFont},
									dcl.PushButton{Text: "复制", Font: uiFont, MaxSize: dcl.Size{Width: 80}, OnClicked: func() {
										ui.copyText(ui.apiEdit.Text(), "API 地址")
									}},

									dcl.Label{Text: "默认 Key:"},
									dcl.LineEdit{AssignTo: &ui.keyEdit, Text: settings.GatewayKey, ReadOnly: true, Font: monoFont},
									dcl.PushButton{Text: "复制", Font: uiFont, MaxSize: dcl.Size{Width: 80}, OnClicked: func() {
										ui.copyText(ui.keyEdit.Text(), "默认 Key")
									}},

									dcl.Label{
										Text:       "Key 填任意非空字符串均可通过（当前未启用校验）。Anthropic 客户端把地址末尾换成 /anthropic/v1。",
										ColumnSpan: 3,
									},
								},
							},

							dcl.GroupBox{
								Title:  "实时免费模型（上游拉取，可直接复制）",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.TextEdit{
										AssignTo: &ui.modelsEdit,
										ReadOnly: true,
										VScroll:  true,
										MinSize:  dcl.Size{Height: 96},
										Font:     monoFont,
										Text:     "正在获取…",
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.PushButton{Text: "复制全部模型名", Font: uiFont, OnClicked: func() {
												ui.copyText(ui.modelsEdit.Text(), "模型列表")
											}},
											dcl.HSpacer{},
										},
									},
								},
							},

							dcl.GroupBox{
								Title:  "实时在线节点",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.Label{Text: "探活通过自动加入、失效自动移除；手动节点永不自动移除:"},
									dcl.TextEdit{
										AssignTo: &ui.poolLive,
										ReadOnly: true,
										VScroll:  true,
										MinSize:  dcl.Size{Height: 120},
										Font:     monoFont,
									},
								},
							},
						},
					},

					{
						Title:  "设置",
						Layout: dcl.VBox{Spacing: 8},
						Children: []dcl.Widget{
							dcl.GroupBox{
								Title:  "出站与超时",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "出站模式:"},
											dcl.ComboBox{
												AssignTo:     &ui.outboundBox,
												Model:        outboundChoices,
												CurrentIndex: outboundIndex,
												MinSize:      dcl.Size{Width: 220},
											},
											dcl.HSpacer{},
											dcl.Label{Text: "首字节超时"},
											dcl.NumberEdit{AssignTo: &ui.firstByte, Value: float64(settings.FirstByteSeconds), MinValue: 3, MaxValue: 600, Decimals: 0, MaxSize: dcl.Size{Width: 70}},
											dcl.Label{Text: "秒     总预算"},
											dcl.NumberEdit{AssignTo: &ui.budget, Value: float64(settings.BudgetSeconds), MinValue: 5, MaxValue: 1800, Decimals: 0, MaxSize: dcl.Size{Width: 80}},
											dcl.Label{Text: "秒"},
										},
									},
									dcl.Label{Text: "代理节点（一行一个；支持 socks5/http、vless://、vmess://、trojan://、ss://、hysteria2://(hy2)、tuic:// 分享链接；手动节点不会被自动删除）:"},
									dcl.TextEdit{
										AssignTo: &ui.proxyEdit,
										Text:     settings.ProxyInput,
										VScroll:  true,
										MinSize:  dcl.Size{Height: 96},
									},
									dcl.CheckBox{
										AssignTo: &ui.raceCheck,
										Text:     "并行竞速：同一请求同时发往多个出口（手动+在线池+直连），最快返回者胜出，无需再设超长超时",
										Checked:  settings.RaceEnabled,
									},
									dcl.Label{Text: "上游镜像（一行一个，请求间轮换；留空只用 opencode.ai）:"},
									dcl.TextEdit{
										AssignTo: &ui.mirrorEdit,
										Text:     settings.MirrorInput,
										VScroll:  true,
										MinSize:  dcl.Size{Height: 64},
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.PushButton{Text: "保存并重启", Font: uiFont, OnClicked: ui.onSave},
											dcl.PushButton{Text: "仅检查格式", Font: uiFont, OnClicked: ui.onValidate},
											dcl.PushButton{Text: "打开配置目录", Font: uiFont, OnClicked: ui.onOpenFolder},
											dcl.HSpacer{},
										},
									},
								},
							},

							dcl.GroupBox{
								Title:  "在线节点池",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.CheckBox{
										AssignTo: &ui.poolCheck,
										Text:     "自动拉取在线节点并探活（每轮用真实 opencode.ai 请求测活，健康节点实时入池、失效自动移除，无需重启）",
										Checked:  settings.PoolEnabled,
									},
									dcl.Label{Text: "节点源链接（一行一个；支持 socks5/http 文本列表、amux JSON、base64 订阅链接（机场订阅，自动解码出 vless/vmess/hy2 等节点）、明文分享链接；github 页面链接自动转 raw）:"},
									dcl.TextEdit{
										AssignTo: &ui.poolEdit,
										Text:     settings.PoolInput,
										VScroll:  true,
										MinSize:  dcl.Size{Height: 96},
									},
								},
							},
						},
					},

					{
						Title:  "实时日志",
						Layout: dcl.VBox{Spacing: 6},
						Children: []dcl.Widget{
							dcl.TextEdit{
								AssignTo: &ui.logEdit,
								ReadOnly: true,
								VScroll:  true,
								HScroll:  true,
								MinSize:  dcl.Size{Height: 320},
								Font:     monoFont,
							},
						},
					},
				},
			},
		},
	}).Create(); err != nil {
		return err
	}

	ui.window.Closing().Attach(func(canceled *bool, reason walk.CloseReason) {
		if ui.shutdownOnce != nil {
			ui.shutdownOnce()
		}
	})

	stopTicker := make(chan struct{})
	ticker := time.NewTicker(300 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stopTicker:
				return
			case <-ticker.C:
				ui.window.Synchronize(ui.tick)
			}
		}
	}()
	defer close(stopTicker)

	go ui.modelWatcher()

	ui.window.Show()

	// Show 之后手动刷一次日志，确保窗口已可见时日志立刻出现。
	ui.tick()

	ui.window.Run()
	return nil
}

// tick 刷新日志与状态，必须在 UI 线程调用。
func (ui *gatewayUI) tick() {
	ui.pumpLogs()
	ui.refreshHeadline()
	ui.refreshStatus()
	ui.refreshPoolLive()
}

// refreshHeadline 更新顶部大号状态行与窗口标题，一眼读出全局状态；
// 颜色反映最近一次请求结果，内容没变化就不重绘。
func (ui *gatewayUI) refreshHeadline() {
	gw := ui.app.gateway
	color := colorOK
	if truncAt := gw.lastTruncation.Load(); truncAt != 0 && time.Since(time.Unix(0, truncAt)) < 2*time.Minute {
		color = colorWarn
	} else {
		switch status := gw.lastStatus.Load(); {
		case status == 0:
			color = colorIdle
		case status >= 200 && status < 400:
			color = colorOK
		case status == http.StatusTooManyRequests || status >= 500:
			color = colorWarn
		default:
			color = colorError
		}
	}
	modelCount := 0
	if ui.modelsSeen != "" {
		modelCount = strings.Count(ui.modelsSeen, "\r\n") + 1
	}
	race := "关"
	if gw.cfg.raceEnabled {
		race = "开"
	}
	text := fmt.Sprintf("● 运行中   :%d   出口 %d   模型 %d   竞速 %s",
		gw.cfg.port, gw.customCount(), modelCount, race)
	if text != ui.headlineText || color != ui.headlineColor {
		ui.headlineText = text
		ui.headlineColor = color
		ui.headline.SetText(text)
		ui.headline.SetTextColor(color)
	}
	if title := "opencode-free-autogate ● 运行中"; title != ui.titleText {
		ui.titleText = title
		_ = ui.window.SetTitle(title)
	}
}

// refreshPoolLive 把当前节点池明细同步到界面；内容没变化就不重绘，几乎零开销。
func (ui *gatewayUI) refreshPoolLive() {
	if ui.poolLive == nil {
		return
	}
	slots := ui.app.gateway.customSnapshot()
	var text string
	if len(slots) == 0 {
		text = "（暂无在线节点，等待探活…）"
	} else {
		lines := make([]string, 0, len(slots))
		for _, s := range slots {
			if ui.app.gateway.isManual(s.addr) {
				lines = append(lines, s.addr+"　（手动，不自动移除）")
			} else {
				lines = append(lines, s.addr)
			}
		}
		text = fmt.Sprintf("共 %d 个：\r\n%s", len(slots), strings.Join(lines, "\r\n"))
	}
	if text == ui.poolLiveText {
		return
	}
	ui.poolLiveText = text
	ui.poolLive.SetText(text)
}

func (ui *gatewayUI) pumpLogs() {
	lines, cursor := uiLog.Since(ui.logCursor)
	if len(lines) == 0 {
		return
	}
	ui.logCursor = cursor
	// 最新的日志放最上面（倒序显示），这样新日志出现时不需要滚动。
	newText := strings.Join(lines, "\r\n")
	if ui.shownText == "" {
		ui.shownText = newText
	} else {
		ui.shownText = newText + "\r\n" + ui.shownText
	}
	// 超长时从尾部（最旧的内容）截断。
	if len(ui.shownText) > 80000 {
		ui.shownText = ui.shownText[:50000]
	}
	hwnd := ui.logEdit.Handle()
	win.SendMessage(hwnd, win.WM_SETREDRAW, 0, 0)
	ui.logEdit.SetText(ui.shownText)
	win.SendMessage(hwnd, win.WM_SETREDRAW, 1, 0)
	win.UpdateWindow(hwnd)
}

func (ui *gatewayUI) refreshStatus() {
	gw := ui.app.gateway
	mode := "仅直连"
	if ui.settings.Outbound == outboundProxy {
		mode = fmt.Sprintf("走代理 %d/%d 在线", gw.customCount(), len(ui.settings.Proxies))
	}
	poolInfo := ""
	if ui.settings.PoolEnabled {
		sources := parsePoolSources(ui.settings.PoolInput)
		poolInfo = fmt.Sprintf("     节点池 %d 源自动探活", len(sources))
	}
	text := fmt.Sprintf("端口 %d     %s%s     上游 %d 个轮换",
		gw.cfg.port, mode, poolInfo, len(gw.cfg.upstreamPool()))
	// 内容没变化就不重绘，避免高频 SetText 打扰 UI 线程。
	if text == ui.statusText {
		return
	}
	ui.statusText = text
	ui.statusLabel.SetText(text)
}

// modelWatcher 后台定时拉取模型列表并同步到界面。
func (ui *gatewayUI) modelWatcher() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ids := ui.app.gateway.modelUpstreamIDs(ctx)
		cancel()
		if len(ids) > 0 {
			text := strings.Join(ids, "\r\n")
			if text != ui.modelsSeen {
				ui.modelsSeen = text
				ui.window.Synchronize(func() { ui.modelsEdit.SetText(text) })
			}
		}
		time.Sleep(60 * time.Second)
	}
}

func (ui *gatewayUI) copyText(text, label string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	if err := walk.Clipboard().SetText(text); err != nil {
		walk.MsgBox(ui.window, "复制失败", err.Error(), walk.MsgBoxIconError)
		return
	}
	log.Printf("[界面] 已复制%s", label)
}

// collect 读取界面输入，返回规整后的配置与校验报告。
func (ui *gatewayUI) collect() (uiSettings, string) {
	next := ui.settings
	next.Outbound = outboundDirect
	if ui.outboundBox.CurrentIndex() == 0 {
		next.Outbound = outboundProxy
	}
	next.FirstByteSeconds = int(ui.firstByte.Value())
	next.BudgetSeconds = int(ui.budget.Value())
	next.ProxyInput = ui.proxyEdit.Text()
	next.MirrorInput = ui.mirrorEdit.Text()
	next.PoolEnabled = ui.poolCheck.Checked()
	next.PoolInput = ui.poolEdit.Text()
	next.RaceEnabled = ui.raceCheck.Checked()

	proxies, proxyErrors := ParseProxyInput(next.ProxyInput)
	mirrors, mirrorErrors := parseMirrorList(next.MirrorInput)
	next.Proxies = proxies
	next.Mirrors = mirrors

	var report strings.Builder
	fmt.Fprintf(&report, "代理节点：可用 %d 个", len(proxies))
	if len(proxyErrors) > 0 {
		fmt.Fprintf(&report, "，无法识别 %d 个\r\n", len(proxyErrors))
		for _, item := range proxyErrors {
			fmt.Fprintf(&report, "    · %s\r\n", item.Error())
		}
	} else {
		report.WriteString("\r\n")
	}
	if next.PoolEnabled {
		sources := parsePoolSources(next.PoolInput)
		fmt.Fprintf(&report, "在线节点池：开启，%d 个源（后台自动探活入池）\r\n", len(sources))
	} else {
		report.WriteString("在线节点池：关闭\r\n")
	}
	if next.RaceEnabled {
		report.WriteString("并行竞速：开启（最快出口胜出）\r\n")
	} else {
		report.WriteString("并行竞速：关闭\r\n")
	}
	fmt.Fprintf(&report, "上游镜像：可用 %d 个", len(mirrors))
	if len(mirrorErrors) > 0 {
		fmt.Fprintf(&report, "，无法识别 %d 个\r\n", len(mirrorErrors))
		for _, message := range mirrorErrors {
			fmt.Fprintf(&report, "    · %s\r\n", message)
		}
	} else {
		report.WriteString("\r\n")
	}
	if next.Outbound == outboundProxy && len(proxies) == 0 {
		report.WriteString("\r\n提示：选了“走代理”但没有可用节点，实际仍会直连。")
	}
	return next.normalized(), report.String()
}

func (ui *gatewayUI) onValidate() {
	_, report := ui.collect()
	walk.MsgBox(ui.window, "格式检查", report, walk.MsgBoxIconInformation)
}

func (ui *gatewayUI) onSave() {
	next, report := ui.collect()
	if err := next.save(ui.path); err != nil {
		walk.MsgBox(ui.window, "保存失败", err.Error(), walk.MsgBoxIconError)
		return
	}
	ui.settings = next
	message := report + "\r\n配置已保存到 config.json。立即重启程序使其生效？"
	if walk.MsgBox(ui.window, "保存成功", message, walk.MsgBoxIconQuestion|walk.MsgBoxYesNo) != walk.DlgCmdYes {
		return
	}
	if err := restartSelf(); err != nil {
		walk.MsgBox(ui.window, "重启失败", "请手动关闭后重新打开程序。\r\n"+err.Error(), walk.MsgBoxIconError)
		return
	}
	ui.window.Close()
}

func (ui *gatewayUI) onOpenFolder() {
	dir := filepath.Dir(ui.path)
	if err := exec.Command("explorer.exe", dir).Start(); err != nil {
		log.Printf("[界面] 打开目录失败: %v", err)
	}
}

// restartSelf 以相同参数重新启动自身，让新配置生效。
func restartSelf() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Dir = filepath.Dir(exe)
	cmd.Env = restartEnv()
	return cmd.Start()
}

// restartEnv 剔除由 config.json 派生的环境变量，避免旧值覆盖新配置。
func restartEnv() []string {
	managed := map[string]struct{}{
		"PORT":                     {},
		"PROXY_ORDER":              {},
		"CUSTOM_PROXIES":           {},
		"MIRROR_URLS":              {},
		"PROXY_FIRST_BYTE_TIMEOUT": {},
		"HARD_TIMEOUT":             {},
		"PROXY_LIST_URLS":          {}, // 节点池源链接
		"PROXY_RACE":               {}, // 并行竞速开关
		"PROXY_RACE_WIDTH":         {}, // 竞速并发宽度
	}
	env := os.Environ()
	kept := make([]string, 0, len(env))
	for _, entry := range env {
		name := entry
		if i := strings.Index(entry, "="); i >= 0 {
			name = entry[:i]
		}
		if _, skip := managed[strings.ToUpper(name)]; skip {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
}
