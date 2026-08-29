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
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

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

	banner            *walk.CustomWidget // 顶部自绘信息卡（应用名/运行时长/健康状态灯）
	start             time.Time          // 界面启动时刻，用于运行时长显示
	usageLabel        *walk.Label        // 今日用量统计行
	usageText         string
	statusLabel       *walk.Label
	headline          *walk.Label
	headlineText      string
	headlineColor     walk.Color
	titleText         string
	modelsEdit        *walk.TextEdit
	logEdit           *walk.TextEdit
	proxyEdit         *walk.TextEdit
	mirrorEdit        *walk.TextEdit
	poolCheck         *walk.CheckBox
	poolEdit          *walk.TextEdit
	raceCheck         *walk.CheckBox
	raceWidth         *walk.NumberEdit // 竞速并发路数
	poolLive          *walk.TextEdit
	apiEdit           *walk.LineEdit
	keyEdit           *walk.LineEdit
	firstByte         *walk.NumberEdit
	budget            *walk.NumberEdit
	absorbCheck       *walk.CheckBox   // 吸收模式开关
	absorbAttempt     *walk.NumberEdit // 吸收模式最大尝试次数
	clineAccountEdit  *walk.TextEdit   // Cline 账号列表
	opencodePoolCheck *walk.CheckBox   // OpenCode 供应商节点池出口开关
	clinePoolCheck    *walk.CheckBox   // Cline 供应商节点池出口开关
	regionEdit        *walk.LineEdit   // 地区偏好（US,JP,SG）
	ipinfoEdit        *walk.LineEdit   // IPinfo token
	abuseEdit         *walk.LineEdit   // AbuseIPDB key
	dqsEdit           *walk.LineEdit   // Spamhaus DQS key
	deepProbe         *walk.NumberEdit
	probeConc         *walk.NumberEdit // 检测并发路数（初检/深检共用）
	probeModelBox     *walk.ComboBox
	outboundBox       *walk.ComboBox
	logCursor         int
	modelsSeen        string
	shownText         string
	poolLiveText      string
	statusText        string
	shutdownOnce      func()
}

var outboundChoices = []string{"走代理（失败自动直连兜底）", "仅直连"}

// runGatewayUI 创建主窗口并进入消息循环；返回时代表窗口已关闭。
func runGatewayUI(handler *app, settings uiSettings, path string, shutdown func()) error {
	ui := &gatewayUI{app: handler, settings: settings, path: path, shutdownOnce: shutdown}
	ui.start = time.Now()

	// 强制绑定到当前 OS 线程：Walk 的 initWindowWithCfg 内部也会调用
	// runtime.LockOSThread()，但那是在 CreateWindowEx 之前才执行的 atomic
	// 分支里——如果 Go 调度器在此之前把 goroutine 调走，后续 Win32 调用可能
	// 在错误的线程上执行，导致 TTM_ADDTOOL 等消息失败。
	runtime.LockOSThread()

	// 预注册 Common Controls（含 tooltip 类）：Walk 的 initWindowWithCfg 调用
	// InitCommonControlsEx 时缺少 ICC_STANDARD_CLASSES，导致 tooltip 子类化
	// 在首次进程启动时 TTM_ADDTOOL 失败。提前注册可绕过此问题。
	var icc win.INITCOMMONCONTROLSEX
	icc.DwSize = uint32(unsafe.Sizeof(icc))
	icc.DwICC = 0x00004000 // ICC_STANDARD_CLASSES
	win.InitCommonControlsEx(&icc)

	outboundIndex := 1
	if settings.Outbound == outboundProxy {
		outboundIndex = 0
	}
	apiBase := fmt.Sprintf("http://localhost:%d/v1", settings.Port)

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
		Layout:   dcl.VBox{Spacing: 10, Margins: dcl.Margins{Left: 14, Top: 12, Right: 14, Bottom: 12}},
		Children: []dcl.Widget{
			dcl.CustomWidget{
				AssignTo:  &ui.banner,
				MinSize:   dcl.Size{Height: 66},
				MaxSize:   dcl.Size{Height: 66},
				PaintMode: dcl.PaintBuffered,
				Paint:     ui.paintBanner,
			},
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

									dcl.Label{Text: "今日用量:", Font: uiFont},
									dcl.Label{AssignTo: &ui.usageLabel, Text: "—", Font: monoFont, ColumnSpan: 2},

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
										Text:       "设置 GATEWAY_KEY 后启用校验（格式 sk-xxx）。未设置时自动生成随机 Key。兼容路径：/vscode/{key}/v1/chat/completions。",
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
											dcl.PushButton{Text: "刷新 Cline 模型", Font: uiFont, OnClicked: func() {
												go func() {
													refreshClineModels()
													ui.window.Synchronize(func() {
														ui.modelsSeen = ""
													})
												}()
											}},
											dcl.HSpacer{},
										},
									},
								},
							},

							dcl.GroupBox{
								Title:  "正式节点（复检合格）",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.Label{Text: "以下节点已通过初检＋复检双重验证，可放心使用；手动节点永不自动移除:"},
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
						Title:  "OpenCode",
						Layout: dcl.VBox{Spacing: 8},
						Children: []dcl.Widget{
							dcl.GroupBox{
								Title:  "OpenCode 供应商（opencode.ai/zen · OpenAI/Anthropic/Codex 兼容）",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.CheckBox{
										AssignTo: &ui.opencodePoolCheck,
										Text:     "启用节点池 IP 出口（多 IP 竞速/轮询，失败自动换出口；关闭 = 全部直连）",
										Checked:  !settings.OpenCodePoolOff,
									},
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
										},
									},
									dcl.CheckBox{
										AssignTo: &ui.raceCheck,
										Text:     "并行竞速：同一请求同时发往多个出口（手动+在线池+直连），最快返回者胜出，无需再设超长超时",
										Checked:  settings.RaceEnabled,
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "竞速并发路数（同时发往几个出口，2-32）:"},
											dcl.NumberEdit{AssignTo: &ui.raceWidth, Value: float64(settings.RaceWidth), MinValue: 2, MaxValue: 32, Decimals: 0, MaxSize: dcl.Size{Width: 60}},
											dcl.HSpacer{},
										},
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
											dcl.CheckBox{AssignTo: &ui.absorbCheck, Text: "吸收重试（上游截断/失败时网关内自动换道重试，客户端只见等待）", Checked: settings.AbsorbStreaming},
											dcl.Label{Text: "最多"},
											dcl.NumberEdit{AssignTo: &ui.absorbAttempt, Value: float64(settings.AbsorbAttempts), MinValue: 1, MaxValue: 50, Decimals: 0, MaxSize: dcl.Size{Width: 60}},
											dcl.Label{Text: "次（总预算 10 分钟）"},
											dcl.HSpacer{},
										},
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "深检间隔"},
											dcl.NumberEdit{AssignTo: &ui.deepProbe, Value: float64(settings.DeepProbeMinutes), MinValue: 10, MaxValue: 1440, Decimals: 0, MaxSize: dcl.Size{Width: 70}},
											dcl.Label{Text: "分     检测并发"},
											dcl.NumberEdit{AssignTo: &ui.probeConc, Value: float64(settings.ProbeConcurrency), MinValue: 1, MaxValue: 128, Decimals: 0, MaxSize: dcl.Size{Width: 60}},
											dcl.Label{Text: "路（节点探活/深检共用）"},
											dcl.HSpacer{},
										},
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "深检模型（真实对话探测专用；列表来自上游实时拉取）:"},
											dcl.ComboBox{
												AssignTo: &ui.probeModelBox,
												Model:    probeModelSeed(settings.ProbeModel),
												MinSize:  dcl.Size{Width: 260},
											},
											dcl.HSpacer{},
										},
									},
								},
							},
						},
					},

					{
						Title:  "Cline",
						Layout: dcl.VBox{Spacing: 8},
						Children: []dcl.Widget{
							dcl.GroupBox{
								Title:  "Cline 供应商（api.cline.bot 反代 · OAuth 账号制）",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.CheckBox{
										AssignTo: &ui.clinePoolCheck,
										Text:     "启用节点池 IP 出口（最多 3 个池出口按表现轮询 + 直连兜底；默认关闭 = 直连）",
										Checked:  settings.ClinePoolEnabled,
									},
									dcl.Label{Text: "开启后 Cline 聊天请求与 OpenCode 共用同一套出口记账：坏出口两家一起避开。"},
								},
							},
							dcl.GroupBox{
								Title:  "Cline 账号管理",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.Label{Text: "Cline 免费模型通过 OAuth 账号访问。点击「导入账号」添加 Cline 账号。"},
									dcl.TextEdit{
										AssignTo: &ui.clineAccountEdit,
										ReadOnly: true,
										VScroll:  true,
										MinSize:  dcl.Size{Height: 200},
										Font:     monoFont,
										Text:     "正在加载…",
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.PushButton{Text: "导入账号 (OAuth)", Font: uiFont, OnClicked: func() {
												go ui.clineImportAccount()
											}},
											dcl.PushButton{Text: "刷新列表", Font: uiFont, OnClicked: func() {
												ui.refreshClineAccounts()
											}},
											dcl.HSpacer{},
										},
									},
								},
							},
							dcl.Label{Text: "提示：Model 字段必须带 cline/ 前缀（如 cline/deepseek-deepseek-v4-flash），网关据此走 Cline 上游。"},
						},
					},

					{
						Title:  "设置",
						Layout: dcl.VBox{Spacing: 8},
						Children: []dcl.Widget{
							dcl.GroupBox{
								Title:  "超时（全局，两个供应商共用）",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "流式首字节超时"},
											dcl.NumberEdit{AssignTo: &ui.firstByte, Value: float64(settings.FirstByteSeconds), MinValue: 3, MaxValue: 600, Decimals: 0, MaxSize: dcl.Size{Width: 70}},
											dcl.Label{Text: "秒     总预算"},
											dcl.NumberEdit{AssignTo: &ui.budget, Value: float64(settings.BudgetSeconds), MinValue: 5, MaxValue: 1800, Decimals: 0, MaxSize: dcl.Size{Width: 80}},
											dcl.Label{Text: "秒"},
											dcl.HSpacer{},
										},
									},
									dcl.Label{Text: "代理节点（一行一个；支持 socks5/http、vless://、vmess://、trojan://、ss://、hysteria2://(hy2)、tuic:// 分享链接；手动节点不会被自动删除。全局资源：各供应商页的「节点池出口」开关决定谁使用）:"},
									dcl.TextEdit{
										AssignTo: &ui.proxyEdit,
										Text:     settings.ProxyInput,
										VScroll:  true,
										MinSize:  dcl.Size{Height: 96},
									},
								},
							}, dcl.GroupBox{
								Title:  "IP 信誉体检（可选，只体检正式池节点，缓存 7 天）",
								Font:   uiFont,
								Layout: dcl.VBox{Spacing: 6},
								Children: []dcl.Widget{
									dcl.Label{Text: "数据源：AbuseIPDB（滥用举报）+ IPinfo（地区/ASN）+ Spamhaus DQS（黑名单）。缺哪个 key 就跳过哪个源；信誉只影响出场顺序，绝不单独剔除节点。"},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "地区偏好:"},
											dcl.LineEdit{AssignTo: &ui.regionEdit, Text: settings.PreferredRegions, MinSize: dcl.Size{Width: 180}},
											dcl.Label{Text: "逗号分隔国家码（如 US,JP,SG），留空不偏好；命中的出口优先出场"},
											dcl.HSpacer{},
										},
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "IPinfo Token:"},
											dcl.LineEdit{AssignTo: &ui.ipinfoEdit, Text: settings.IpinfoToken, MinSize: dcl.Size{Width: 260}},
											dcl.HSpacer{},
										},
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "AbuseIPDB Key:"},
											dcl.LineEdit{AssignTo: &ui.abuseEdit, Text: settings.AbuseIPDBKey, MinSize: dcl.Size{Width: 260}},
											dcl.HSpacer{},
										},
									},
									dcl.Composite{
										Layout: dcl.HBox{MarginsZero: true},
										Children: []dcl.Widget{
											dcl.Label{Text: "Spamhaus DQS Key:"},
											dcl.LineEdit{AssignTo: &ui.dqsEdit, Text: settings.SpamhausDQSKey, MinSize: dcl.Size{Width: 260}},
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

	// 窗口 chrome 现代化：深色标题栏跟随系统；Win11 上标题栏染色 + Mica（静默降级）。
	applyWindowTheme(ui.window)

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
	ui.refreshUsage()
}

func formatTokens(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// refreshUsage 同步今日用量行；内容没变化不重绘。
func (ui *gatewayUI) refreshUsage() {
	if ui.usageLabel == nil || ui.app.gateway == nil || ui.app.gateway.usage == nil {
		return
	}
	rows := ui.app.gateway.usage.Snapshot()
	var reqs, prompt, completion int64
	for _, r := range rows {
		reqs += r.Requests
		prompt += r.PromptTokens
		completion += r.CompletionTokens
	}
	text := fmt.Sprintf("%d 次 · 入 %s / 出 %s tokens", reqs, formatTokens(prompt), formatTokens(completion))
	if len(rows) > 0 {
		text += fmt.Sprintf(" · %d 个模型", len(rows))
	}
	if text != ui.usageText {
		ui.usageText = text
		ui.usageLabel.SetText(text)
	}
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
	// 横幅卡每拍重绘：运行时长秒级跳动，状态灯/短语随 headlineColor 联动。
	if ui.banner != nil {
		ui.banner.Invalidate()
	}
}

// 横幅自绘字体：进程级复用，GDI 资源随进程退出回收。
var (
	bannerTitleFont *walk.Font
	bannerSubFont   *walk.Font
	bannerFontOnce  sync.Once
)

func ensureBannerFonts() {
	bannerFontOnce.Do(func() {
		bannerTitleFont, _ = walk.NewFont("Microsoft YaHei UI", 12, walk.FontBold)
		bannerSubFont, _ = walk.NewFont("Microsoft YaHei UI", 8, 0)
	})
}

// paintBanner 自绘顶部信息卡：深色圆角底 + 应用名/运行时长 + 大号健康状态灯。
func (ui *gatewayUI) paintBanner(canvas *walk.Canvas, bounds walk.Rectangle) error {
	ensureBannerFonts()
	bg, err := walk.NewSolidColorBrush(walk.RGB(24, 26, 32))
	if err != nil {
		return err
	}
	defer bg.Dispose()
	if err := canvas.FillRoundedRectangle(bg, bounds, walk.Size{Width: 12, Height: 12}); err != nil {
		return err
	}
	pen, err := walk.NewCosmeticPen(walk.PenSolid, walk.RGB(56, 60, 72))
	if err != nil {
		return err
	}
	defer pen.Dispose()
	if err := canvas.DrawRoundedRectangle(pen, bounds, walk.Size{Width: 12, Height: 12}); err != nil {
		return err
	}

	pad, w, h := 16, bounds.Width, bounds.Height
	if err := canvas.DrawText("opencode-free-autogate", bannerTitleFont, walk.RGB(236, 239, 246),
		walk.Rectangle{X: bounds.X + pad, Y: bounds.Y + 9, Width: w - 330, Height: 26}, 0); err != nil {
		return err
	}
	sub := fmt.Sprintf("本地免费网关 · 已运行 %s · OpenCode + Cline 双供应商",
		time.Since(ui.start).Round(time.Second))
	if err := canvas.DrawText(sub, bannerSubFont, walk.RGB(150, 158, 172),
		walk.Rectangle{X: bounds.X + pad, Y: bounds.Y + 37, Width: w - 330, Height: 20}, 0); err != nil {
		return err
	}

	light := ui.headlineColor
	if light == 0 {
		light = colorIdle // 首帧尚未刷新状态时兜底
	}
	textW := 132
	dotRect := walk.Rectangle{X: bounds.X + w - pad - textW - 26, Y: bounds.Y + h/2 - 8, Width: 16, Height: 16}
	dotBrush, err := walk.NewSolidColorBrush(light)
	if err != nil {
		return err
	}
	defer dotBrush.Dispose()
	if err := canvas.FillEllipse(dotBrush, dotRect); err != nil {
		return err
	}
	return canvas.DrawText(statusPhrase(light), bannerTitleFont, light,
		walk.Rectangle{X: bounds.X + w - pad - textW, Y: bounds.Y + h/2 - 13, Width: textW, Height: 26},
		walk.TextRight)
}

// statusPhrase 把健康四色映射成横幅右侧的短句。
func statusPhrase(c walk.Color) string {
	switch c {
	case colorOK:
		return "运行正常"
	case colorWarn:
		return "有限流/截断"
	case colorError:
		return "出现失败"
	default:
		return "待命"
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
		text = "（暂无正式节点，初检通过的候选正在流水线复检中…）"
	} else {
		lines := make([]string, 0, len(slots))
		for _, s := range slots {
			line := s.addr
			if tag := ui.app.gateway.exits.repTag(s.addr); tag != "" {
				line += "　" + tag
			}
			if ui.app.gateway.isManual(s.addr) {
				line += "　（手动，不自动移除）"
			}
			lines = append(lines, line)
		}
		text = fmt.Sprintf("共 %d 个：\r\n%s", len(slots), strings.Join(lines, "\r\n"))
	}
	if text == ui.poolLiveText {
		return
	}
	ui.poolLiveText = text
	ui.poolLive.SetText(text)
}

// pumpLogs 增量刷新日志文本，同时保持用户当前的滚动位置。
//
// 滚动条与刷新解绑的关键：walk 的 TextEdit 是标准 "EDIT" 控件，不支持
// RichEdit 的 EM_GETSCROLLPOS/EM_SETSCROLLPOS（发过去被静默忽略，这是前几次
// 修复无效的原因），也不能用 EM_REPLACESEL（它会把插入符滚进视野，导致跳到
// 末尾）。标准 EDIT 只认按行的 EM_GETFIRSTVISIBLELINE / EM_LINESCROLL。
//
// 新日志拼在文本最前面，旧内容整体下移 len(lines) 行，因此分两种情形：
//   - 停在顶部（firstVisible==0）：跟随最新日志，滚回第 0 行；
//   - 已向下滚动：加上新增行数滚回等效位置，用户看的那一段纹丝不动。
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
	// 替换前记下首个可见行；SetText 后控件回到第 0 行，再滚回等效位置。
	firstVisible := int(win.SendMessage(hwnd, win.EM_GETFIRSTVISIBLELINE, 0, 0))
	target := 0
	if firstVisible > 0 {
		target = firstVisible + len(lines) // 已滚开：补上新增行数守住原位
	}

	win.SendMessage(hwnd, win.WM_SETREDRAW, 0, 0)
	ui.logEdit.SetText(ui.shownText)
	if target > 0 {
		// EM_LINESCROLL 自动夹紧到实际行数，截断掉尾部旧行也不会越界。
		win.SendMessage(hwnd, win.EM_LINESCROLL, 0, uintptr(target))
	}
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
// 显示格式：opencode 模型带 opencode/ 前缀，中间用分隔线与 Cline 模型隔开。
func (ui *gatewayUI) modelWatcher() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		// modelUpstreamIDs 返回原始上游 ID（供深检模型下拉框使用）。
		upstreamIDs := ui.app.gateway.modelUpstreamIDs(ctx)
		// modelIDs 返回带 opencode/ 前缀的展示名（供列表文本使用）。
		displayIDs := ui.app.gateway.modelIDs(ctx)
		cancel()
		if len(upstreamIDs) > 0 {
			// 构建合并展示文本：opencode 模型 + 分隔线 + Cline 模型。
			var sb strings.Builder
			for _, id := range displayIDs {
				sb.WriteString(id)
				sb.WriteString("\r\n")
			}
			clineModels := clineFreeModelIDs()
			if len(clineModels) > 0 {
				sb.WriteString("──────────── Cline ────────────\r\n")
				for _, id := range clineModels {
					sb.WriteString(id)
					sb.WriteString("\r\n")
				}
			}
			text := sb.String()
			if text != ui.modelsSeen {
				ui.modelsSeen = text
				ui.window.Synchronize(func() {
					ui.modelsEdit.SetText(text)
					ui.syncProbeModelBox(upstreamIDs)
				})
			}
		}
		time.Sleep(60 * time.Second)
	}
}

// probeModelSeed 生成下拉框的初始候补：优先用已保存的选择，否则默认
// big-pickle。真实列表由 modelWatcher 拉到后替换。
func probeModelSeed(saved string) []string {
	if saved = strings.TrimSpace(saved); saved != "" {
		return []string{saved}
	}
	return []string{"big-pickle"}
}

// syncProbeModelBox 用上游实时模型列表刷新下拉框，并尽量选回已保存/当前
// 生效的值；若该值已从上游下架，则置顶保留显示——用户能看到现在实际用
// 的是哪个，也随时可以换。
func (ui *gatewayUI) syncProbeModelBox(ids []string) {
	chosen := strings.TrimSpace(ui.probeModelBox.Text())
	if chosen == "" {
		chosen = strings.TrimSpace(ui.settings.ProbeModel)
	}
	if chosen == "" {
		chosen = "big-pickle"
	}
	for i, id := range ids {
		if id == chosen {
			ui.probeModelBox.SetModel(ids)
			ui.probeModelBox.SetCurrentIndex(i)
			return
		}
	}
	padded := append([]string{chosen}, ids...)
	ui.probeModelBox.SetModel(padded)
	ui.probeModelBox.SetCurrentIndex(0)
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
	next.AbsorbStreaming = ui.absorbCheck.Checked()
	next.AbsorbAttempts = int(ui.absorbAttempt.Value())
	next.DeepProbeMinutes = int(ui.deepProbe.Value())
	next.ProbeConcurrency = int(ui.probeConc.Value())
	next.ProbeModel = strings.TrimSpace(ui.probeModelBox.Text())
	next.ProxyInput = ui.proxyEdit.Text()
	next.MirrorInput = ui.mirrorEdit.Text()
	next.PoolEnabled = ui.poolCheck.Checked()
	next.PoolInput = ui.poolEdit.Text()
	next.RaceEnabled = ui.raceCheck.Checked()
	next.RaceWidth = int(ui.raceWidth.Value())
	next.OpenCodePoolOff = !ui.opencodePoolCheck.Checked()
	next.ClinePoolEnabled = ui.clinePoolCheck.Checked()
	next.PreferredRegions = strings.TrimSpace(ui.regionEdit.Text())
	next.IpinfoToken = strings.TrimSpace(ui.ipinfoEdit.Text())
	next.AbuseIPDBKey = strings.TrimSpace(ui.abuseEdit.Text())
	next.SpamhausDQSKey = strings.TrimSpace(ui.dqsEdit.Text())

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
		fmt.Fprintf(&report, "并行竞速：开启（%d 路并发，最快出口胜出）\r\n", next.RaceWidth)
	} else {
		report.WriteString("并行竞速：关闭\r\n")
	}
	repSources := 0
	if next.AbuseIPDBKey != "" {
		repSources++
	}
	if next.IpinfoToken != "" {
		repSources++
	}
	if next.SpamhausDQSKey != "" {
		repSources++
	}
	fmt.Fprintf(&report, "IP 信誉体检：%d 个数据源已配置（正式池节点，缓存 7 天）", repSources)
	if next.PreferredRegions != "" {
		fmt.Fprintf(&report, "，地区偏好：%s", next.PreferredRegions)
	}
	report.WriteString("\r\n")
	if next.OpenCodePoolOff {
		report.WriteString("OpenCode 节点池出口：关闭（全部直连）\r\n")
	} else {
		report.WriteString("OpenCode 节点池出口：开启（多 IP 竞速/轮询）\r\n")
	}
	if next.ClinePoolEnabled {
		report.WriteString("Cline 节点池出口：开启（出口轮询 + 直连兜底）\r\n")
	} else {
		report.WriteString("Cline 节点池出口：关闭（直连）\r\n")
	}
	fmt.Fprintf(&report, "深检间隔：%d 分钟\r\n", next.DeepProbeMinutes)
	fmt.Fprintf(&report, "检测并发：%d 路（初检/复检共用）\r\n", next.ProbeConcurrency)
	if next.ProbeModel != "" {
		fmt.Fprintf(&report, "深检模型：%s\r\n", next.ProbeModel)
	} else {
		report.WriteString("深检模型：自动（big-pickle）\r\n")
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
// 受管键清单与 applyEnv 共用 settings.go 里的 configManagedEnvKeys。
func restartEnv() []string {
	managed := make(map[string]struct{}, len(configManagedEnvKeys))
	for _, key := range configManagedEnvKeys {
		managed[key] = struct{}{}
	}
	env := os.Environ()
	kept := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if _, skip := managed[strings.ToUpper(name)]; skip {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
}

// ── Cline 页面辅助函数 ──────────────────────────────────────────────────────

// refreshClineAccounts 刷新 Cline 账号列表显示。
func (ui *gatewayUI) refreshClineAccounts() {
	accounts := clineListAccounts()
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("共 %d 个账号\r\n\r\n", len(accounts)))
	for i, acc := range accounts {
		statusIcon := "●"
		switch acc.Status {
		case "active":
			statusIcon = "🟢"
		case "cooldown":
			statusIcon = "🟡"
		case "expired":
			statusIcon = "🔴"
		}
		sb.WriteString(fmt.Sprintf("%d. %s %s\r\n", i+1, statusIcon, acc.Email))
		sb.WriteString(fmt.Sprintf("   状态: %s  今日调用: %d  累计Token: %d\r\n",
			acc.Status, acc.UsageCountToday, acc.TokensTotal))
		if !acc.CooldownUntil.IsZero() && time.Now().Before(acc.CooldownUntil) {
			sb.WriteString(fmt.Sprintf("   冷却至: %s\r\n", acc.CooldownUntil.Format("2006-01-02 15:04:05")))
		}
		sb.WriteString("\r\n")
	}
	if len(accounts) == 0 {
		sb.WriteString("暂无账号。点击「导入账号」添加。\r\n")
	}
	ui.clineAccountEdit.SetText(sb.String())
}

// clineImportAccount 在后台执行 OAuth 导入流程。
func (ui *gatewayUI) clineImportAccount() {
	acc, err := clineAddAccountFromDeviceAuth()
	if err != nil {
		log.Printf("[Cline] 导入失败: %v", err)
		return
	}
	log.Printf("[Cline] 账号导入成功: %s", acc.Email)
	ui.refreshClineAccounts()
}
