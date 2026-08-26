//go:build windows

package main

import (
	"log"
	"strconv"
	"unsafe"

	"github.com/lxn/walk"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

// 窗口 chrome 现代化：深色标题栏跟随系统暗色开关；Win11 上额外做标题栏染色
// 与 Mica 材质（静默降级，Win10 自动跳过）。手法移植自 Wails v3 的
// pkg/w32/theme.go（MIT）。全部经 dwmapi.dll 动态加载，保持零 cgo 构建。

var procDwmSetWindowAttribute = windows.NewLazySystemDLL("dwmapi.dll").NewProc("DwmSetWindowAttribute")

// DwmSetWindowAttribute 属性编号（微软文档）与 Mica 背景取值。
const (
	dwmwaUseImmersiveDarkMode = 20 // Win10 20H1+；更早版本用 19
	dwmwaBorderColor          = 34
	dwmwaCaptionColor         = 35
	dwmwaTextColor            = 36
	dwmwaSystemBackdrop       = 38
	dwmsbtMica                = 2 // DWMSBT_MAINWINDOW，Win11 22H2+
)

func dwmSetU32(hwnd windows.HWND, attr uint32, value uint32) {
	if procDwmSetWindowAttribute.Find() != nil {
		return
	}
	v := value
	_, _, _ = procDwmSetWindowAttribute.Call(
		uintptr(hwnd),
		uintptr(attr),
		uintptr(unsafe.Pointer(&v)),
		unsafe.Sizeof(v),
	)
}

// systemAppDark 读“应用暗色模式”注册表开关；读不到时默认深色（更耐看）。
func systemAppDark() bool {
	k, err := registry.OpenKey(registry.CURRENT_USER,
		`Software\Microsoft\Windows\CurrentVersion\Themes\Personalize`, registry.QUERY_VALUE)
	if err != nil {
		return true
	}
	defer k.Close()
	v, _, err := k.GetIntegerValue("AppsUseLightTheme")
	if err != nil {
		return true
	}
	return v == 0
}

// windowsBuild 返回系统 build 号（19044=Win10 21H2，22000=Win11，22631=Win11 22H2）。
func windowsBuild() int {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE,
		`SOFTWARE\Microsoft\Windows NT\CurrentVersion`, registry.QUERY_VALUE)
	if err != nil {
		return 0
	}
	defer k.Close()
	s, _, err := k.GetStringValue("CurrentBuildNumber")
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// applyWindowTheme 在窗口 Create 之后、Show 之前调用一次。
func applyWindowTheme(w *walk.MainWindow) {
	hwnd := windows.HWND(w.Handle())
	dark := uint32(0)
	if systemAppDark() {
		dark = 1
	}
	// 新旧两个属性号都写一遍：无效属性会被系统忽略，兼容 19/20 两代实现。
	dwmSetU32(hwnd, dwmwaUseImmersiveDarkMode, dark)
	dwmSetU32(hwnd, dwmwaUseImmersiveDarkMode-1, dark)

	build := windowsBuild()
	if build >= 22000 {
		// Win11：标题栏染深板岩色（与界面横幅卡片同系），边框同色。
		dwmSetU32(hwnd, dwmwaCaptionColor, colorref(30, 32, 40))
		dwmSetU32(hwnd, dwmwaTextColor, colorref(235, 238, 245))
		dwmSetU32(hwnd, dwmwaBorderColor, colorref(56, 60, 72))
	}
	if build >= 22631 {
		// Win11 22H2：尝试 Mica 背景；不支持时系统忽略该调用，无副作用。
		dwmSetU32(hwnd, dwmwaSystemBackdrop, dwmsbtMica)
	}
	log.Printf("[界面] 窗口主题已应用（build=%d 深色标题栏=%v）", build, dark == 1)
}

// colorref 拼 COLORREF（0x00BBGGRR）。
func colorref(r, g, b uint8) uint32 {
	return uint32(r) | uint32(g)<<8 | uint32(b)<<16
}
