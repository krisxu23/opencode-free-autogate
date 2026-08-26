package main

import (
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
)

// 回显取证日志：把胜者响应头脱敏后落日志，用于判断上游是否在响应中下发
// turn-scoped 状态头（如 Codex 的 x-codex-turn-state）。若确认存在，后续
// 将实现「回显出处守卫」：failover 换出口后剥掉出处不符的状态头，避免
// 客户端回显与当前出口身份矛盾引发的 4xx。默认关闭，取证期手动开启。
//
// 脱敏规则：键名命中敏感词（auth/cookie/token/secret/credential/key）时
// 只记录值长度不记录内容；其余键超长截断。日志仅供人眼排查，不含凭据原文。

var echoSensitiveRe = regexp.MustCompile(`(?i)auth|cookie|token|secret|credential|api[-_]?key|^key$`)

const echoValueClip = 200

func maskEchoValue(key, value string) string {
	if echoSensitiveRe.MatchString(key) {
		return fmt.Sprintf("<已脱敏 len=%d>", len(value))
	}
	if len(value) > echoValueClip {
		return value[:echoValueClip] + "…"
	}
	return value
}

// logEchoHeaders 按键名字典序输出全部响应头（顺序确定便于对比多次请求）。
func logEchoHeaders(path string, status int, header http.Header) {
	keys := make([]string, 0, len(header))
	for key := range header {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for i, value := range header.Values(key) {
			log.Printf("[回显] %s <- %d %s[%d]: %s", path, status, key, i, maskEchoValue(key, value))
		}
	}
}
