package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// 统一日志出口（logkit）：防洪水 → 脱敏 → 双写（界面/控制台 + 轮转文件）。
//
// 设计借鉴 OmniRoute（全局 redact 兜底、重复合并、限速、文件轮转）与
// 9router（请求开头汇总 + done 收尾两行制），按单文件工具裁剪：
// 不引入日志库与结构化 JSON，人读格式不变，只在标准库 log 外套三层。
//
// 脱敏是全局兜底：无论哪个调用点把凭据打进日志（sk- Key、Bearer 头、
// api_key= 查询参数），上屏/落盘前都会被替换。调用点自身的脱敏
// （echolog 的键名脱敏、redactProxy）仍然保留——兜底不是借口。
//
// 防洪水解决的是"事故风暴冲掉现场"：熔断风暴时每出口每尝试一行 [错]，
// 会在几秒内刷满界面环形缓冲。重复行合并 + 每秒限速把风暴压成摘要，
// 让 [全局熔断]/[流截断] 这类关键行留得住。

const (
	logFileMaxSize  = 10 << 20        // 单个日志文件上限，超过轮转
	logFileBackups  = 5               // 保留的历史日志份数
	logFloodPerSec  = 50              // 每秒最多放行的行数，超出丢弃
	logDupWindow    = 5 * time.Second // 相同日志的合并窗口
	logDupSummaryIn = 2 * time.Second // 合并统计的最长汇报延迟
)

var (
	logSecretRe   = regexp.MustCompile(`sk-[A-Za-z0-9_-]{12,}`)
	logBearerRe   = regexp.MustCompile(`Bearer [A-Za-z0-9._~+/=-]{16,}`)
	logQueryRe    = regexp.MustCompile(`(?i)(api_key|access_token|refresh_token|id_token)=([A-Za-z0-9._%~+-]+)`)
	logTsPrefixRe = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(\.\d+)? `)
)

// redactLine 行级脱敏：只处理可能含敏感串的行，其余零开销直通。
func redactLine(line string) string {
	if !strings.Contains(line, "sk-") &&
		!strings.Contains(line, "Bearer ") &&
		!strings.Contains(line, "key=") &&
		!strings.Contains(line, "token=") {
		return line
	}
	line = logSecretRe.ReplaceAllString(line, "sk-<已脱敏>")
	line = logBearerRe.ReplaceAllString(line, "Bearer <已脱敏>")
	line = logQueryRe.ReplaceAllString(line, "$1=<已脱敏>")
	return line
}

// maskSecret 掩码展示：保留前缀便于确认是哪把 Key，不泄露其余部分。
func maskSecret(v string) string {
	if v == "" {
		return ""
	}
	prefix := v
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	return fmt.Sprintf("%s…(len=%d)", prefix, len(v))
}

func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// logKit 是 log.SetOutput 的总出口。
type logKit struct {
	mu    sync.Mutex
	sinks []io.Writer

	// 轮转文件（可空：目录创建失败时放弃文件侧，不影响其余输出）
	file     *os.File
	filePath string
	fileSize int64

	// 可注入参数（测试调小）
	floodPerSec int
	maxFileSize int64
	maxBackups  int
	dupWindow   time.Duration
	summaryIn   time.Duration

	// 重复行合并状态
	lastMsg   string
	lastTime  time.Time
	lastCount int
	dupTimer  *time.Timer

	// 限速状态
	sec     int64
	secCnt  int
	secDrop int
}

func newLogKit(sinks ...io.Writer) *logKit {
	return &logKit{
		sinks:       sinks,
		floodPerSec: logFloodPerSec,
		maxFileSize: logFileMaxSize,
		maxBackups:  logFileBackups,
		dupWindow:   logDupWindow,
		summaryIn:   logDupSummaryIn,
	}
}

// openFile 在 dir/logs/gateway.log 打开轮转文件，返回路径；失败返回空串。
func (k *logKit) openFile(dir string) string {
	logsDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return ""
	}
	path := filepath.Join(logsDir, "gateway.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return ""
	}
	k.file, k.filePath = f, path
	if info, err := f.Stat(); err == nil {
		k.fileSize = info.Size()
	}
	return path
}

// Write 实现 io.Writer，直接挂到 log.SetOutput。
func (k *logKit) Write(p []byte) (int, error) {
	text := strings.TrimRight(string(p), "\r\n")
	k.mu.Lock()
	defer k.mu.Unlock()
	msg := text
	if m := logTsPrefixRe.FindString(text); m != "" {
		msg = text[len(m):]
	}
	if k.duplicateLocked(msg) {
		return len(p), nil
	}
	if k.floodLocked() {
		return len(p), nil
	}
	k.emitLocked(text)
	return len(p), nil
}

// duplicateLocked 合并窗口内的完全相同日志：首条放行，后续抑制计数，
// 由摘要或下一条不同日志触发汇报。返回 true 表示该行被抑制。
func (k *logKit) duplicateLocked(msg string) bool {
	now := time.Now()
	if msg != "" && msg == k.lastMsg && now.Sub(k.lastTime) < k.dupWindow {
		k.lastCount++
		return true
	}
	k.flushDupLocked()
	k.lastMsg = msg
	k.lastTime = now
	k.lastCount = 1
	k.armDupLocked()
	return false
}

func (k *logKit) armDupLocked() {
	if k.dupTimer != nil {
		k.dupTimer.Stop()
	}
	if k.lastCount <= 0 || k.summaryIn <= 0 {
		return
	}
	k.dupTimer = time.AfterFunc(k.summaryIn, func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		k.flushDupLocked()
	})
}

// flushDupLocked 汇报合并统计并重置计数。持锁调用。
func (k *logKit) flushDupLocked() {
	if k.dupTimer != nil {
		k.dupTimer.Stop()
		k.dupTimer = nil
	}
	if k.lastCount > 1 && k.lastMsg != "" {
		k.emitSummaryLocked(fmt.Sprintf("[日志] 相同日志 ×%d 已合并：%s",
			k.lastCount, truncateForLog(k.lastMsg, 160)))
	}
	k.lastCount = 0
	k.lastMsg = ""
}

// floodLocked 每秒限速：超出的行丢弃并计数，跨秒时汇报。返回 true 表示丢弃。
func (k *logKit) floodLocked() bool {
	if k.floodPerSec <= 0 {
		return false
	}
	now := time.Now().Unix()
	if now != k.sec {
		if k.secDrop > 0 {
			k.emitSummaryLocked(fmt.Sprintf("[日志] 过去 1 秒限流丢弃 %d 行", k.secDrop))
			k.secDrop = 0
		}
		k.sec = now
		k.secCnt = 0
	}
	k.secCnt++
	if k.secCnt > k.floodPerSec {
		k.secDrop++
		return true
	}
	return false
}

func (k *logKit) emitLocked(text string) {
	out := redactLine(text) + "\n"
	for _, s := range k.sinks {
		_, _ = io.WriteString(s, out)
	}
	k.writeFileLocked(out)
}

// emitSummaryLocked 以标准日志时间戳格式输出合并/限流摘要（绕过限速与去重）。
func (k *logKit) emitSummaryLocked(text string) {
	stamp := time.Now().Format("2006/01/02 15:04:05.000000")
	k.emitLocked(stamp + " " + text)
}

func (k *logKit) writeFileLocked(out string) {
	if k.file == nil {
		return
	}
	if k.fileSize+int64(len(out)) > k.maxFileSize {
		k.rotateLocked()
		if k.file == nil {
			return
		}
	}
	n, _ := k.file.WriteString(out)
	k.fileSize += int64(n)
}

// rotateLocked 把当前文件改名归档并新建空文件。Windows 不允许重命名打开中
// 的文件，必须先 Close。持锁调用。
func (k *logKit) rotateLocked() {
	if k.file != nil {
		_ = k.file.Close()
		k.file = nil
	}
	stamp := time.Now().Format("20060102-150405")
	_ = os.Rename(k.filePath, filepath.Join(filepath.Dir(k.filePath), "gateway-"+stamp+".log"))
	f, err := os.OpenFile(k.filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	k.file = f
	k.fileSize = 0
	k.pruneLocked()
}

// pruneLocked 按份数上限清理历史归档。时间戳命名字典序即时间序。
func (k *logKit) pruneLocked() {
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(k.filePath), "gateway-*.log"))
	if len(matches) <= k.maxBackups {
		return
	}
	sort.Strings(matches)
	for _, path := range matches[:len(matches)-k.maxBackups] {
		_ = os.Remove(path)
	}
}
