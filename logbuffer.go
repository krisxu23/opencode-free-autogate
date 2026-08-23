package main

import (
	"strings"
	"sync"
)

const logRingCapacity = 1500

// logRing 是线程安全的日志环形缓冲，界面按序号增量读取新行。
type logRing struct {
	mu    sync.Mutex
	lines []string
	total int // 自启动以来写入的总行数（含已丢弃的）
}

var uiLog = &logRing{lines: make([]string, 0, logRingCapacity)}

// Write 实现 io.Writer，可直接挂到 log.SetOutput。
func (r *logRing) Write(p []byte) (int, error) {
	text := strings.TrimRight(string(p), "\r\n")
	if text != "" {
		r.append(strings.Split(text, "\n"))
	}
	return len(p), nil
}

func (r *logRing) append(lines []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range lines {
		r.lines = append(r.lines, line)
		r.total++
	}
	if overflow := len(r.lines) - logRingCapacity; overflow > 0 {
		r.lines = append(r.lines[:0], r.lines[overflow:]...)
	}
}

// Since 返回序号 from 起的新行与新的序号；调用方用返回的序号进行下一次增量读取。
func (r *logRing) Since(from int) ([]string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	oldest := r.total - len(r.lines)
	if from < oldest {
		from = oldest
	}
	if from >= r.total {
		return nil, r.total
	}
	out := make([]string, r.total-from)
	copy(out, r.lines[from-oldest:])
	return out, r.total
}
