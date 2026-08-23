package main

import (
	"context"
	"log"
	"time"
)

// startWakeDetector 检测系统休眠恢复：Windows 睡眠时定时器冻结、池内
// 连接全部死亡，恢复后若不做处理，第一批请求会集体撞上陈旧连接。
// 5 秒心跳发现墙钟跳变超过 30 秒即触发一次恢复流程（清空连接池闲置
// 连接 + 触发一次槽位补充），并做 15 秒去抖。
func startWakeDetector(g *gateway, ctx context.Context) {
	go func() {
		last := time.Now()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		var lastRecovery time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				if now.Sub(last) > 30*time.Second {
					if lastRecovery.IsZero() || now.Sub(lastRecovery) > 15*time.Second {
						lastRecovery = now
						log.Printf("[唤醒] 检测到系统休眠恢复（时钟跳变 %s），重置连接池", (now.Sub(last)).Round(time.Second))
						sharedTransports.resetAll()
						g.scheduleFill()
					}
				}
				last = now
			}
		}
	}()
}
